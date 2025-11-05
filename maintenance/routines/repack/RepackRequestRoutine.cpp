/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/exception/NoSuchObject.hpp"
#include "RepackRequestRoutine.hpp"
#include "scheduler/Scheduler.hpp"
#include "common/semconv/Attributes.hpp"

namespace cta::maintenance {

RepackRequestRoutine::RepackRequestRoutine(cta::log::LogContext& lc, cta::Scheduler& scheduler, int rmrtte, int timeout)
    : m_lc(lc),
      m_scheduler(scheduler),
      m_repackMaxRequestsToToExpand(rmrtte),
      m_softTimeout(timeout) {
  log::ScopedParamContainer params(m_lc);
  params.add("maxRequestsToToExpand", rmrtte);
  params.add("softTimeout", timeout);
  m_lc.log(cta::log::INFO, "Created RepackRequestRoutine");
}

void RepackRequestRoutine::execute() {
  // We give ourselves a budget of 30s for those operations...
  utils::Timer t;
  log::TimingList timingList;
  // First expand any request to expand
  // Next promote requests to ToExpand if needed
  m_lc.log(log::DEBUG, "In RepackRequestRoutine::execute(): before promoteRepackRequestsToToExpand()");

  //Putting pending repack request into the RepackQueueToExpand queue
  m_scheduler.promoteRepackRequestsToToExpand(m_lc, m_repackMaxRequestsToToExpand);
  m_lc.log(log::DEBUG, "In RepackRequestRoutine::execute(): after promoteRepackRequestsToToExpand()");

  //Retrieve the first repack request from the RepackQueueToExpand queue
  if (const auto repackRequest = m_scheduler.getNextRepackRequestToExpand(); repackRequest != nullptr) {
    m_lc.log(log::DEBUG, "In RepackRequestRoutine::execute(): after getNextRepackRequestToExpand()");
    //We have a RepackRequest that has the status ToExpand, expand it
    try {
      try {
        m_scheduler.expandRepackRequest(repackRequest, timingList, t, m_lc);
        m_lc.log(log::DEBUG, "In RepackRequestRoutine::execute(): after expandRepackRequest()");
      } catch (const ExpandRepackRequestException& ex) {
        log::ScopedParamContainer spc(m_lc);
        spc.add("vid", repackRequest->getRepackInfo().vid);
        m_lc.log(log::ERR, ex.getMessageValue());
        repackRequest->fail();
      } catch (const cta::exception::Exception& e) {
        log::ScopedParamContainer spc(m_lc);
        spc.add("vid", repackRequest->getRepackInfo().vid);
        m_lc.log(log::ERR, e.getMessageValue());
        repackRequest->fail();
        throw;
      }
    } catch (const cta::exception::NoSuchObject&) {
      //In case the repack request is deleted during expansion, avoid a segmentation fault of the tapeserver
      m_lc.log(log::WARNING,
               "In RepackRequestRoutine::execute(), RepackRequest object does not exist in the objectstore");
    }
  }

  reportBatch("RetrieveSuccesses", [this] { return m_scheduler.getNextSuccessfulRetrieveRepackReportBatch(m_lc); });
  reportBatch("ArchiveSuccesses", [this] { return m_scheduler.getNextSuccessfulArchiveRepackReportBatch(m_lc); });
  reportBatch("RetrieveFailed", [this] { return m_scheduler.getNextFailedRetrieveRepackReportBatch(m_lc); });
  reportBatch("ArchiveFailed", [this] { return m_scheduler.getNextFailedArchiveRepackReportBatch(m_lc); });
}

template<typename GetBatchFunc>
void RepackRequestRoutine::reportBatch(std::string_view reportingType, GetBatchFunc getBatchFunc) const {
  utils::Timer totalTime;
  bool moreBatch = true;
  log::ScopedParamContainer params(m_lc);
  params.add("reportingType", reportingType);

  uint64_t numberOfBatchReported = 0;

  while (totalTime.secs() < m_softTimeout && moreBatch) {
    utils::Timer t;
    log::TimingList tl;

    cta::Scheduler::RepackReportBatch reportBatch = getBatchFunc();
    tl.insertAndReset("getNextRepackReportBatchTime", t);

    if (!reportBatch.empty()) {
      reportBatch.report(m_lc);
      numberOfBatchReported++;
      tl.insertAndReset("reportingTime", t);

      log::ScopedParamContainer paramsReport(m_lc);
      tl.addToLog(paramsReport);
      m_lc.log(log::INFO, "In RepackRequestRoutine::reportBatch(), reported a batch of reports.");
    } else {
      moreBatch = false;
    }
  }

  if (numberOfBatchReported > 0) {
    params.add("numberOfBatchReported", numberOfBatchReported);
    params.add("totalRunTime", totalTime.secs());
    params.add("moreBatchToDo", moreBatch);
    m_lc.log(log::INFO, "In RepackReportThread::run(), exiting.");
  }
}
}  // namespace cta::maintenance
