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
#include "RepackRequestManager.hpp"
#include "scheduler/Scheduler.hpp"
#include "common/semconv/Attributes.hpp"

namespace cta::maintenance {

void RepackRequestManager::executeRunner(cta::log::LogContext &lc) {
  // We give ourselves a budget of 30s for those operations...
  utils::Timer t;
  log::TimingList timingList;
  // First expand any request to expand
  // Next promote requests to ToExpand if needed
  lc.log(log::DEBUG,"In RepackRequestManager::executeRunner(): before promoteRepackRequestsToToExpand()");

  //Putting pending repack request into the RepackQueueToExpand queue
  m_scheduler.promoteRepackRequestsToToExpand(lc, m_repackMaxRequestsToToExpand);
  lc.log(log::DEBUG,"In RepackRequestManager::executeRunner(): after promoteRepackRequestsToToExpand()");

  //Retrieve the first repack request from the RepackQueueToExpand queue
  if(const auto repackRequest = m_scheduler.getNextRepackRequestToExpand(); repackRequest != nullptr) {
    lc.log(log::DEBUG,"In RepackRequestManager::executeRunner(): after getNextRepackRequestToExpand()");
    //We have a RepackRequest that has the status ToExpand, expand it
    try{
      try{
        m_scheduler.expandRepackRequest(repackRequest,timingList,t,lc);
        lc.log(log::DEBUG,"In RepackRequestManager::executeRunner(): after expandRepackRequest()");
      } catch (const ExpandRepackRequestException& ex){
        log::ScopedParamContainer spc(lc);
        spc.add("vid",repackRequest->getRepackInfo().vid);
        lc.log(log::ERR,ex.getMessageValue());
        repackRequest->fail();
      } catch (const cta::exception::Exception &e){
        log::ScopedParamContainer spc(lc);
        spc.add("vid",repackRequest->getRepackInfo().vid);
        lc.log(log::ERR,e.getMessageValue());
        repackRequest->fail();
        throw;
      }
    } catch (const cta::exception::NoSuchObject &){
      //In case the repack request is deleted during expansion, avoid a segmentation fault of the tapeserver
      lc.log(log::WARNING,"In RepackRequestManager::executeRunner(), RepackRequest object does not exist in the objectstore");
    }
  }

  reportBatch("RetrieveSuccesses", [this, &lc] { return m_scheduler.getNextSuccessfulRetrieveRepackReportBatch(lc);}, lc);
  reportBatch("ArchiveSuccesses", [this, &lc] { return m_scheduler.getNextSuccessfulArchiveRepackReportBatch(lc);}, lc);
  reportBatch("RetrieveFailed", [this, &lc] { return m_scheduler.getNextFailedRetrieveRepackReportBatch(lc);}, lc);
  reportBatch("ArchiveFailed", [this, &lc] { return m_scheduler.getNextFailedArchiveRepackReportBatch(lc);}, lc);
}

template <typename GetBatchFunc>
void RepackRequestManager::reportBatch(std::string_view reportingType, GetBatchFunc getBatchFunc, cta::log::LogContext &lc) const {
  utils::Timer totalTime;
  bool moreBatch = true;
  log::ScopedParamContainer params(lc);
  params.add("reportingType",reportingType);

  uint64_t numberOfBatchReported = 0;

  while (totalTime.secs() < m_softTimeout && moreBatch) {
    utils::Timer t;
    log::TimingList tl;

    cta::Scheduler::RepackReportBatch reportBatch = getBatchFunc();
    tl.insertAndReset("getNextRepackReportBatchTime", t);

    if (!reportBatch.empty()) {
      reportBatch.report(lc);
      numberOfBatchReported++;
      tl.insertAndReset("reportingTime", t);
      
      log::ScopedParamContainer paramsReport(lc);
      tl.addToLog(paramsReport);
      lc.log(log::INFO, "In RepackReportThread::run(), reported a batch of reports.");
    } else {
      moreBatch = false;
    }
  }

  if (numberOfBatchReported > 0) {
    params.add("numberOfBatchReported", numberOfBatchReported);
    params.add("totalRunTime", totalTime.secs());
    params.add("moreBatchToDo", moreBatch);
    lc.log(log::INFO, "In RepackReportThread::run(), exiting.");
  }
}
} // namespace cta::maintenance
