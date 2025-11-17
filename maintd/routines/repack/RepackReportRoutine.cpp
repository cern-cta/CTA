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
#include "RepackReportRoutine.hpp"
#include "scheduler/Scheduler.hpp"
#include "common/semconv/Attributes.hpp"

namespace cta::maintd {

RepackReportRoutine::RepackReportRoutine(cta::log::LogContext& lc, cta::Scheduler& scheduler, int timeout)
    : m_lc(lc),
      m_scheduler(scheduler),
      m_softTimeout(timeout) {
  log::ScopedParamContainer params(m_lc);
  params.add("softTimeout", timeout);
  m_lc.log(cta::log::INFO, "Created RepackReportRoutine");
}

void RepackReportRoutine::execute() {
  reportBatch("RetrieveSuccesses", [this] { return m_scheduler.getNextSuccessfulRetrieveRepackReportBatch(m_lc); });
  reportBatch("ArchiveSuccesses", [this] { return m_scheduler.getNextSuccessfulArchiveRepackReportBatch(m_lc); });
  reportBatch("RetrieveFailed", [this] { return m_scheduler.getNextFailedRetrieveRepackReportBatch(m_lc); });
  reportBatch("ArchiveFailed", [this] { return m_scheduler.getNextFailedArchiveRepackReportBatch(m_lc); });
}

template<typename GetBatchFunc>
void RepackReportRoutine::reportBatch(std::string_view reportingType, GetBatchFunc getBatchFunc) const {
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

    if (reportBatch.empty()) {
      // Nothing to do
      moreBatch = false;
    }

    reportBatch.report(m_lc);
    numberOfBatchReported++;
    tl.insertAndReset("reportingTime", t);

    log::ScopedParamContainer paramsReport(m_lc);
    tl.addToLog(paramsReport);
    m_lc.log(log::INFO, "In RepackReportRoutine::reportBatch(), reported a batch of reports.");
  }

  if (numberOfBatchReported > 0) {
    params.add("numberOfBatchReported", numberOfBatchReported);
    params.add("totalRunTime", totalTime.secs());
    params.add("moreBatchToDo", moreBatch);
    m_lc.log(log::INFO, "In RepackReportThread::run(), exiting.");
  }
}

std::string RepackReportRoutine::getName() const {
  return "RepackReportRoutine";
}

}  // namespace cta::maintd
