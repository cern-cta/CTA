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

#include "RepackReportThread.hpp"

namespace cta {

RepackReportThread::~RepackReportThread() {}

void RepackReportThread::run() {
  utils::Timer totalTime;
  bool moreBatch = true;
  log::ScopedParamContainer params(m_lc);
  params.add("reportingType", getReportingType());
  uint64_t numberOfBatchReported = 0;
  while (totalTime.secs() < c_maxTimeToReport && moreBatch) {
    utils::Timer t;
    log::TimingList tl;
    cta::Scheduler::RepackReportBatch reportBatch = getNextRepackReportBatch(m_lc);
    tl.insertAndReset("getNextRepackReportBatchTime", t);
    if (!reportBatch.empty()) {
      reportBatch.report(m_lc);
      numberOfBatchReported++;
      tl.insertAndReset("reportingTime", t);
      log::ScopedParamContainer paramsReport(m_lc);
      tl.addToLog(paramsReport);
      m_lc.log(log::INFO, "In RepackReportThread::run(), reported a batch of reports.");
    }
    else {
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

cta::Scheduler::RepackReportBatch RetrieveSuccessesRepackReportThread::getNextRepackReportBatch(log::LogContext& lc) {
  return m_scheduler.getNextSuccessfulRetrieveRepackReportBatch(lc);
}

cta::Scheduler::RepackReportBatch ArchiveSuccessesRepackReportThread::getNextRepackReportBatch(log::LogContext& lc) {
  return m_scheduler.getNextSuccessfulArchiveRepackReportBatch(lc);
}

cta::Scheduler::RepackReportBatch RetrieveFailedRepackReportThread::getNextRepackReportBatch(log::LogContext& lc) {
  return m_scheduler.getNextFailedRetrieveRepackReportBatch(lc);
}

cta::Scheduler::RepackReportBatch ArchiveFailedRepackReportThread::getNextRepackReportBatch(log::LogContext& lc) {
  return m_scheduler.getNextFailedArchiveRepackReportBatch(lc);
}
}  // namespace cta