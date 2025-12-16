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

#include "DiskReportRetrieveRoutine.hpp"

#include "common/log/TimingList.hpp"
#include "scheduler/ArchiveJob.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "scheduler/Scheduler.hpp"

namespace cta::maintd {

DiskReportRetrieveRoutine::DiskReportRetrieveRoutine(cta::log::LogContext& lc,
                                                     cta::Scheduler& scheduler,
                                                     int batchSize,
                                                     int softTimeout)
    : m_lc(lc),
      m_scheduler(scheduler),
      m_batchSize(batchSize),
      m_softTimeout(softTimeout) {
  log::ScopedParamContainer params(m_lc);
  params.add("softTimeout", softTimeout);
  params.add("batchSize", batchSize);
  m_lc.log(cta::log::INFO, "Created DiskReportRetrieveRoutine");
}

void DiskReportRetrieveRoutine::execute() {
  utils::Timer totalTime;
  uint64_t numberOfBatchReported = 0;

  while (totalTime.secs() < m_softTimeout) {
    log::TimingList timings;
    m_lc.log(cta::log::DEBUG,
             "In DiskReportRetrieveRoutine::execute(): getting next retrieve jobs to report from Scheduler DB");
    utils::Timer t1;
    auto retrieveJobsToReport = m_scheduler.getNextRetrieveJobsToReportBatch(m_batchSize, m_lc);

    if (retrieveJobsToReport.empty()) {
      // We are done for now
      break;
    }

    timings.insertAndReset("getRetrieveJobsToReportTime", t1);
    log::ScopedParamContainer params(m_lc);
    params.add("retrieveJobsReported", retrieveJobsToReport.size());
    utils::Timer t2;
    m_scheduler.reportRetrieveJobsBatch(retrieveJobsToReport, m_reporterFactory, timings, t1, m_lc);
    numberOfBatchReported += retrieveJobsToReport.size();
    timings.insertAndReset("reportRetrieveJobsTime", t2);
    timings.addToLog(params);
    m_lc.log(cta::log::DEBUG, "In DiskReportRetrieveRoutine::execute(): did one round of retrieve reports.");
  }

  if (numberOfBatchReported > 0) {
    log::ScopedParamContainer params(m_lc);
    params.add("batchesReported", numberOfBatchReported);
    params.add("duration", totalTime.secs());
    m_lc.log(log::INFO, "In DiskReportRetrieveRoutine::execute(): finished one pass.");
  }
}

std::string DiskReportRetrieveRoutine::getName() const {
  return "DiskReportRetrieveRoutine";
}

}  // namespace cta::maintd
