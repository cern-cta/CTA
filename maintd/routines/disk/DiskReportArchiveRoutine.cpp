/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DiskReportArchiveRoutine.hpp"

#include "common/log/TimingList.hpp"
#include "scheduler/ArchiveJob.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "scheduler/Scheduler.hpp"

namespace cta::maintd {

DiskReportArchiveRoutine::DiskReportArchiveRoutine(cta::log::LogContext& lc,
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
  m_lc.log(cta::log::INFO, "In DiskReportArchiveRoutine: Created DiskReportArchiveRoutine");
}

void DiskReportArchiveRoutine::execute() {
  utils::Timer totalTime;
  uint64_t numberOfBatchReported = 0;

  while (totalTime.secs() < m_softTimeout) {
    log::TimingList timings;
    m_lc.log(cta::log::DEBUG,
             "In DiskReportArchiveRoutine::execute(): getting next archive jobs to report from Scheduler DB");
    utils::Timer t1;
    auto archiveJobsToReport = m_scheduler.getNextArchiveJobsToReportBatch(m_batchSize, m_lc);

    if (archiveJobsToReport.empty()) {
      // We are done for now
      break;
    }

    timings.insertAndReset("getArchiveJobsToReportTime", t1);
    log::ScopedParamContainer params(m_lc);
    params.add("archiveJobsReported", archiveJobsToReport.size());
    utils::Timer t2;
    m_scheduler.reportArchiveJobsBatch(archiveJobsToReport, m_reporterFactory, timings, t1, m_lc);
    numberOfBatchReported += archiveJobsToReport.size();
    timings.insertAndReset("reportArchiveJobsTime", t2);
    timings.addToLog(params);
    m_lc.log(cta::log::DEBUG, "In DiskReportArchiveRoutine::execute(): did one round of archive reports.");
  }

  if (numberOfBatchReported > 0) {
    log::ScopedParamContainer params(m_lc);
    params.add("batchesReported", numberOfBatchReported);
    params.add("duration", totalTime.secs());
    m_lc.log(log::INFO, "In DiskReportArchiveRoutine::execute(): finished one pass.");
  }
}

std::string DiskReportArchiveRoutine::getName() const {
  return "DiskReportArchiveRoutine";
}

}  // namespace cta::maintd
