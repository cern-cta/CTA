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

#include "DiskReportArchiveRoutine.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/ArchiveJob.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "common/log/TimingList.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/SchedulerInstruments.hpp"

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
  m_lc.log(cta::log::INFO, "Created DiskReportArchiveRoutine");
}

void DiskReportArchiveRoutine::execute() {
  utils::Timer totalTime;
  size_t roundCount = 0;

  while (totalTime.secs() < m_softTimeout) {
    roundCount += 1;
    log::TimingList timings;
    m_lc.log(cta::log::DEBUG,
             "In DiskReportArchiveRoutine::execute(): getting next archive jobs to report from Scheduler DB");
    utils::Timer t1;
    auto archiveJobsToReport = m_scheduler.getNextArchiveJobsToReportBatch(m_batchSize, m_lc);

    if (archiveJobsToReport.empty()) {
      // We are done for now
      break;
    }

    const auto reportJobCount = archiveJobsToReport.size();
    timings.insertAndReset("getArchiveJobsToReportTime", t1);
    log::ScopedParamContainer params(m_lc);
    params.add("archiveJobsReported", archiveJobsToReport.size());
    utils::Timer t2;
    m_scheduler.reportArchiveJobsBatch(archiveJobsToReport, m_reporterFactory, timings, t1, m_lc);
    timings.insertAndReset("reportArchiveJobsTime", t2);
    timings.addToLog(params);
    m_lc.log(cta::log::INFO, "In DiskReportArchiveRoutine::execute(): did one round of archive reports.");
    cta::telemetry::metrics::ctaSchedulerDiskReportCount->Add(
      reportJobCount,
      {
        {cta::semconv::attr::kCtaTransferDirection, cta::semconv::attr::CtaTransferDirectionValues::kArchive}
    });
  }

  log::ScopedParamContainer params(m_lc);
  auto passTime = totalTime.secs();
  params.add("roundCount", roundCount).add("passTime", passTime);
  m_lc.log(log::INFO, "In DiskReportArchiveRoutine::execute(): finished one pass.");
}

std::string DiskReportArchiveRoutine::getName() const {
  return "DiskReportArchiveRoutine";
}

}  // namespace cta::maintd
