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

#include "DiskReportRunner.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/ArchiveJob.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "common/log/TimingList.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/MaintenanceInstruments.hpp"

namespace cta::maintenance {

void DiskReportRunner::executeRunner(cta::log::LogContext& lc) {
  utils::Timer t;
  size_t roundCount = 0;
  for (bool is_done = false; !is_done && t.secs() < m_softTimeout;) {
    roundCount += 1;
    log::TimingList timings;
    lc.log(cta::log::DEBUG, "In DiskReportRunner::executeRunner(): getting next archive jobs to report from Scheduler DB");
    utils::Timer t2;
    auto archiveJobsToReport = m_scheduler.getNextArchiveJobsToReportBatch(m_batchSize, lc);
    is_done = archiveJobsToReport.empty();
    if (!is_done) {
      const auto reportJobCount = archiveJobsToReport.size();
      timings.insertAndReset("getArchiveJobsToReportTime", t2);
      log::ScopedParamContainer params(lc);
      params.add("archiveJobsReported", archiveJobsToReport.size());
      utils::Timer t3;
      m_scheduler.reportArchiveJobsBatch(archiveJobsToReport, m_reporterFactory, timings, t2, lc);
      timings.insertAndReset("reportArchiveJobsTime", t3);
      timings.addToLog(params);
      lc.log(cta::log::INFO, "In DiskReportRunner::executeRunner(): did one round of archive reports.");
      cta::telemetry::metrics::ctaMaintenanceDiskReporterCount->Add(reportJobCount);
    } else {
      lc.log(cta::log::DEBUG, "In DiskReportRunner::executeRunner(): archiveJobsToReport is empty.");
    }
    utils::Timer t4;
    auto retrieveJobsToReport = m_scheduler.getNextRetrieveJobsToReportBatch(m_batchSize, lc);
    is_done = is_done && retrieveJobsToReport.empty();
    if (!retrieveJobsToReport.empty()) {
      const auto reportJobCount =  retrieveJobsToReport.size();
      timings.insertAndReset("getRetrieveJobsToReportTime", t4);
      log::ScopedParamContainer params(lc);
      params.add("retrieveJobsReported", retrieveJobsToReport.size());
      utils::Timer t5;
      m_scheduler.reportRetrieveJobsBatch(retrieveJobsToReport, m_reporterFactory, timings, t4, lc);
      timings.insertAndReset("reportRetrieveJobsTime", t5);
      timings.addToLog(params);
      lc.log(cta::log::INFO, "In DiskReportRunner::executeRunner(): did one round of retrieve reports.");
      cta::telemetry::metrics::ctaMaintenanceDiskReporterCount->Add(reportJobCount);
    } else {
      lc.log(cta::log::DEBUG, "In DiskReportRunner::executeRunner(): retrieveJobsToReport is empty.");
    }
  }
  log::ScopedParamContainer params(lc);
  auto passTime = t.secs();
  params.add("roundCount", roundCount).add("passTime", passTime);
  lc.log(log::DEBUG, "In DiskReportRunner::executeRunner(): finished one pass.");
  if (passTime > 1) {
    lc.log(log::INFO, "In DiskReportRunner::executeRunner(): finished one pass.");
  }
}

}  // namespace cta::maintenance
