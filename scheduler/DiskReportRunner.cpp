  /*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "DiskReportRunner.hpp"
#include "Scheduler.hpp"
#include "ArchiveJob.hpp"
#include "common/log/TimingList.hpp"

namespace cta {

void DiskReportRunner::runOnePass(log::LogContext& lc) {
  // We currently got for a hardcoded 500 jobs batch to report.
  // We report with a budget of 30 seconds, as long as we find something to report.
  utils::Timer t;
  size_t roundCount = 0;
  while (t.secs() < 30) {
    log::TimingList timings;
    utils::Timer t2, roundTime;
    auto archiveJobsToReport = m_scheduler.getNextArchiveJobsToReportBatch(500, lc);
    if (archiveJobsToReport.empty()) break;
    timings.insertAndReset("getJobsToReportTime", t2);
    m_scheduler.reportArchiveJobsBatch(archiveJobsToReport, m_reporterFactory, timings, t2, lc);
    log::ScopedParamContainer params(lc);
    params.add("roundTime", roundTime.secs());
    lc.log(cta::log::INFO,"In DiskReportRunner::runOnePass(): did one round of reports.");
  }
  log::ScopedParamContainer params(lc);  
  params.add("roundCount", roundCount)
        .add("passTime", t.secs());
  lc.log(log::INFO, "In ReporterProcess::runOnePass(): finished one pass.");
}

} // namespace cta
