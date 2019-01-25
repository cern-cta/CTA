/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2018  CERN
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
#include "RetrieveJob.hpp"
#include "common/log/TimingList.hpp"

namespace cta {

void DiskReportRunner::runOnePass(log::LogContext& lc) {
  // We currently got for a hardcoded 500 jobs batch to report
  const int BATCH_SIZE = 500;
  // We report with a budget of 30 seconds, as long as we find something to report
  const int TIME_UNTIL_DONE = 30;

  utils::Timer t;
  size_t roundCount = 0;
  for(bool is_done = false; !is_done && t.secs() < TIME_UNTIL_DONE; ) {
    log::TimingList timings;
    utils::Timer t2, roundTime;

    auto archiveJobsToReport = m_scheduler.getNextArchiveJobsToReportBatch(BATCH_SIZE, lc);
    {
      log::ScopedParamContainer params(lc);
      params.add("jobsToReport", archiveJobsToReport.size());
      lc.log(cta::log::DEBUG,"In DiskReportRunner::runOnePass(): ready to process archive reports.");
    }
    is_done = archiveJobsToReport.empty();
    if(!archiveJobsToReport.empty()) {
      timings.insertAndReset("getArchiveJobsToReportTime", t2);
      m_scheduler.reportArchiveJobsBatch(archiveJobsToReport, m_reporterFactory, timings, t2, lc);
      log::ScopedParamContainer params(lc);
      params.add("roundTime", roundTime.secs());
      lc.log(cta::log::INFO,"In DiskReportRunner::runOnePass(): did one round of archive reports.");
    }

    auto retrieveJobsToReport = m_scheduler.getNextRetrieveJobsToReportBatch(BATCH_SIZE, lc);
    {
      log::ScopedParamContainer params(lc);
      params.add("jobsToReport", retrieveJobsToReport.size());
      lc.log(cta::log::DEBUG,"In DiskReportRunner::runOnePass(): ready to process retrieve reports.");
    }
    is_done = retrieveJobsToReport.empty();
    if(!retrieveJobsToReport.empty()) {
      timings.insertAndReset("getRetrieveJobsToReportTime", t2);
      m_scheduler.reportRetrieveJobsBatch(retrieveJobsToReport, m_reporterFactory, timings, t2, lc);
      log::ScopedParamContainer params(lc);
      params.add("roundTime", roundTime.secs());
      lc.log(cta::log::INFO,"In DiskReportRunner::runOnePass(): did one round of retrieve reports.");
    }
  }
  log::ScopedParamContainer params(lc);  
  params.add("roundCount", roundCount)
        .add("passTime", t.secs());
  lc.log(log::INFO, "In DiskReportRunner::runOnePass(): finished one pass.");
}

} // namespace cta
