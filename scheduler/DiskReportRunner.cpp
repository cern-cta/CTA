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

namespace cta {

void DiskReportRunner::runOnePass(log::LogContext& lc) {
  // We currently got for a hardcoded 500 jobs batch to report.
  // We report with a budget of 30 seconds, as long as we find something to report.
  utils::Timer t;
  size_t roundCount = 0;
  while (t.secs() < 30) {
    utils::Timer t2;
    auto archiveJobsToReport = m_scheduler.getNextArchiveJobsToReportBatch(500, lc);
    if (archiveJobsToReport.empty()) break;
    double getJobsToReportTime = t2.secs(utils::Timer::resetCounter);
    for (auto &job: archiveJobsToReport) {
      job->asyncReportComplete(m_reporterFactory);
    }
    double asyncStartReports = t2.secs(utils::Timer::resetCounter);
    size_t successfulReports = 0, failedReports = 0;
    // Now gather the result of the reporting to client.
    for (auto &job: archiveJobsToReport) {
      try {
        job->waitForReporting();
        log::ScopedParamContainer params(lc);
        params.add("fileId", job->archiveFile.archiveFileID)
              .add("diskInstance", job->archiveFile.diskInstance)
              .add("diskFileId", job->archiveFile.diskFileId)
              .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path)
              .add("reportURL", job->reportURL())
              .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path)
              .add("reportTime", job->reportTime());
        lc.log(cta::log::INFO,"Reported to the client a full file archival");
      } catch(cta::exception::Exception &ex) {
        cta::log::ScopedParamContainer params(lc);
        params.add("fileId", job->archiveFile.archiveFileID)
              .add("diskInstance", job->archiveFile.diskInstance)
              .add("diskFileId", job->archiveFile.diskFileId)
              .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path).add("reportURL", job->reportURL())
              .add("errorMessage", ex.getMessage().str());
        lc.log(cta::log::ERR,"Unsuccessful report to the client a full file archival:");
      }
    }
    double asyncCheckJobCompletionTime = t2.secs(utils::Timer::resetCounter);
    log::ScopedParamContainer params (lc);
    params.add("successfulReports", successfulReports)
          .add("failedReports", failedReports)
          .add("totalReports", successfulReports + failedReports)
          .add("getJobsToReportTime", getJobsToReportTime)
          .add("asyncStartReports", asyncStartReports)
          .add("asyncCheckJobCompletionTime", asyncCheckJobCompletionTime);
    
  }
  double clientReportingTime=t.secs();
  cta::log::ScopedParamContainer params(lc);
  params.add("roundCount", roundCount)
        .add("clientReportingTime", clientReportingTime);
  lc.log(log::INFO, "In ReporterProcess::runOnePass(): finished one pass.");
}

} // namespace cta
