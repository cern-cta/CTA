/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/ArchiveRdbJob.hpp"

#include "common/exception/NotImplementedException.hpp"

namespace cta::schedulerdb {

ArchiveRdbJob::ArchiveRdbJob(rdbms::ConnPool& connPool)
    : m_jobOwned((m_jobRow.mountId.value_or(0) != 0)),
      m_mountId(m_jobRow.mountId.value_or(0)),
      m_tapePool(m_jobRow.tapePool),
      m_connPool(connPool) {
  reset();
};

void ArchiveRdbJob::initialize(const rdbms::Rset& rset, bool jobIsRepack) {
  isRepack = jobIsRepack;
  m_jobRow = rset;
  // Reset or update other member variables as necessary
  m_jobOwned = (m_jobRow.mountId.value_or(0) != 0);
  m_mountId = m_jobRow.mountId.value_or(0);  // use mountId or 0 if not set
  m_tapePool = std::move(m_jobRow.tapePool);
  // Reset copied attributes
  jobID = m_jobRow.jobId;
  archiveRequestId = m_jobRow.reqId;
  requestJobCount = m_jobRow.reqJobCount;
  srcURL = std::move(m_jobRow.srcUrl);
  archiveReportURL = std::move(m_jobRow.archiveReportURL);
  errorReportURL = std::move(m_jobRow.archiveErrorReportURL);
  // latestError mirrors FAILURE_LOG so exceptionThrowingReportURL() has a failure reason to
  // report to EOS. Copied, not moved: failTransfer()/updateJobRowFailureLog() still append to it.
  latestError = m_jobRow.failureLogs.value_or("");

  archiveFile.archiveFileID = m_jobRow.archiveFileID;
  archiveFile.creationTime = m_jobRow.creationTime;
  archiveFile.diskFileId = std::move(m_jobRow.diskFileId);
  archiveFile.diskInstance = std::move(m_jobRow.diskInstance);
  archiveFile.fileSize = m_jobRow.fileSize;
  archiveFile.storageClass = std::move(m_jobRow.storageClass);
  archiveFile.diskFileInfo.path = std::move(m_jobRow.diskFileInfoPath);
  archiveFile.diskFileInfo.owner_uid = m_jobRow.diskFileInfoOwnerUid;
  archiveFile.diskFileInfo.gid = m_jobRow.diskFileInfoGid;
  archiveFile.checksumBlob = std::move(m_jobRow.checksumBlob);

  tapeFile.vid = std::move(m_jobRow.vid);
  tapeFile.copyNb = m_jobRow.copyNb;
  // Re-initialize report type
  if (m_jobRow.status == ArchiveJobStatus::AJS_ToReportToUserForSuccess
      || m_jobRow.status == ArchiveJobStatus::AJS_ToReportToRepackForSuccess) {
    reportType = ReportType::CompletionReport;
  } else if (m_jobRow.status == ArchiveJobStatus::AJS_ToReportToUserForFailure
             || m_jobRow.status == ArchiveJobStatus::AJS_ToReportToRepackForFailure) {
    reportType = ReportType::FailureReport;
  } else {
    reportType = ReportType::NoReportRequired;
  }
}

ArchiveRdbJob::ArchiveRdbJob(rdbms::ConnPool& connPool, const rdbms::Rset& rset, bool rowFromRepack)
    : ArchiveRdbJob(connPool) {
  initialize(rset, rowFromRepack);
};

void ArchiveRdbJob::reset() {
  // Reset job row state
  m_jobRow.reset();  // Reset the entire job row
  // Reset the core job-specific fields
  jobID = 0;                 // Resetting the job identifier
  srcURL.clear();            // Clearing source URL
  archiveReportURL.clear();  // Clearing the archive report URL
  errorReportURL.clear();    // Clearing the error report URL
  latestError.clear();       // Clearing the last failure reason (pooled jobs are re-used)

  archiveFile.archiveFileID = 0;
  archiveFile.diskFileId.clear();
  archiveFile.diskInstance.clear();
  archiveFile.fileSize = 0;
  archiveFile.storageClass.clear();
  archiveFile.diskFileInfo.path.clear();
  archiveFile.diskFileInfo.owner_uid = 0;
  archiveFile.diskFileInfo.gid = 0;
  archiveFile.checksumBlob.clear();
  archiveFile.tapeFiles.clear();  // Clearing vector to reset it

  tapeFile = common::dataStructures::TapeFile();
  m_mountId = 0;                              // Resetting mount ID to default
  m_tapePool.clear();                         // Clearing the tape pool name
  m_jobOwned = false;                         // Resetting ownership flag
  reportType = ReportType::NoReportRequired;  // Resetting report type
  isRepack = false;

  // No need to reset the connection pool (m_connPool) as it's shared and passed by reference
}

void ArchiveRdbJob::prepareFailTransfer(const std::string& failureReason) {
  m_jobRow.updateJobRowFailureLog(failureReason);
  m_jobRow.updateRetryCounts(m_mountId);
}

void ArchiveRdbJob::prepareFailReport(const std::string& failureReason) {
  // is_report_log=true: appends to reportFailureLogs and increments totalReportRetries.
  // The updated counters are read by failReportBatch() to decide per-job whether to
  // mark ReadyForDeletion (exceed retries) or reset IS_REPORTING (retry later).
  m_jobRow.updateJobRowFailureLog(failureReason, true);
}

void ArchiveRdbJob::failTransfer([[maybe_unused]] const std::string& failureReason,
                                 [[maybe_unused]] log::LogContext& lc) {
  // Never reached under pgsched: MigrationReportPacker calls prepareFailTransfer() (above) to
  // accumulate, then ArchiveMount::failTransferBatch() to flush via
  // ArchiveJobQueueRow::failTransferBatchRequeue()/failTransferBatchExceedRetries() - see
  // docs/content/dev/internals/shared-state/scheduler-db/postgresql/failure-handling.md for the
  // batched design. Only the objectstore backend's ArchiveJob::failTransfer() calls this
  // interface method for real.
  throw cta::exception::NotImplementedException(
    "ArchiveRdbJob::failTransfer(): not supported under pgsched; use prepareFailTransfer() + "
    "ArchiveMount::failTransferBatch() instead.");
}

void ArchiveRdbJob::failReport([[maybe_unused]] const std::string& failureReason,
                               [[maybe_unused]] log::LogContext& lc) {
  // Never reached under pgsched: MigrationReportPacker calls prepareFailReport() (above) to
  // accumulate, then ArchiveMount::failReportBatch() to flush via
  // ArchiveJobQueueRow::failReportBatchRequeue()/failReportBatchExceedRetries(). Only the
  // objectstore backend's ArchiveJob::failReport() calls this interface method for real.
  throw cta::exception::NotImplementedException(
    "ArchiveRdbJob::failReport(): not supported under pgsched; use prepareFailReport() + "
    "ArchiveMount::failReportBatch() instead.");
}

void ArchiveRdbJob::bumpUpTapeFileCount(uint64_t newFileCount) {
  throw cta::exception::NotImplementedException();
}

}  // namespace cta::schedulerdb
