/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/RetrieveRdbJob.hpp"

#include "common/exception/NotImplementedException.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/rdbms/postgres/RetrieveJobQueue.hpp"

namespace cta::schedulerdb {

RetrieveRdbJob::RetrieveRdbJob(rdbms::ConnPool& connPool)
    : m_jobOwned((m_jobRow.mountId.value_or(0) != 0)),
      m_mountId(m_jobRow.mountId.value_or(0)),
      m_tapePool(m_jobRow.tapePool),
      m_connPool(connPool) {
  // Copying relevant data from RetrieveJobQueueRow to RetrieveRdbJob
  archiveFile = cta::common::dataStructures::ArchiveFile();
  reset();
};

void RetrieveRdbJob::initialize(const rdbms::Rset& rset, bool jobIsRepack) {
  // we can safely move the values from m_jobRow since any further DB
  // operations are based on jobID if performed directly on this job/row itself
  m_jobRow = rset;
  // Copying relevant data from RetrieveJobQueueRow to RetrieveRdbJob
  jobID = m_jobRow.jobId;
  retrieveRequest.requester.name = std::move(m_jobRow.requesterName);
  retrieveRequest.requester.group = std::move(m_jobRow.requesterGroup);
  retrieveRequest.archiveFileID = m_jobRow.archiveFileID;
  retrieveRequest.dstURL = std::move(m_jobRow.dstURL);
  retrieveRequest.retrieveReportURL = std::move(m_jobRow.retrieveReportURL);
  retrieveRequest.errorReportURL = std::move(m_jobRow.retrieveErrorReportURL);
  retrieveRequest.diskFileInfo = archiveFile.diskFileInfo;
  retrieveRequest.creationLog.time = m_jobRow.creationTime;  // EntryLog
  retrieveRequest.isVerifyOnly =
    m_jobRow.isVerifyOnly;             // request to retrieve file from tape but do not write a disk copy
  retrieveRequest.vid = m_jobRow.vid;  // limit retrieve requests to the specified vid (in the case of dual-copy files)
  retrieveRequest.mountPolicy =
    m_jobRow.mountPolicy;  // limit retrieve requests to a specified mount policy (only used for verification requests)

  archiveFile.archiveFileID = m_jobRow.archiveFileID;
  archiveFile.diskFileId = std::move(m_jobRow.diskFileId);
  archiveFile.diskInstance = std::move(m_jobRow.diskInstance);
  archiveFile.fileSize = m_jobRow.fileSize;
  archiveFile.storageClass = std::move(m_jobRow.storageClass);
  archiveFile.diskFileInfo.path = std::move(m_jobRow.diskFileInfoPath);
  archiveFile.diskFileInfo.owner_uid = m_jobRow.diskFileInfoOwnerUid;
  archiveFile.diskFileInfo.gid = m_jobRow.diskFileInfoGid;
  archiveFile.checksumBlob = m_jobRow.checksumBlob;
  // we should later iterate through all the alternate fields to
  // get all the possible tape files for this archive file
  archiveFile.tapeFiles.emplace_back(std::move(m_jobRow.vid),
                                     m_jobRow.fSeq,
                                     m_jobRow.blockId,
                                     m_jobRow.fileSize,
                                     m_jobRow.copyNb,
                                     m_jobRow.creationTime,
                                     std::move(m_jobRow.checksumBlob));
  errorReportURL = std::move(m_jobRow.retrieveErrorReportURL);
  selectedCopyNb = m_jobRow.copyNb;
  isRepack = jobIsRepack;

  if (m_jobRow.activity) {
    retrieveRequest.activity = m_jobRow.activity.value();
  }
  if (m_jobRow.diskSystemName) {
    diskSystemName = m_jobRow.diskSystemName.value();
  }
  // Reset or update other member variables as necessary
  // Re-initialize report type
  if (m_jobRow.status == RetrieveJobStatus::RJS_ToReportToUserForSuccess) {
    reportType = ReportType::CompletionReport;
  } else if (m_jobRow.status == RetrieveJobStatus::RJS_ToReportToUserForFailure
             || m_jobRow.status == RetrieveJobStatus::RJS_ToReportToRepackForFailure) {
    reportType = ReportType::FailureReport;
  } else {
    reportType = ReportType::NoReportRequired;
  }
}

// contructor to get next job batch to report
RetrieveRdbJob::RetrieveRdbJob(rdbms::ConnPool& connPool, const rdbms::Rset& rset, bool rowFromRepack)
    : RetrieveRdbJob(connPool) {
  initialize(rset, rowFromRepack);
};

void RetrieveRdbJob::reset() {
  jobID = 0;

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

  errorReportURL.clear();
  selectedCopyNb = 0;
  isRepack = false;

  retrieveRequest.requester.name.clear();
  retrieveRequest.requester.group.clear();
  retrieveRequest.archiveFileID = 0;
  retrieveRequest.dstURL.clear();
  retrieveRequest.retrieveReportURL.clear();
  retrieveRequest.errorReportURL.clear();
  retrieveRequest.diskFileInfo.path.clear();
  retrieveRequest.diskFileInfo.owner_uid = 0;
  retrieveRequest.diskFileInfo.gid = 0;

  retrieveRequest.creationLog.username.clear();
  retrieveRequest.creationLog.host.clear();
  retrieveRequest.creationLog.time = 0;

  retrieveRequest.isVerifyOnly = false;
  retrieveRequest.vid = std::nullopt;
  retrieveRequest.mountPolicy = std::nullopt;

  retrieveRequest.activity.reset();
  diskSystemName.reset();

  reportType = ReportType::NoReportRequired;
}

#ifdef CTA_PGSCHED
void RetrieveRdbJob::prepareFailTransfer(const std::string& failureReason) {
  m_jobRow.updateJobRowFailureLog(failureReason);
  m_jobRow.updateRetryCounts(m_mountId);
  // If not terminal and within-mount limit exceeded, rotate to alternate VID.
  // Reset retriesWithinMount to 0 to signal keepMountId=false to the batch SQL
  // (retriesWithinMount > 0 means keep same mount, 0 means requeue to new mount).
  if (m_jobRow.totalRetries < m_jobRow.maxTotalRetries
      && m_jobRow.retriesWithinMount >= m_jobRow.maxRetriesWithinMount) {
    m_jobRow.retriesWithinMount = 0;
    // A row without a usable alternate still requeues to a new mount, just on its unchanged source
    // coordinates. AddCopiesOnly repack rows legitimately carry empty alternate lists (the recalled
    // source copy is not a rearchive target, so makeJobRow() had no jobs to list), and
    // selectNextString() then returns the current VID with index 0 - indexing the empty coordinate
    // vectors with it would be undefined behaviour. The same guard keeps a row whose current VID is
    // not in its list from pairing that VID with another tape's copyNb/fSeq/blockId.
    auto [newVid, index] = cta::utils::selectNextString(m_jobRow.vid, m_jobRow.alternateVids);
    if (newVid != m_jobRow.vid) {
      std::vector<std::string> alternateCopyNbsVec = cta::utils::splitStringToVector(m_jobRow.alternateCopyNbs);
      std::vector<std::string> alternateFSeqVec = cta::utils::splitStringToVector(m_jobRow.alternateFSeq);
      std::vector<std::string> alternateBlockIdVec = cta::utils::splitStringToVector(m_jobRow.alternateBlockId);
      if (index < alternateCopyNbsVec.size() && index < alternateFSeqVec.size() && index < alternateBlockIdVec.size()) {
        m_jobRow.copyNb = static_cast<uint8_t>(std::stoi(alternateCopyNbsVec[index]));
        m_jobRow.fSeq = static_cast<uint64_t>(std::stoi(alternateFSeqVec[index]));
        m_jobRow.blockId = static_cast<uint64_t>(std::stoi(alternateBlockIdVec[index]));
        m_jobRow.vid = newVid;
      }
    }
  }
}

void RetrieveRdbJob::prepareFailReport(const std::string& failureReason) {
  // updateJobRowFailureLog with is_report_log=true appends to reportFailureLogs
  // and increments totalReportRetries.
  m_jobRow.updateJobRowFailureLog(failureReason, true);
}
#endif

void RetrieveRdbJob::failTransfer([[maybe_unused]] const std::string& failureReason,
                                  [[maybe_unused]] log::LogContext& lc) {
  // Never reached under pgsched: RecallReportPacker calls prepareFailTransfer() (below) to
  // accumulate (including the alternate-VID rotation), then RetrieveMount::failTransferBatch()
  // to flush via RetrieveJobQueueRow::failTransferBatchRequeue()/failTransferBatchExceedRetries()
  // - see docs/content/dev/internals/shared-state/scheduler-db/postgresql/failure-handling.md
  // for the batched design. Only the objectstore backend's RetrieveJob::failTransfer() calls
  // this interface method for real.
  throw cta::exception::NotImplementedException(
    "RetrieveRdbJob::failTransfer(): not supported under pgsched; use prepareFailTransfer() + "
    "RetrieveMount::failTransferBatch() instead.");
}

void RetrieveRdbJob::failReport([[maybe_unused]] const std::string& failureReason,
                                [[maybe_unused]] log::LogContext& lc) {
  // Never reached under pgsched: RecallReportPacker calls prepareFailReport() (below) to
  // accumulate, then RelationalDB::failRetrieveJobBatchReport() to flush. Only the objectstore
  // backend's RetrieveJob::failReport() calls this interface method for real.
  throw cta::exception::NotImplementedException(
    "RetrieveRdbJob::failReport(): not supported under pgsched; use prepareFailReport() instead.");
}

void RetrieveRdbJob::abort(const std::string& abortReason, log::LogContext& lc) {
  throw cta::exception::NotImplementedException();
}

void RetrieveRdbJob::asyncSetSuccessful() {
  // since the RecalReportPacker calling this method does bunch the successful
  // jobs in int member m_successfulRetrieveJobs,
  //    m_successfulRetrieveJob->asyncSetSuccessful();
  //    parent.m_successfulRetrieveJobs.push(std::move(m_successfulRetrieveJob));
  // we do not contact the DB here for every job,
  // we will make the operation on the bunch of jobs directly in flushAsyncSuccessReports
  // called by the mount itself
  // any jobs that might be missed being reported need to be spotted by the garbage collection

  // This method intentionally does nothing, but needs to be here
  // for the compatibility with OStoreDB Scheduler
}

void RetrieveRdbJob::fail() {
  // it seems this method was just internal for OStoreDB and not needed here since we cover the use case in failTransfer
  throw cta::exception::NotImplementedException();
}

}  // namespace cta::schedulerdb
