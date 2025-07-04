/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include "common/exception/Exception.hpp"
#include "scheduler/rdbms/RetrieveRdbJob.hpp"
#include "scheduler/rdbms/postgres/RetrieveJobQueue.hpp"

#include "scheduler/rdbms/postgres/Transaction.hpp"
// includes for debug timings only below
#include "common/log/TimingList.hpp"
#include "common/utils/utils.hpp"
#include "common/Timer.hpp"

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

void RetrieveRdbJob::initialize(const rdbms::Rset& rset) {
  //cta::log::TimingList timings;
  //cta::utils::Timer t;
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
  retrieveRequest.creationLog.username = std::move(m_jobRow.srrUsername);  // EntryLog
  retrieveRequest.creationLog.host = std::move(m_jobRow.srrHost);
  retrieveRequest.creationLog.time = m_jobRow.srrTime;
  retrieveRequest.isVerifyOnly =
    m_jobRow.isVerifyOnly;             // request to retrieve file from tape but do not write a disk copy
  retrieveRequest.vid = m_jobRow.vid;  // limit retrieve requests to the specified vid (in the case of dual-copy files)
  retrieveRequest.mountPolicy = m_jobRow.mountPolicy;  // limit retrieve requests to a specified mount policy (only used for verification requests)
  retrieveRequest.lifecycleTimings.creation_time = m_jobRow.lifecycleTimings_creation_time;
  retrieveRequest.lifecycleTimings.first_selected_time = m_jobRow.lifecycleTimings_first_selected_time;
  retrieveRequest.lifecycleTimings.completed_time = m_jobRow.lifecycleTimings_completed_time;

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
  isRepack = false;  // for the moment hardcoded as repqck is not implemented

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
  } else if (m_jobRow.status == RetrieveJobStatus::RJS_ToReportToUserForFailure) {
    reportType = ReportType::FailureReport;
  } else {
    reportType = ReportType::NoReportRequired;
  }
}

// contructor to get next job batch to report
RetrieveRdbJob::RetrieveRdbJob(rdbms::ConnPool& connPool, const rdbms::Rset& rset) : RetrieveRdbJob(connPool) {
  initialize(rset);
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
  retrieveRequest.lifecycleTimings.creation_time = 0;
  retrieveRequest.lifecycleTimings.first_selected_time = 0;
  retrieveRequest.lifecycleTimings.completed_time = 0;

  retrieveRequest.activity.reset();
  diskSystemName.reset();

  reportType = ReportType::NoReportRequired;
}


void RetrieveRdbJob::handleExceedTotalRetries(cta::schedulerdb::Transaction& txn,
                                             log::LogContext& lc,
                                             const std::string& reason) {
  if (m_jobRow.status != RetrieveJobStatus::RJS_ToTransfer) {
    lc.log(log::WARNING,
           std::string("RetrieveRdbJob::handleExceedTotalRetries(): Unexpected status: ") + to_string(m_jobRow.status) +
           std::string(". Assuming ToTransfer anyway and changing status to RJS_ToReportToUserForFailure."));
  }

  try {
    uint64_t nrows = m_jobRow.updateFailedJobStatus(txn, RetrieveJobStatus::RJS_ToReportToUserForFailure);
    txn.commit();
    if (nrows != 1) {
      lc.log(log::WARNING,
             "RetrieveRdbJob::handleExceedTotalRetries(): ArchiveJobQueueRow::updateFailedJobStatus() failed; job not "
             "found (possibly deleted).");
    }
    reportType = ReportType::FailureReport;
  } catch (const exception::Exception& ex) {
    lc.log(cta::log::WARNING,
           "Failed to update job status for reporting failure. Aborting txn: " + ex.getMessageValue());
    txn.abort();
  }
}

void RetrieveRdbJob::requeueToNewMount(cta::schedulerdb::Transaction& txn,
                                      log::LogContext& lc,
                                      const std::string& reason) {
  try {
    // requeue by changing status, reset the mount_id to NULL and updating all other stat fields
    // change VID if alternate exists trying to fetch the file from another tape !
    m_jobRow.retriesWithinMount = 0;
    auto [newVid, index]  = cta::utils::selectNextString(m_jobRow.vid, m_jobRow.alternateVids);
    std::vector<std::string> alternateCopyNbsVec = splitStringToVector(m_jobRow.alternateCopyNbs);
    std::vector<std::string> alternateFSeqVec = splitStringToVector(m_jobRow.alternateFSeq);
    std::vector<std::string> alternateBlockIdVec = splitStringToVector(m_jobRow.alternateBlockId);
    m_jobRow.copyNb = alternateCopyNbsVec[index];
    m_jobRow.fSeq = alternateFSeqVec[index];
    m_jobRow.blockId = alternateBlockIdVec[index];
    m_jobRow.vid = newVid;
    uint64_t nrows = m_jobRow.requeueFailedJob(txn, RetrieveJobStatus::RJS_ToTransfer, false);
    txn.commit();
    if (nrows != 1) {
      lc.log(log::WARNING,
             "RetrieveRdbJob::requeueToNewMount(): requeueFailedJob (new mount) failed; job not found in DB.");
    }
    // since requeueing, we do not report and we do not
    // set reportType to a particular value here
  } catch (const exception::Exception& ex) {
    lc.log(log::WARNING,
           "RetrieveRdbJob::requeueToNewMount(): Failed to requeue to new mount. Aborting txn: " + ex.getMessageValue());
    txn.abort();
  }
}

void RetrieveRdbJob::requeueToSameMount(cta::schedulerdb::Transaction& txn, log::LogContext& lc, const std::string& reason) {
  try {
    // requeue to the same mount simply by moving it to PENDING QUEUE and updating all other stat fields
    uint64_t nrows = m_jobRow.requeueFailedJob(txn, RetrieveJobStatus::RJS_ToTransfer, true);
    txn.commit();
    if (nrows != 1) {
      lc.log(log::WARNING, "RetrieveRdbJob::requeueFailedJob(): (same mount) failed; job not found.");
    }
  } catch (const exception::Exception& ex) {
    lc.log(log::WARNING, "RetrieveRdbJob::requeueFailedJob(): Failed to requeue to same mount. Aborting txn: " + ex.getMessageValue());
    txn.abort();
  }
}

void RetrieveRdbJob::failTransfer(const std::string& failureReason, log::LogContext& lc) {
  log::ScopedParamContainer(lc)
    .add("jobID", jobID)
    .add("archiveFile.archiveFileID", archiveFile.archiveFileID)
    .add("mountId", m_mountId)
    .add("tapePool", m_tapePool)
    .add("failureReason", m_jobRow.failureLogs.value_or(""));
  lc.log(log::INFO, "In schedulerdb::RetrieveRdbJob::failTransfer(): received failed job to be reported.");
  m_jobRow.updateJobRowFailureLog(failureReason);
  m_jobRow.updateRetryCounts(m_mountId);

  cta::schedulerdb::Transaction txn(m_connPool);
  // here we either decide if we report the failure to user or requeue the job
  if (m_jobRow.totalRetries >= m_jobRow.maxTotalRetries) {
    handleExceedTotalRetries(txn, lc, failureReason);
    return;
  }
  // Decide if we want the job to have a chance to come back to this mount (requeue) or not.
  if (m_jobRow.retriesWithinMount >= m_jobRow.maxRetriesWithinMount) {
    requeueToNewMount(txn, lc, failureReason);
  } else {
    requeueToSameMount(txn, lc, failureReason);
  }
}

void RetrieveRdbJob::failReport(const std::string& failureReason, log::LogContext& lc) {
  lc.log(log::WARNING, "In schedulerdb::RetrieveRdbJob::failReport(): passes as half-dummy implementation !");
  m_jobRow.updateJobRowFailureLog(failureReason, true);
  log::ScopedParamContainer(lc)
    .add("jobID", jobID)
    .add("archiveFile.archiveFileID", archiveFile.archiveFileID)
    .add("mountId", m_mountId)
    .add("tapePool", m_tapePool)
    .add("reportFailureReason", m_jobRow.reportFailureLogs.value_or(""))
    .log(log::INFO, "In schedulerdb::RetrieveRdbJob::failReport(): reporting failed.");
  cta::schedulerdb::Transaction txn(m_connPool);
  try {
    cta::utils::Timer t;
    uint64_t nrowsdeleted = 0;
    if (reportType == ReportType::NoReportRequired || m_jobRow.totalReportRetries >= m_jobRow.maxReportRetries) {
      // the job will be moved to FAILED_QUEUE table, and delted from the ACTIVE_QUEUE.
      uint64_t nrows = m_jobRow.updateJobStatusForFailedReport(txn, RetrieveJobStatus::ReadyForDeletion);
      nrowsdeleted = nrows;
      if (nrows != 1) {
        log::ScopedParamContainer(lc)
          .add("jobID", jobID)
          .add("archiveFile.archiveFileID", archiveFile.archiveFileID)
          .add("mountId", m_mountId)
          .add("tapePool", m_tapePool)
          .add("reportFailureReason", m_jobRow.reportFailureLogs.value_or(""))
          .log(
            log::WARNING,
            "In schedulerdb::RetrieveJobQueueRow::updateJobStatusForFailedReport(): reporting failed terminally, but "
            "no job was found in the DB (possibly deleted by frontend cancelRetrieveJob() call in the meantime).");
      }
      // requeue job to failure table !
    } else {
      // Status is unchanged, but we reset the IS_REPORTING flag to FALSE
      m_jobRow.isReporting = false;
      uint64_t nrows = m_jobRow.updateJobStatusForFailedReport(txn, m_jobRow.status);
      if (nrows != 1) {
        log::ScopedParamContainer(lc)
          .add("jobID", jobID)
          .add("archiveFile.archiveFileID", archiveFile.archiveFileID)
          .add("mountId", m_mountId)
          .add("tapePool", m_tapePool)
          .add("reportFailureReason", m_jobRow.reportFailureLogs.value_or(""))
          .log(
            log::WARNING,
            "In schedulerdb::RetrieveJobQueueRow::updateJobStatusForFailedReport(): requeue reporting did not succeed, "
            "no job was found in the DB (possibly deleted by frontend cancelRetrieveJob() call in the meantime).");
      }
    }
    txn.commit();
    if (reportType == ReportType::NoReportRequired || m_jobRow.totalReportRetries >= m_jobRow.maxReportRetries) {
       log::ScopedParamContainer(lc)
        .add("rowDeletionCount", nrowsdeleted)
        .add("rowDeletionTime", t.secs())
        .log(log::INFO, "In schedulerdb::RetrieveJobQueueRow::updateJobStatusForFailedReport(): deleted jobs");
    }
  } catch (exception::Exception& ex) {
    lc.log(cta::log::WARNING,
           "In schedulerdb::RetrieveRdbJob::failReport(): failed to update job status for failed "
           "report case. Aborting the transaction." +
             ex.getMessageValue());
    txn.abort();
  }
  return;
}

void RetrieveRdbJob::abort(const std::string& abortReason, log::LogContext& lc) {
  throw cta::exception::Exception("Not implemented");
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
  throw cta::exception::Exception("Not implemented");
}

}  // namespace cta::schedulerdb
