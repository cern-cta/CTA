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
    : m_jobRow(),
      m_jobOwned((m_jobRow.mountId.value_or(0) != 0)),
      m_mountId(m_jobRow.mountId.value_or(0)),  // use mountId or 0 if not set
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
  retrieveRequest.creationLog.time = std::move(m_jobRow.srrTime);
  retrieveRequest.isVerifyOnly =
    m_jobRow.isVerifyOnly;             // request to retrieve file from tape but do not write a disk copy
  retrieveRequest.vid = m_jobRow.vid;  // limit retrieve requests to the specified vid (in the case of dual-copy files)
  retrieveRequest.mountPolicy =
    m_jobRow.mountPolicy;  // limit retrieve requests to a specified mount policy (only used for verification requests)
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
  /* rj retrieve job setting:
     rj->archiveFile = rr.m_archiveFile;

     rj->diskSystemName = rr.m_diskSystemName;
     rj->retrieveRequest = rr.m_schedRetrieveReq;
     rj->selectedCopyNb = rr.m_actCopyNb;
     rj->isRepack = rr.m_repackInfo.isRepack;
     rj->m_repackInfo = rr.m_repackInfo;
     //   rj->m_jobOwned = true;
     rj->m_mountId = mountInfo.mountId;
     */

  //timings.insOrIncAndReset("RetrieveRdbJob_rset_init", t);
  // Reset or update other member variables as necessary
  //timings.insOrIncAndReset("RetrieveRdbJob_dbjobmembers_init", t);
  // Re-initialize report type
  if (m_jobRow.status == RetrieveJobStatus::RJS_ToTransfer) {
    reportType = ReportType::CompletionReport;
  } else if (m_jobRow.status == RetrieveJobStatus::RJS_ToReportToUserForFailure) {
    reportType = ReportType::FailureReport;
  } else {
    reportType = ReportType::NoReportRequired;
  }
  //timings.insOrIncAndReset("RetrieveRdbJob_status_init", t);
  //cta::log::ScopedParamContainer logParams(lc);
  //timings.addToLog(logParams);
  //lc.log(cta::log::INFO, "In RetrieveRdbJob::initialize(): Finished initializing job object with new values.");
}

// contructor for get next job batch to report
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

void RetrieveRdbJob::failTransfer(const std::string& failureReason, log::LogContext& lc) {
  std::string failureLog =
    cta::utils::getCurrentLocalTime() + " " + cta::utils::getShortHostname() + " " + failureReason;
  if (m_jobRow.failureLogs.has_value()) {
    m_jobRow.failureLogs.value() += failureLog;
  } else {
    m_jobRow.failureLogs = failureLog;
  }
  lc.log(log::WARNING, "In schedulerdb::RetrieveRdbJob::failTransfer(): passes as half-dummy implementation !");
  log::ScopedParamContainer(lc)
    .add("jobID", jobID)
    .add("archiveFile.archiveFileID", archiveFile.archiveFileID)
    .add("mountId", m_mountId)
    .add("tapePool", m_tapePool)
    .add("failureReason", m_jobRow.failureLogs.value_or(""))
    .log(log::INFO, "In schedulerdb::RetrieveRdbJob::failTransfer(): received failed job to be reported.");

  if (m_jobRow.lastMountWithFailure == m_mountId) {
    m_jobRow.retriesWithinMount += 1;
  } else {
    m_jobRow.retriesWithinMount = 1;
    m_jobRow.lastMountWithFailure = m_mountId;
  }
  m_jobRow.totalRetries += 1;

  cta::schedulerdb::Transaction txn(m_connPool);
  // here we either decide if we report the failure to user or requeue the job
  if (m_jobRow.totalRetries >= m_jobRow.maxTotalRetries) {
    // We have to determine if this was the last copy to fail/succeed.
    if (m_jobRow.status != RetrieveJobStatus::RJS_ToTransfer) {
      // Wrong status, but the context leaves no ambiguity and we set the job to report failure.
      lc.log(log::WARNING,
             std::string("In RetrieveRdbJob::failTransfer(): unexpected status: ") + to_string(m_jobRow.status) +
               std::string("Assuming ToTransfer."));
    }
    try {
      uint64_t nrows = m_jobRow.updateFailedJobStatus(txn, RetrieveJobStatus::RJS_ToReportToUserForFailure);
      txn.commit();
      if (nrows != 1) {
        log::ScopedParamContainer(lc)
          .add("jobID", jobID)
          .add("archiveFile.archiveFileID", archiveFile.archiveFileID)
          .add("mountId", m_mountId)
          .add("tapePool", m_tapePool)
          .add("failureReason", m_jobRow.failureLogs.value_or(""))
          .log(log::WARNING,
               "In schedulerdb::RetrieveJobQueueRow::updateFailedJobStatus(): all job retries failed and "
               "update of the failure failed as well since, no jobs were found in DB (possibly deleted "
               "by frontend cancelRetrieveJob() call in the meantime).");
      }
      reportType = ReportType::FailureReport;
    } catch (exception::Exception& ex) {
      lc.log(cta::log::WARNING,
             "In schedulerdb::RetrieveRdbJob::failTransfer(): failed to update job status for "
             "reporting a failure. Aborting the transaction." +
               ex.getMessageValue());
      txn.abort();
    }
  } else {
    // Decide if we want the job to have a chance to come back to this mount (requeue) or not.
    if (m_jobRow.retriesWithinMount >= m_jobRow.maxRetriesWithinMount) {
      try {
        // requeue by changing status, reset the mount_id to NULL and updating all other stat fields
        m_jobRow.retriesWithinMount = 0;
        uint64_t nrows = m_jobRow.requeueFailedJob(txn, RetrieveJobStatus::RJS_ToTransfer, false);
        txn.commit();
        if (nrows != 1) {
          log::ScopedParamContainer(lc)
            .add("jobID", jobID)
            .add("archiveFile.archiveFileID", archiveFile.archiveFileID)
            .add("mountId", m_mountId)
            .add("tapePool", m_tapePool)
            .add("failureReason", m_jobRow.failureLogs.value_or(""))
            .log(log::WARNING,
                 "In schedulerdb::RetrieveJobQueueRow::requeueFailedJob(): requeue job to a new mount failed, no "
                 "job found in DB (possibly deleted by frontend cancelRetrieveJob() call in the meantime).");
        }
        // since requeueing, we do not report and we do not
        // set reportType to a particular value here
      } catch (exception::Exception& ex) {
        lc.log(cta::log::WARNING,
               "In schedulerdb::RetrieveRdbJob::failTransfer(): failed to update job status to "
               "requeue the failed job on a new mount. Aborting the transaction." +
                 ex.getMessageValue());
        txn.abort();
      }
    } else {
      try {
        // requeue to the same mount simply by changing IN_DRIVE_QUEUE to False and updating all other stat fields
        uint64_t nrows = m_jobRow.requeueFailedJob(txn, RetrieveJobStatus::RJS_ToTransfer, true);
        txn.commit();
        if (nrows != 1) {
          log::ScopedParamContainer(lc)
            .add("jobID", jobID)
            .add("archiveFile.archiveFileID", archiveFile.archiveFileID)
            .add("mountId", m_mountId)
            .add("tapePool", m_tapePool)
            .add("failureReason", m_jobRow.failureLogs.value_or(""))
            .log(log::WARNING,
                 "In schedulerdb::RetrieveJobQueueRow::requeueFailedJob(): requeue job on the same mount failed, "
                 "no job found in DB (possibly deleted by frontend cancelRetrieveJob() call in the meantime).");
        }
        // since requeueing, we do not report and we do not
        // set reportType to a particular value here
      } catch (exception::Exception& ex) {
        lc.log(cta::log::WARNING,
               "In schedulerdb::RetrieveRdbJob::failTransfer(): failed to update job status to "
               "requeue the failed job. Aborting the transaction." +
                 ex.getMessageValue());
        txn.abort();
      }
    }
  }
}

void RetrieveRdbJob::failReport(const std::string& failureReason, log::LogContext& lc) {
  lc.log(log::WARNING, "In schedulerdb::RetrieveRdbJob::failReport(): passes as half-dummy implementation !");
  std::string reportFailureLog =
    cta::utils::getCurrentLocalTime() + " " + cta::utils::getShortHostname() + " " + failureReason;
  if (m_jobRow.reportFailureLogs.has_value()) {
    m_jobRow.reportFailureLogs.value() += reportFailureLog;
  } else {
    m_jobRow.reportFailureLogs = reportFailureLog;
  }
  m_jobRow.totalReportRetries += 1;
  log::ScopedParamContainer(lc)
    .add("jobID", jobID)
    .add("archiveFile.archiveFileID", archiveFile.archiveFileID)
    .add("mountId", m_mountId)
    .add("tapePool", m_tapePool)
    .add("reportFailureReason", m_jobRow.reportFailureLogs.value_or(""))
    .log(log::INFO, "In schedulerdb::RetrieveRdbJob::failReport(): reporting failed.");

  // We could use reportType NoReportRequired for cancelling the request. For the moment it is not used
  // and we directly delet ethe job.
  // We could also use it for a case whena a previous attempt to report failed
  // due to an exception, for example if the file was deleted on close.
  cta::schedulerdb::Transaction txn(m_connPool);
  try {
    cta::utils::Timer t;
    uint64_t nrowsdeleted = 0;
    if (reportType == ReportType::NoReportRequired || m_jobRow.totalReportRetries >= m_jobRow.maxReportRetries) {
      //m_jobRow.updateJobStatusForFailedReport(txn, RetrieveJobStatus::RJS_Failed);
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
