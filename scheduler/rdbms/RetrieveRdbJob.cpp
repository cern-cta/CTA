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

RetrieveRdbJob::RetrieveRdbJob(rdbms::ConnPool& connPool, const rdbms::Rset& rset)
    : m_jobRow(rset),
      m_jobOwned((m_jobRow.mountId.value_or(0) != 0)),
      m_mountId(m_jobRow.mountId.value_or(0)),  // use mountId or 0 if not set
      m_tapePool(m_jobRow.tapePool),
      m_connPool(connPool) {
  // Copying relevant data from RetrieveJobQueueRow to RetrieveRdbJob
  jobID = m_jobRow.jobId;
  errorReportURL = m_jobRow.retrieveErrorReportURL;
  archiveFile = m_jobRow.archiveFile;
  selectedCopyNb = m_jobRow.copyNb;
  isRepack = false;  // for the moment hardcoded as repqck is not implemented
  retrieveRequest.requester.name = m_jobRow.requesterName;
  retrieveRequest.requester.group = m_jobRow.requesterGroup;
  retrieveRequest.archiveFileID = m_jobRow.archiveFile.archiveFileID;
  retrieveRequest.dstURL = m_jobRow.dstURL;
  retrieveRequest.retrieveReportURL = m_jobRow.retrieveReportURL;
  retrieveRequest.errorReportURL = m_jobRow.retrieveErrorReportURL;
  retrieveRequest.diskFileInfo = m_jobRow.archiveFile.diskFileInfo;
  retrieveRequest.creationLog.username = m_jobRow.srrUsername;  // EntryLog
  retrieveRequest.creationLog.host = m_jobRow.srrHost;
  retrieveRequest.creationLog.time = m_jobRow.srrTime;
  retrieveRequest.isVerifyOnly =
    m_jobRow.isVerifyOnly;             // request to retrieve file from tape but do not write a disk copy
  retrieveRequest.vid = m_jobRow.vid;  // limit retrieve requests to the specified vid (in the case of dual-copy files)
  retrieveRequest.mountPolicy =
    m_jobRow.mountPolicy;  // limit retrieve requests to a specified mount policy (only used for verification requests)
  retrieveRequest.lifecycleTimings.creation_time = m_jobRow.lifecycleTimings_creation_time;
  retrieveRequest.lifecycleTimings.first_selected_time = m_jobRow.lifecycleTimings_first_selected_time;
  retrieveRequest.lifecycleTimings.completed_time = m_jobRow.lifecycleTimings_completed_time;
  if(m_jobRow.activity){
    retrieveRequest.activity = m_jobRow.activity.value();
  }
  if(m_jobRow.diskSystemName){
    diskSystemName = m_jobRow.diskSystemName.value();
  }

  // Set other attributes or perform any necessary initialization
  // Setting the internal report type - in case isReporting == false No Report type required
  if (m_jobRow.status == RetrieveJobStatus::RJS_ToTransfer) {
    reportType = ReportType::CompletionReport;
  } else if (m_jobRow.status == RetrieveJobStatus::RJS_ToReportToUserForFailure) {
    reportType = ReportType::FailureReport;
  } else {
    reportType = ReportType::NoReportRequired;
  }
};

RetrieveRdbJob::RetrieveRdbJob(rdbms::ConnPool& connPool)
    : m_jobOwned((m_jobRow.mountId.value_or(0) != 0)),
      m_mountId(m_jobRow.mountId.value_or(0)),  // use mountId or 0 if not set
      m_tapePool(m_jobRow.tapePool),
      m_connPool(connPool) {
  // Copying relevant data from RetrieveJobQueueRow to RetrieveRdbJob
  jobID = m_jobRow.jobId;
  errorReportURL = m_jobRow.retrieveErrorReportURL;
  archiveFile = m_jobRow.archiveFile;
  selectedCopyNb = m_jobRow.copyNb;
  isRepack = false;  // for the moment hardcoded as repqck is not implemented
  retrieveRequest.requester.name = m_jobRow.requesterName;
  retrieveRequest.requester.group = m_jobRow.requesterGroup;
  retrieveRequest.archiveFileID = m_jobRow.archiveFile.archiveFileID;
  retrieveRequest.dstURL = m_jobRow.dstURL;
  retrieveRequest.retrieveReportURL = m_jobRow.retrieveReportURL;
  retrieveRequest.errorReportURL = m_jobRow.retrieveErrorReportURL;
  retrieveRequest.diskFileInfo = m_jobRow.archiveFile.diskFileInfo;
  retrieveRequest.creationLog.username = m_jobRow.srrUsername;  // EntryLog
  retrieveRequest.creationLog.host = m_jobRow.srrHost;
  retrieveRequest.creationLog.time = m_jobRow.srrTime;
  retrieveRequest.isVerifyOnly =
    m_jobRow.isVerifyOnly;             // request to retrieve file from tape but do not write a disk copy
  retrieveRequest.vid = m_jobRow.vid;  // limit retrieve requests to the specified vid (in the case of dual-copy files)
  retrieveRequest.mountPolicy =
    m_jobRow.mountPolicy;  // limit retrieve requests to a specified mount policy (only used for verification requests)
  retrieveRequest.lifecycleTimings.creation_time = m_jobRow.lifecycleTimings_creation_time;
  retrieveRequest.lifecycleTimings.first_selected_time = m_jobRow.lifecycleTimings_first_selected_time;
  retrieveRequest.lifecycleTimings.completed_time = m_jobRow.lifecycleTimings_completed_time;
  retrieveRequest.activity = m_jobRow.activity.value_or("");
  if(m_jobRow.activity){
    retrieveRequest.activity = m_jobRow.activity.value();
  }
  if(m_jobRow.diskSystemName){
    diskSystemName = m_jobRow.diskSystemName.value();
  }
  // Set other attributes or perform any necessary initialization
  // Setting the internal report type - in case isReporting == false No Report type required
  if (m_jobRow.status == RetrieveJobStatus::RJS_ToTransfer) {
    reportType = ReportType::CompletionReport;
  } else if (m_jobRow.status == RetrieveJobStatus::RJS_ToReportToUserForFailure) {
    reportType = ReportType::FailureReport;
  } else {
    reportType = ReportType::NoReportRequired;
  }
};

void RetrieveRdbJob::initialize(const rdbms::Rset& rset, log::LogContext& lc) {
  //cta::log::TimingList timings;
  //cta::utils::Timer t;
  m_jobRow = rset;
  // Copying relevant data from RetrieveJobQueueRow to RetrieveRdbJob
  jobID = m_jobRow.jobId;
  errorReportURL = m_jobRow.retrieveErrorReportURL;
  archiveFile = m_jobRow.archiveFile;
  selectedCopyNb = m_jobRow.copyNb;
  isRepack = false;  // for the moment hardcoded as repqck is not implemented
  retrieveRequest.requester.name = m_jobRow.requesterName;
  retrieveRequest.requester.group = m_jobRow.requesterGroup;
  retrieveRequest.archiveFileID = m_jobRow.archiveFile.archiveFileID;
  retrieveRequest.dstURL = m_jobRow.dstURL;
  retrieveRequest.retrieveReportURL = m_jobRow.retrieveReportURL;
  retrieveRequest.errorReportURL = m_jobRow.retrieveErrorReportURL;
  retrieveRequest.diskFileInfo = m_jobRow.archiveFile.diskFileInfo;
  retrieveRequest.creationLog.username = m_jobRow.srrUsername;  // EntryLog
  retrieveRequest.creationLog.host = m_jobRow.srrHost;
  retrieveRequest.creationLog.time = m_jobRow.srrTime;
  retrieveRequest.isVerifyOnly =
    m_jobRow.isVerifyOnly;             // request to retrieve file from tape but do not write a disk copy
  retrieveRequest.vid = m_jobRow.vid;  // limit retrieve requests to the specified vid (in the case of dual-copy files)
  retrieveRequest.mountPolicy =
    m_jobRow.mountPolicy;  // limit retrieve requests to a specified mount policy (only used for verification requests)
  retrieveRequest.lifecycleTimings.creation_time = m_jobRow.lifecycleTimings_creation_time;
  retrieveRequest.lifecycleTimings.first_selected_time = m_jobRow.lifecycleTimings_first_selected_time;
  retrieveRequest.lifecycleTimings.completed_time = m_jobRow.lifecycleTimings_completed_time;
  if(m_jobRow.activity){
    retrieveRequest.activity = m_jobRow.activity.value();
  }
  if(m_jobRow.diskSystemName){
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

void RetrieveRdbJob::reset() {
  // Reset job row state
  m_jobRow.reset();  // Reset the entire job row
  // Reset the core job-specific fields
  jobID = 0;
  errorReportURL.clear();
  archiveFile = m_jobRow.archiveFile;
  selectedCopyNb = 0;
  isRepack = false;  // for the moment hardcoded as repqck is not implemented
  retrieveRequest.requester.name.clear();
  retrieveRequest.requester.group.clear();
  retrieveRequest.archiveFileID = 0;
  retrieveRequest.dstURL.clear();
  retrieveRequest.retrieveReportURL.clear();
  retrieveRequest.errorReportURL.clear();
  retrieveRequest.diskFileInfo = m_jobRow.archiveFile.diskFileInfo;
  retrieveRequest.creationLog.username.clear();  // EntryLog
  retrieveRequest.creationLog.host.clear();
  retrieveRequest.creationLog.time = m_jobRow.srrTime;
  retrieveRequest.isVerifyOnly =
    m_jobRow.isVerifyOnly;      // request to retrieve file from tape but do not write a disk copy
  retrieveRequest.vid->clear();  // limit retrieve requests to the specified vid (in the case of dual-copy files)
  retrieveRequest.mountPolicy
    ->clear();  // limit retrieve requests to a specified mount policy (only used for verification requests)
  retrieveRequest.activity->clear();
  diskSystemName = std::nullopt;
  retrieveRequest.lifecycleTimings.creation_time = m_jobRow.lifecycleTimings_creation_time;
  retrieveRequest.lifecycleTimings.first_selected_time = m_jobRow.lifecycleTimings_first_selected_time;
  retrieveRequest.lifecycleTimings.completed_time = m_jobRow.lifecycleTimings_completed_time;
  reportType = ReportType::NoReportRequired;  // Resetting report type
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

  // For multiple jobs existing more might need to be done to ensure the file on EOS
  // is deleted only when both requests succeed !
  //if (m_jobRow.reqJobCount > 1){
  //query
  //}

  // I need to add handling of multiple JOBS OF THE SAME REQUEST in cases when
  // not all jobs succeeded - do we report failure for all ? - I guess so,
  // check how this case is handled in the objectstore

  if (m_jobRow.lastMountWithFailure == m_mountId) {
    m_jobRow.retriesWithinMount += 1;
  } else {
    m_jobRow.retriesWithinMount = 1;
    m_jobRow.lastMountWithFailure = m_mountId;
  }
  m_jobRow.totalRetries += 1;
  // for now we do not pur failure log into the DB just log it
  // not sure if this is necessary
  //m_jobRow.failureLogs.emplace_back(failureReason);
  //cta::rdbms::Conn txn_conn = m_connPool.getConn();
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
    // decide if the failure comes from full tape or failed task queue - then avoid re-queueing to the same mount
    // for the moment this is driven by failureReason - needs more robust design in the future !
    std::string substring = "In TapeWriteSingleThread::run(): cleaning failed task queue";
    bool failedtaskqueuejob = false;
    if (failureReason.find(substring) != std::string::npos) {
      failedtaskqueuejob = true;
    }
    // Decide if we want the job to have a chance to come back to this mount (requeue) or not.
    if (m_jobRow.retriesWithinMount >= m_jobRow.maxRetriesWithinMount || failedtaskqueuejob) {
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
    if (reportType == ReportType::NoReportRequired || m_jobRow.totalReportRetries >= m_jobRow.maxReportRetries) {
      //m_jobRow.updateJobStatusForFailedReport(txn, RetrieveJobStatus::RJS_Failed);
      uint64_t nrows = m_jobRow.updateJobStatusForFailedReport(txn, RetrieveJobStatus::ReadyForDeletion);
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
  throw cta::exception::Exception("Not implemented");
}

void RetrieveRdbJob::fail() {
  throw cta::exception::Exception("Not implemented");
}

}  // namespace cta::schedulerdb
