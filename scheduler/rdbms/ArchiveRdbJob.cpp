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
#include "scheduler/rdbms/ArchiveRdbJob.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"
// includes for debug timings only below
#include "common/log/TimingList.hpp"
#include "common/utils/utils.hpp"
#include "common/Timer.hpp"

namespace cta::schedulerdb {

ArchiveRdbJob::ArchiveRdbJob(rdbms::ConnPool& connPool)
    : m_jobOwned((m_jobRow.mountId.value_or(0) != 0)),
      m_mountId(m_jobRow.mountId.value_or(0)),
      m_tapePool(m_jobRow.tapePool),
      m_connPool(connPool) {
  reset();
};

void ArchiveRdbJob::initialize(const rdbms::Rset& rset) {
  m_jobRow = rset;
  // Reset or update other member variables as necessary
  m_jobOwned = (m_jobRow.mountId.value_or(0) != 0);
  m_mountId = m_jobRow.mountId.value_or(0);  // use mountId or 0 if not set
  m_tapePool = std::move(m_jobRow.tapePool);
  // Reset copied attributes
  jobID = m_jobRow.jobId;
  srcURL = std::move(m_jobRow.srcUrl);
  archiveReportURL = std::move(m_jobRow.archiveReportURL);
  errorReportURL = std::move(m_jobRow.archiveErrorReportURL);

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
  if (m_jobRow.status == ArchiveJobStatus::AJS_ToReportToUserForSuccess) {
    reportType = ReportType::CompletionReport;
  } else if (m_jobRow.status == ArchiveJobStatus::AJS_ToReportToUserForFailure) {
    reportType = ReportType::FailureReport;
  } else {
    reportType = ReportType::NoReportRequired;
  }
}

ArchiveRdbJob::ArchiveRdbJob(rdbms::ConnPool& connPool, const rdbms::Rset& rset) : ArchiveRdbJob(connPool) {
  initialize(rset);
};

void ArchiveRdbJob::reset() {
  // Reset job row state
  m_jobRow.reset();  // Reset the entire job row
  // Reset the core job-specific fields
  jobID = 0;                 // Resetting the job identifier
  srcURL.clear();            // Clearing source URL
  archiveReportURL.clear();  // Clearing the archive report URL
  errorReportURL.clear();    // Clearing the error report URL

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

  // No need to reset the connection pool (m_connPool) as it's shared and passed by reference
}

void ArchiveRdbJob::handleExceedTotalRetries(cta::schedulerdb::Transaction& txn,
                                             log::LogContext& lc,
                                             const std::string& reason) {
  if (m_jobRow.status != ArchiveJobStatus::AJS_ToTransferForUser) {
    lc.log(log::WARNING,
           std::string("ArchiveRdbJob::handleExceedTotalRetries(): Unexpected status: ") + to_string(m_jobRow.status) +
           std::string(". Assuming ToTransfer anyway and changing status to AJS_ToReportToUserForFailure."));
  }

  try {
    uint64_t nrows = m_jobRow.updateFailedJobStatus(txn, ArchiveJobStatus::AJS_ToReportToUserForFailure);
    txn.commit();
    if (nrows != 1) {
      lc.log(log::WARNING,
             "ArchiveRdbJob::handleExceedTotalRetries(): ArchiveJobQueueRow::updateFailedJobStatus() failed; job not "
             "found (possibly deleted).");
    }
    reportType = ReportType::FailureReport;
  } catch (const exception::Exception& ex) {
    lc.log(cta::log::WARNING,
           "Failed to update job status for reporting failure. Aborting txn: " + ex.getMessageValue());
    txn.abort();
  }
}

void ArchiveRdbJob::requeueToNewMount(cta::schedulerdb::Transaction& txn,
                                      log::LogContext& lc,
                                      const std::string& reason) {
  try {
    // requeue by changing status, reset the mount_id to NULL and updating all other stat fields
    m_jobRow.retriesWithinMount = 0;
    uint64_t nrows = m_jobRow.requeueFailedJob(txn, ArchiveJobStatus::AJS_ToTransferForUser, false);
    txn.commit();
    if (nrows != 1) {
      lc.log(log::WARNING,
             "ArchiveRdbJob::requeueToNewMount(): requeueFailedJob (new mount) failed; job not found in DB.");
    }
    // since requeueing, we do not report and we do not
    // set reportType to a particular value here
  } catch (const exception::Exception& ex) {
    lc.log(log::WARNING,
           "ArchiveRdbJob::requeueToNewMount(): Failed to requeue to new mount. Aborting txn: " + ex.getMessageValue());
    txn.abort();
  }
}

void ArchiveRdbJob::requeueToSameMount(cta::schedulerdb::Transaction& txn, log::LogContext& lc, const std::string& reason) {
  try {
    // requeue to the same mount simply by moving it to PENDING QUEUE and updating all other stat fields
    uint64_t nrows = m_jobRow.requeueFailedJob(txn, ArchiveJobStatus::AJS_ToTransferForUser, true);
    txn.commit();
    if (nrows != 1) {
      lc.log(log::WARNING, "ArchiveRdbJob::requeueFailedJob(): (same mount) failed; job not found.");
    }
  } catch (const exception::Exception& ex) {
    lc.log(log::WARNING, "ArchiveRdbJob::requeueFailedJob(): Failed to requeue to same mount. Aborting txn: " + ex.getMessageValue());
    txn.abort();
  }
}

void ArchiveRdbJob::failTransfer(const std::string& failureReason, log::LogContext& lc) {
  log::ScopedParamContainer(lc)
    .add("jobID", jobID)
    .add("archiveFile.archiveFileID", archiveFile.archiveFileID)
    .add("mountId", m_mountId)
    .add("tapePool", m_tapePool)
    .add("failureReason", m_jobRow.failureLogs.value_or(""));
  lc.log(log::WARNING, "In schedulerdb::ArchiveRdbJob::failTransfer(): received failed job to be reported.");
  m_jobRow.updateJobRowFailureLog(failureReason);

  // For multiple jobs existing more might need to be done to ensure the file on EOS
  // is deleted only when both requests succeed !
  //if (m_jobRow.reqJobCount > 1){
  //query
  //}

  // I need to add handling of multiple JOBS OF THE SAME REQUEST in cases when
  // not all jobs succeeded - do we report failure for all ? - I guess so,
  // check how this case is handled in the objectstore

  m_jobRow.updateRetryCounts(m_mountId);
  // for now we do not pur failure log into the DB just log it
  // not sure if this is necessary
  //m_jobRow.failureLogs.emplace_back(failureReason);
  //cta::rdbms::Conn txn_conn = m_connPool.getConn();
  cta::schedulerdb::Transaction txn(m_connPool);
  // here we either decide if we report the failure to user or requeue the job
  if (m_jobRow.totalRetries >= m_jobRow.maxTotalRetries) {
    handleExceedTotalRetries(txn, lc, failureReason);
    return;
  }

  // decide if the failure comes from full tape or failed task queue - then avoid re-queueing to the same mount
  // for the moment this is driven by failureReason - needs more robust design in the future !
  const bool failedTaskQueue =
    failureReason.find("In TapeWriteSingleThread::run(): cleaning failed task queue") != std::string::npos;
  if (m_jobRow.retriesWithinMount >= m_jobRow.maxRetriesWithinMount || failedTaskQueue) {
    requeueToNewMount(txn, lc, failureReason);
  } else {
    requeueToSameMount(txn, lc, failureReason);
  }
}

void ArchiveRdbJob::failReport(const std::string& failureReason, log::LogContext& lc) {
  log::ScopedParamContainer(lc)
    .add("jobID", jobID)
    .add("archiveFile.archiveFileID", archiveFile.archiveFileID)
    .add("mountId", m_mountId)
    .add("tapePool", m_tapePool)
    .add("reportFailureReason", m_jobRow.reportFailureLogs.value_or(""));
  lc.log(log::INFO, "In schedulerdb::ArchiveRdbJob::failReport(): reporting failed.");
  m_jobRow.updateJobRowFailureLog(failureReason, true);
  cta::schedulerdb::Transaction txn(m_connPool);
  try {
    cta::utils::Timer t;
    uint64_t deletionCount = 0;
    if (reportType == ReportType::NoReportRequired || m_jobRow.totalReportRetries >= m_jobRow.maxReportRetries) {
      // the job will be moved to FAILED_QUEUE table, and deleted from the ACTIVE_QUEUE.
      uint64_t nrows = m_jobRow.updateJobStatusForFailedReport(txn, ArchiveJobStatus::ReadyForDeletion);
      deletionCount = nrows;
      if (nrows != 1) {
        lc.log(log::WARNING,
               "In schedulerdb::ArchiveJobQueueRow::updateJobStatusForFailedReport(): reporting failed terminally, but "
               "no job was found in the DB (possibly deleted by frontend cancelArchiveJob() call in the meantime).");
      }
      // requeue job to failure table !
    } else {
      // Status is unchanged, but we reset the IS_REPORTING flag to FALSE
      m_jobRow.isReporting = false;
      uint64_t nrows = m_jobRow.updateJobStatusForFailedReport(txn, m_jobRow.status);
      if (nrows != 1) {
        lc.log(
          log::WARNING,
          "In schedulerdb::ArchiveJobQueueRow::updateJobStatusForFailedReport(): requeue reporting did not succeed, "
          "no job was found in the DB (possibly deleted by frontend cancelArchiveJob() call in the meantime).");
      }
    }
    txn.commit();
    if (reportType == ReportType::NoReportRequired || m_jobRow.totalReportRetries >= m_jobRow.maxReportRetries) {
      log::ScopedParamContainer(lc)
        .add("rowDeletionTime", t.secs())
        .add("rowDeletionCount", deletionCount)
        .log(log::INFO, "ArchiveRdbJob::failReport(): deleted job.");
    }
  } catch (exception::Exception& ex) {
    lc.log(cta::log::WARNING,
           "In schedulerdb::ArchiveRdbJob::failReport(): failed to update job status for failed "
           "report case. Aborting the transaction." +
             ex.getMessageValue());
    txn.abort();
  }
  return;
}

void ArchiveRdbJob::bumpUpTapeFileCount(uint64_t newFileCount) {
  throw cta::exception::Exception("Not implemented");
}

}  // namespace cta::schedulerdb
