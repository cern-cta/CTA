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
#include "scheduler/rdbms/postgres/ArchiveJobQueue.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"

namespace cta::schedulerdb {

ArchiveRdbJob::ArchiveRdbJob(rdbms::ConnPool& connPool, const postgres::ArchiveJobQueueRow& jobQueueRow):
  m_jobOwned((jobQueueRow.mountId.value_or(0) != 0)),
  m_mountId(jobQueueRow.mountId.value_or(0)), // use mountId or 0 if not set
  m_tapePool(jobQueueRow.tapePool),
  m_connPool(connPool),
  m_jobRow(jobQueueRow)
  {
    // Copying relevant data from ArchiveJobQueueRow to ArchiveRdbJob
    jobID = jobQueueRow.jobId;
    srcURL = jobQueueRow.srcUrl;
    archiveReportURL = jobQueueRow.archiveReportUrl;
    errorReportURL = jobQueueRow.archiveErrorReportUrl;
    archiveFile = jobQueueRow.archiveFile; // assuming ArchiveFile is copyable
    tapeFile.vid = jobQueueRow.vid;
    tapeFile.copyNb = jobQueueRow.copyNb;
    // Set other attributes or perform any necessary initialization
    // Setting the internal report type - in case is_reporting == false No Report type required
    if (jobQueueRow.status == schedulerdb::ArchiveJobStatus::AJS_ToReportToUserForTransfer) {
      reportType = ReportType::CompletionReport;
    } else if (jobQueueRow.status == schedulerdb::ArchiveJobStatus::AJS_ToReportToUserForFailure) {
      reportType = ReportType::FailureReport;
    } else {
      reportType = ReportType::NoReportRequired;
    }
  };

void ArchiveRdbJob::failTransfer(const std::string & failureReason, log::LogContext & lc) {

  std::string failureLog = cta::utils::getCurrentLocalTime() + " " + cta::utils::getShortHostname() +
                           " " + failureReason;
  if (m_jobRow.failureLogs.has_value()) {
    m_jobRow.failureLogs.value() += failureLog;
  } else {
    m_jobRow.failureLogs = failureLog;
  }
  lc.log(log::WARNING,
         "In schedulerdb::ArchiveRdbJob::failTransfer(): passes as half-dummy implementation !");
  log::ScopedParamContainer(lc)
          .add("jobID", jobID)
          .add("archiveFile.archiveFileID", archiveFile.archiveFileID)
          .add("mountId", m_mountId)
          .add("tapePool", m_tapePool)
          .add("failureReason", m_jobRow.failureLogs.value_or(""))
          .log(log::INFO,
               "In schedulerdb::ArchiveRdbJob::failTransfer(): received failed job to be reported.");

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
    if (m_jobRow.status != ArchiveJobStatus::AJS_ToTransferForUser) {
      // Wrong status, but the context leaves no ambiguity and we set the job to report failure.
      lc.log(log::WARNING,
             std::string("In ArchiveRdbJob::failTransfer(): unexpected status: ")
             + to_string(m_jobRow.status)
             + std::string("Assuming ToTransfer."));
    }
    try {
      m_jobRow.updateFailedJobStatus(txn, ArchiveJobStatus::AJS_ToReportToUserForFailure);
      txn.commit();
      reportType = ReportType::FailureReport;
    } catch (exception::Exception &ex) {
      lc.log(cta::log::WARNING,
             "In schedulerdb::ArchiveRdbJob::failTransfer(): failed to update job status for reporting a failure. Aborting the transaction." +
             ex.getMessageValue());
      txn.abort();
    }
  } else {
      // Decide if we want the job to have a chance to come back to this mount (requeue) or not.
      if (m_jobRow.retriesWithinMount >= m_jobRow.maxRetriesWithinMount){
        try {
          // requeue by changing status, reset the mount_id to NULL and updating all other stat fields
          m_jobRow.updateFailedJobStatus(txn, ArchiveJobStatus::AJS_ToTransferForUser, 0);
          txn.commit();
          // since requeueing, we do not report and we do not
          // set reportType to a particular value here
        } catch (exception::Exception &ex) {
          lc.log(cta::log::WARNING,
                 "In schedulerdb::ArchiveRdbJob::failTransfer(): failed to update job status to requeue the failed job on a new mount. Aborting the transaction." +
                 ex.getMessageValue());
          txn.abort();
        }
      } else {
        try {
          // requeue to the same mount simply by changing IN_DRIVE_QUEUE to False and updating all other stat fields
          m_jobRow.updateFailedJobStatus(txn, ArchiveJobStatus::AJS_ToTransferForUser);
          txn.commit();
          // since requeueing, we do not report and we do not
          // set reportType to a particular value here
        } catch (exception::Exception &ex) {
          lc.log(cta::log::WARNING,
                 "In schedulerdb::ArchiveRdbJob::failTransfer(): failed to update job status to requeue the failed job. Aborting the transaction." +
                 ex.getMessageValue());
          txn.abort();
        }
      }
  }
}

void ArchiveRdbJob::failReport(const std::string & failureReason, log::LogContext & lc)
{
  lc.log(log::WARNING,
         "In schedulerdb::ArchiveRdbJob::failReport(): passes as half-dummy implementation !");
  std::string reportFailureLog = cta::utils::getCurrentLocalTime() + " " + cta::utils::getShortHostname() +
                                 " " + failureReason;
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
          .log(log::INFO,
               "In schedulerdb::ArchiveRdbJob::failReport(): reporting failed.");

  // Don't re-queue the job if reportType is set to NoReportRequired. This can happen if a previous
  // attempt to report failed due to an exception, for example if the file was deleted on close.
  //cta::rdbms::Conn txn_conn = m_connPool.getConn();
  cta::schedulerdb::Transaction txn(m_connPool);
  try {
    if (reportType == ReportType::NoReportRequired || m_jobRow.totalReportRetries >= m_jobRow.maxReportRetries) {

      //m_jobRow.updateJobStatusForFailedReport(txn, ArchiveJobStatus::AJS_Failed);
      m_jobRow.updateJobStatusForFailedReport(txn, ArchiveJobStatus::ReadyForDeletion);
      // requeue job to failure table !
    } else {
      // Status is unchanged, but we reset the IS_REPORTING flag to FALSE
      m_jobRow.is_reporting = false;
      m_jobRow.updateJobStatusForFailedReport(txn, m_jobRow.status);
    }
    txn.commit();
  } catch (exception::Exception &ex) {
    lc.log(cta::log::WARNING,
           "In schedulerdb::ArchiveRdbJob::failReport(): failed to update job status for failed report case. Aborting the transaction." +
           ex.getMessageValue());
    txn.abort();
  }
  return;
}

void ArchiveRdbJob::bumpUpTapeFileCount(uint64_t newFileCount)
{
   throw cta::exception::Exception("Not implemented");
}

} // namespace cta::schedulerdb
