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

#include "scheduler/rdbms/ArchiveMount.hpp"
#include "scheduler/rdbms/ArchiveJob.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/rdbms/postgres/ArchiveJobQueue.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"
#include "catalogue/TapeDrivesCatalogueState.hpp"

#include <unordered_map>

namespace cta::schedulerdb {

const SchedulerDatabase::ArchiveMount::MountInfo &ArchiveMount::getMountInfo()
{
  return mountInfo;
}

std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ArchiveMount::getNextJobBatch(uint64_t filesRequested,
      uint64_t bytesRequested, log::LogContext& logContext) {
  logContext.log(cta::log::DEBUG, "Entering ArchiveMount::getNextJobBatch()");
  rdbms::Rset updatedJobIDset;
  using queueType = common::dataStructures::JobQueueType;
  ArchiveJobStatus queriedJobStatus = (m_queueType == queueType::JobsToTransferForUser) ?
                                      ArchiveJobStatus::AJS_ToTransferForUser :
                                      ArchiveJobStatus::AJS_ToTransferForRepack;
  // mark the next job batch as owned by a specific mountId
  // and return the list of JOB_IDs which we have modified
  std::list <std::string> jobIDsList;
  std::string jobIDsString;
  // start a new transaction
  cta::schedulerdb::Transaction txn(m_RelationalDB.m_connPool);
  try {
    logContext.log(cta::log::DEBUG,
                   "In postgres::ArchiveJobQueueRow::updateMountInfo: attempting to update Mount ID and VID for a batch of jobs.");
    updatedJobIDset = postgres::ArchiveJobQueueRow::updateMountInfo(txn, queriedJobStatus, mountInfo.tapePool,
                                                                  mountInfo.mountId, mountInfo.vid, filesRequested);
    // we need to extract the JOB_IDs which were updated before we release the lock
    while (updatedJobIDset.next()) {
      jobIDsList.emplace_back(std::to_string(updatedJobIDset.columnUint64("JOB_ID")));
    }
    txn.commit();
    for (const auto &piece: jobIDsList) jobIDsString += piece;
    logContext.log(cta::log::DEBUG,
                   "Successfully finished to update Mount ID: " + std::to_string(mountInfo.mountId) + " for JOB IDs: " +
                   jobIDsString);
  } catch (exception::Exception &ex) {
    logContext.log(cta::log::DEBUG,
                   "In postgres::ArchiveJobQueueRow::updateMountInfo: failed to update Mount ID. Aborting the transaction." +
                   ex.getMessageValue());
    txn.abort();
  }
  std::list <std::unique_ptr<SchedulerDatabase::ArchiveJob>> ret;
  // Fetch job info only in case there were jobs found and updated
  if (!jobIDsList.empty()) {
    // fetch a non transactional connection from the PGSCHED connection pool
    auto conn = m_RelationalDB.m_connPool.getConn();
    rdbms::Rset resultSet;
    // retrieve more job information about the updated batch
    logContext.log(cta::log::DEBUG, "Query for job IDs " + jobIDsString + " ArchiveMount::getNextJobBatch()");
    resultSet = cta::schedulerdb::postgres::ArchiveJobQueueRow::selectJobsByJobID(conn, jobIDsList);
    logContext.log(cta::log::DEBUG, "Job info of the updated jobs has been queueried, passing it on for execution");
    std::list <postgres::ArchiveJobQueueRow> jobs;
    // Construct the return value
    uint64_t totalBytes = 0;
    while (resultSet.next()) {
      jobs.emplace_back(resultSet);
      jobs.back().jobId = resultSet.columnUint64("JOB_ID");
      totalBytes += jobs.back().archiveFile.fileSize;
      auto aj = std::make_unique<schedulerdb::ArchiveJob>(true, mountInfo.mountId, jobs.back().jobId, mountInfo.tapePool);
      aj->jobID = jobs.back().jobId;
      aj->tapeFile.copyNb = jobs.back().copyNb;
      aj->archiveFile = jobs.back().archiveFile;
      aj->archiveReportURL = jobs.back().archiveReportUrl;
      aj->errorReportURL = jobs.back().archiveErrorReportUrl;
      aj->srcURL = jobs.back().srcUrl;
      aj->tapeFile.fSeq = ++nbFilesCurrentlyOnTape;
      aj->tapeFile.vid = mountInfo.vid;
      aj->tapeFile.blockId = std::numeric_limits<decltype(aj->tapeFile.blockId)>::max();
      // reportType ?
      ret.emplace_back(std::move(aj));
      if (totalBytes >= bytesRequested) break;
    }
    // returning connection to the pool
    conn.commit();
  }
  logContext.log(cta::log::DEBUG, "Returning result of ArchiveMount::getNextJobBatch()");
  return ret;
}

void ArchiveMount::setDriveStatus(common::dataStructures::DriveStatus status, common::dataStructures::MountType mountType,
                                time_t completionTime, const std::optional<std::string>& reason)
{
  // We just report the drive status as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = mountInfo.drive;
  driveInfo.logicalLibrary = mountInfo.logicalLibrary;
  driveInfo.host = mountInfo.host;
  ReportDriveStatusInputs inputs;
  inputs.mountType = mountType;
  inputs.mountSessionId = mountInfo.mountId;
  inputs.reportTime = completionTime;
  inputs.status = status;
  inputs.vid = mountInfo.vid;
  inputs.tapepool = mountInfo.tapePool;
  inputs.vo = mountInfo.vo;
  inputs.reason = reason;
  // TODO: statistics!
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  log::LogContext lc(m_RelationalDB.m_logger);
  m_RelationalDB.m_tapeDrivesState->updateDriveStatus(driveInfo, inputs, lc);
}

void ArchiveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats)
{
  // We just report the tape session statistics as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = mountInfo.drive;
  driveInfo.logicalLibrary = mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;

  ReportDriveStatsInputs inputs;
  inputs.reportTime = time(nullptr);
  inputs.bytesTransferred = stats.dataVolume;
  inputs.filesTransferred = stats.filesCount;

  log::LogContext lc(m_RelationalDB.m_logger);
  m_RelationalDB.m_tapeDrivesState->updateDriveStatistics(driveInfo, inputs, lc);
}

void ArchiveMount::setJobBatchTransferred(
      std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> & jobsBatch, log::LogContext & lc)
{
  lc.log(log::WARNING,
         "In schedulerdb::ArchiveMount::setJobBatchTransferred(): passes as half-dummy implementation !");
  std::list<std::string> jobIDsList;
  auto jobsBatchItor = jobsBatch.begin();
  while (jobsBatchItor != jobsBatch.end()) {
    jobIDsList.emplace_back(std::to_string((*jobsBatchItor)->jobID));
    log::ScopedParamContainer(lc)
            .add("jobID", (*jobsBatchItor)->jobID)
            .add("tapeVid", (*jobsBatchItor)->tapeFile.vid)
            .add("archiveFileID", (*jobsBatchItor)->archiveFile.archiveFileID)
            .add("diskInstance", (*jobsBatchItor)->archiveFile.diskInstance)
            .log(log::INFO,
                 "In schedulerdb::ArchiveMount::setJobBatchTransferred(): received a job to be reported.");
    jobsBatchItor++;
  }
  /* Update Status in ARCHIVE_JOB_QUEUE and table to either of the following 2 states:
   * AJS_ToReportToUserForFailure
   * AJS_ToReportToUserForTransfer
   */
  cta::schedulerdb::Transaction txn(m_RelationalDB.m_connPool);
  try {
    // To be checked if all jobs for which setJobBatchTransferred is called can be reported as SUCCESS !
    postgres::ArchiveJobQueueRow::updateJobStatus(txn, ArchiveJobStatus::AJS_ToReportToUserForTransfer, jobIDsList);
    txn.commit();
  } catch (exception::Exception &ex) {
    lc.log(cta::log::DEBUG,
                   "In schedulerdb::ArchiveMount::setJobBatchTransferred(): failed to update job status. Aborting the transaction." +
                   ex.getMessageValue());
    txn.abort();
  }
}
} // namespace cta::schedulerdb
