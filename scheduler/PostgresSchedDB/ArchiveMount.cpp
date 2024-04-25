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

#include "scheduler/PostgresSchedDB/ArchiveMount.hpp"
#include "scheduler/PostgresSchedDB/ArchiveJob.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/PostgresSchedDB/sql/ArchiveJobQueue.hpp"
#include "scheduler/PostgresSchedDB/sql/Transaction.hpp"
#include "catalogue/TapeDrivesCatalogueState.hpp"

#include <unordered_map>

namespace cta::postgresscheddb {

const SchedulerDatabase::ArchiveMount::MountInfo &ArchiveMount::getMountInfo()
{
  return mountInfo;
}

std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ArchiveMount::getNextJobBatch(uint64_t filesRequested,
      uint64_t bytesRequested, log::LogContext& logContext)
{
  logContext.log(cta::log::DEBUG, "Entering ArchiveMount::getNextJobBatch()");
  rdbms::Rset resultSet;
  // fetch a non transactional connection from the PGSCHED connection pool
  auto conn = m_PostgresSchedDB.m_connPool.getConn();
  // retrieve batch up to file limit
  if(m_queueType == common::dataStructures::JobQueueType::JobsToTransferForUser) {
    logContext.log(cta::log::DEBUG, "Query JobsToTransferForUser ArchiveMount::getNextJobBatch()");
    resultSet = cta::postgresscheddb::sql::ArchiveJobQueueRow::select(
            conn, ArchiveJobStatus::AJS_ToTransferForUser, mountInfo.tapePool, filesRequested);
    logContext.log(cta::log::DEBUG, "After first selection of AJS_ToTransferForUser ArchiveMount::getNextJobBatch()");
  } else {
    logContext.log(cta::log::DEBUG, "Query JobsToTransferForRepack ArchiveMount::getNextJobBatch()");
    resultSet = cta::postgresscheddb::sql::ArchiveJobQueueRow::select(
            conn, ArchiveJobStatus::AJS_ToTransferForRepack, mountInfo.tapePool, filesRequested);
  }
  logContext.log(cta::log::DEBUG, "Filling jobs in ArchiveMount::getNextJobBatch()");
  std::list<sql::ArchiveJobQueueRow> jobs;
  std::string jobIDsString;
  std::list<std::string> jobIDsList;
  // filter retrieved batch up to size limit
  uint64_t totalBytes = 0;
  while(resultSet.next()) {
    uint64_t assigned_jobid =  resultSet.columnUint64("JOB_ID");
    jobs.emplace_back(resultSet);
    jobs.back().jobId = assigned_jobid;
    jobIDsList.emplace_back(assigned_jobid);
    jobIDsString += std::to_string(assigned_jobid) + ",";
    logContext.log(cta::log::DEBUG, "jobIDsString: " + jobIDsString);
    totalBytes += jobs.back().archiveFile.fileSize;
    if(totalBytes >= bytesRequested) break;
  }
  logContext.log(cta::log::DEBUG, "Ended filling jobs in ArchiveMount::getNextJobBatch() executing ArchiveJobQueueRow::updateMountID");

  // Construct the return value
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ret;

  if (!jobIDsString.empty()) {
    jobIDsString.pop_back(); // Remove the trailing comma
    // mark the jobs in the batch as owned
    try {
      cta::postgresscheddb::Transaction txn(m_PostgresSchedDB.m_connPool);
      txn.lockGlobal(0);
      sql::ArchiveJobQueueRow::updateMountID(txn, jobIDsList, mountInfo.mountId);
      txn.commit();
      logContext.log(cta::log::DEBUG, "Finished to update Mount ID to: " + std::to_string(mountInfo.mountId)+ " for JOB IDs: " + jobIDsString);
      for (const auto &j : jobs) {
        auto aj = std::make_unique<postgresscheddb::ArchiveJob>(true, mountInfo.mountId, j.jobId, mountInfo.tapePool);
        aj->tapeFile.copyNb = j.copyNb;
        aj->archiveFile = j.archiveFile;
        aj->archiveReportURL = j.archiveReportUrl;
        aj->errorReportURL = j.archiveErrorReportUrl;
        aj->srcURL = j.srcUrl;
        aj->tapeFile.fSeq = ++nbFilesCurrentlyOnTape;
        aj->tapeFile.vid = mountInfo.vid;
        aj->tapeFile.blockId = std::numeric_limits<decltype(aj->tapeFile.blockId)>::max();
        // reportType ?
        ret.emplace_back(std::move(aj));
      }
    } catch (exception::Exception& ex) {
      logContext.log(cta::log::DEBUG, "In sql::ArchiveJobQueueRow::updateMountID: failed to update Mount ID." + ex.getMessageValue());
    }
    logContext.log(cta::log::DEBUG, "Finished updating Mount ID for the selected jobs  ArchiveJobQueueRow::updateMountID" + jobIDsString);
  }
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
  log::LogContext lc(m_PostgresSchedDB.m_logger);
  m_PostgresSchedDB.m_tapeDrivesState->updateDriveStatus(driveInfo, inputs, lc);
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

  log::LogContext lc(m_PostgresSchedDB.m_logger);
  m_PostgresSchedDB.m_tapeDrivesState->updateDriveStatistics(driveInfo, inputs, lc);
}

void ArchiveMount::setJobBatchTransferred(
      std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> & jobsBatch, log::LogContext & lc)
{
  lc.log(log::WARNING,
         "In postgresscheddb::ArchiveMount::setJobBatchTransferred(): set ArchiveRequests passes as dummy implementation !");
}

} // namespace cta::postgresscheddb
