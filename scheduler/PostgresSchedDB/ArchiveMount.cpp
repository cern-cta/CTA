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
#include "scheduler/PostgresSchedDB/sql/ArchiveJobQueue.hpp"

namespace cta::postgresscheddb {

const SchedulerDatabase::ArchiveMount::MountInfo &ArchiveMount::getMountInfo()
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ArchiveMount::getNextJobBatch(uint64_t filesRequested,
      uint64_t bytesRequested, log::LogContext& logContext)
{

  rdbms::Rset resultSet;
  // make the txn connection commit and can any pending transactions
  auto& nonTxnConn = m_txn.getNonTxnConn();
  // retrieve batch up to file limit
  if(m_queueType == common::dataStructures::JobQueueType::JobsToTransferForUser) {
    resultSet = cta::postgresscheddb::sql::ArchiveJobQueueRow::select(
            nonTxnConn, ArchiveJobStatus::AJS_ToTransferForUser, mountInfo.tapePool, filesRequested);

  } else {
    resultSet = cta::postgresscheddb::sql::ArchiveJobQueueRow::select(
            nonTxnConn, ArchiveJobStatus::AJS_ToTransferForRepack, mountInfo.tapePool, filesRequested);
  }

  std::list<sql::ArchiveJobQueueRow> jobs;
  // filter retrieved batch up to size limit
  uint64_t totalBytes = 0;
  while(resultSet.next()) {
    jobs.emplace_back(resultSet);
    totalBytes += jobs.back().archiveFile.fileSize;
    if(totalBytes >= bytesRequested) break;
  }

  // mark the jobs in the batch as owned
  m_txn.start();
  sql::ArchiveJobQueueRow::updateMountId(m_txn, jobs, mountInfo.mountId);
  m_txn.commit();

  // Construct the return value
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ret;
  for (const auto &j : jobs) {
    auto aj = std::make_unique<postgresscheddb::ArchiveJob>(/* j.jobId */);
    aj->tapeFile.copyNb = j.copyNb;
    aj->archiveFile = j.archiveFile;
    aj->archiveReportURL = j.archiveReportUrl;
    aj->errorReportURL = j.archiveErrorReportUrl;
    aj->srcURL = j.srcUrl;
    aj->tapeFile.fSeq = ++nbFilesCurrentlyOnTape;
    aj->tapeFile.vid = mountInfo.vid;
    aj->tapeFile.blockId = std::numeric_limits<decltype(aj->tapeFile.blockId)>::max();
// m_jobOwned ?
    aj->m_mountId = mountInfo.mountId;
    aj->m_tapePool = mountInfo.tapePool;
// reportType ?
    ret.emplace_back(std::move(aj));
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
   throw cta::exception::Exception("Not implemented");
}

void ArchiveMount::setJobBatchTransferred(
      std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> & jobsBatch, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

} // namespace cta::postgresscheddb
