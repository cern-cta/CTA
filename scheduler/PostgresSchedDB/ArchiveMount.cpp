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

  // retrieve batch up to file limit
  if(m_queueType == common::dataStructures::JobQueueType::JobsToTransferForUser) {
    resultSet = cta::postgresscheddb::sql::ArchiveJobQueueRow::select(
      m_txn, ArchiveJobStatus::AJS_ToTransferForUser, mountInfo.tapePool, filesRequested);

  } else {
    resultSet = cta::postgresscheddb::sql::ArchiveJobQueueRow::select(
      m_txn, ArchiveJobStatus::AJS_ToTransferForRepack, mountInfo.tapePool, filesRequested);
  }

  std::list<sql::ArchiveJobQueueRow> jobs;
  // filter retrieved batch up to size limit
  uint64_t totalBytes = 0;
  while(resultSet.next()) {
    jobs.emplace_back(sql::ArchiveJobQueueRow(resultSet));
    totalBytes += jobs.back().archiveFile.fileSize;
    if(totalBytes >= bytesRequested) break;
  }

  // mark the jobs in the batch as owned
  sql::ArchiveJobQueueRow::updateMountId(m_txn, jobs, mountInfo.mountId);
  m_txn.commit();

  // Construct the return value
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ret;
  for (auto &j : jobs) {
    std::unique_ptr<postgresscheddb::ArchiveJob> aj(new postgresscheddb::ArchiveJob(/* j.jobId */));
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
   throw cta::exception::Exception("Not implemented");
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
