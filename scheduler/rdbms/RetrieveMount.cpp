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

#include "scheduler/rdbms/RetrieveMount.hpp"
#include "scheduler/rdbms/RetrieveJob.hpp"
#include "scheduler/rdbms/RetrieveRequest.hpp"
#include "common/exception/Exception.hpp"

namespace cta::schedulerdb {

const SchedulerDatabase::RetrieveMount::MountInfo &RetrieveMount::getMountInfo()
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> RetrieveMount::getNextJobBatch(uint64_t filesRequested,
     uint64_t bytesRequested, log::LogContext& logContext)
{

  rdbms::Rset resultSet;

  // retrieve batch up to file limit
  resultSet = cta::schedulerdb::postgres::RetrieveJobQueueRow::select(
    m_txn, RetrieveJobStatus::RJS_ToTransfer, mountInfo.vid, filesRequested);

  std::list<postgres::RetrieveJobQueueRow> jobs;
  // filter retrieved batch up to size limit
  uint64_t totalBytes = 0;
  while(resultSet.next()) {
    jobs.emplace_back(resultSet);
    totalBytes += jobs.back().archiveFile.fileSize;
    if(totalBytes >= bytesRequested) break;
  }

  // mark the jobs in the batch as owned
  postgres::RetrieveJobQueueRow::updateMountID(m_txn, jobs, mountInfo.mountId);
  m_txn.commit();

  // Construct the return value
  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> ret;
  for (const auto &j : jobs) {
    // each row represents an entire retreieverequest (including all jobs)
    // and also the indication of which is the current active one
    schedulerdb::RetrieveRequest rr(logContext, j);

    auto rj = std::make_unique<schedulerdb::RetrieveJob>(/* j.jobId */);
    rj->archiveFile = rr.m_archiveFile;
    rj->diskSystemName = rr.m_diskSystemName;
    rj->retrieveRequest = rr.m_schedRetrieveReq;
    rj->selectedCopyNb = rr.m_actCopyNb;
    rj->isRepack = rr.m_repackInfo.isRepack;
    rj->m_repackInfo = rr.m_repackInfo;
 //   rj->m_jobOwned = true;
    rj->m_mountId = mountInfo.mountId;
    ret.emplace_back(std::move(rj));
  }
  return ret;
}

bool RetrieveMount::reserveDiskSpace(const cta::DiskSpaceReservationRequest &request,
      const std::string &externalFreeDiskSpaceScript, log::LogContext& logContext)
{
   throw cta::exception::Exception("Not implemented");
}

bool RetrieveMount::testReserveDiskSpace(const cta::DiskSpaceReservationRequest &request,
      const std::string &externalFreeDiskSpaceScript, log::LogContext& logContext)
{
   throw cta::exception::Exception("Not implemented");
}

void RetrieveMount::requeueJobBatch(std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>& jobBatch,
      log::LogContext& logContext)
{
   throw cta::exception::Exception("Not implemented");
}

void RetrieveMount::setDriveStatus(common::dataStructures::DriveStatus status, common::dataStructures::MountType mountType,
                                time_t completionTime, const std::optional<std::string> & reason)
{
   throw cta::exception::Exception("Not implemented");
}

void RetrieveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats)
{
   throw cta::exception::Exception("Not implemented");
}

void RetrieveMount::flushAsyncSuccessReports(std::list<SchedulerDatabase::RetrieveJob *> & jobsBatch, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

void RetrieveMount::addDiskSystemToSkip(const DiskSystemToSkip &diskSystemToSkip)
{
   throw cta::exception::Exception("Not implemented");
}

void RetrieveMount::putQueueToSleep(const std::string &diskSystemName, const uint64_t sleepTime, log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

} // namespace cta::schedulerdb
