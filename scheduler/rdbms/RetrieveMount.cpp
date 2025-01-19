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
#include "common/exception/Exception.hpp"
#include "common/log/TimingList.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"
#include "common/Timer.hpp"

namespace cta::schedulerdb {

const SchedulerDatabase::RetrieveMount::MountInfo& RetrieveMount::getMountInfo() {
    return mountInfo;
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>
RetrieveMount::getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested, log::LogContext& logContext) {
  logContext.log(cta::log::DEBUG, "Entering RetrieveMount::getNextJobBatch()");
  RetrieveJobStatus queriedJobStatus = RetrieveJobStatus::RJS_ToTransfer;

  // start a new transaction
  cta::schedulerdb::Transaction txn(m_connPool);
  // require VID named lock in order to minimise tapePool fragmentation of the rows
  txn.takeNamedLock(mountInfo.vid);
  cta::log::TimingList timings;
  cta::utils::Timer t;
  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> ret;
  std::vector<std::unique_ptr<SchedulerDatabase::RetrieveJob>> retVector;
  try {
    auto [queuedJobs, nrows] = postgres::RetrieveJobQueueRow::moveJobsToDbQueue(txn,
                                                                                queriedJobStatus,
                                                                                mountInfo,
                                                                                bytesRequested,
                                                                                filesRequested);
    timings.insertAndReset("mountUpdateBatchTime", t);
    cta::log::ScopedParamContainer params(logContext);
    params.add("updateMountInfoRowCount", nrows);
    params.add("MountID", mountInfo.mountId);
    logContext.log(cta::log::INFO,
                   "In postgres::RetrieveJobQueueRow::moveJobsToDbQueue: successfully assigned Mount ID to DB jobs.");
    retVector.reserve(nrows);
    // Fetch job info only in case there were jobs found and updated
    if (!queuedJobs.isEmpty()) {
      // Construct the return value
      // Precompute the maximum value before the loop
      //common::dataStructures::TapeFile tpfile;
      //auto maxBlockId = std::numeric_limits<decltype(tpfile.blockId)>::max();
      while (queuedJobs.next()) {
        retVector.emplace_back(m_jobPool.acquireJob());
        retVector.back()->initialize(queuedJobs, logContext);
      }
      timings.insertAndReset("mountJobInitBatchTime", t);
      logContext.log(cta::log::INFO,
                     "In postgres::RetrieveJobQueueRow::moveJobsToDbQueue: successfully prepared queueing for " +
                       std::to_string(retVector.size()) + " jobs.");
    } else {
      logContext.log(cta::log::WARNING,
                     "In postgres::RetrieveJobQueueRow::moveJobsToDbQueue: no DB jobs queued for Mount ID: " +
                       std::to_string(mountInfo.mountId));
      return ret;
    }
    txn.commit();
  } catch (exception::Exception& ex) {
    logContext.log(cta::log::ERR,
                   "In postgres::RetrieveJobQueueRow::moveJobsToDbQueue: failed to queue jobs for given Mount ID. "
                   "Aborting the transaction." +
                     ex.getMessageValue());
    txn.abort();
    throw;
  }
  // Convert vector to list (which is expected as return type)
  ret.assign(std::make_move_iterator(retVector.begin()), std::make_move_iterator(retVector.end()));
  cta::log::ScopedParamContainer logParams(logContext);
  timings.insertAndReset("mountTransformBatchTime", t);
  timings.addToLog(logParams);
  logContext.log(cta::log::INFO, "In RetrieveMount::getNextJobBatch(): Finished fetching new jobs for execution.");
  return ret;
}

bool RetrieveMount::reserveDiskSpace(const cta::DiskSpaceReservationRequest& request,
                                     const std::string& externalFreeDiskSpaceScript,
                                     log::LogContext& logContext) {
  throw cta::exception::Exception("Not implemented");
}

bool RetrieveMount::testReserveDiskSpace(const cta::DiskSpaceReservationRequest& request,
                                         const std::string& externalFreeDiskSpaceScript,
                                         log::LogContext& logContext) {
  throw cta::exception::Exception("Not implemented");
}


uint64_t RetrieveMount::requeueJobBatch(const std::list<std::string>& jobIDsList,
                                       cta::log::LogContext& logContext) const {
  // here we will do the same as for ArchiveRdbJob::failTransfer but for bunch of jobs
  cta::schedulerdb::Transaction txn(m_connPool);
  uint64_t nrows = 0;
  try {
    nrows =
      postgres::RetrieveJobQueueRow::requeueJobBatch(txn, RetrieveJobStatus::RJS_ToTransfer, jobIDsList);
    if (nrows != jobIDsList.size()){
      cta::log::ScopedParamContainer params(logContext);
      params.add("jobCountToRequeue", jobIDsList.size());
      params.add("jobCountRequeued", nrows);
      logContext.log(cta::log::ERR,
                     "In schedulerdb::RetrieveMount::requeueJobBatch(): failed to requeue all jobs !");
    }
    txn.commit();
  } catch (exception::Exception& ex) {
    logContext.log(cta::log::ERR,
                   "In schedulerdb::RetrieveMount::requeueJobBatch(): failed to update job status for failed task queue." +
                     ex.getMessageValue());
    txn.abort();
    return 0;
  }
  return nrows;
}


void RetrieveMount::setDriveStatus(common::dataStructures::DriveStatus status,
                                   common::dataStructures::MountType mountType,
                                   time_t completionTime,
                                   const std::optional<std::string>& reason) {
  throw cta::exception::Exception("Not implemented");
}

void RetrieveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats& stats) {
  throw cta::exception::Exception("Not implemented");
}

void RetrieveMount::flushAsyncSuccessReports(std::list<SchedulerDatabase::RetrieveJob*>& jobsBatch,
                                             log::LogContext& lc) {
  throw cta::exception::Exception("Not implemented");
}

void RetrieveMount::addDiskSystemToSkip(const DiskSystemToSkip& diskSystemToSkip) {
  throw cta::exception::Exception("Not implemented");
}

void RetrieveMount::putQueueToSleep(const std::string& diskSystemName,
                                    const uint64_t sleepTime,
                                    log::LogContext& logContext) {
  throw cta::exception::Exception("Not implemented");
}

}  // namespace cta::schedulerdb
