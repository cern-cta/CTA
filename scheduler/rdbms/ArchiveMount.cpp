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
#include "common/exception/Exception.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/log/TimingList.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/rdbms/postgres/ArchiveJobQueue.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"
#include "catalogue/TapeDrivesCatalogueState.hpp"
#include "common/Timer.hpp"
#include "common/dataStructures/TapeFile.hpp"

#include <unordered_map>

namespace cta::schedulerdb {

const SchedulerDatabase::ArchiveMount::MountInfo& ArchiveMount::getMountInfo() {
  return mountInfo;
}

std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>
ArchiveMount::getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested, log::LogContext& logContext) {
  logContext.log(cta::log::DEBUG, "Entering ArchiveMount::getNextJobBatch()");
  using queueType = common::dataStructures::JobQueueType;
  ArchiveJobStatus queriedJobStatus = (m_queueType == queueType::JobsToTransferForUser) ?
                                        ArchiveJobStatus::AJS_ToTransferForUser :
                                        ArchiveJobStatus::AJS_ToTransferForRepack;
  // start a new transaction
  cta::schedulerdb::Transaction txn(m_connPool);
  // require tapePool named lock in order to minimise tapePool fragmentation of the rows
  txn.takeNamedLock(mountInfo.tapePool);
  cta::log::TimingList timings;
  cta::utils::Timer t;
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ret;
  std::vector<std::unique_ptr<SchedulerDatabase::ArchiveJob>> retVector;
  cta::log::ScopedParamContainer params(logContext);
  try {
    auto [queuedJobs, nrows] =
      postgres::ArchiveJobQueueRow::moveJobsToDbQueue(txn, queriedJobStatus, mountInfo, bytesRequested, filesRequested);
    timings.insertAndReset("mountUpdateBatchTime", t);
    params.add("updateMountInfoRowCount", nrows);
    params.add("MountID", mountInfo.mountId);
    //logContext.log(cta::log::INFO,
    //               "In postgres::ArchiveJobQueueRow::moveJobsToDbQueue: successfully assigned Mount ID to DB jobs.");
    retVector.reserve(nrows);
    // Fetch job info only in case there were jobs found and updated
    if (!queuedJobs.isEmpty()) {
      // Construct the return value
      // Precompute the maximum value before the loop
      common::dataStructures::TapeFile tpfile;
      auto maxBlockId = std::numeric_limits<decltype(tpfile.blockId)>::max();
      while (queuedJobs.next()) {
        retVector.emplace_back(m_jobPool.acquireJob());
        retVector.back()->initialize(queuedJobs);
        auto& tapeFile = retVector.back()->tapeFile;
        tapeFile.fSeq = ++nbFilesCurrentlyOnTape;
        tapeFile.blockId = maxBlockId;
      }
      txn.commit();
      params.add("queuedJobCount", retVector.size());
      timings.insertAndReset("mountJobInitBatchTime", t);
      logContext.log(cta::log::INFO,
                     "In postgres::ArchiveJobQueueRow::moveJobsToDbQueue: successfully queued to the DB.");
    } else {
      logContext.log(cta::log::WARNING, "In postgres::ArchiveJobQueueRow::moveJobsToDbQueue: no jobs queued.");
      txn.commit();
      return ret;
    }
  } catch (exception::Exception& ex) {
    params.add("exceptionMessage", ex.getMessageValue());
    logContext.log(cta::log::ERR,
                   "In postgres::ArchiveJobQueueRow::moveJobsToDbQueue: failed to queue jobs." + ex.getMessageValue());
    txn.abort();
    throw;
  }
  // Convert vector to list (which is expected as return type)
  ret.assign(std::make_move_iterator(retVector.begin()), std::make_move_iterator(retVector.end()));
  cta::log::ScopedParamContainer logParams(logContext);
  timings.insertAndReset("mountTransformBatchTime", t);
  timings.addToLog(logParams);
  logContext.log(cta::log::INFO, "In ArchiveMount::getNextJobBatch(): Finished fetching new jobs for execution.");
  return ret;
}

void ArchiveMount::setDriveStatus(common::dataStructures::DriveStatus status,
                                  common::dataStructures::MountType mountType,
                                  time_t completionTime,
                                  const std::optional<std::string>& reason) {
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

void ArchiveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats& stats) {
  // We just report the tape session statistics as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = mountInfo.drive;
  driveInfo.logicalLibrary = mountInfo.logicalLibrary;
  driveInfo.host = mountInfo.host;

  ReportDriveStatsInputs inputs;
  inputs.reportTime = time(nullptr);
  inputs.bytesTransferred = stats.dataVolume;
  inputs.filesTransferred = stats.filesCount;

  log::LogContext lc(m_RelationalDB.m_logger);
  m_RelationalDB.m_tapeDrivesState->updateDriveStatistics(driveInfo, inputs, lc);
}

uint64_t ArchiveMount::requeueJobBatch(const std::list<std::string>& jobIDsList,
                                       cta::log::LogContext& logContext) const {
  // here we will do the same as for ArchiveRdbJob::failTransfer but for bunch of jobs
  cta::schedulerdb::Transaction txn(m_connPool);
  uint64_t nrows = 0;
  try {
    nrows = postgres::ArchiveJobQueueRow::requeueJobBatch(txn, ArchiveJobStatus::AJS_ToTransferForUser, jobIDsList);
    if (nrows != jobIDsList.size()) {
      cta::log::ScopedParamContainer params(logContext);
      params.add("jobCountToRequeue", jobIDsList.size());
      params.add("jobCountRequeued", nrows);
      logContext.log(cta::log::ERR, "In schedulerdb::ArchiveMount::requeueJobBatch(): failed to requeue all jobs !");
    }
    txn.commit();
  } catch (exception::Exception& ex) {
    logContext.log(
      cta::log::ERR,
      "In schedulerdb::ArchiveMount::requeueJobBatch(): failed to update job status for failed task queue." +
        ex.getMessageValue());
    txn.abort();
    return 0;
  }
  return nrows;
}

void ArchiveMount::setJobBatchTransferred(std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>& jobsBatch,
                                          log::LogContext& lc) {
  lc.log(log::WARNING,
         "In schedulerdb::ArchiveMount::setJobBatchTransferred(): passes as half-dummy implementation "
         "valid only for AJS_ToReportToUserForTransfer !");
  std::vector<std::string> jobIDsList;
  jobIDsList.reserve(jobsBatch.size());
  auto jobsBatchItor = jobsBatch.begin();
  while (jobsBatchItor != jobsBatch.end()) {
    jobIDsList.emplace_back(std::to_string((*jobsBatchItor)->jobID));
    log::ScopedParamContainer(lc)
      .add("jobID", (*jobsBatchItor)->jobID)
      .add("tapeVid", (*jobsBatchItor)->tapeFile.vid)
      .add("archiveFileID", (*jobsBatchItor)->archiveFile.archiveFileID)
      .add("diskInstance", (*jobsBatchItor)->archiveFile.diskInstance)
      .log(log::INFO,
           "In schedulerdb::ArchiveMount::setJobBatchTransferred(): received a job to sent to report queue.");
    jobsBatchItor++;
  }
  cta::schedulerdb::Transaction txn(m_connPool);
  try {
    // all jobs for which setJobBatchTransferred is called shall be reported as successful
    uint64_t nrows =
      postgres::ArchiveJobQueueRow::updateJobStatus(txn, ArchiveJobStatus::AJS_ToReportToUserForTransfer, jobIDsList);
    txn.commit();
    if (nrows != jobIDsList.size()) {
      log::ScopedParamContainer(lc)
        .add("updatedRows", nrows)
        .add("jobListSize", jobIDsList.size())
        .log(log::ERR,
             "In ArchiveMount::setJobBatchTransferred(): Failed to ArchiveJobQueueRow::updateJobStatus() for "
             "entire job list provided.");
    }
    // After processing, return the job object to the job pool for re-use
    try {
      for (auto& job : jobsBatch) {
        // Attempt to release the job back to the pool
        auto castedJob = std::unique_ptr<ArchiveRdbJob>(static_cast<ArchiveRdbJob*>(job.release()));
        m_jobPool.releaseJob(std::move(castedJob));
      }
      jobsBatch.clear();  // Clear the container after all jobs are successfully processed
    } catch (const exception::Exception& ex) {
      lc.log(cta::log::ERR,
             "In ArchiveMount::setJobBatchTransferred(): Failed to recycle all job objects for the job pool: " +
               ex.getMessageValue());

      // Destroy all remaining jobs in case of failure
      for (auto& job : jobsBatch) {
        // Release the unique_ptr ownership and delete the underlying object
        delete job.release();
      }
      jobsBatch.clear();  // Ensure the container is emptied
    }
  } catch (exception::Exception& ex) {
    lc.log(cta::log::ERR,
           "In schedulerdb::ArchiveMount::setJobBatchTransferred(): Failed to update job status for "
           "reporting. Aborting the transaction." +
             ex.getMessageValue());
    txn.abort();
  }
}
}  // namespace cta::schedulerdb
