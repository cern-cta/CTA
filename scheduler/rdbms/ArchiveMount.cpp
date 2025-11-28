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

void ArchiveMount::setIsRepack(log::LogContext &logContext) {
  if (mountInfo.mountType == common::dataStructures::MountType::ArchiveForRepack) {
    m_isRepack = true;
    logContext.log(cta::log::INFO,
                   "In ArchiveMount::setIsRepack(): Marked ArchiveMount for repack.");
  }
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
  // using vector instead of list, since we can preallocate the size,
  // better cache locality for search/loop, faster insertion (less pointer manipulations)
  std::vector<std::unique_ptr<SchedulerDatabase::ArchiveJob>> retVector;
  cta::log::ScopedParamContainer params(logContext);
  try {
    auto [queuedJobs, nrows] = postgres::ArchiveJobQueueRow::moveJobsToDbActiveQueue(txn,
                                                                                     queriedJobStatus,
                                                                                     mountInfo,
                                                                                     bytesRequested,
                                                                                     filesRequested, m_isRepack);
    timings.insertAndReset("mountUpdateBatchTime", t);
    params.add("updateMountInfoRowCount", nrows);
    params.add("MountID", mountInfo.mountId);
    retVector.reserve(nrows);
    // Fetch job info only in case there were jobs found and updated
    if (!queuedJobs.isEmpty()) {
      // Construct the return value
      // Precompute the maximum value before the loop
      common::dataStructures::TapeFile tpfile;
      auto maxBlockId = std::numeric_limits<decltype(tpfile.blockId)>::max();
      while (queuedJobs.next()) {
        auto job = m_jobPool->acquireJob();
        if (!job) {
          throw exception::Exception("In ArchiveMount::getNextJobBatch(): Failed to acquire job from pool.");
        }
        retVector.emplace_back(std::move(job));
        retVector.back()->initialize(queuedJobs, m_isRepack);
        auto& tapeFile = retVector.back()->tapeFile;
        tapeFile.fSeq = ++nbFilesCurrentlyOnTape;
        tapeFile.blockId = maxBlockId;
      }
      txn.commit();
      params.add("queuedJobCount", retVector.size());
      timings.insertAndReset("mountJobInitBatchTime", t);
      logContext.log(cta::log::INFO,
                     "In postgres::ArchiveJobQueueRow::moveJobsToDbActiveQueue: successfully queued to the DB.");
    } else {
      logContext.log(cta::log::WARNING, "In postgres::ArchiveJobQueueRow::moveJobsToDbActiveQueue: no jobs queued.");
      txn.commit();
      return ret;
    }
  } catch (exception::Exception& ex) {
    params.add("exceptionMessage", ex.getMessageValue());
    logContext.log(cta::log::ERR,
                   "In postgres::ArchiveJobQueueRow::moveJobsToDbActiveQueue: failed to queue jobs." +
                     ex.getMessageValue());
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
  inputs.reportTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  inputs.bytesTransferred = stats.dataVolume;
  inputs.filesTransferred = stats.filesCount;

  log::LogContext lc(m_RelationalDB.m_logger);
  m_RelationalDB.m_tapeDrivesState->updateDriveStatistics(driveInfo, inputs, lc);
}

uint64_t ArchiveMount::requeueJobBatch(const std::list<std::string>& jobIDsList,
                                       cta::log::LogContext& logContext) const {
  // here we will do ALMOST the same as for ArchiveRdbJob::failTransfer for a bunch of jobs,
  // but it will not update the statistics on the number of retries as `failTransfer` does!
  cta::schedulerdb::Transaction txn(m_connPool);
  uint64_t nrows = 0;
  auto status = m_isRepack ? ArchiveJobStatus::AJS_ToTransferForRepack : ArchiveJobStatus::AJS_ToTransferForUser;

  try {
    nrows = postgres::ArchiveJobQueueRow::requeueJobBatch(txn, status, m_isRepack, jobIDsList);
    if (nrows != jobIDsList.size()) {
      cta::log::ScopedParamContainer params(logContext);
      params.add("jobsToRequeue", jobIDsList.size());
      params.add("jobsRequeued", nrows);
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
           "In schedulerdb::ArchiveMount::setJobBatchTransferred(): received a job to send to report queue.");
    jobsBatchItor++;
  }
  // For Repack workflow: We know that as soon as the recall of a file is done,
  // the row will get moved to the archive pending table and any additional
  // copies needed will be recorded as a separate row in this table as well.
  // This means that as soon as there is a request for one successful job to be updated/deleted
  // the second on will have its state in the table already as well
  // we need to check in the query if any rows with the same archive ID exist.
  // if they do AND if they do not have already the status AJS_ToReportToRepackForSuccess
  // we will update only the status of the job passed, if all the copies are in state of AJS_ToReportToRepackForSuccess,
  // we will return them all back for deletion from disk and eventually from the queue.
  cta::schedulerdb::Transaction txn(m_connPool);
  try {
    // all jobs for which setJobBatchTransferred is called shall be reported as successful
    uint64_t nrows = 0;
    auto status = m_isRepack ? ArchiveJobStatus::AJS_ToReportToRepackForSuccess : ArchiveJobStatus::AJS_ToReportToUserForSuccess;
    nrows = postgres::ArchiveJobQueueRow::updateJobStatus(txn, status, m_isRepack, jobIDsList);
    if (!m_isRepack && nrows != jobIDsList.size()) {
      log::ScopedParamContainer(lc)
              .add("updatedRows", nrows)
              .add("jobListSize", jobIDsList.size())
              .log(log::ERR,
                   "In ArchiveMount::setJobBatchTransferred(): Failed to ArchiveJobQueueRow::updateJobStatus() for "
                   "entire job list provided.");
    }
    txn.commit();
    // After processing, return the job object to the job pool for re-use
    recycleTransferredJobs(jobsBatch, lc);
  } catch (exception::Exception& ex) {
    lc.log(cta::log::ERR,
           "In schedulerdb::ArchiveMount::setJobBatchTransferred(): Failed to update job status for "
           "reporting. Aborting the transaction." +
             ex.getMessageValue());
    txn.abort();
    return;
  }
}

void ArchiveMount::recycleTransferredJobs(std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>& jobsBatch,
                                          log::LogContext& lc) {
  try {
    for (auto& job : jobsBatch) {
      if (job->releaseToPool()) {
        // Prevent deletion via unique_ptr - correct handling here would be
        // to introduce custom deleter for the unique_ptr (would make recycleTransferredJobs obsolete),
        // but this would mean changing types all across the CTA code
        job.release();
      } else {
        // Let unique_ptr delete it
      }
    }
  } catch (const exception::Exception& ex) {
    lc.log(cta::log::ERR,
           "In ArchiveMount::recycleTransferredJobs(): Failed to recycle all job objects for the job pool: " +
             ex.getMessageValue());
  }
  jobsBatch.clear();
}
}  // namespace cta::schedulerdb
