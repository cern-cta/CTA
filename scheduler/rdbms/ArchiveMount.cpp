/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/ArchiveMount.hpp"

#include "catalogue/TapeDrivesCatalogueState.hpp"
#include "common/Timer.hpp"
#include "common/dataStructures/TapeFile.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/log/TimingList.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/rdbms/postgres/ArchiveJobQueue.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"

#include <unordered_map>

namespace cta::schedulerdb {

const SchedulerDatabase::ArchiveMount::MountInfo& ArchiveMount::getMountInfo() {
  return mountInfo;
}

void ArchiveMount::setIsRepack(log::LogContext& lc) {
  if (mountInfo.mountType == common::dataStructures::MountType::ArchiveForRepack) {
    m_isRepack = true;
    lc.log(cta::log::INFO, "In ArchiveMount::setIsRepack(): Marked ArchiveMount for repack.");
  }
}

std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>
ArchiveMount::getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested, log::LogContext& lc) {
  lc.log(cta::log::DEBUG, "Entering ArchiveMount::getNextJobBatch()");
  using queueType = common::dataStructures::JobQueueType;
  ArchiveJobStatus queriedJobStatus = (m_queueType == queueType::JobsToTransferForUser) ?
                                        ArchiveJobStatus::AJS_ToTransferForUser :
                                        ArchiveJobStatus::AJS_ToTransferForRepack;
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ret;
  // start a new transaction
  cta::schedulerdb::Transaction txn(m_connPool, lc);
  // require tapePool named lock in order to minimise tapePool fragmentation of the rows
  txn.takeNamedLock(mountInfo.tapePool);
  cta::log::TimingList timings;
  cta::utils::Timer t;
  // using vector instead of list, since we can preallocate the size,
  // better cache locality for search/loop, faster insertion (less pointer manipulations)
  std::vector<std::unique_ptr<SchedulerDatabase::ArchiveJob>> retVector;
  cta::log::ScopedParamContainer params(lc);
  try {
    auto [queuedJobs, nrows] = postgres::ArchiveJobQueueRow::moveJobsToDbActiveQueue(txn,
                                                                                     queriedJobStatus,
                                                                                     mountInfo,
                                                                                     bytesRequested,
                                                                                     filesRequested,
                                                                                     m_isRepack);
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
      lc.log(cta::log::INFO,
             "In postgres::ArchiveJobQueueRow::moveJobsToDbActiveQueue: successfully queued to the DB.");
    } else {
      lc.log(cta::log::WARNING, "In postgres::ArchiveJobQueueRow::moveJobsToDbActiveQueue: no jobs queued.");
      txn.commit();
      return ret;
    }
  } catch (exception::Exception& ex) {
    params.add("exceptionMessage", ex.getMessageValue());
    lc.log(cta::log::ERR, "In postgres::ArchiveJobQueueRow::moveJobsToDbActiveQueue: failed to queue jobs.");
    txn.abort();
    throw;
  }
  // Convert vector to list (which is expected as return type)
  ret.assign(std::make_move_iterator(retVector.begin()), std::make_move_iterator(retVector.end()));
  cta::log::ScopedParamContainer logParams(lc);
  timings.insertAndReset("mountTransformBatchTime", t);
  timings.addToLog(logParams);
  lc.log(cta::log::INFO, "In ArchiveMount::getNextJobBatch(): Finished fetching new jobs for execution.");
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

uint64_t ArchiveMount::requeueJobBatch(const std::list<std::string>& jobIDsList, cta::log::LogContext& lc) const {
  // here we will do ALMOST the same as for ArchiveRdbJob::failTransfer for a bunch of jobs,
  // but it will not update the statistics on the number of retries as `failTransfer` does!
  cta::schedulerdb::Transaction txn(m_connPool, lc);
  uint64_t nrows = 0;
  auto status = m_isRepack ? ArchiveJobStatus::AJS_ToTransferForRepack : ArchiveJobStatus::AJS_ToTransferForUser;

  try {
    nrows = postgres::ArchiveJobQueueRow::requeueJobBatch(txn, status, jobIDsList, m_isRepack);
    if (nrows != jobIDsList.size()) {
      cta::log::ScopedParamContainer params(lc);
      params.add("jobsToRequeue", jobIDsList.size());
      params.add("jobsRequeued", nrows);
      lc.log(cta::log::ERR, "In schedulerdb::ArchiveMount::requeueJobBatch(): failed to requeue all jobs !");
    }
    txn.commit();
  } catch (exception::Exception& ex) {
    cta::log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(cta::log::ERR,
           "In schedulerdb::ArchiveMount::requeueJobBatch(): failed to update job status for failed task queue.");
    txn.abort();
    return 0;
  }
  return nrows;
}

void ArchiveMount::setJobBatchTransferred(std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>& jobsBatch,
                                          log::LogContext& lc) {
  if (m_isRepack) {
    // For Repack workflow: We know that as soon as the recall of a file is done,
    // the row will get moved to the archive pending table and any additional
    // copies needed will be recorded as a separate row in this table as well.
    // This means that as soon as there is a request for one successful job to be updated/deleted
    // the second on will have its state in the table already as well
    // we need to check in the query if any rows with the same archive ID exist.
    // if they do AND if they do not have already the status AJS_ToReportToRepackForSuccess
    // we will update only the status of the job passed, if all the copies are in state of AJS_ToReportToRepackForSuccess,
    // we will return them all back for deletion from disk and eventually from the queue.
    setRepackJobBatchTransferred(jobsBatch, lc);
    return;
  }
  std::vector<std::string> jobIDsList_single_copy;
  std::vector<std::string> jobIDsList_multi_copy;
  jobIDsList_single_copy.reserve(jobsBatch.size());
  jobIDsList_multi_copy.reserve(jobsBatch.size());
  auto jobsBatchItor = jobsBatch.begin();
  while (jobsBatchItor != jobsBatch.end()) {
    const auto& job = *jobsBatchItor;
    if (job->requestJobCount == 1) {
      jobIDsList_single_copy.emplace_back(std::to_string(job->jobID));
    } else {
      jobIDsList_multi_copy.emplace_back(std::to_string(job->jobID));
    }
    log::ScopedParamContainer(lc)
      .add("jobID", job->jobID)
      .add("tapeVid", job->tapeFile.vid)
      .add("archiveFileID", job->archiveFile.archiveFileID)
      .add("diskInstance", job->archiveFile.diskInstance)
      .add("archiveRequestId", job->archiveRequestId)
      .add("requestJobCount", job->requestJobCount)
      .log(log::INFO,
           "In schedulerdb::ArchiveMount::setJobBatchTransferred(): received a job to send to report queue.");
    jobsBatchItor++;
  }
  cta::schedulerdb::Transaction txn(m_connPool, lc);
  try {
    // all jobs for which setJobBatchTransferred is called shall be reported as successful
    auto status = ArchiveJobStatus::AJS_ToReportToUserForSuccess;
    if (!jobIDsList_single_copy.empty()) {
      uint64_t nrows = 0;
      nrows = postgres::ArchiveJobQueueRow::updateJobStatus(txn, status, jobIDsList_single_copy);
      if (nrows != jobIDsList_single_copy.size()) {
        log::ScopedParamContainer(lc)
          .add("updatedRows", nrows)
          .add("jobListSize", jobIDsList_single_copy.size())
          .log(log::ERR,
               "In ArchiveMount::setJobBatchTransferred(): Failed to ArchiveJobQueueRow::updateJobStatus() for "
               "entire job list provided.");
      }
    }
    if (!jobIDsList_multi_copy.empty()) {
      uint64_t nrows = 0;
      nrows = postgres::ArchiveJobQueueRow::updateMultiCopyJobSuccess(txn, jobIDsList_multi_copy);
      log::ScopedParamContainer(lc)
        .add("updatedRows", nrows)
        .add("jobListSize", jobIDsList_multi_copy.size())
        .log(log::INFO,
             "In ArchiveMount::setJobBatchTransferred(): Finished ArchiveJobQueueRow::updateJobStatus() for "
             "the job list provided.");
    }
    txn.commit();
    // After processing, return the job object to the job pool for re-use
    recycleTransferredJobs(jobsBatch, lc);
  } catch (exception::Exception& ex) {
    txn.abort();
    throw exception::Exception(
      "In schedulerdb::ArchiveMount::setJobBatchTransferred(): Failed to update job status for "
      "reporting. Aborting the transaction."
      + ex.getMessageValue());
  }
}

void ArchiveMount::setRepackJobBatchTransferred(std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>& jobsBatch,
                                                log::LogContext& lc) {
  if (!m_isRepack) {
    throw exception::Exception("In ArchiveMount::setRepackJobBatchTransferred(): This method must not be used for "
                               "Archive mounts which are not repack.");
  }
  std::vector<std::string> jobIDsList;
  auto jobsBatchItor = jobsBatch.begin();
  while (jobsBatchItor != jobsBatch.end()) {
    jobIDsList.emplace_back(std::to_string((*jobsBatchItor)->jobID));
    log::ScopedParamContainer(lc)
      .add("jobID", (*jobsBatchItor)->jobID)
      .add("tapeVid", (*jobsBatchItor)->tapeFile.vid)
      .add("archiveFileID", (*jobsBatchItor)->archiveFile.archiveFileID)
      .add("diskInstance", (*jobsBatchItor)->archiveFile.diskInstance)
      .add("archiveRequestId", (*jobsBatchItor)->archiveRequestId)
      .add("requestJobCount", (*jobsBatchItor)->requestJobCount)
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
  cta::schedulerdb::Transaction txn(m_connPool, lc);
  try {
    // all jobs for which setRepackJobBatchTransferred is called shall be reported as successful
    uint64_t nrows = 0;
    if (!jobIDsList.empty()) {
      nrows = postgres::ArchiveJobQueueRow::updateRepackJobSuccess(txn, jobIDsList);
    }
    log::ScopedParamContainer(lc)
      .add("updatedRows", nrows)
      .add("jobListSize", jobIDsList.size())
      .log(log::INFO, "In ArchiveMount::updateRepackJobSuccess(): Finished DB report for job list provided.");
    txn.commit();
    // After processing, return the job object to the job pool for re-use
    recycleTransferredJobs(jobsBatch, lc);
  } catch (exception::Exception& ex) {
    txn.abort();
    throw exception::Exception(
      "In schedulerdb::ArchiveMount::updateRepackJobSuccess(): Failed to update job status for "
      "reporting. Aborting the transaction."
      + ex.getMessageValue());
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
    cta::log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(cta::log::ERR,
           "In ArchiveMount::recycleTransferredJobs(): Failed to recycle all job objects for the job pool: ");
  }
  jobsBatch.clear();
}
}  // namespace cta::schedulerdb
