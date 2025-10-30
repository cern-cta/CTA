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

#include "catalogue/Catalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/TimingList.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"
#include "common/Timer.hpp"
#include "scheduler/rdbms/RetrieveMount.hpp"
#include "common/threading/MutexLocker.hpp"

namespace cta::schedulerdb {

const SchedulerDatabase::RetrieveMount::MountInfo& RetrieveMount::getMountInfo() {
  return mountInfo;
}

  void RetrieveMount::setIsRepack(std::string_view defaultRepackVO, log::LogContext &logContext) {
    m_isRepack = false;
    if (defaultRepackVO.empty()) {
      logContext.log(cta::log::WARNING,
                     "In RetrieveMount::setIsRepack(): no default repack VO found, no repack jobs will get picked up !");
    }
    if (mountInfo.vo == defaultRepackVO) {
      logContext.log(cta::log::INFO,
                     "In RetrieveMount::setIsRepack(): Marked RetrieveMount for repack.");
      m_isRepack = true;
    }
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
  cta::log::ScopedParamContainer params(logContext);
  try {
    auto noSpaceDiskSystemNamesMap = m_RelationalDB.getActiveSleepDiskSystemNamesToFilter(logContext);
    std::vector<std::string> noSpaceDiskSystemNames;
    noSpaceDiskSystemNames.reserve(noSpaceDiskSystemNamesMap.size());
    for (const auto &pair : noSpaceDiskSystemNamesMap) {
        noSpaceDiskSystemNames.push_back(pair.first);
    }
    auto [queuedJobs, nrows] = postgres::RetrieveJobQueueRow::moveJobsToDbActiveQueue(txn,
                                                                                      queriedJobStatus,
                                                                                      mountInfo,
                                                                                      noSpaceDiskSystemNames,
                                                                                      bytesRequested,
                                                                                      filesRequested,
                                                                                      m_isRepack);
    timings.insertAndReset("mountUpdateBatchTime", t);
    params.add("updateMountInfoRowCount", nrows);
    params.add("MountID", mountInfo.mountId);
    logContext.log(
      cta::log::INFO,
      "In postgres::RetrieveJobQueueRow::moveJobsToDbActiveQueue: successfully assigned Mount ID to DB jobs.");
    retVector.reserve(nrows);
    // Fetch job info only in case there were jobs found and updated
    if (!queuedJobs.isEmpty()) {
      // Construct the return value
      // Precompute the maximum value before the loop
      //common::dataStructures::TapeFile tpfile;
      //auto maxBlockId = std::numeric_limits<decltype(tpfile.blockId)>::max();
      while (queuedJobs.next()) {
        auto job = m_jobPool->acquireJob();
        if (!job) {
          throw exception::Exception("In RetrieveMount::getNextJobBatch(): Failed to acquire job from pool.");
        }
        retVector.emplace_back(std::move(job));
        retVector.back()->initialize(queuedJobs, m_isRepack);
      }
      txn.commit();
      params.add("queuedJobCount", retVector.size());
      timings.insertAndReset("mountJobInitBatchTime", t);
      logContext.log(cta::log::INFO,
                     "In postgres::RetrieveJobQueueRow::moveJobsToDbActiveQueue: successfully queued to the DB.");
    } else {
      logContext.log(cta::log::WARNING, "In postgres::RetrieveJobQueueRow::moveJobsToDbActiveQueue: no jobs queued.");
      txn.commit();
      return ret;
    }
  } catch (exception::Exception& ex) {
    params.add("exceptionMessage", ex.getMessageValue());
    logContext.log(
      cta::log::ERR,
      "In postgres::RetrieveJobQueueRow::moveJobsToDbActiveQueue: failed to queue jobs. Aborting the transaction.");
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

uint64_t RetrieveMount::requeueJobBatch(const std::list<std::string>& jobIDsList,
                                        cta::log::LogContext& logContext) const {
  // here we will do ALMOST the same as for RetrieveRdbJob::failTransfer for a bunch of jobs,
  // but it will not update the statistics on the number of retries as `failTransfer` does!
  cta::schedulerdb::Transaction txn(m_connPool);
  uint64_t nrows = 0;
  try {
    nrows = postgres::RetrieveJobQueueRow::requeueJobBatch(txn, RetrieveJobStatus::RJS_ToTransfer, jobIDsList, m_isRepack);
    if (nrows != jobIDsList.size()) {
      cta::log::ScopedParamContainer params(logContext);
      params.add("jobsToRequeue", jobIDsList.size());
      params.add("jobsRequeued", nrows);
      logContext.log(cta::log::ERR, "In schedulerdb::RetrieveMount::requeueJobBatch(): failed to requeue all jobs !");
    }
    txn.commit();
  } catch (exception::Exception& ex) {
    logContext.log(
      cta::log::ERR,
      "In schedulerdb::RetrieveMount::requeueJobBatch(): failed to update job status for failed task queue." +
        ex.getMessageValue());
    txn.abort();
    return 0;
  }
  return nrows;
}

void RetrieveMount::requeueJobBatch(std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>& jobBatch,
                                    cta::log::LogContext& logContext) {
  std::list<std::string> jobIDsList;
  for (const auto& job : jobBatch) {
    jobIDsList.push_back(std::to_string(job->jobID));
  }
  uint64_t njobs = RetrieveMount::requeueJobBatch(jobIDsList, logContext);
  if (njobs != jobIDsList.size()) {
    // handle the case of failed bunch update of the jobs !
    std::string jobIDsString;
    for (const auto& piece : jobIDsList) {
      jobIDsString += piece + ",";
    }
    if (!jobIDsString.empty()) {
      jobIDsString.pop_back();
    }
    logContext.log(cta::log::ERR,
                   std::string("In RetrieveMount::requeueJobBatch(): Did not requeue all task jobs of "
                               "the queue, there was no space on disk, job IDs attempting to update were: ") +
                     jobIDsString);
  }
}

void RetrieveMount::setDriveStatus(common::dataStructures::DriveStatus status,
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
  inputs.activity = mountInfo.activity;
  // TODO: statistics!
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  log::LogContext lc(m_RelationalDB.m_logger);
  m_RelationalDB.m_tapeDrivesState->updateDriveStatus(driveInfo, inputs, lc);
}

void RetrieveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats& stats) {
  // We just report tthe tape session statistics as instructed by the tape thread.
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

void RetrieveMount::updateRetrieveJobStatusWrapper(const std::vector<std::string>& jobIDs,
                                                   cta::schedulerdb::RetrieveJobStatus newStatus,
                                                   log::LogContext& lc) {
  cta::schedulerdb::Transaction txn(m_connPool);
  try {
    cta::utils::Timer t;

    if (!jobIDs.empty()) {
      uint64_t nrows = schedulerdb::postgres::RetrieveJobQueueRow::updateJobStatus(txn, newStatus, jobIDs, m_isRepack);

      if (nrows != jobIDs.size()) {
        log::ScopedParamContainer(lc)
          .add("updatedRows", nrows)
          .add("jobListSize", jobIDs.size())
          .add("targetStatus", to_string(newStatus))
          .log(log::ERR,
               "In RetrieveMount::updateRetrieveJobStatusWrapper(): Failed to update all jobs to target status. "
               "Aborting transaction.");
        txn.abort();
        return;
      }

      txn.commit();
      log::ScopedParamContainer(lc)
        .add("rowUpdateCount", nrows)
        .add("rowUpdateTime", t.secs())
        .add("targetStatus", to_string(newStatus))
        .log(log::INFO, "In RetrieveMount::updateRetrieveJobStatusWrapper(): updated job statuses.");
    }
  } catch (const exception::Exception& ex) {
    lc.log(cta::log::ERR,
           "In RetrieveMount::updateRetrieveJobStatusWrapper(): Exception while updating job status. Aborting "
           "transaction. " +
             ex.getMessageValue());
    txn.abort();
  }
}

void RetrieveMount::setJobBatchTransferred(std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>& jobsBatch,
                                           log::LogContext& lc) {
  std::vector<std::string> jobIDs_success, jobIDs_repackSuccess, jobIDs_reportToUser;
  // This method will remove the rows of the jobs from the DB or update jobs to get reported to the disk buffer
  // REPACK USE CASE TO-BE-DONE
  // we update/release the space reservation of the drive in the catalogue
  cta::DiskSpaceReservationRequest diskSpaceReservationRequest;
  jobIDs_success.reserve(jobsBatch.size());
  for (const auto& rdbJob : jobsBatch) {
    if (rdbJob->diskSystemName) {
      diskSpaceReservationRequest.addRequest(rdbJob->diskSystemName.value(), rdbJob->archiveFile.fileSize);
    }
    // Once we implement Repack we can compare mountInfo.vo with the defaultRepack VO of the scheduler
    if (m_isRepack) {
      // If the job is from a repack subrequest, we change its status (to report
      // for repack success).
      jobIDs_repackSuccess.push_back(std::to_string(rdbJob->jobID));
    } else if (rdbJob->retrieveRequest.retrieveReportURL.empty()) {
      // Set the user transfer request as successful (delete it).
      jobIDs_success.push_back(std::to_string(rdbJob->jobID));
    } else {
      // else we change its status (to report for transfer success).
      jobIDs_reportToUser.push_back(std::to_string(rdbJob->jobID));
    }
  }
  this->m_RelationalDB.m_catalogue.DriveState()->releaseDiskSpace(mountInfo.drive,
                                                                  mountInfo.mountId,
                                                                  diskSpaceReservationRequest,
                                                                  lc);
  if (!jobIDs_success.empty()) {
    updateRetrieveJobStatusWrapper(jobIDs_success, RetrieveJobStatus::ReadyForDeletion, lc);
  }
  if (!jobIDs_repackSuccess.empty()) {
    updateRetrieveJobStatusWrapper(jobIDs_repackSuccess, RetrieveJobStatus::RJS_ToReportToRepackForSuccess, lc);
  }
  if (!jobIDs_reportToUser.empty()) {
    updateRetrieveJobStatusWrapper(jobIDs_reportToUser, RetrieveJobStatus::RJS_ToReportToUserForSuccess, lc);
  }
  // After processing - we free the memory object
  // in case the flush and DB update failed, we still want to clean the jobs from memory
  // (they need to be garbage collected in case of a crash)
  recycleTransferredJobs(jobsBatch, lc);
}

void RetrieveMount::addDiskSystemToSkip(const DiskSystemToSkip& diskSystemToSkip) {
  // This method is actually not being used anywhere, not even in the OStoreDB code - we should remove it
  throw cta::exception::Exception("Not implemented");
}

void RetrieveMount::putQueueToSleep(const std::string &diskSystemName,
                                    const uint64_t sleepTime,
                                    log::LogContext &logContext) {
  if (!diskSystemName.empty()) {
    RelationalDB::DiskSleepEntry dse(sleepTime, time(nullptr));
    cta::threading::MutexLocker ml(m_RelationalDB.m_diskSystemSleepMutex);
    //m_RelationalDB.m_diskSystemSleepCacheMap[diskSystemName] = dse;
    cta::schedulerdb::Transaction txn(m_connPool);
    try {
      m_RelationalDB.insertOrUpdateDiskSleepEntry(txn, diskSystemName, dse);
      txn.commit();

    } catch (const exception::Exception &ex) {
      logContext.log(cta::log::ERR,
             "In RetrieveMount::putQueueToSleep(): Exception while updating job status. Aborting "
             "transaction. " +
             ex.getMessageValue());
      txn.abort();
    }
  }
}

void RetrieveMount::recycleTransferredJobs(std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>& jobsBatch,
                                           log::LogContext& lc) {
  try {
    for (auto& job : jobsBatch) {
      if (job->releaseToPool()) {
        /* Prevent deletion via unique_ptr - correct handling here would be
           to introduce custom deleter for the unique_ptr
           (would make recycleTransferredJobs obsolete),
           but this would mean changing types all across the CTA code */
        job.release();
      } else {
        // Let unique_ptr delete it
      }
    }
  } catch (const exception::Exception& ex) {
    lc.log(cta::log::ERR,
           "In RetrieveMount::recycleTransferredJobs(): Failed to recycle all job objects for the job pool: " +
             ex.getMessageValue());
  }
  jobsBatch.clear();
}
}  // namespace cta::schedulerdb
