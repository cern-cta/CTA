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
    std::vector<std::string> noSpaceDiskSystemNames = m_RelationalDB.getActiveSleepDiskSystemNamesToFilter();
    auto [queuedJobs, nrows] = postgres::RetrieveJobQueueRow::moveJobsToDbQueue(txn,
                                                                                queriedJobStatus,
                                                                                mountInfo,
                                                                                noSpaceDiskSystemNames,
                                                                                bytesRequested,
                                                                                filesRequested);
    timings.insertAndReset("mountUpdateBatchTime", t);
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
      txn.commit();
      params.add("queuedJobCount", retVector.size());
      timings.insertAndReset("mountJobInitBatchTime", t);
      logContext.log(cta::log::INFO,
                     "In postgres::RetrieveJobQueueRow::moveJobsToDbQueue: successfully queued to the DB.");
    } else {
      logContext.log(cta::log::WARNING,
                     "In postgres::RetrieveJobQueueRow::moveJobsToDbQueue: no jobs queued.");
      txn.commit();
      return ret;
    }
  } catch (exception::Exception& ex) {
    params.add("exceptionMessage",  ex.getMessageValue());
    logContext.log(cta::log::ERR,
                   "In postgres::RetrieveJobQueueRow::moveJobsToDbQueue: failed to queue jobs. Aborting the transaction.");
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

bool RetrieveMount::reserveDiskSpace(const cta::DiskSpaceReservationRequest& diskSpaceReservationRequest,
                                     const std::string& externalFreeDiskSpaceScript,
                                     log::LogContext& logContext) {
  // Get the current file systems list from the catalogue
  cta::disk::DiskSystemList diskSystemList;
  diskSystemList = m_RelationalDB.m_catalogue.DiskSystem()->getAllDiskSystems();
  diskSystemList.setExternalFreeDiskSpaceScript(externalFreeDiskSpaceScript);
  cta::disk::DiskSystemFreeSpaceList diskSystemFreeSpace(diskSystemList);

  // Get the existing reservation map from drives.
  auto previousDrivesReservations = m_RelationalDB.m_catalogue.DriveState()->getDiskSpaceReservations();
  // Get the free space from disk systems involved.
  std::set<std::string> diskSystemNames;
  for (const auto& dsrr : diskSpaceReservationRequest) {
    diskSystemNames.insert(dsrr.first);
  }

  try {
    diskSystemFreeSpace.fetchDiskSystemFreeSpace(diskSystemNames, m_RelationalDB.m_catalogue, logContext);
  } catch (const cta::disk::DiskSystemFreeSpaceListException& ex) {
    // Could not get free space for one of the disk systems due to a script error.
    // The queue will not be put to sleep (backpressure will not be applied), and we return
    // true, because we want to allow staging files for retrieve in case of script errors.
    for (const auto& failedDiskSystem : ex.m_failedDiskSystems) {
      auto sleepTime = diskSystemFreeSpace.getDiskSystemList().at(failedDiskSystem.first).sleepTime;
      cta::log::ScopedParamContainer(logContext)
        .add("diskSystemName", failedDiskSystem.first)
        .add("failureReason", failedDiskSystem.second.getMessageValue())
        .add("sleepTime", sleepTime)
        .log(cta::log::ERR,
           "In RelationalDB::RetrieveMount::reserveDiskSpace(): unable to request EOS free space for "
           "disk system, putting queue to sleep");
      putQueueToSleep(failedDiskSystem.first, sleepTime, logContext);
    }
    return true;
  } catch (std::exception& ex) {
    // Leave a log message before letting the possible exception go up the stack.
    cta::log::ScopedParamContainer(logContext)
      .add("exceptionWhat", ex.what())
      .log(cta::log::ERR,
           "In RelationalDB::RetrieveMount::reserveDiskSpace(): got an exception from "
           "diskSystemFreeSpace.fetchDiskSystemFreeSpace().");
    throw;
  }

  // If a file system does not have enough space fail the disk space reservation,  put the queue to sleep and
  // the retrieve mount will immediately stop
  for (const auto& ds : diskSystemNames) {
    uint64_t previousDrivesReservationTotal = 0;
    auto diskSystem = diskSystemFreeSpace.getDiskSystemList().at(ds);
    // Compute previous drives reservation for the physical space of the current disk system.
    for (auto previousDriveReservation : previousDrivesReservations) {
      //avoid empty string when no disk space reservation exists for drive
      if (previousDriveReservation.second != 0) {
        auto previousDiskSystem = diskSystemFreeSpace.getDiskSystemList().at(previousDriveReservation.first);
        if (diskSystem.diskInstanceSpace.freeSpaceQueryURL == previousDiskSystem.diskInstanceSpace.freeSpaceQueryURL) {
          previousDrivesReservationTotal += previousDriveReservation.second;
        }
      }
    }
    if (diskSystemFreeSpace.at(ds).freeSpace < diskSpaceReservationRequest.at(ds) +
                                                 diskSystemFreeSpace.at(ds).targetedFreeSpace +
                                                 previousDrivesReservationTotal) {
      cta::log::ScopedParamContainer(logContext)
        .add("diskSystemName", ds)
        .add("freeSpace", diskSystemFreeSpace.at(ds).freeSpace)
        .add("existingReservations", previousDrivesReservationTotal)
        .add("spaceToReserve", diskSpaceReservationRequest.at(ds))
        .add("targetedFreeSpace", diskSystemFreeSpace.at(ds).targetedFreeSpace)
        .log(cta::log::WARNING,
             "In RelationalDB::RetrieveMount::reservediskSpace(): could not allocate disk space for job, "
             "applying backpressure");

      auto sleepTime = diskSystem.sleepTime;
      putQueueToSleep(ds, sleepTime, logContext);
      return false;
    }
  }

  m_RelationalDB.m_catalogue.DriveState()->reserveDiskSpace(mountInfo.drive,
                                                            mountInfo.mountId,
                                                            diskSpaceReservationRequest,
                                                            logContext);
  return true;
}

bool RetrieveMount::testReserveDiskSpace(const cta::DiskSpaceReservationRequest& diskSpaceReservationRequest,
                                         const std::string& externalFreeDiskSpaceScript,
                                         log::LogContext& logContext) {
  // if the problem is that the script is throwing errors (and not that the disk space is insufficient),
  // we will issue a warning, but otherwise we will not sleep the queues and we will act like no disk
  // system was present
  // Get the current file systems list from the catalogue
  cta::disk::DiskSystemList diskSystemList;
  diskSystemList = m_RelationalDB.m_catalogue.DiskSystem()->getAllDiskSystems();
  diskSystemList.setExternalFreeDiskSpaceScript(externalFreeDiskSpaceScript);
  cta::disk::DiskSystemFreeSpaceList diskSystemFreeSpace(diskSystemList);

  // Get the existing reservation map from drives.
  auto previousDrivesReservations = m_RelationalDB.m_catalogue.DriveState()->getDiskSpaceReservations();
  // Get the free space from disk systems involved.
  std::set<std::string> diskSystemNames;
  for (const auto& dsrr : diskSpaceReservationRequest) {
    diskSystemNames.insert(dsrr.first);
  }

  try {
    diskSystemFreeSpace.fetchDiskSystemFreeSpace(diskSystemNames, m_RelationalDB.m_catalogue, logContext);
  } catch (const cta::disk::DiskSystemFreeSpaceListException& ex) {
    // Could not get free space for one of the disk systems due to a script error.
    // The queue will not be put to sleep (backpressure will not be applied), and we return true
    // because we want to allow staging files for retrieve in case of script errors.
    for (const auto& failedDiskSystem : ex.m_failedDiskSystems) {
      auto sleepTime = diskSystemFreeSpace.getDiskSystemList().at(failedDiskSystem.first).sleepTime;
      cta::log::ScopedParamContainer(logContext)
        .add("diskSystemName", failedDiskSystem.first)
        .add("failureReason", failedDiskSystem.second.getMessageValue())
        .add("sleepTime", sleepTime)
        .log(cta::log::ERR,
             "In RelationalDB::RetrieveMount::testReserveDiskSpace(): unable to request EOS free space "
             "for disk system, putting queue to sleep");
    }
    return true;
  } catch (std::exception& ex) {
    // Leave a log message before letting the possible exception go up the stack.
    cta::log::ScopedParamContainer(logContext)
      .add("exceptionWhat", ex.what())
      .log(cta::log::ERR,
           "In RelationalDB::RetrieveMount::testReserveDiskSpace(): got an exception from "
           "diskSystemFreeSpace.fetchDiskSystemFreeSpace().");
    throw;
  }

  // If a file system does not have enough space fail the disk space reservation,  put the queue to sleep and
  // the retrieve mount will immediately stop
  for (const auto& ds : diskSystemNames) {
    uint64_t previousDrivesReservationTotal = 0;
    auto diskSystem = diskSystemFreeSpace.getDiskSystemList().at(ds);
    // Compute previous drives reservation for the physical space of the current disk system.
    for (auto previousDriveReservation : previousDrivesReservations) {
      //avoid empty string when no disk space reservation exists for drive
      if (previousDriveReservation.second != 0) {
        auto previousDiskSystem = diskSystemFreeSpace.getDiskSystemList().at(previousDriveReservation.first);
        if (diskSystem.diskInstanceSpace.freeSpaceQueryURL == previousDiskSystem.diskInstanceSpace.freeSpaceQueryURL) {
          previousDrivesReservationTotal += previousDriveReservation.second;
        }
      }
    }
    if (diskSystemFreeSpace.at(ds).freeSpace < diskSpaceReservationRequest.at(ds) +
                                                 diskSystemFreeSpace.at(ds).targetedFreeSpace +
                                                 previousDrivesReservationTotal) {
      cta::log::ScopedParamContainer(logContext)
        .add("diskSystemName", ds)
        .add("freeSpace", diskSystemFreeSpace.at(ds).freeSpace)
        .add("existingReservations", previousDrivesReservationTotal)
        .add("spaceToReserve", diskSpaceReservationRequest.at(ds))
        .add("targetedFreeSpace", diskSystemFreeSpace.at(ds).targetedFreeSpace)
        .log(cta::log::WARNING,
             "In RelationalDB::RetrieveMount::testReserveDiskSpace(): could not allocate disk space for job, "
             "applying backpressure");

      auto sleepTime = diskSystem.sleepTime;
      putQueueToSleep(ds, sleepTime, logContext);
      return false;
    }
  }
  return true;
}

uint64_t RetrieveMount::requeueJobBatch(const std::list<std::string>& jobIDsList,
                                        cta::log::LogContext& logContext) const {
  // here we will do the same as for ArchiveRdbJob::failTransfer but for bunch of jobs
  cta::schedulerdb::Transaction txn(m_connPool);
  uint64_t nrows = 0;
  try {
    nrows = postgres::RetrieveJobQueueRow::requeueJobBatch(txn, RetrieveJobStatus::RJS_ToTransfer, jobIDsList);
    if (nrows != jobIDsList.size()) {
      cta::log::ScopedParamContainer params(logContext);
      params.add("jobCountToRequeue", jobIDsList.size());
      params.add("jobCountRequeued", nrows);
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
  for (auto& job : jobBatch) {
    jobIDsList.push_back(std::to_string(job->jobID));
  }
  uint64_t njobs = RetrieveMount::requeueJobBatch(jobIDsList, logContext);
  if (njobs != jobIDsList.size()) {
    // handle the case of failed bunch update of the jobs !
    std::string jobIDsString;
    for (const auto& piece : jobIDsList) {
      jobIDsString += piece;
    }
    logContext.log(cta::log::ERR,
                   std::string("In RetrieveMount::requeueJobBatch(): Did not requeue all task jobs of "
                               "the queue for this there was no space on disk, job IDs attempting to update were: ") +
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

void RetrieveMount::flushAsyncSuccessReports(std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>& jobsBatch,
                                             log::LogContext& lc) {
  // this method will remove the rows of the jobs from the DB
  //if (isRepack) {
  // If the job is from a repack subrequest, we change its status (to report
  // for repack success). Queueing will be done in batch in
  // REPACK USE CASE TO-BE-DONE
  // }
  // we update/release the space reservation of the drive in the catalogue
  cta::DiskSpaceReservationRequest diskSpaceReservationRequest;
  std::vector<std::string> jobIDsList_success;
  jobIDsList_success.reserve(jobsBatch.size());
  for (auto& rdbJob : jobsBatch) {
    if (rdbJob->diskSystemName) {
      diskSpaceReservationRequest.addRequest(rdbJob->diskSystemName.value(), rdbJob->archiveFile.fileSize);
    }
    jobIDsList_success.emplace_back(std::to_string(rdbJob->jobID));
    // we collect the jobIDs to delete form the JOB table since they are done successfully
  }
  this->m_RelationalDB.m_catalogue.DriveState()->releaseDiskSpace(mountInfo.drive,
                                                                  mountInfo.mountId,
                                                                  diskSpaceReservationRequest,
                                                                  lc);
  cta::schedulerdb::Transaction txn(m_connPool);
  try {
    if (jobIDsList_success.size() > 0) {
      uint64_t nrows = schedulerdb::postgres::RetrieveJobQueueRow::updateJobStatus(
        txn,
        cta::schedulerdb::RetrieveJobStatus::ReadyForDeletion,
        jobIDsList_success);
      if (nrows != jobIDsList_success.size()) {
        log::ScopedParamContainer(lc)
          .add("updatedRows", nrows)
          .add("jobListSize", jobIDsList_success.size())
          .log(log::ERR,
               "In RetrieveMount::flushAsyncSuccessReports(): Failed to RetrieveJobQueueRow::updateJobStatus() - job "
               "deletion - "
               "for the full entire list of successful retrieve jobs. Aborting the transaction.");
        txn.abort();
      } else {
        txn.commit();
      }
    }
  } catch (exception::Exception& ex) {
    lc.log(cta::log::ERR,
           "In schedulerdb::RetrieveMount::flushAsyncSuccessReports(): Failed to delete jobs. "
           "Aborting the transaction." +
             ex.getMessageValue());
    txn.abort();
  }
  // After processing - we free the memory object
  // in case the flush and DB update failed, we still want to clean the jobs from memory
  // (they need to be garbage collected in case of a crash)
  try {
    for (auto& job : jobsBatch) {
      // Attempt to release the job back to the pool
      auto castedJob = std::unique_ptr<RetrieveRdbJob>(static_cast<RetrieveRdbJob*>(job.release()));
      m_jobPool.releaseJob(std::move(castedJob));
    }
    jobsBatch.clear();  // Clear the container after all jobs are successfully processed
  } catch (const exception::Exception& ex) {
    lc.log(cta::log::ERR,
           "In RetrieveMount::flushAsyncSuccessReports(): Failed to recycle all job objects for the job pool: " +
             ex.getMessageValue());

    // Destroy all remaining jobs in case of failure
    for (auto& job : jobsBatch) {
      // Release the unique_ptr ownership and delete the underlying object
      delete job.release();
    }
    jobsBatch.clear();  // Ensure the container is emptied
  }
}

void RetrieveMount::addDiskSystemToSkip(const DiskSystemToSkip& diskSystemToSkip) {
  // This method is actually not being used anywhere, not even in the OStoreDB code - we should remove it
  throw cta::exception::Exception("Not implemented");
}

void RetrieveMount::putQueueToSleep(const std::string& diskSystemName,
                                    const uint64_t sleepTime,
                                    log::LogContext& logContext) {
  if (!diskSystemName.empty()) {
    RelationalDB::DiskSleepEntry dse(sleepTime, time(nullptr));
    cta::threading::MutexLocker ml(m_RelationalDB.m_diskSystemSleepCacheMutex);
    m_RelationalDB.m_diskSystemSleepCacheMap[diskSystemName] = dse;
  }
}

}  // namespace cta::schedulerdb
