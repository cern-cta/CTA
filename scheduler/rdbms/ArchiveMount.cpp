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
  rdbms::Rset updatedJobIDset;
  using queueType = common::dataStructures::JobQueueType;
  ArchiveJobStatus queriedJobStatus = (m_queueType == queueType::JobsToTransferForUser) ?
                                        ArchiveJobStatus::AJS_ToTransferForUser :
                                        ArchiveJobStatus::AJS_ToTransferForRepack;
  // mark the next job batch as owned by a specific mountId
  // and return the list of JOB_IDs which we have modified
  std::list<std::string> jobIDsList;
  // start a new transaction
  cta::schedulerdb::Transaction txn(m_connPool);
  // require tapePool named lock in order to minimise tapePool fragmentation of the rows
  txn.takeNamedLock(mountInfo.tapePool);
  cta::log::TimingList timings;
  cta::utils::Timer t;
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ret;
  std::vector<std::unique_ptr<SchedulerDatabase::ArchiveJob>> retVector;
  try {
    updatedJobIDset =
      postgres::ArchiveJobQueueRow::updateMountInfo(txn, queriedJobStatus, mountInfo, bytesRequested, filesRequested);
    timings.insertAndReset("mountUpdateBatchTime", t);
    while (updatedJobIDset.next()) {
      jobIDsList.emplace_back(std::to_string(updatedJobIDset.columnUint64("JOB_ID")));
    }
    txn.commit();
    logContext.log(cta::log::INFO,
                   "Successfully assigned in DB Mount ID: " + std::to_string(mountInfo.mountId) + " to " +
                     std::to_string(jobIDsList.size()) + " jobs.");
    retVector.reserve(jobIDsList.size());
    // Fetch job info only in case there were jobs found and updated
    if (!jobIDsList.empty()) {
      rdbms::Rset resultSet;
      // retrieve more job information about the updated batch
      auto& selconn = txn.getConn();
      try {
        resultSet = cta::schedulerdb::postgres::ArchiveJobQueueRow::selectJobsByJobID(selconn, jobIDsList);
        timings.insertAndReset("mountFetchBatchTime", t);
        // Construct the return value
        // Precompute the maximum value before the loop
        common::dataStructures::TapeFile tpfile;
        auto maxBlockId = std::numeric_limits<decltype(tpfile.blockId)>::max();
        while (resultSet.next()) {
          auto job = m_jobPool.acquireJob();
          job->initialize(resultSet, logContext);
          retVector.emplace_back(std::move(job));
          auto& tapeFile = retVector.back()->tapeFile;
          tapeFile.fSeq = ++nbFilesCurrentlyOnTape;
          tapeFile.blockId = maxBlockId;
        }
        logContext.log(cta::log::INFO,
                       "Successfully prepared queueing for " + std::to_string(retVector.size()) + " jobs.");
      } catch (exception::Exception& ex) {
        // we will roll back the previous update operation by calling ArchiveJobQueueRow::updateFailedTaskQueueJobStatus
        logContext.log(cta::log::ERR,
                       "In postgres::ArchiveJobQueueRow::updateMountInfo: failed to select jobs after update: " +
                         ex.getMessageValue());
        try {
          txn.start();
          auto nrows = cta::schedulerdb::postgres::ArchiveJobQueueRow::updateFailedTaskQueueJobStatus(
            txn,
            ArchiveJobStatus::AJS_ToTransferForUser,
            jobIDsList);
          txn.commit();
          if (nrows != !jobIDsList.size()) {
            logContext.log(cta::log::ERR,
                           "In postgres::ArchiveJobQueueRow::updateMountInfo failed, reverting by "
                           "updateFailedTaskQueueJobStatus failed as well ! ");
          } else {
            logContext.log(cta::log::INFO,
                           "Successfully reverted back the previous DB update for Mount ID: " +
                             std::to_string(mountInfo.mountId) + " for " + std::to_string(nrows) + " jobs.");
          }
        } catch (exception::Exception& ex) {
          logContext.log(cta::log::ERR,
                         "In postgres::ArchiveJobQueueRow::updateMountInfo failed, reverting by "
                         "updateFailedTaskQueueJobStatus failed as well !  " +
                           ex.getMessageValue());
          txn.abort();
        }
        return ret;
      }
    }
  } catch (exception::Exception& ex) {
    logContext.log(
      cta::log::ERR,
      "In postgres::ArchiveJobQueueRow::updateMountInfo: failed to update Mount ID. Aborting the transaction." +
        ex.getMessageValue());
    txn.abort();
    // returning empty list
    return ret;
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
    nrows = postgres::ArchiveJobQueueRow::updateFailedTaskQueueJobStatus(txn,
                                                                         ArchiveJobStatus::AJS_ToTransferForUser,
                                                                         jobIDsList);
    txn.commit();
  } catch (exception::Exception& ex) {
    logContext.log(cta::log::ERR,
                   "In schedulerdb::ArchiveMount::failJobBatch(): failed to update job status for failed task queue." +
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
    for (auto& job : jobsBatch) {
      // check we can downcast (runtime check)
      if (dynamic_cast<ArchiveRdbJob*>(job.get())) {
        // Downcast to ArchiveRdbJob before returning it to the pool
        std::unique_ptr<ArchiveRdbJob> castedJob(static_cast<ArchiveRdbJob*>(job.release()));
        // Return the casted job to the pool
        m_jobPool.releaseJob(std::move(castedJob));
      } else {
        lc.log(cta::log::ERR,
               "In schedulerdb::ArchiveMount::setJobBatchTransferred(): Failed to cast ArchiveJob to "
               "ArchiveRdbJob and return the object to the pool for reuse.");
      }
    }
    jobsBatch.clear();
  } catch (exception::Exception& ex) {
    lc.log(cta::log::ERR,
           "In schedulerdb::ArchiveMount::setJobBatchTransferred(): Failed to update job status for "
           "reporting. Aborting the transaction." +
             ex.getMessageValue());
    txn.abort();
  }
}
}  // namespace cta::schedulerdb
