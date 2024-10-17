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

const SchedulerDatabase::ArchiveMount::MountInfo &ArchiveMount::getMountInfo()
{
  return mountInfo;
}

std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ArchiveMount::getNextJobBatch(uint64_t filesRequested,
      uint64_t bytesRequested, log::LogContext& logContext) {
  rdbms::Rset updatedJobIDset;
  using queueType = common::dataStructures::JobQueueType;
  ArchiveJobStatus queriedJobStatus = (m_queueType == queueType::JobsToTransferForUser) ?
                                      ArchiveJobStatus::AJS_ToTransferForUser :
                                      ArchiveJobStatus::AJS_ToTransferForRepack;
  // mark the next job batch as owned by a specific mountId
  // and return the list of JOB_IDs which we have modified
  std::list <std::string> jobIDsList;
  //std::string jobIDsString;
  // start a new transaction
  cta::schedulerdb::Transaction txn(m_connPool);
  // require tapePool named lock in order to minimise tapePool fragmentation of the rows
  txn.takeNamedLock(mountInfo.tapePool);
  cta::log::TimingList timings;
  cta::utils::Timer t;
  try {
    updatedJobIDset = postgres::ArchiveJobQueueRow::updateMountInfo(txn, queriedJobStatus, mountInfo, filesRequested);
    timings.insertAndReset("mountUpdateBatchTime", t);
    //logContext.log(cta::log::DEBUG,
    //               "In postgres::ArchiveJobQueueRow::updateMountInfo: attempting to update Mount ID and VID for a batch of jobs.");
    // we need to extract the JOB_IDs which were updated before we release the lock
    while (updatedJobIDset.next()) {
      jobIDsList.emplace_back(std::to_string(updatedJobIDset.columnUint64("JOB_ID")));
    }
    txn.commit();
    //for (const auto &piece: jobIDsList) jobIDsString += piece;
    //logContext.log(cta::log::DEBUG,
    //               "Successfully finished to update Mount ID: " + std::to_string(mountInfo.mountId) + " for JOB IDs: " +
    //               jobIDsString);
  } catch (exception::Exception &ex) {
    //logContext.log(cta::log::DEBUG,
    //               "In postgres::ArchiveJobQueueRow::updateMountInfo: failed to update Mount ID. Aborting the transaction." +
    //               ex.getMessageValue());
    txn.abort();
  }
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ret;
  std::vector <std::unique_ptr<SchedulerDatabase::ArchiveJob>> retVector;
  retVector.reserve(jobIDsList.size());
  // Fetch job info only in case there were jobs found and updated
  if (!jobIDsList.empty()) {
    rdbms::Rset resultSet;
    // retrieve more job information about the updated batch
    auto selconn = m_connPool.getConn();
    resultSet = cta::schedulerdb::postgres::ArchiveJobQueueRow::selectJobsByJobID(selconn, jobIDsList);
    timings.insertAndReset("mountFetchBatchTime", t);

    cta::utils::Timer t3;
    // Construct the return value
    uint64_t totalBytes = 0;
    // Precompute the maximum value before the loop
    common::dataStructures::TapeFile tpfile;
    auto maxBlockId = std::numeric_limits<decltype(tpfile.blockId)>::max();
    while (true) {
      cta::utils::Timer ta;
      cta::utils::Timer t2;
      bool hasNext = resultSet.next(); // Call to next
      timings.insOrIncAndReset("mountFetchBatchCallNextTime", t2);
      if (!hasNext) break; // Exit if no more rows
      auto job = m_jobPool.acquireJob();
      timings.insOrIncAndReset("mountFetchBatchAquireJobTime", t2);

      job->initialize(resultSet, logContext);
      timings.insOrIncAndReset("mountFetchBatchinitializeJobTime", t2);
      //auto job = std::make_unique<schedulerdb::ArchiveRdbJob>(m_RelationalDB.m_connPool, resultSet);
      retVector.emplace_back(std::move(job));
      timings.insOrIncAndReset("mountFetchBatchinitializeEmplaceTime", t2);
      uint64_t sizeInBytes = retVector.back()->archiveFile.fileSize;
      totalBytes += sizeInBytes;
      auto& tapeFile = retVector.back()->tapeFile;
      tapeFile.fSeq = ++nbFilesCurrentlyOnTape;
      tapeFile.blockId = maxBlockId;
      timings.insOrIncAndReset("mountFetchBatchRestOpsTime", t2);
      timings.insertAndReset("mountFetchBatchRowTime", ta);
      // the break below must not happen - we must check the bytesRequested condition in the SQL query or do another update in case
      // we hit this if statement ! TO BE FIXED  - otherwise we generate jobs in the DB which will never get queued in the task queue !!!
      if (totalBytes >= bytesRequested) break;
    }
    selconn.commit();
  }
  // Convert vector to list (which is expected as return type)
  ret.assign(std::make_move_iterator(retVector.begin()), std::make_move_iterator(retVector.end()));
  cta::log::ScopedParamContainer logParams(logContext);
  timings.insertAndReset("mountTransformBatchTime", t);
  timings.addToLog(logParams);
  logContext.log(cta::log::INFO, "In ArchiveMount::getNextJobBatch(): Finished fetching new jobs for execution.");

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
  log::LogContext lc(m_RelationalDB.m_logger);
  m_RelationalDB.m_tapeDrivesState->updateDriveStatus(driveInfo, inputs, lc);
}

void ArchiveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats)
{
  // We just report the tape session statistics as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = mountInfo.drive;
  driveInfo.logicalLibrary = mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;

  ReportDriveStatsInputs inputs;
  inputs.reportTime = time(nullptr);
  inputs.bytesTransferred = stats.dataVolume;
  inputs.filesTransferred = stats.filesCount;

  log::LogContext lc(m_RelationalDB.m_logger);
  m_RelationalDB.m_tapeDrivesState->updateDriveStatistics(driveInfo, inputs, lc);
}

uint64_t ArchiveMount::requeueJobBatch(const std::list<std::string>& jobIDsList, cta::log::LogContext& logContext) const {
  // here we will do the same as for ArchiveRdbJob::failTransfer but for bunch of jobs
  cta::schedulerdb::Transaction txn(m_connPool);
  uint64_t nrows = 0;
  try {
    nrows = postgres::ArchiveJobQueueRow::updateFailedTaskQueueJobStatus(txn, ArchiveJobStatus::AJS_ToTransferForUser, jobIDsList);
    txn.commit();
  } catch (exception::Exception &ex) {
    logContext.log(cta::log::ERR,
           "In schedulerdb::ArchiveMount::failJobBatch(): failed to update job status for failed task queue." +
           ex.getMessageValue());
    txn.abort();
    return 0;
  }
  return nrows;
}

void ArchiveMount::setJobBatchTransferred(
      std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> & jobsBatch, log::LogContext & lc)
{
  lc.log(log::WARNING,
         "In schedulerdb::ArchiveMount::setJobBatchTransferred(): passes as half-dummy implementation valid only for AJS_ToReportToUserForTransfer !");
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
    uint64_t nrows = postgres::ArchiveJobQueueRow::updateJobStatus(txn, ArchiveJobStatus::AJS_ToReportToUserForTransfer, jobIDsList);
    txn.commit();
    if (nrows != jobIDsList.size()){
      log::ScopedParamContainer(lc)
              .add("updatedRows", nrows)
              .add("jobListSize", jobIDsList.size())
              .log(log::ERR,
                   "In ArchiveMount::setJobBatchTransferred(): Failed to ArchiveJobQueueRow::updateJobStatus() for entire job list provided.");
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
               "In schedulerdb::ArchiveMount::setJobBatchTransferred(): Failed to cast ArchiveJob to ArchiveRdbJob and return the object to the pool for reuse.");
      }
    }
    jobsBatch.clear();
  } catch (exception::Exception &ex) {
    lc.log(cta::log::ERR,
                   "In schedulerdb::ArchiveMount::setJobBatchTransferred(): Failed to update job status for reporting. Aborting the transaction." +
                   ex.getMessageValue());
    txn.abort();
  }
}
} // namespace cta::schedulerdb
