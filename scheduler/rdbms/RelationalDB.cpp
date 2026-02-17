/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/RelationalDB.hpp"

#include "catalogue/Catalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"
#include "common/log/TimingList.hpp"
#include "common/process/threading/MutexLocker.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/NullDbValue.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/rdbms/ArchiveRdbJob.hpp"
#include "scheduler/rdbms/ArchiveRequest.hpp"
#include "scheduler/rdbms/Helpers.hpp"
#include "scheduler/rdbms/RepackRequest.hpp"
#include "scheduler/rdbms/RetrieveRdbJob.hpp"
#include "scheduler/rdbms/RetrieveRequest.hpp"
#include "scheduler/rdbms/TapeMountDecisionInfo.hpp"
#include "scheduler/rdbms/postgres/ArchiveJobSummary.hpp"
#include "scheduler/rdbms/postgres/RepackRequestTracker.hpp"
#include "scheduler/rdbms/postgres/RetrieveJobSummary.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"

#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

namespace cta {

RelationalDB::RelationalDB(const std::string& ownerId,
                           log::Logger& logger,
                           catalogue::Catalogue& catalogue,
                           const rdbms::Login& login,
                           const uint64_t nbConns)
    : m_ownerId(ownerId),
      m_connPool(login, nbConns),
      m_catalogue(catalogue),
      m_logger(logger) {
  m_tapeDrivesState = std::make_unique<TapeDrivesCatalogueState>(m_catalogue);
}

RelationalDB::~RelationalDB() = default;

void RelationalDB::waitSubthreadsComplete() {
  // This method is only used by unit tests so calling usleep() is good enough
  std::this_thread::sleep_for(std::chrono::seconds(1));
}

void RelationalDB::ping() {
  auto conn = m_connPool.getConn();
  try {
    // TO-DO: we might prefer to check schema version instead
    const auto names = conn.getTableNames();
    bool found_scheddb = false;
    for (auto& name : names) {
      if ("CTA_SCHEDULER" == name) {
        found_scheddb = true;
      }
    }
    conn.reset();
    if (!found_scheddb) {
      throw cta::exception::Exception("Did not find CTA_SCHEDULER table in the Postgres Scheduler DB.");
    }
  } catch (exception::Exception& ex) {
    conn.rollback();
    throw;
  }
}

std::optional<common::dataStructures::VirtualOrganization> RelationalDB::getDefaultRepackVo() {
  return m_catalogue.VO()->getDefaultVirtualOrganizationForRepack();
}

std::string RelationalDB::queueArchive(const std::string& instanceName,
                                       const cta::common::dataStructures::ArchiveRequest& request,
                                       const cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId& criteria,
                                       log::LogContext& lc) {
  // Construct the archive request object
  utils::Timer timeTotal;
  utils::Timer timeGetConn;
  auto sqlconn = m_connPool.getConn();
  log::ScopedParamContainer params(lc);
  params.add("connCountOnLoad", m_connPool.getNbConnsOnLoan());
  params.add("getConnTime", timeGetConn.secs());
  schedulerdb::ArchiveRequest aReq(sqlconn, lc);

  // Summarize all as an archiveFile
  common::dataStructures::ArchiveFile aFile;
  aFile.archiveFileID = criteria.fileId;
  aFile.checksumBlob = request.checksumBlob;
  aFile.creationTime = std::numeric_limits<decltype(aFile.creationTime)>::min();
  aFile.reconciliationTime = 0;
  aFile.diskFileId = request.diskFileID;
  aFile.diskFileInfo = request.diskFileInfo;
  aFile.diskInstance = instanceName;
  aFile.fileSize = request.fileSize;
  aFile.storageClass = request.storageClass;
  aReq.setArchiveFile(aFile);

  utils::Timer timeSetters;
  aReq.setMountPolicy(criteria.mountPolicy);
  aReq.setArchiveReportURL(request.archiveReportURL);
  aReq.setArchiveErrorReportURL(request.archiveErrorReportURL);
  aReq.setRequester(request.requester);
  aReq.setSrcURL(request.srcURL);
  aReq.setEntryLog(request.creationLog);
  params.add("timeSetters", timeSetters.secs());
  auto archiveRequestId = cta::schedulerdb::postgres::ArchiveJobQueueRow::getNextArchiveRequestID(sqlconn);
  int count_jobs = 0;
  for (auto& [key, value] : criteria.copyToPoolMap) {
    count_jobs++;
    aReq.addJob(key,
                value,
                schedulerdb::ArchiveRequest::RETRIES_WITHIN_MOUNT,
                schedulerdb::ArchiveRequest::TOTAL_RETRIES,
                schedulerdb::ArchiveRequest::REPORT_RETRIES,
                archiveRequestId);
  }

  if (count_jobs == 0) {
    throw schedulerdb::ArchiveRequestHasNoCopies("In RelationalDB::queueArchive: the archive request has no copies");
  }

  utils::Timer timeInsert;

  aReq.insert();

  params.add("fileId", aFile.archiveFileID)
    .add("diskInstance", aFile.diskInstance)
    .add("diskFilePath", aFile.diskFileInfo.path)
    .add("diskFileId", aFile.diskFileId)
    .add("insertTime", timeInsert.secs())
    .add("totalTime", timeTotal.secs());
  lc.log(log::INFO, "In RelationalDB::queueArchive(): Finished enqueueing request.");
  return aReq.getIdStr();
}

std::map<std::string, std::list<common::dataStructures::ArchiveJob>, std::less<>> RelationalDB::getArchiveJobs() const {
  std::map<std::string, std::list<common::dataStructures::ArchiveJob>, std::less<>> ret;
  std::list<cta::common::dataStructures::ArchiveJob> archiveJobData = RelationalDB::getArchiveJobs(std::nullopt);
  for (auto job : archiveJobData) {
    ret[job.tapePool].push_back(job);
  }
  return ret;
}

std::list<cta::common::dataStructures::ArchiveJob>
RelationalDB::getArchiveJobs(const std::optional<std::string>& tapePoolName) const {
  std::list<cta::common::dataStructures::ArchiveJob> ret;

  // Get a connection
  auto conn = m_connPool.getConn();

  // Query archive jobs for specific tape pool from ARCHIVE_PENDING_QUEUE
  std::string sql = R"SQL(
    SELECT
      TAPE_POOL,
      ARCHIVE_FILE_ID,
      COPY_NB,
      CREATION_TIME,
      ARCHIVE_REPORT_URL,
      ARCHIVE_ERROR_REPORT_URL,
      DISK_FILE_ID,
      DISK_FILE_PATH,
      DISK_FILE_OWNER_UID,
      DISK_FILE_GID,
      SIZE_IN_BYTES,
      CHECKSUMBLOB,
      STORAGE_CLASS,
      SRC_URL,
      REQUESTER_NAME,
      REQUESTER_GROUP
    FROM ARCHIVE_PENDING_QUEUE
  )SQL";
  if (tapePoolName.has_value()) {
    sql += R"SQL(
    WHERE TAPE_POOL = :TAPE_POOL
  )SQL";
  }
  auto stmt = conn.createStmt(sql);
  if (tapePoolName.has_value()) {
    stmt.bindString(":TAPE_POOL", tapePoolName.value());
  }
  auto rset = stmt.executeQuery();

  while (rset.next()) {
    common::dataStructures::ArchiveJob job;

    // Extract tape pool
    job.tapePool = rset.columnString("TAPE_POOL");

    // Extract archive file ID and copy number
    job.archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
    job.copyNumber = rset.columnUint64("COPY_NB");

    // Fill in the request fields
    job.request.creationLog.time = rset.columnUint64("CREATION_TIME");
    job.request.archiveReportURL = rset.columnString("ARCHIVE_REPORT_URL");
    job.request.archiveErrorReportURL = rset.columnString("ARCHIVE_ERROR_REPORT_URL");
    job.request.diskFileID = rset.columnString("DISK_FILE_ID");
    job.request.diskFileInfo.path = rset.columnString("DISK_FILE_PATH");
    job.request.diskFileInfo.owner_uid = rset.columnUint64("DISK_FILE_OWNER_UID");
    job.request.diskFileInfo.gid = rset.columnUint64("DISK_FILE_GID");
    job.request.fileSize = rset.columnUint64("SIZE_IN_BYTES");
    job.request.storageClass = rset.columnString("STORAGE_CLASS");
    job.request.srcURL = rset.columnString("SRC_URL");
    job.request.requester.name = rset.columnString("REQUESTER_NAME");
    job.request.requester.group = rset.columnString("REQUESTER_GROUP");

    // Extract checksum blob
    std::string checksumBlobStr = rset.columnBlob("CHECKSUMBLOB");

    ret.push_back(job);
  }

  return ret;
}

std::unique_ptr<SchedulerDatabase::IArchiveJobQueueItor>
RelationalDB::getArchiveJobQueueItor(const std::string& tapePoolName,
                                     common::dataStructures::JobQueueType queueType) const {
  throw cta::exception::NotImplementedException();
}

std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>
RelationalDB::getNextArchiveJobsToReportBatch(uint64_t filesRequested, log::LogContext& lc) {
  // This method is not being used for repack which has a separate workflow !
  log::TimingList timings;
  cta::utils::Timer t;
  cta::log::ScopedParamContainer logParams(lc);
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ret;
  schedulerdb::Transaction txn(m_connPool, lc);
  // retrieve batch up to file limit
  std::list<schedulerdb::ArchiveJobStatus> statusList;
  statusList.emplace_back(schedulerdb::ArchiveJobStatus::AJS_ToReportToUserForSuccess);
  statusList.emplace_back(schedulerdb::ArchiveJobStatus::AJS_ToReportToUserForFailure);
  rdbms::Rset resultSet;
  try {
    // gc_delay 3h delay for each report, if not reported, requeue for reporting
    uint64_t gc_delay = 10800;
    resultSet =
      schedulerdb::postgres::ArchiveJobQueueRow::flagReportingJobsByStatus(txn, statusList, gc_delay, filesRequested);
    if (resultSet.isEmpty()) {
      lc.log(cta::log::INFO, "In RelationalDB::getNextArchiveJobsToReportBatch(): nothing to report.");
      return ret;
    }
    while (resultSet.next()) {
      // last parameter is false = signaling that this is not a repack workflow
      ret.emplace_back(std::make_unique<schedulerdb::ArchiveRdbJob>(m_connPool, resultSet, false));
    }
    txn.commit();
    timings.insertAndReset("fetchedArchiveJobs", t);
    timings.addToLog(logParams);
    lc.log(cta::log::INFO, "Successfully flagged jobs for reporting.");
  } catch (exception::Exception& ex) {
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMessage", ex.getMessageValue());
    lc.log(cta::log::ERR, "In RelationalDB::getNextArchiveJobsToReportBatch(): failed to flagReportingJobsByStatus.");
    txn.abort();
    return ret;
  }
  lc.log(log::INFO, "In RelationalDB::getNextArchiveJobsToReportBatch(): Finished getting archive jobs for reporting.");
  return ret;
}

SchedulerDatabase::JobsFailedSummary RelationalDB::getArchiveJobsFailedSummary(log::LogContext& lc) {
  SchedulerDatabase::JobsFailedSummary ret;
  // Get the jobs from DB
  cta::schedulerdb::Transaction txn(m_connPool, lc);
  auto rset = cta::schedulerdb::postgres::ArchiveJobSummaryRow::selectFailedJobSummary(txn);
  while (rset.next()) {
    cta::schedulerdb::postgres::ArchiveJobSummaryRow afjsr(rset);
    ret.totalFiles += afjsr.jobsCount;
    ret.totalBytes += afjsr.jobsTotalSize;
  }
  try {
    txn.commit();
  } catch (cta::exception::Exception& e) {
    std::string bt = e.backtrace();
    lc.log(log::ERR, "In RelationalDB::getNextArchiveJobsToReportBatch(): Exception thrown: " + bt);
    txn.abort();
  }
  return ret;
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>
RelationalDB::getNextRetrieveJobsToTransferBatch(const std::string& vid, uint64_t filesRequested, log::LogContext& lc) {
  throw cta::exception::NotImplementedException();
}

void RelationalDB::requeueRetrieveRequestJobs(std::list<cta::SchedulerDatabase::RetrieveJob*>& jobs,
                                              const std::string& toReportQueueAddress,
                                              log::LogContext& lc) {
  throw cta::exception::NotImplementedException();
}

std::string RelationalDB::blockRetrieveQueueForCleanup(const std::string& vid) {
  throw cta::exception::NotImplementedException();
}

void RelationalDB::unblockRetrieveQueueForCleanup(const std::string& vid) {
  throw cta::exception::NotImplementedException();
};

bool RelationalDB::trimEmptyToReportQueue(const std::string& queueName, log::LogContext& lc) {
  throw cta::exception::NotImplementedException();
}

void RelationalDB::setArchiveJobBatchReported(std::list<SchedulerDatabase::ArchiveJob*>& jobsBatch,
                                              log::TimingList& timingList,
                                              utils::Timer& t,
                                              log::LogContext& lc) {
  // This method is NOT being used for repack which has a separate workflow !
  // If job from a single copy archive request is done, we will delete it
  // For multi-copy we will proceed the same as it would not be in the list unless second copy succeeded
  // (which is ensured by the ArchiveMount when updating successfully transferred jobs)

  std::vector<std::string> jobIDsList_success;
  std::vector<std::string> jobIDsList_failure;
  // reserving space to avoid multiple re-allocations during emplace_back
  jobIDsList_success.reserve(jobsBatch.size());
  jobIDsList_failure.reserve(jobsBatch.size());
  auto jobsBatchItor = jobsBatch.begin();
  while (jobsBatchItor != jobsBatch.end()) {
    switch ((*jobsBatchItor)->reportType) {
      case SchedulerDatabase::ArchiveJob::ReportType::CompletionReport:
        jobIDsList_success.emplace_back(std::to_string((*jobsBatchItor)->jobID));
        break;
      case SchedulerDatabase::ArchiveJob::ReportType::FailureReport:
        jobIDsList_failure.emplace_back(std::to_string((*jobsBatchItor)->jobID));
        break;
      default:
        log::ScopedParamContainer(lc)
          .add("jobID", (*jobsBatchItor)->jobID)
          .add("archiveFileID", (*jobsBatchItor)->archiveFile.archiveFileID)
          .add("diskInstance", (*jobsBatchItor)->archiveFile.diskInstance)
          .log(cta::log::WARNING,
               "In schedulerdb::RelationalDB::setArchiveJobBatchReported(): Skipping handling of a reported job");
        jobsBatchItor++;
        continue;
    }
    log::ScopedParamContainer(lc)
      .add("jobID", (*jobsBatchItor)->jobID)
      .add("archiveRequestId", (*jobsBatchItor)->archiveRequestId)
      .add("requestJobCount", (*jobsBatchItor)->requestJobCount)
      .add("archiveFileID", (*jobsBatchItor)->archiveFile.archiveFileID)
      .add("diskInstance", (*jobsBatchItor)->archiveFile.diskInstance)
      .log(log::INFO,
           "In schedulerdb::RelationalDB::setArchiveJobBatchReported(): received a reported job for a "
           "status change to Failed or Completed.");
    jobsBatchItor++;
  }
  schedulerdb::Transaction txn(m_connPool, lc);
  try {
    cta::utils::Timer t2;
    uint64_t deletionCount = 0;
    uint64_t failedCount = 0;
    // false in the updateJobStatus calls below =  this is not repack workflow !!!
    if (!jobIDsList_success.empty()) {
      uint64_t nrows =
        schedulerdb::postgres::ArchiveJobQueueRow::updateJobStatus(txn,
                                                                   cta::schedulerdb::ArchiveJobStatus::ReadyForDeletion,
                                                                   jobIDsList_success);
      deletionCount += nrows;
      if (nrows != jobIDsList_success.size()) {
        log::ScopedParamContainer(lc)
          .add("updatedRows", nrows)
          .add("jobListSize", jobIDsList_success.size())
          .log(log::ERR,
               "In RelationalDB::setArchiveJobBatchReported(): Failed to ArchiveJobQueueRow::updateJobStatus() "
               "for successful single copy job list provided.");
      }
    }
    if (!jobIDsList_failure.empty()) {
      uint64_t nrows =
        schedulerdb::postgres::ArchiveJobQueueRow::updateJobStatus(txn,
                                                                   cta::schedulerdb::ArchiveJobStatus::AJS_Failed,
                                                                   jobIDsList_failure);
      failedCount += nrows;
      if (nrows != jobIDsList_failure.size()) {
        log::ScopedParamContainer(lc)
          .add("updatedRows", nrows)
          .add("jobListSize", jobIDsList_failure.size())
          .log(log::ERR,
               "In RelationalDB::setArchiveJobBatchReported: Failed to ArchiveJobQueueRow::updateJobStatus() "
               "for failed single copy job list provided.");
      }
    }
    txn.commit();
    log::ScopedParamContainer(lc)
      .add("rowDeletionTime", t2.secs())
      .add("rowDeletionCount", deletionCount)
      .add("failedCount", failedCount)
      .log(log::INFO,
           "RelationalDB::setArchiveJobBatchReported(): jobs reported to disk were deleted or moved to failed table in "
           "the DB.");

  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(cta::log::ERR,
           "In schedulerdb::RelationalDB::setArchiveJobBatchReported(): failed to update job status. "
           "Aborting the transaction.");
    txn.abort();
  }
  return;
}

std::list<SchedulerDatabase::RetrieveQueueStatistics>
RelationalDB::getRetrieveQueueStatistics(const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria,
                                         const std::set<std::string>& vidsToConsider) {
  throw cta::exception::NotImplementedException();
}

void RelationalDB::clearStatisticsCache(const std::string& vid) {
  schedulerdb::Helpers::flushStatisticsCacheForVid(vid);
}

void RelationalDB::setStatisticsCacheConfig(const StatisticsCacheConfig& conf) {
  if (conf.retrieveQueueCacheMaxAgeSecs.has_value()) {
    schedulerdb::Helpers::setRetrieveQueueCacheMaxAgeSecs(conf.retrieveQueueCacheMaxAgeSecs.value());
  }
  if (conf.tapeCacheMaxAgeSecs.has_value()) {
    schedulerdb::Helpers::setTapeCacheMaxAgeSecs(conf.tapeCacheMaxAgeSecs.value());
  }
}

SchedulerDatabase::RetrieveRequestInfo
RelationalDB::queueRetrieve(cta::common::dataStructures::RetrieveRequest& rqst,
                            const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria,
                            const std::optional<std::string>& diskSystemName,
                            log::LogContext& lc) {
  utils::Timer timeTotal;
  auto rreqMutex = std::make_unique<cta::threading::Mutex>();
  cta::threading::MutexLocker rreqMutexLock(*rreqMutex);
  SchedulerDatabase::RetrieveRequestInfo ret;
  log::ScopedParamContainer(lc).add("diskSystemName", diskSystemName);
  std::string tfvids = "";
  try {
    // Get the best vid from the cache
    std::set<std::string, std::less<>> candidateVids;
    for (auto& tf : criteria.archiveFile.tapeFiles) {
      candidateVids.insert(tf.vid);
      tfvids += std::string(tf.vid);
    }
    lc.log(cta::log::DEBUG, "In RelationalDB::queueRetrieve(): starting. ");

    auto sqlconn = m_connPool.getConn();
    /* The current selectBestVid4Retrieve implementation makes
     * a query for every single file to the scheduler db summary stats
     * for existing queues, this is very inefficient !
     * to-do options: a) cache, b) query in batches and insert VIDs available in the info
     * about the archive file from the catalogue and decide on the best during the getNextJobBatch phase ! */
    ret.selectedVid = cta::schedulerdb::Helpers::selectBestVid4Retrieve(candidateVids, m_catalogue, sqlconn, false);

    uint8_t bestCopyNb = 0;
    for (auto& tf : criteria.archiveFile.tapeFiles) {
      if (tf.vid == ret.selectedVid) {
        bestCopyNb = tf.copyNb;
        // Appending the file size to the dstURL so that
        // XrootD will fail to retrieve if there is not enough free space
        // in the eos disk
        rqst.appendFileSizeToDstURL(tf.fileSize);
        break;
      }
    }

    // In order to queue the job, construct it first in memory.
    schedulerdb::RetrieveRequest rReq(sqlconn, lc);
    // the order of the following calls in important - we should rewise
    // the whole logic here and metadata object separation
    rReq.setActivityIfNeeded(rqst, criteria);
    ret.requestId = rReq.getIdStr();
    rReq.setSchedulerRequest(rqst);
    rReq.fillJobsSetRetrieveFileQueueCriteria(criteria);  // fills also m_jobs
    rReq.setActiveCopyNumber(bestCopyNb);
    rReq.setIsVerifyOnly(rqst.isVerifyOnly);
    rReq.setDiskSystemName(diskSystemName);
    rreqMutex.release();
    rReq.insert();
    log::ScopedParamContainer(lc)
      .add("totalTime", timeTotal.secs())
      .log(cta::log::INFO, "In RelationalDB::queueRetrieve(): Finished enqueueing request.");
    return ret;
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(lc)
      .add("tfvids", tfvids)
      .add("ret.selectedVid", ret.selectedVid)
      .add("totalTime", timeTotal.secs())
      .add("exceptionMessage", ex.getMessageValue())
      .log(cta::log::ERR, "In schedulerdb::RelationalDB::queueRetrieve(): failed to queue retrieve.");
    return ret;
  }
}

void RelationalDB::cancelRetrieve(const std::string& instanceName,
                                  const cta::common::dataStructures::CancelRetrieveRequest& request,
                                  log::LogContext& lc) {
  schedulerdb::Transaction txn(m_connPool, lc);
  try {
    uint64_t cancelledJobs = schedulerdb::postgres::RetrieveJobQueueRow::cancelRetrieveJob(txn, request.archiveFileID);
    log::ScopedParamContainer(lc)
      .add("archiveFileID", request.archiveFileID)
      .add("cancelledJobs", cancelledJobs)
      .log(log::INFO, "In RelationalDB::cancelRetrieve(): removing retrieve request from the queue");
    if (cancelledJobs != 1) {
      lc.log(cta::log::WARNING,
             "In RelationalDB::cancelRetrieve(): cancellation affected more than 1 job, check if that is expected !");
    }
    txn.commit();
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(cta::log::ERR,
           "In RelationalDB::cancelRetrieve(): failed to cancel retrieve job. Aborting the transaction.");
    txn.abort();
    throw;
  }
  return;
}

std::map<std::string, std::list<RetrieveRequestDump>> RelationalDB::getRetrieveRequests() const {
  throw cta::exception::NotImplementedException();
}

std::list<RetrieveRequestDump> RelationalDB::getRetrieveRequestsByVid(const std::string& vid) const {
  throw cta::exception::NotImplementedException();
}

std::list<RetrieveRequestDump> RelationalDB::getRetrieveRequestsByRequester(const std::string& vid) const {
  throw cta::exception::NotImplementedException();
}

void RelationalDB::deleteRetrieveRequest(const common::dataStructures::SecurityIdentity& requester,
                                         const std::string& remoteFile) {
  throw cta::exception::NotImplementedException();
}

void RelationalDB::cancelArchive(const common::dataStructures::DeleteArchiveRequest& request, log::LogContext& lc) {
  schedulerdb::Transaction txn(m_connPool, lc);
  try {
    uint64_t cancelledJobs =
      schedulerdb::postgres::ArchiveJobQueueRow::cancelArchiveJob(txn, request.diskInstance, request.archiveFileID);
    log::ScopedParamContainer(lc)
      .add("archiveFileID", request.archiveFileID)
      .add("diskInstance", request.diskInstance)
      .add("cancelledJobs", cancelledJobs)
      .log(log::INFO, "In RelationalDB::cancelArchive(): removing archive request from the queue");
    if (cancelledJobs != 1) {
      lc.log(cta::log::WARNING,
             "In RelationalDB::cancelArchive(): cancellation affected more than 1 job, check if that is expected !");
    }
    txn.commit();
  } catch (exception::Exception& ex) {
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMessage", ex.getMessageValue());
    lc.log(cta::log::ERR, "In RelationalDB::cancelArchive(): failed to cancel archive job. Aborting the transaction.");
    txn.abort();
    throw;
  }
  return;
}

void RelationalDB::deleteFailed(const std::string& objectId, log::LogContext& lc) {
  throw cta::exception::NotImplementedException();
}

std::map<std::string, std::list<common::dataStructures::RetrieveJob>, std::less<>>
RelationalDB::getPendingRetrieveJobs() const {
  std::map<std::string, std::list<common::dataStructures::RetrieveJob>, std::less<>> ret;
  std::list<cta::common::dataStructures::RetrieveJob> ret_list = getPendingRetrieveJobs(std::nullopt);
  for (auto& job : ret_list) {
    std::string vid = "";

    if (!job.tapeCopies.empty()) {
      vid = job.tapeCopies.begin()->first;
    }
    ret[vid].push_back(job);
  }
  return ret;
}

std::list<cta::common::dataStructures::RetrieveJob>
RelationalDB::getPendingRetrieveJobs(const std::optional<std::string>& vid) const {
  std::list<cta::common::dataStructures::RetrieveJob> ret;
  // Get a connection
  auto conn = m_connPool.getConn();

  // Query all retrieve jobs from RETRIEVE_PENDING_QUEUE
  std::string sql = R"SQL(
    SELECT
      VID,
      ARCHIVE_FILE_ID,
      COPY_NB,
      SIZE_IN_BYTES,
      CREATION_TIME,
      DST_URL,
      RETRIEVE_ERROR_REPORT_URL,
      DISK_FILE_ID,
      DISK_FILE_PATH,
      DISK_FILE_OWNER_UID,
      DISK_FILE_GID,
      CHECKSUMBLOB,
      STORAGE_CLASS,
      REQUESTER_NAME,
      REQUESTER_GROUP,
      DISK_INSTANCE
    FROM RETRIEVE_PENDING_QUEUE
  )SQL";
  if (vid) {
    sql += R"SQL(
      WHERE VID = :VID
    )SQL";
  }
  auto stmt = conn.createStmt(sql);
  if (vid) {
    stmt.bindString(":VID", vid.value());
  }
  auto rset = stmt.executeQuery();

  while (rset.next()) {
    common::dataStructures::RetrieveJob job;
    std::string vid = rset.columnString("VID");

    // Fill in the request fields
    job.request.archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
    job.request.creationLog.time = rset.columnUint64("CREATION_TIME");
    job.request.dstURL = rset.columnString("DST_URL");
    job.request.errorReportURL = rset.columnString("RETRIEVE_ERROR_REPORT_URL");
    job.request.diskFileInfo.path = rset.columnString("DISK_FILE_PATH");
    job.request.diskFileInfo.owner_uid = rset.columnUint64("DISK_FILE_OWNER_UID");
    job.request.diskFileInfo.gid = rset.columnUint64("DISK_FILE_GID");
    job.request.requester.name = rset.columnString("REQUESTER_NAME");
    job.request.requester.group = rset.columnString("REQUESTER_GROUP");

    // Fill in other fields
    job.fileSize = rset.columnUint64("SIZE_IN_BYTES");
    job.diskInstance = rset.columnString("DISK_INSTANCE");
    job.storageClass = rset.columnString("STORAGE_CLASS");
    job.diskFileId = rset.columnString("DISK_FILE_ID");

    // Extract checksum blob
    std::string checksumBlobStr = rset.columnBlob("CHECKSUMBLOB");

    // Create tape copy entry
    uint32_t copyNb = rset.columnUint32("COPY_NB");
    common::dataStructures::TapeFile tf;
    tf.vid = vid;
    tf.fSeq = 0;     // Not available in pending queue
    tf.blockId = 0;  // Not available in pending queue
    tf.fileSize = job.fileSize;
    tf.copyNb = copyNb;
    tf.creationTime = job.request.creationLog.time;
    tf.checksumBlob.deserialize(checksumBlobStr);

    job.tapeCopies[vid] = std::make_pair(copyNb, tf);

    ret.push_back(job);
  }

  return ret;
}

std::unique_ptr<SchedulerDatabase::IRetrieveJobQueueItor>
RelationalDB::getRetrieveJobQueueItor(const std::string& vid, common::dataStructures::JobQueueType queueType) const {
  throw cta::exception::NotImplementedException();
}

std::string RelationalDB::queueRepack(const SchedulerDatabase::QueueRepackRequest& repackRequest, log::LogContext& lc) {
  cta::utils::Timer t;
  auto rr = std::make_unique<cta::schedulerdb::RepackRequest>(m_connPool, m_catalogue, lc);
  rr->setVid(repackRequest.m_vid);
  rr->setType(repackRequest.m_repackType);
  rr->setBufferURL(repackRequest.m_repackBufferURL);
  rr->setMountPolicy(repackRequest.m_mountPolicy);
  rr->setNoRecall(repackRequest.m_noRecall);
  rr->setCreationLog(repackRequest.m_creationLog);
  rr->setMaxFilesToSelect(repackRequest.m_maxFilesToSelect);
  rr->insert();
  lc.log(log::INFO, "RelationalDB::queueRepack() successfully queued request.");
  return rr->getIdStr();
}

//------------------------------------------------------------------------------
// RelationalDB::repackExists()
//------------------------------------------------------------------------------
bool RelationalDB::repackExists() {
  std::list<common::dataStructures::RepackInfo> repack_list = RelationalDB::fetchRepackInfo("all");
  if (repack_list.empty()) {
    return false;
  } else {
    return true;
  }
}

std::list<common::dataStructures::RepackInfo> RelationalDB::getRepackInfo() {
  return RelationalDB::fetchRepackInfo("all");
}

std::list<common::dataStructures::RepackInfo> RelationalDB::fetchRepackInfo(const std::string& vid) {
  log::LogContext lc(m_logger);
  lc.log(log::DEBUG, "In RelationalDB::getRepackInfo().");
  auto sqlconn = m_connPool.getConn();
  std::list<common::dataStructures::RepackInfo> ret;
  std::unordered_map<uint64_t, common::dataStructures::RepackInfo> repackMap;
  try {
    auto rset = cta::schedulerdb::postgres::RepackRequestTrackingRow::selectRepackRows(sqlconn, vid);
    while (rset.next()) {
      uint64_t reqId = rset.columnUint64NoOpt("REPACK_REQUEST_ID");
      if (!repackMap.contains(reqId)) {
        cta::schedulerdb::postgres::RepackRequestTrackingRow rrtrackrow(rset);
        auto rr = std::make_unique<cta::schedulerdb::RepackRequest>(m_connPool, m_catalogue, lc, rrtrackrow);
        repackMap[reqId] = std::move(rr->repackInfo);
      }
      // Now append the destination info for this VID
      try {
        auto& repackInfo = repackMap[reqId];
        common::dataStructures::RepackInfo::RepackDestinationInfo destInfo;
        destInfo.vid = rset.columnString("DESTINATION_VID");
        destInfo.files = rset.columnUint64NoOpt("DESTINATION_ARCHIVED_FILES");
        destInfo.bytes = rset.columnUint64NoOpt("DESTINATION_ARCHIVED_BYTES");
        repackInfo.destinationInfos.emplace_back(std::move(destInfo));
      } catch (cta::rdbms::NullDbValue&) {  // pass, the string was Null as there is no destination info
      }
    }
    for (auto& kv : repackMap) {
      ret.emplace_back(std::move(kv.second));
    }
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(cta::log::ERR, "In RelationalDB::getRepackInfo(): failed to get repack info.");
    throw;
  }
  sqlconn.commit();
  return ret;
}

common::dataStructures::RepackInfo RelationalDB::getRepackInfo(const std::string& vid) {
  auto result = RelationalDB::fetchRepackInfo(vid);
  if (result.empty()) {
    throw exception::UserError("No repack request found for VID: " + vid);
  }
  return result.front();
}

// this method now serve more as a delete repack method deleting all the jobs from queues which do not have active jobs
void RelationalDB::cancelRepack(const std::string& vid, log::LogContext& lc) {
  schedulerdb::Transaction txn(m_connPool, lc);
  try {
    uint64_t cancelledRepackRequests = schedulerdb::postgres::RepackRequestTrackingRow::cancelRepack(txn, vid);
    log::ScopedParamContainer(lc).add("VID", vid);
    if (cancelledRepackRequests > 1) {
      lc.log(cta::log::WARNING,
             "In RelationalDB::cancelRepack(): deleted more than 1 request (in any state except RRS_Running), please "
             "check if that is expected !");
    } else if (cancelledRepackRequests == 1) {
      lc.log(log::INFO, "In RelationalDB::cancelRepack(): deleted repack request.");
    } else if (cancelledRepackRequests == 0) {
      lc.log(
        cta::log::WARNING,
        "In RelationalDB::cancelRepack(): nothing to cancel, failed to find request in ToExpand or Pending state.");
    }
    txn.commit();
  } catch (exception::Exception& ex) {
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMessage", ex.getMessageValue());
    lc.log(cta::log::ERR,
           "In RelationalDB::cancelRepack(): failed to cancel repack request. Aborting the transaction.");
    txn.abort();
    throw;
  } catch (std::exception& ex) {
    lc.log(cta::log::ERR, std::string("In RelationalDB::cancelRepack(): unexpected std::exception: ") + ex.what());
    txn.abort();
    throw;
  } catch (...) {
    lc.log(cta::log::ERR, "In RelationalDB::cancelRepack(): unknown exception occurred. Aborting the transaction.");
    txn.abort();
    throw;
  }
  return;
}

cta::rdbms::Conn RelationalDB::getConn() {
  return m_connPool.getConn();
}

//------------------------------------------------------------------------------
// RelationalDB::RepackRequestPromotionStatistics::RepackRequestPromotionStatistics()
//------------------------------------------------------------------------------
RelationalDB::RepackRequestPromotionStatistics::RepackRequestPromotionStatistics(RelationalDB& parent)
    : m_parentdb(parent) {}

//------------------------------------------------------------------------------
// RelationalDB::RepackRequestPromotionStatistics::promotePendingRequestsForExpansion()
//------------------------------------------------------------------------------
auto RelationalDB::RepackRequestPromotionStatistics::promotePendingRequestsForExpansion(size_t requestCount,
                                                                                        log::LogContext& lc)
  -> PromotionToToExpandResult {
  lc.log(log::INFO,
         "RelationalDB::RepackRequestPromotionStatistics::promotePendingRequestsForExpansion() updating request to "
         "ToExpand state.");
  PromotionToToExpandResult ret;
  using Status = common::dataStructures::RepackInfo::Status;
  ret.pendingBefore = at(Status::Pending);
  ret.toExpandBefore = at(Status::ToExpand);
  schedulerdb::Transaction txn(m_parentdb.m_connPool, lc);
  try {
    txn.takeNamedLock("promotePendingRequestsForExpansion");
    auto nrows = schedulerdb::postgres::RepackRequestTrackingRow::updateRepackRequestForExpansion(txn, requestCount);
    log::ScopedParamContainer(lc).add("updatedRows", nrows).add("requestCount", requestCount);
    if (nrows != requestCount) {
      lc.log(log::WARNING,
             "In RelationalDB::RepackRequestPromotionStatistics::promotePendingRequestsForExpansion(): updated less "
             "rows than specified requestCount. Check if this is expected.");
    }
    ret.promotedRequests = nrows;
    ret.pendingAfter = ret.pendingBefore - nrows;
    ret.toExpandAfter = ret.toExpandBefore + nrows;
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(cta::log::ERR,
           "In RelationalDB::RepackRequestPromotionStatistics::promotePendingRequestsForExpansion(): failed to update "
           "rows.");
    txn.abort();
  }
  txn.commit();
  return ret;
}

//------------------------------------------------------------------------------
// RelationalDB::populateRepackRequestsStatistics()
//------------------------------------------------------------------------------
void RelationalDB::populateRepackRequestsStatistics(SchedulerDatabase::RepackRequestStatistics& stats) {
  log::LogContext lc(m_logger);
  lc.log(log::INFO,
         "RelationalDB::populateRepackRequestsStatistics(): fetching repack tracking summary from Scheduler DB.");
  auto sqlconn = m_connPool.getConn();
  auto rSet = schedulerdb::postgres::RepackRequestTrackingRow::getRepackRequestStatistics(sqlconn);
  // Ensure existence of stats for important statuses
  using Status = common::dataStructures::RepackInfo::Status;
  for (auto s : {Status::Pending, Status::ToExpand, Status::Starting, Status::Running}) {
    stats[s] = 0;
  }
  while (rSet.next()) {
    auto& row = rSet;
    auto status = from_string<cta::schedulerdb::RepackJobStatus>(row.columnString("STATUS"));
    auto repackInfoStatus = common::dataStructures::RepackInfo::Status::Undefined;
    switch (status) {
      case cta::schedulerdb::RepackJobStatus::RRS_Pending:
        repackInfoStatus = common::dataStructures::RepackInfo::Status::Pending;
        break;
      case cta::schedulerdb::RepackJobStatus::RRS_ToExpand:
        repackInfoStatus = common::dataStructures::RepackInfo::Status::ToExpand;
        break;
      case cta::schedulerdb::RepackJobStatus::RRS_Starting:
        repackInfoStatus = common::dataStructures::RepackInfo::Status::Starting;
        break;
      case cta::schedulerdb::RepackJobStatus::RRS_Running:
        repackInfoStatus = common::dataStructures::RepackInfo::Status::Running;
        break;
      case cta::schedulerdb::RepackJobStatus::RRS_Complete:
        repackInfoStatus = common::dataStructures::RepackInfo::Status::Complete;
        break;
      case cta::schedulerdb::RepackJobStatus::RRS_Failed:
        repackInfoStatus = common::dataStructures::RepackInfo::Status::Failed;
        break;
      default:
        repackInfoStatus = common::dataStructures::RepackInfo::Status::Undefined;
        break;
    }
    stats[repackInfoStatus] += row.columnUint64("JOBS_COUNT");
  }
}

auto RelationalDB::getRepackStatisticsNoLock() -> std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> {
  log::LogContext lc(m_logger);
  lc.log(
    log::INFO,
    "RelationalDB::getRepackStatisticsNoLock(): calling populateRepackRequestsStatistics() select call to the DB.");

  auto ret = std::make_unique<RelationalDB::RepackRequestPromotionStatistics>(*this);
  populateRepackRequestsStatistics(*ret);
  return ret;
}

auto RelationalDB::getRepackStatistics() -> std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> {
  log::LogContext lc(m_logger);
  lc.log(log::WARNING,
         "RelationalDB::getRepackStatistics() calling getRepackStatisticsNoLock as no lock is taken anyway !");
  return getRepackStatisticsNoLock();
}

std::unique_ptr<SchedulerDatabase::RepackRequest> RelationalDB::getNextRepackJobToExpand() {
  log::LogContext lc(m_logger);
  lc.log(log::INFO, "In RelationalDB::getNextRepackJobToExpand(): marking one request as IS_EXPAND_STARTED.");
  schedulerdb::Transaction txn(m_connPool, lc);
  txn.takeNamedLock("getNextRepackJobToExpand");
  cta::schedulerdb::postgres::RepackRequestTrackingRow rrjtr;
  bool found = false;
  try {
    auto rset = schedulerdb::postgres::RepackRequestTrackingRow::markStartOfExpansion(txn);
    while (rset.next()) {
      found = true;
      rrjtr = cta::schedulerdb::postgres::RepackRequestTrackingRow(rset);
      log::ScopedParamContainer params(lc);
      params.add("row.vid", rrjtr.vid);
      params.add("row.bufferUrl", rrjtr.bufferUrl);
      lc.log(log::DEBUG, "In RelationalDB::getNextRepackJobToExpand(): RepackRequest row constructed.");
    }
    txn.commit();
  } catch (exception::Exception& ex) {
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMessage", ex.getMessageValue());
    lc.log(cta::log::ERR, "In RelationalDB::getNextRepackJobToExpand(): failed to mark IS_EXPAND_STARTED.");
    txn.abort();
    return nullptr;
  }
  if (!found) {
    lc.log(log::DEBUG, "In RelationalDB::getNextRepackJobToExpand(): no repack request found, returning nullptr.");
    return nullptr;
  }
  lc.log(log::DEBUG, "In RelationalDB::getNextRepackJobToExpand(): finished marking IS_EXPAND_STARTED.");
  auto rreq = std::make_unique<schedulerdb::RepackRequest>(m_connPool, m_catalogue, lc, rrjtr);
  return rreq;
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>
RelationalDB::getNextRetrieveJobsToReportBatch(uint64_t filesRequested, log::LogContext& lc) {
  log::TimingList timings;
  cta::utils::Timer t;
  cta::log::ScopedParamContainer logParams(lc);
  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> ret;
  schedulerdb::Transaction txn(m_connPool, lc);
  // retrieve batch up to file limit
  std::list<schedulerdb::RetrieveJobStatus> statusList;
  statusList.emplace_back(schedulerdb::RetrieveJobStatus::RJS_ToReportToUserForSuccess);
  statusList.emplace_back(schedulerdb::RetrieveJobStatus::RJS_ToReportToUserForFailure);
  rdbms::Rset resultSet;
  try {
    // gc_delay 3h delay for each report, if not reported, requeue for reporting
    uint64_t gc_delay = 10800;
    resultSet =
      schedulerdb::postgres::RetrieveJobQueueRow::flagReportingJobsByStatus(txn, statusList, gc_delay, filesRequested);
    if (resultSet.isEmpty()) {
      lc.log(cta::log::INFO, "In RelationalDB::getNextRetrieveJobsToReportBatch(): nothing to report.");
      return ret;
    }
    while (resultSet.next()) {
      ret.emplace_back(std::make_unique<schedulerdb::RetrieveRdbJob>(m_connPool, resultSet, false));
    }
    txn.commit();
    timings.insertAndReset("fetchedRetrieveJobs", t);
    timings.addToLog(logParams);
    lc.log(cta::log::INFO, "Successfully flagged jobs for reporting.");
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(cta::log::ERR, "In RelationalDB::getNextRetrieveJobsToReportBatch(): failed to flagReportingJobsByStatus: ");
    txn.abort();
    return ret;
  }
  lc.log(log::INFO,
         "In RelationalDB::getNextRetrieveJobsToReportBatch(): Finished getting retrieve jobs for reporting.");
  return ret;
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>
RelationalDB::getRetrieveJobs(uint64_t filesRequested, bool fetchFailed, log::LogContext& lc) const {
  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> ret;
  schedulerdb::Transaction txn(m_connPool, lc);
  try {
    auto resultSet = schedulerdb::postgres::RetrieveJobQueueRow::getRetrieveJobs(txn, filesRequested, fetchFailed);
    if (resultSet.isEmpty()) {
      lc.log(cta::log::INFO, "In RelationalDB::getRetrieveJobs(): no jobs found.");
      return ret;
    }
    while (resultSet.next()) {
      // last parameter is false = signaling that this is not for a repack workflow
      ret.emplace_back(std::make_unique<schedulerdb::RetrieveRdbJob>(m_connPool, resultSet, false /* isRepack */));
    }
    txn.commit();
    lc.log(cta::log::INFO, "Successfully fetched jobs for unit test.");
  } catch (exception::Exception& ex) {
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMessage", ex.getMessageValue());
    lc.log(cta::log::ERR, "In RelationalDB::getRetrieveJobs(): fetched retrieve jobs for unit test.");
    txn.abort();
    return ret;
  }
  return ret;
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>
RelationalDB::getNextRetrieveJobsFailedBatch(uint64_t filesRequested, log::LogContext& lc) {
  return getRetrieveJobs(filesRequested, true, lc);
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> RelationalDB::getNextRepackReportBatch(log::LogContext& lc) {
  lc.log(log::WARNING, "RelationalDB::getNextRepackReportBatch() dummy implementation !");
  return nullptr;
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch>
RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(log::LogContext& lc) {
  lc.log(log::INFO,
         "RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(): starting job transformation and reporting.");
  cta::utils::Timer t;
  log::TimingList timings;
  std::unique_ptr<SchedulerDatabase::RepackReportBatch> ret;
  std::vector<schedulerdb::postgres::RepackRequestProgress> statUpdates;
  schedulerdb::Transaction txn(m_connPool, lc);
  // move all finished REPACK_RETRIEVE_ACTIVE_QUEUE to the REPACK_ARCHIVE_PENDING_QUEUE
  // return back statistics for:
  //   1) all retrieve rows moved to archive table
  //   2) all rearchive copies inserted additionally
  // 1)+2) = the total number of columns being queued to the REPACK_ARCHIVE_PENDING_QUEUE
  try {
    auto count_rset =
      schedulerdb::postgres::RetrieveJobQueueRow::transformJobBatchToArchive(txn, c_repackRetrieveReportBatchSize);
    while (count_rset.next()) {
      schedulerdb::postgres::RepackRequestProgress update;
      update.reqId = count_rset.columnUint64("REPACK_REQUEST_ID");
      update.retrievedFiles = count_rset.columnUint64("BASE_INSERTED_COUNT");
      update.retrievedBytes = count_rset.columnUint64("BASE_INSERTED_BYTES");
      update.rearchiveCopyNbs = count_rset.columnUint64("ALTERNATE_INSERTED_COUNT");
      update.rearchiveBytes = count_rset.columnUint64("ALTERNATE_INSERTED_BYTES");
      log::ScopedParamContainer params(lc);
      params.add("repackRequestId", update.reqId);
      params.add("retrievedFiles", update.retrievedFiles);
      params.add("retrievedBytes", update.retrievedBytes);
      params.add("rearchiveCopyNbs", update.rearchiveCopyNbs);
      params.add("rearchiveBytes", update.rearchiveBytes);
      params.add("totalToBeArchivedFiles", update.retrievedFiles + update.rearchiveCopyNbs);
      params.add("totalToBeArchivedBytes", update.retrievedBytes + update.rearchiveBytes);
      lc.log(cta::log::INFO,
             "In RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(): Successfully transformed repack retrieve "
             "rows to archive queue table.");
      statUpdates.emplace_back(update);
    }
    txn.commit();
    timings.insertAndReset("movedRetrieveRepackJobs", t);
    log::ScopedParamContainer params(lc);
    timings.addToLog(params);
  } catch (exception::Exception& ex) {
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMessage", ex.getMessageValue());
    lc.log(cta::log::ERR,
           "In RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(): failed to transform retrieve jobs to "
           "archive jobs: ");
    txn.abort();
  }
  if (statUpdates.empty()) {
    lc.log(cta::log::INFO,
           "In RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(): no Repack Retrieve jobs finished, nothing "
           "to archive.");
    // return empty report batch since the rest of the original OStoreDB logic is not needed here
    return ret;
  }
  schedulerdb::Transaction txn2(m_connPool, lc);
  // report back to the REPACK_REQUEST_TRACKING table
  try {
    auto vidrset = schedulerdb::postgres::RepackRequestTrackingRow::updateRepackRequestsProgress(txn2, statUpdates);
    uint64_t nrepreq = 0;
    while (vidrset.next()) {
      nrepreq++;
    }
    log::ScopedParamContainer params(lc);
    params.add("updatedRepackRequests", nrepreq);
    lc.log(cta::log::INFO,
           "In RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(): no Repack Retrieve jobs finished, nothing "
           "to archive.");
    txn2.commit();
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(cta::log::ERR,
           "In RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(): failed to updateRepackRequestsProgress(): ");
    txn2.abort();
  }
  // return empty report batch since the rest of the original OStoreDB logic is not needed here
  return ret;
}

bool RelationalDB::deleteDiskFiles(std::unordered_set<std::string>& jobSrcUrls, log::LogContext& lc) {
  struct DiskFileRemovers {
    std::unique_ptr<cta::disk::AsyncDiskFileRemover> asyncRemover;
    std::string jobUrl;
    using List = std::list<DiskFileRemovers>;
  };

  DiskFileRemovers::List deletersList;
  for (const auto& jobUrl : jobSrcUrls) {
    // async delete the file from the disk
    try {
      cta::disk::AsyncDiskFileRemoverFactory asyncDiskFileRemoverFactory;
      std::unique_ptr<cta::disk::AsyncDiskFileRemover> asyncRemover(
        asyncDiskFileRemoverFactory.createAsyncDiskFileRemover(jobUrl));
      deletersList.emplace_back(DiskFileRemovers {std::move(asyncRemover), jobUrl});
      deletersList.back().asyncRemover->asyncDelete();
    } catch (const cta::exception::Exception& ex) {
      if (ex.getMessageValue().find("No such file or directory") != std::string::npos) {
        log::ScopedParamContainer(lc)
          .add("jobUrl", jobUrl)
          .add("exceptionMessage", ex.getMessageValue())
          .log(log::WARNING,
               "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): async deletion of disk file failed, file "
               "not found.");
      } else {
        log::ScopedParamContainer(lc)
          .add("jobUrl", jobUrl)
          .add("exceptionMessage", ex.getMessageValue())
          .log(log::ERR,
               "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): async deletion of disk file failed.");
        return false;
      }
    }
  }
  for (auto& dfr : deletersList) {
    try {
      dfr.asyncRemover->wait();
      lc.log(log::INFO, "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): async deleted file.");
    } catch (const cta::exception::Exception& ex) {
      if (ex.getMessageValue().find("No such file or directory") != std::string::npos) {
        cta::log::ScopedParamContainer params(lc);
        params.add("exceptionMessage", ex.getMessageValue());
        lc.log(log::WARNING,
               "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): async file not found anymore.");
      } else {
        log::ScopedParamContainer(lc)
          .add("exceptionMessage", ex.getMessageValue())
          .log(
            log::ERR,
            "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): async file not deleted. Exception thrown: ");
        return false;
      }
    }
  }
  return true;
}

// Candidate for renaming in the future to e.g. processNextSuccessfulArchiveRepackReportBatch
std::unique_ptr<SchedulerDatabase::RepackReportBatch>
RelationalDB::getNextSuccessfulArchiveRepackReportBatch(log::LogContext& lc) {
  lc.log(log::INFO,
         "RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): deleting successfully processed job rows and "
         "collecting statistics.");
  cta::utils::Timer t;
  log::TimingList timings;
  std::unique_ptr<SchedulerDatabase::RepackReportBatch> ret;
  schedulerdb::Transaction txn(m_connPool, lc);
  std::vector<std::string> jobIDs;
  std::unordered_set<std::string> jobSrcUrls;
  try {
    // We pick up only jobs which are in ReadyForDeletion status (i.e. all copies were moved to tape).
    auto batch_rset = schedulerdb::postgres::ArchiveJobQueueRow::getNextSuccessfulArchiveRepackReportBatch(
      txn,
      c_repackArchiveReportBatchSize);
    while (batch_rset.next()) {
      jobIDs.emplace_back(batch_rset.columnString("JOB_ID"));
      jobSrcUrls.insert(batch_rset.columnString("SRC_URL"));
    }
  } catch (exception::Exception& ex) {
    cta::log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(cta::log::ERR, "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): Failed to get jobs.");
    txn.abort();
    return ret;
  }
  // ------------------------------------------
  // calling the deletion for the jobSrcUrls
  // ------------------------------------------
  if (!deleteDiskFiles(jobSrcUrls, lc)) {
    txn.abort();
    return ret;
  }
  std::vector<schedulerdb::postgres::RepackRequestProgress> statUpdates;
  if (!jobIDs.empty()) {
    try {
      auto count_rset = schedulerdb::postgres::ArchiveJobQueueRow::deleteSuccessfulRepackArchiveJobBatch(txn, jobIDs);
      while (count_rset.next()) {
        schedulerdb::postgres::RepackRequestProgress update;
        update.reqId = count_rset.columnUint64("REPACK_REQUEST_ID");
        update.vid = count_rset.columnString("VID");
        update.archivedFiles = count_rset.columnUint64("ARCHIVED_COUNT");
        update.archivedBytes = count_rset.columnUint64("ARCHIVED_BYTES");
        log::ScopedParamContainer params(lc);
        params.add("repackRequestId", update.reqId);
        params.add("archivedFiles", update.archivedFiles);
        params.add("archivedBytes", update.archivedBytes);
        lc.log(
          cta::log::INFO,
          "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): Successfully deleted finished archive jobs.");
        statUpdates.emplace_back(update);
      }
      txn.commit();
      timings.insertAndReset("deletedArchiveRepackJobs", t);
    } catch (exception::Exception& ex) {
      log::ScopedParamContainer(lc)
        .add("exceptionMessage", ex.getMessageValue())
        .log(cta::log::ERR, "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): Failed to delete jobs: ");
      txn.abort();
      return ret;
    }
  }
  txn.commit();
  if (statUpdates.empty()) {
    lc.log(cta::log::INFO,
           "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): No Repack progress statistics collected.");
    // return empty report batch since the rest of the OStoreDB machinery is not needed here
    return ret;
  }

  schedulerdb::Transaction txn3(m_connPool, lc);
  // report back to the REPACK_REQUEST_TRACKING table
  uint64_t nrepreq = 0;
  std::vector<std::string> repackBufferUrlsToDelete;
  try {
    auto vidrset = schedulerdb::postgres::RepackRequestTrackingRow::updateRepackRequestsProgress(txn3, statUpdates);
    while (vidrset.next()) {
      nrepreq++;
      // Get the repack request VID for which files were deleted !
      // PS: the deletion from the catalogue was handled by the Scheduler before,
      // PPS: the deletion of the temporary file from EOS disk was handled above
      std::string repack_vid = vidrset.columnString("VID");
      std::string repack_buffer_path = vidrset.columnString("BUFFER_URL") + "/" + repack_vid;
      this->m_catalogue.Tape()->setTapeDirty(repack_vid);
      bool isCompleteStatusChanged = vidrset.columnBool("IS_COMPLETE_CHANGED");
      if (isCompleteStatusChanged) {
        repackBufferUrlsToDelete.push_back(repack_buffer_path);
        log::ScopedParamContainer(lc)
          .add("isCompleteStatusChanged", isCompleteStatusChanged)
          .add("repackBufferUrlsToDelete", repack_buffer_path)
          .log(cta::log::DEBUG,
               "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): pushing back repackBufferUrlsToDelete.");
      }
    }
    log::ScopedParamContainer params(lc);
    params.add("updatedRepackRequests", nrepreq);
    lc.log(cta::log::INFO,
           "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): Updated Repack progress statistics.");
    txn3.commit();
  } catch (exception::Exception& ex) {
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMessage", ex.getMessageValue());
    lc.log(
      cta::log::ERR,
      "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): failed to update  Repack progress statistics.");
    txn3.abort();
  }
  // check if status is Failed or Complete and delete the disk directory for this repack in such a case
  for (auto& bufferURL : repackBufferUrlsToDelete) {
    //Repack Request is complete, delete the directory in the buffer
    cta::disk::DirectoryFactory directoryFactory;
    std::unique_ptr<cta::disk::Directory> directory;
    try {
      directory.reset(directoryFactory.createDirectory(bufferURL));
      directory->rmdir();
      log::ScopedParamContainer(lc)
        .add("bufferURL", bufferURL)
        .log(log::INFO,
             "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): deleted the repack buffer directory");
    } catch (const cta::exception::Exception&) {
      log::ScopedParamContainer(lc)
        .add("bufferURL", bufferURL)
        .log(
          log::WARNING,
          "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): failed to remove the repack buffer directory");
    }
  }
  // return empty report batch since the rest of the OStoreDB machinery is not needed here
  return ret;
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch>
RelationalDB::getNextFailedRetrieveRepackReportBatch(log::LogContext& lc) {
  std::unique_ptr<SchedulerDatabase::RepackReportBatch> ret;
  schedulerdb::Transaction txn(m_connPool, lc);
  std::vector<uint64_t> rrIDs;
  std::vector<uint64_t> flcnts;
  std::vector<uint64_t> flbytes;
  try {
    auto batch_rset = schedulerdb::postgres::RetrieveJobQueueRow::moveFailedRepackJobBatchToFailedQueueTable(
      txn,
      c_repackRetrieveReportBatchSize);
    while (batch_rset.next()) {
      rrIDs.emplace_back(batch_rset.columnUint64NoOpt("REPACK_REQUEST_ID"));
      flcnts.emplace_back(batch_rset.columnUint64NoOpt("FILE_COUNT"));
      flbytes.emplace_back(batch_rset.columnUint64NoOpt("FILE_BYTES"));
    }
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(cta::log::ERR, "In RelationalDB::getNextFailedRetrieveRepackReportBatch(): Failed to move failed jobs: ");
    txn.abort();
    return ret;
  }
  try {
    uint64_t nrows = schedulerdb::postgres::RepackRequestTrackingRow::updateRepackRequestFailuresBatch(txn,
                                                                                                       rrIDs,
                                                                                                       flcnts,
                                                                                                       flbytes,
                                                                                                       true);
    txn.commit();
    log::ScopedParamContainer(lc)
      .add("nrows", nrows)
      .log(log::INFO, "In RelationalDB::getNextFailedRetrieveRepackReportBatch(): updated repack request statistics.");
  } catch (exception::Exception& ex) {
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMessage", ex.getMessageValue());
    lc.log(cta::log::ERR,
           "In RelationalDB::getNextFailedRetrieveRepackReportBatch(): Failed to updateRepackRequestFailuresBatch() "
           "aborting the entire transaction.");
    txn.abort();
    return ret;
  }
  return ret;
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch>
RelationalDB::getNextFailedArchiveRepackReportBatch(log::LogContext& lc) {
  std::unique_ptr<SchedulerDatabase::RepackReportBatch> ret;
  schedulerdb::Transaction txn(m_connPool, lc);
  std::unordered_map<uint64_t, uint64_t> summaryFlCountMap;
  std::unordered_map<uint64_t, uint64_t> summaryFlBytesMap;
  std::vector<uint64_t> rrIDs;
  std::vector<uint64_t> flcnts;
  std::vector<uint64_t> flbytes;
  std::unordered_set<std::string> jobSrcUrls;
  try {
    auto batch_rset = schedulerdb::postgres::ArchiveJobQueueRow::moveFailedRepackJobBatchToFailedQueueTable(
      txn,
      c_repackArchiveReportBatchSize);
    while (batch_rset.next()) {
      uint64_t reqId = batch_rset.columnUint64NoOpt("REPACK_REQUEST_ID");
      // Increment count and total bytes
      summaryFlCountMap[reqId] += 1;
      summaryFlBytesMap[reqId] += batch_rset.columnUint64NoOpt("SIZE_IN_BYTES");
      jobSrcUrls.insert(batch_rset.columnStringNoOpt("SRC_URL"));
    }
    for (const auto& [reqId, count] : summaryFlCountMap) {
      rrIDs.emplace_back(reqId);
      flcnts.emplace_back(count);
      flbytes.emplace_back(summaryFlBytesMap[reqId]);  // safe lookup
    }
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(cta::log::ERR, "In RelationalDB::getNextFailedArchiveRepackReportBatch(): Failed to move failed jobs: ");
    txn.abort();
    return ret;
  }
  // ------------------------------------------
  // calling the deletion for the jobSrcUrls
  // ------------------------------------------
  if (!deleteDiskFiles(jobSrcUrls, lc)) {
    lc.log(cta::log::WARNING,
           "In RelationalDB::getNextFailedArchiveRepackReportBatch(): Failed to delete files from disk.");
  }
  try {
    uint64_t nrows = schedulerdb::postgres::RepackRequestTrackingRow::updateRepackRequestFailuresBatch(txn,
                                                                                                       rrIDs,
                                                                                                       flcnts,
                                                                                                       flbytes,
                                                                                                       false);
    txn.commit();
    log::ScopedParamContainer(lc)
      .add("nrows", nrows)
      .log(log::INFO, "In RelationalDB::getNextFailedRetrieveRepackReportBatch(): updated repack request statistics.");
  } catch (exception::Exception& ex) {
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMessage", ex.getMessageValue());
    lc.log(cta::log::ERR,
           "In RelationalDB::getNextFailedArchiveRepackReportBatch(): Failed to updateRepackRequestFailuresBatch() "
           "aborting the entire transaction.");
    txn.abort();
    return ret;
  }
  return ret;
}

std::list<std::unique_ptr<SchedulerDatabase::RepackReportBatch>>
RelationalDB::getRepackReportBatches(log::LogContext& lc) {
  lc.log(log::WARNING, "RelationalDB::getRepackReportBatches() dummy implementation !");
  std::list<std::unique_ptr<SchedulerDatabase::RepackReportBatch>> ret;
  return ret;
}

// The name of setRetrieveJobBatchReportedToUser method shall be aligned with
// the Archive method setArchiveJobBatchReported (OStoreDB refactoring task)
// and since they are very similar code duplication shall be avoided
void RelationalDB::setRetrieveJobBatchReportedToUser(std::list<SchedulerDatabase::RetrieveJob*>& jobsBatch,
                                                     log::TimingList& timingList,
                                                     utils::Timer& t,
                                                     log::LogContext& lc) {
  // If job is done we will delete it (if the full request was served) - to be implemented !
  std::vector<std::string> jobIDsList_success;
  std::vector<std::string> jobIDsList_failure;
  // reserving space to avoid multiple re-allocations during emplace_back
  jobIDsList_success.reserve(jobsBatch.size());
  jobIDsList_failure.reserve(jobsBatch.size());
  auto jobsBatchItor = jobsBatch.begin();
  while (jobsBatchItor != jobsBatch.end()) {
    switch ((*jobsBatchItor)->reportType) {
      case SchedulerDatabase::RetrieveJob::ReportType::CompletionReport:
        jobIDsList_success.emplace_back(std::to_string((*jobsBatchItor)->jobID));
        break;
      case SchedulerDatabase::RetrieveJob::ReportType::FailureReport:
        jobIDsList_failure.emplace_back(std::to_string((*jobsBatchItor)->jobID));
        break;
      default:
        log::ScopedParamContainer(lc)
          .add("jobID", (*jobsBatchItor)->jobID)
          .add("archiveFileID", (*jobsBatchItor)->archiveFile.archiveFileID)
          .add("diskInstance", (*jobsBatchItor)->archiveFile.diskInstance)
          .log(
            cta::log::WARNING,
            "In schedulerdb::RelationalDB::setRetrieveJobBatchReportedToUser(): Skipping handling of a reported job");
        jobsBatchItor++;
        continue;
    }
    log::ScopedParamContainer(lc)
      .add("jobID", (*jobsBatchItor)->jobID)
      .add("archiveFileID", (*jobsBatchItor)->archiveFile.archiveFileID)
      .add("diskInstance", (*jobsBatchItor)->archiveFile.diskInstance)
      .log(log::INFO,
           "In schedulerdb::RelationalDB::setRetrieveJobBatchReportedToUser(): received a reported job for a "
           "status change to Failed or Completed.");
    jobsBatchItor++;
  }
  schedulerdb::Transaction txn(m_connPool, lc);
  try {
    cta::utils::Timer t2;
    uint64_t deletionCount = 0;
    if (!jobIDsList_success.empty()) {
      uint64_t nrows = schedulerdb::postgres::RetrieveJobQueueRow::updateJobStatus(
        txn,
        cta::schedulerdb::RetrieveJobStatus::ReadyForDeletion,
        jobIDsList_success,
        false);
      deletionCount += nrows;
      if (nrows != jobIDsList_success.size()) {
        log::ScopedParamContainer(lc)
          .add("updatedRows", nrows)
          .add("jobListSize", jobIDsList_success.size())
          .log(log::ERR,
               "In RelationalDB::setRetrieveJobBatchReportedToUser(): Failed to RetrieveJobQueueRow::updateJobStatus() "
               "for entire job list provided.");
      }
    }
    if (!jobIDsList_failure.empty()) {
      uint64_t nrows =
        schedulerdb::postgres::RetrieveJobQueueRow::updateJobStatus(txn,
                                                                    cta::schedulerdb::RetrieveJobStatus::RJS_Failed,
                                                                    jobIDsList_failure,
                                                                    false);
      if (nrows != jobIDsList_failure.size()) {
        log::ScopedParamContainer(lc)
          .add("updatedRows", nrows)
          .add("jobListSize", jobIDsList_failure.size())
          .log(log::ERR,
               "In RelationalDB::setRetrieveJobBatchReportedToUser(): Failed to RetrieveJobQueueRow::updateJobStatus() "
               "for entire job list provided.");
      }
    }
    txn.commit();
    log::ScopedParamContainer(lc)
      .add("rowDeletionTime", t2.secs())
      .add("rowDeletionCount", deletionCount)
      .log(log::INFO, "RelationalDB::setRetrieveJobBatchReportedToUser(): deleted job.");

  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(cta::log::ERR,
           "In schedulerdb::RelationalDB::setRetrieveJobBatchReportedToUser(): failed to update job status. "
           "Aborting the transaction.");
    txn.abort();
  }
  return;
}

void RelationalDB::trimEmptyQueues(log::LogContext& lc) {
  throw cta::exception::NotImplementedException();
}

SchedulerDatabase::JobsFailedSummary RelationalDB::getRetrieveJobsFailedSummary(log::LogContext& lc) {
  throw cta::exception::NotImplementedException();
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> RelationalDB::getMountInfo(log::LogContext& lc) {
  return RelationalDB::getMountInfo(lc, 0);
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> RelationalDB::getMountInfo(log::LogContext& lc,
                                                                                     uint64_t timeout_us) {
  return RelationalDB::getMountInfo(std::nullopt, lc, timeout_us);
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo>
RelationalDB::getMountInfo(std::optional<std::string_view> logicalLibraryName,
                           log::LogContext& lc,
                           uint64_t timeout_us) {
  utils::Timer t;
  // Allocate the getMountInfostructure to return.
  auto privateRet = std::make_unique<schedulerdb::TapeMountDecisionInfo>(*this, m_ownerId, m_tapeDrivesState.get(), lc);
  TapeMountDecisionInfo& tmdi = *privateRet;
  std::string lockValue = "all";
  if (logicalLibraryName.has_value()) {
    lockValue = logicalLibraryName.value();
  }
  privateRet->lock(lockValue);

  // Get all the tape pools and tapes with queues (potential mounts)
  auto lockSchedGlobalTime = t.secs(utils::Timer::resetCounter);
  fetchMountInfo(tmdi, SchedulerDatabase::PurposeGetMountInfo::GET_NEXT_MOUNT, lc);

  auto fetchMountInfoTime = t.secs(utils::Timer::resetCounter);
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> ret(std::move(privateRet));
  log::ScopedParamContainer params(lc);
  params.add("lockSchedGlobalTime", lockSchedGlobalTime).add("fetchMountInfoTime", fetchMountInfoTime);
  lc.log(log::INFO, "In RelationalDB::getMountInfo(): success.");

  return ret;
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> RelationalDB::getMountInfoNoLock(PurposeGetMountInfo purpose,
                                                                                           log::LogContext& lc) {
  utils::Timer t;

  // Allocate the getMountInfostructure to return
  auto privateRet = std::make_unique<schedulerdb::TapeMountDecisionInfo>(*this, m_ownerId, m_tapeDrivesState.get(), lc);
  TapeMountDecisionInfo& tmdi = *privateRet;

  // Get all the tape pools and tapes with queues (potential mounts)
  auto fetchNoLockTime = t.secs(utils::Timer::resetCounter);
  fetchMountInfo(tmdi, purpose, lc);
  auto fetchMountInfoTime = t.secs(utils::Timer::resetCounter);
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> ret(std::move(privateRet));
  log::ScopedParamContainer params(lc);
  params.add("fetchNoLockTime", fetchNoLockTime).add("fetchMountInfoTime", fetchMountInfoTime);
  lc.log(log::INFO, "In RelationalDB::getMountInfoNoLock(): success.");
  return ret;
}

void RelationalDB::fetchMountInfo(SchedulerDatabase::TapeMountDecisionInfo& tmdi,
                                  [[maybe_unused]] SchedulerDatabase::PurposeGetMountInfo purpose,
                                  log::LogContext& lc) {
  utils::Timer t;
  utils::Timer ttotal;
  log::TimingList timings;
  // Get a reference to the transaction, which may or may not be holding the scheduler global lock
  const auto& txn = static_cast<const schedulerdb::TapeMountDecisionInfo*>(&tmdi)->m_txn;

  timings.insertAndReset("fetchMountPolicyCatalogueTime", t);
  // Iterate over all archive queues
  auto rset = cta::schedulerdb::postgres::ArchiveJobSummaryRow::selectNewJobs(*txn);
  // here we do not do a separate call to the DB as we drive the difference betwee User table and Repack table by the status
  // this can be done for retrieve as well, or, if we want to separate the summaries, we can separate the calls here as well
  // This could be best decided once we design better the summaries (using triggers for example).
  while (rset.next()) {
    cta::schedulerdb::postgres::ArchiveJobSummaryRow ajsr(rset);
    // Set the queue type
    common::dataStructures::MountType mountType;
    switch (ajsr.status) {
      case schedulerdb::ArchiveJobStatus::AJS_ToTransferForUser:
        mountType = common::dataStructures::MountType::ArchiveForUser;
        break;
      case schedulerdb::ArchiveJobStatus::AJS_ToTransferForRepack:
        mountType = common::dataStructures::MountType::ArchiveForRepack;
        break;
      default:
        continue;
    }
    // Get statistics for User and Repack archive queues
    tmdi.potentialMounts.emplace_back();
    auto& m = tmdi.potentialMounts.back();
    m.type = mountType;
    m.tapePool = ajsr.tapePool;
    m.bytesQueued += ajsr.jobsTotalSize;
    m.filesQueued += ajsr.jobsCount;
    m.mountPolicyCountMap[ajsr.mountPolicy] = ajsr.jobsCount;
    m.oldestJobStartTime =
      ajsr.oldestJobStartTime < m.oldestJobStartTime ? ajsr.oldestJobStartTime : m.oldestJobStartTime;
    m.minRequestAge = ajsr.archiveMinRequestAge;
    m.priority = ajsr.archivePriority;
    m.logicalLibrary = "";
  }
  timings.insertAndReset("getScheduledArchiveJobSummariesTime", t);

  // Walk the retrieve queues for statistics
  auto rrset = cta::schedulerdb::postgres::RetrieveJobSummaryRow::selectNewJobs(*txn);
  int cnt = 0;
  uint64_t highestPriority = 0;
  uint64_t lowestRequestAge = 9999999999999;
  std::optional<std::string> highestPriorityMountPolicyName;
  std::optional<std::string> lowestRequestAgeMountPolicyName;
  std::vector<cta::schedulerdb::postgres::RetrieveJobSummaryRow> rjsr_vector;
  // first we find out from all the  mount policies in the queues
  // with the highest priority and lowest request age which we collect for later
  while (rrset.next()) {
    cnt++;
    rjsr_vector.emplace_back(rrset);
    if (cnt == 1) {
      highestPriority = rjsr_vector.back().priority;
      highestPriorityMountPolicyName = rjsr_vector.back().mountPolicy;
      lowestRequestAge = rjsr_vector.back().minRetrieveRequestAge;
      lowestRequestAgeMountPolicyName = rjsr_vector.back().mountPolicy;
    } else {
      if (rjsr_vector.back().priority > highestPriority) {
        highestPriority = rjsr_vector.back().priority;
        highestPriorityMountPolicyName = rjsr_vector.back().mountPolicy;
      }
      if (rjsr_vector.back().minRetrieveRequestAge < lowestRequestAge) {
        lowestRequestAge = rjsr_vector.back().minRetrieveRequestAge;
        lowestRequestAgeMountPolicyName = rjsr_vector.back().mountPolicy;
      }
    }
  }
  auto repack_rrset = cta::schedulerdb::postgres::RetrieveJobSummaryRow::selectNewRepackJobs(*txn);
  while (repack_rrset.next()) {
    cnt++;
    rjsr_vector.emplace_back(repack_rrset);
    if (cnt == 1) {
      highestPriority = rjsr_vector.back().priority;
      highestPriorityMountPolicyName = rjsr_vector.back().mountPolicy;
      lowestRequestAge = rjsr_vector.back().minRetrieveRequestAge;
      lowestRequestAgeMountPolicyName = rjsr_vector.back().mountPolicy;
    } else {
      if (rjsr_vector.back().priority > highestPriority) {
        highestPriority = rjsr_vector.back().priority;
        highestPriorityMountPolicyName = rjsr_vector.back().mountPolicy;
      }
      if (rjsr_vector.back().minRetrieveRequestAge < lowestRequestAge) {
        lowestRequestAge = rjsr_vector.back().minRetrieveRequestAge;
        lowestRequestAgeMountPolicyName = rjsr_vector.back().mountPolicy;
      }
    }
  }
  //getting info about sleep disk systems
  std::unordered_map<std::string, RelationalDB::DiskSleepEntry> diskSystemSleepMap =
    getActiveSleepDiskSystemNamesToFilter(lc);
  // for now we create a mount per summary row (assuming there would not be
  // the same activity twice with 2 mount policies or 2 different VIDs selected)
  for (const auto& rjsr : rjsr_vector) {
    tmdi.potentialMounts.emplace_back();
    auto& m = tmdi.potentialMounts.back();
    m.priority = rjsr.priority;
    m.minRequestAge = rjsr.minRetrieveRequestAge;
    m.vid = rjsr.vid;
    m.type = cta::common::dataStructures::MountType::Retrieve;
    m.bytesQueued = rjsr.jobsTotalSize;
    m.filesQueued = rjsr.jobsCount;
    m.oldestJobStartTime = rjsr.oldestJobStartTime;
    m.youngestJobStartTime = rjsr.youngestJobStartTime;
    m.highestPriorityMountPolicyName = highestPriorityMountPolicyName;
    m.lowestRequestAgeMountPolicyName = lowestRequestAgeMountPolicyName;
    m.logicalLibrary = "";         // The logical library is not known here, and will be determined by the caller.
    m.tapePool = "";               // The tape pool is not know and will be determined by the caller.
    m.vendor = "";                 // The vendor is not known here, and will be determined by the caller.
    m.mediaType = "";              // The logical library is not known here, and will be determined by the caller.
    m.vo = "";                     // The vo is not known here, and will be determined by the caller.
    m.capacityInBytes = 0;         // The capacity is not known here, and will be determined by the caller.
    m.labelFormat = std::nullopt;  // The labelFormat is not known here, and may be determined by the caller.
    m.mountPolicyCountMap[rjsr.mountPolicy] = rjsr.jobsCount;
    if (rjsr.diskSystemName) {
      auto it = diskSystemSleepMap.find(rjsr.diskSystemName.value());
      if (it != diskSystemSleepMap.end()) {
        const auto& entry = it->second;
        auto now = static_cast<uint64_t>(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

        if (now < (entry.timestamp + entry.sleepTime)) {
          m.sleepingMount = true;
          m.sleepStartTime = entry.timestamp;
          m.diskSystemName = rjsr.diskSystemName.value();
          m.sleepTime = entry.sleepTime;
        } else {
          m.sleepingMount = false;
        }
      }
    }
  }
  timings.insertAndReset("getScheduledRetrieveJobSummariesTime", t);
  log::ScopedParamContainer params(lc);
  params.add("totalTime", ttotal.secs());
  timings.addToLog(params);
  lc.log(log::INFO, "In RelationalDB::fetchMountInfo(): populated TapeMountDecisionInfo with potential mounts.");
}

std::list<SchedulerDatabase::RetrieveQueueCleanupInfo> RelationalDB::getRetrieveQueuesCleanupInfo(log::LogContext& lc) {
  throw cta::exception::NotImplementedException();
}

void RelationalDB::cleanRetrieveQueueForVid(const std::string& vid, log::LogContext& lc) {
  schedulerdb::Transaction txn(m_connPool, lc);
  try {
    uint64_t movedJobsForReporting =
      schedulerdb::postgres::RetrieveJobQueueRow::handlePendingRetrieveJobsAfterTapeStateChange(txn, vid);
    log::ScopedParamContainer(lc)
      .add("VID", vid)
      .add("movedJobsForReporting", movedJobsForReporting)
      .log(log::INFO,
           "In RelationalDB::cleanRetrieveQueueForVid(): removed all retrieve jobs of the specified VID from the "
           "pending queue. If error report URL was available, it moved jobs to the reporting workflow.");
    txn.commit();
  } catch (exception::Exception& ex) {
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMessage", ex.getMessageValue());
    lc.log(cta::log::ERR,
           "In RelationalDB::cleanRetrieveQueueForVid(): failed to remove retrieve jobs from the pending queue of the "
           "specified tape VID. Aborting the transaction.");
    txn.abort();
    throw;
  }
  return;
}

uint64_t RelationalDB::insertOrUpdateDiskSleepEntry(schedulerdb::Transaction& txn,
                                                    const std::string& diskSystemName,
                                                    const RelationalDB::DiskSleepEntry& entry) {
  std::string sql = R"SQL(
        INSERT INTO DISK_SYSTEM_SLEEP_TRACKING (DISK_SYSTEM_NAME, SLEEP_TIME, LAST_UPDATE_TIME)
        VALUES (:DISK_SYSTEM_NAME, :SLEEP_TIME, :LAST_UPDATE_TIME)
        ON CONFLICT (DISK_SYSTEM_NAME)  DO NOTHING
    )SQL";

  auto stmt = txn.getConn().createStmt(sql);

  stmt.bindString(":DISK_SYSTEM_NAME", diskSystemName);
  stmt.bindUint64(":SLEEP_TIME", entry.sleepTime);
  stmt.bindUint64(":LAST_UPDATE_TIME", static_cast<uint64_t>(entry.timestamp));

  stmt.executeNonQuery();

  return stmt.getNbAffectedRows();
}

std::unordered_map<std::string, RelationalDB::DiskSleepEntry>
RelationalDB::getDiskSystemSleepStatus(rdbms::Conn& conn) {
  // SQL to fetch all rows from the table
  std::string sql = R"SQL(
      SELECT DISK_SYSTEM_NAME, SLEEP_TIME, LAST_UPDATE_TIME
      FROM DISK_SYSTEM_SLEEP_TRACKING
  )SQL";

  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();

  std::unordered_map<std::string, RelationalDB::DiskSleepEntry> sleepEntries;

  while (rset.next()) {
    std::string name = rset.columnString("DISK_SYSTEM_NAME");
    uint64_t sleepTime = rset.columnUint64("SLEEP_TIME");
    uint64_t ts = rset.columnUint64("LAST_UPDATE_TIME");
    sleepEntries[name] = RelationalDB::DiskSleepEntry(sleepTime, ts);
  }

  return sleepEntries;
}

uint64_t RelationalDB::removeDiskSystemSleepEntries(schedulerdb::Transaction& txn,
                                                    const std::vector<std::string>& expiredDiskSystemNames) {
  if (expiredDiskSystemNames.empty()) {
    return 0;
  }
  // Construct the SQL with placeholders for each disk name
  std::string sql = "DELETE FROM DISK_SYSTEM_SLEEP_TRACKING WHERE DISK_SYSTEM_NAME IN (";
  for (size_t i = 0; i < expiredDiskSystemNames.size(); ++i) {
    sql += ":DISKNAME" + std::to_string(i);
    if (i < expiredDiskSystemNames.size() - 1) {
      sql += ", ";
    }
  }
  sql += ")";

  auto stmt = txn.getConn().createStmt(sql);

  // Bind each disk name to its corresponding placeholder
  for (size_t i = 0; i < expiredDiskSystemNames.size(); ++i) {
    stmt.bindString(":DISKNAME" + std::to_string(i), expiredDiskSystemNames[i]);
  }

  stmt.executeNonQuery();
  return stmt.getNbAffectedRows();
}

std::unordered_map<std::string, RelationalDB::DiskSleepEntry>
RelationalDB::getActiveSleepDiskSystemNamesToFilter(log::LogContext& lc) {
  cta::threading::MutexLocker ml(m_diskSystemSleepMutex);
  auto conn = m_connPool.getConn();
  std::unordered_map<std::string, RelationalDB::DiskSleepEntry> diskSystemSleepMap = getDiskSystemSleepStatus(conn);
  conn.commit();
  if (diskSystemSleepMap.empty()) {
    return diskSystemSleepMap;
  }
  uint64_t currentTime = static_cast<uint64_t>(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

  std::vector<std::string> expiredDiskSystems;
  auto it = diskSystemSleepMap.begin();
  while (it != diskSystemSleepMap.end()) {
    const RelationalDB::DiskSleepEntry& entry = it->second;
    cta::log::ScopedParamContainer(lc)
      .add("currentTime", currentTime)
      .add("entry.timestamp", entry.timestamp)
      .add("entry.sleepTime", entry.sleepTime)
      .add("currentTime-entry.timestamp", currentTime - entry.timestamp)
      .log(cta::log::DEBUG,
           "In RelationalDB::getActiveSleepDiskSystemNamesToFilter(): Checking sleeping disk systems.");
    if (currentTime - entry.timestamp > entry.sleepTime) {
      const std::string diskName = it->first;
      expiredDiskSystems.push_back(diskName);
      // erase; iterator is invalidated, but the next one is returned
      it = diskSystemSleepMap.erase(it);
    } else {
      ++it;
    }
  }
  if (!expiredDiskSystems.empty()) {
    schedulerdb::Transaction txn(m_connPool, lc);
    try {
      uint64_t nrows = removeDiskSystemSleepEntries(txn, expiredDiskSystems);
      txn.commit();
      cta::log::ScopedParamContainer(lc)
        .add("nrows", nrows)
        .log(
          cta::log::INFO,
          "In RelationalDB::getActiveSleepDiskSystemNamesToFilter(): Removed disk system sleep entries from the DB.");
    } catch (const std::exception& ex) {
      cta::log::ScopedParamContainer(lc)
        .add("exceptionMessage", ex.what())
        .log(cta::log::ERR,
             "In RelationalDB::getActiveSleepDiskSystemNamesToFilter(): Failed to remove disk system sleep entries "
             "from DB.");
      txn.abort();
    }
  }
  return diskSystemSleepMap;
}

// MountQueueCleanup routine methods
cta::common::dataStructures::DeadMountCandidateIDs RelationalDB::fetchDeadMountCandidates(uint64_t mount_gc_delay,
                                                                                          log::LogContext& lc) {
  cta::common::dataStructures::DeadMountCandidateIDs scheduledMountIDs;
  schedulerdb::Transaction txn(m_connPool, lc);
  uint64_t mount_gc_timestamp = static_cast<uint64_t>(cta::utils::getCurrentEpochTime()) - mount_gc_delay;
  try {
    // get candidates for dead mount sessions - no more jobs to fetch since at least gc_delay
    std::string sql = R"SQL(
      SELECT MOUNT_ID, QUEUE_TYPE FROM MOUNT_QUEUE_LAST_FETCH WHERE LAST_UPDATE_TIME < :OLDER_THAN_TIMESTAMP
    )SQL";
    auto stmt = txn.getConn().createStmt(sql);
    stmt.bindUint64(":OLDER_THAN_TIMESTAMP", mount_gc_timestamp);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      std::string queueType = rset.columnString("QUEUE_TYPE");
      uint64_t mountId = rset.columnUint64("MOUNT_ID");
      if (queueType == "ARCHIVE_PENDING") {
        scheduledMountIDs.archivePending.emplace_back(mountId);
      } else if (queueType == "ARCHIVE_ACTIVE") {
        scheduledMountIDs.archiveActive.emplace_back(mountId);
      } else if (queueType == "RETRIEVE_PENDING") {
        scheduledMountIDs.retrievePending.emplace_back(mountId);
      } else if (queueType == "RETRIEVE_ACTIVE") {
        scheduledMountIDs.retrieveActive.emplace_back(mountId);
      } else if (queueType == "REPACK_ARCHIVE_PENDING") {
        scheduledMountIDs.archiveRepackPending.emplace_back(mountId);
      } else if (queueType == "REPACK_ARCHIVE_ACTIVE") {
        scheduledMountIDs.archiveRepackActive.emplace_back(mountId);
      } else if (queueType == "REPACK_RETRIEVE_PENDING") {
        scheduledMountIDs.retrieveRepackPending.emplace_back(mountId);
      } else if (queueType == "REPACK_RETRIEVE_ACTIVE") {
        scheduledMountIDs.retrieveRepackActive.emplace_back(mountId);
      } else {
        lc.log(log::WARNING, "In RelationalDB::fetchDeadMountCandidates(): Unknown QUEUE_TYPE: " + queueType);
      }
    }
    txn.commit();
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(log::ERR, "In RelationalDB::fetchDeadMountCandidates(): failed to get distinct MOUNT_IDs from the queues.");
    txn.abort();
  }
  return scheduledMountIDs;
}

cta::common::dataStructures::DeadMountCandidateIDs RelationalDB::getDeadMounts(uint64_t inactiveTimeLimit,
                                                                               log::LogContext& lc) {
  // Get all active mount IDs for drives which do have an active mount registered in the catalogue
  std::unordered_map<std::string, std::optional<uint64_t>> driveNameMountIdOpt =
    m_catalogue.DriveState()->getTapeDriveMountIDs();
  log::ScopedParamContainer params(lc);
  std::unordered_set<uint64_t> activeMountIds;
  activeMountIds.reserve(driveNameMountIdOpt.size());

  for (const auto& kv : driveNameMountIdOpt) {
    if (kv.second) {
      activeMountIds.insert(*kv.second);
    }
  }
  params.add("activeCatalogueMountIdCount", activeMountIds.size());
  lc.log(cta::log::INFO, "Fetched mounts registered in catalogue for active drives.");
  // Get all active mount IDs from the Scheduler DB
  cta::common::dataStructures::DeadMountCandidateIDs scheduledMountIDs =
    fetchDeadMountCandidates(inactiveTimeLimit, lc);
  params.add("archivePendingDeadMountIdsCount", scheduledMountIDs.archivePending.size());
  params.add("archiveActiveDeadMountIdsCount", scheduledMountIDs.archiveActive.size());
  params.add("retrievePendingDeadMountIdsCount", scheduledMountIDs.retrievePending.size());
  params.add("retrieveActiveDeadMountIdsCount", scheduledMountIDs.retrieveActive.size());
  params.add("archiveRepackPendingDeadMountIdsCount", scheduledMountIDs.archiveRepackPending.size());
  params.add("archiveRepackActiveDeadMountIdsCount", scheduledMountIDs.archiveRepackActive.size());
  params.add("retrieveRepackPendingDeadMountIdsCount", scheduledMountIDs.retrieveRepackPending.size());
  params.add("retrieveRepackActiveDeadMountIdsCount", scheduledMountIDs.retrieveRepackActive.size());
  lc.log(cta::log::INFO, "Fetched dead mounts from scheduler DB.");

  /* We will now filter out all Mount IDs which are still reported as alive in the catalogue
   * in order to be sure no active processes from the mount will be changing the DB rows
   * (in case of wrong timeout input e.g.)
   */
  // Helper lambda to remove IDs present in activeSet
  auto removeActiveIds = [&activeMountIds](std::vector<uint64_t>& vec) {
    std::erase_if(vec, [&activeMountIds](uint64_t id) { return activeMountIds.count(id) > 0; });
  };
  removeActiveIds(scheduledMountIDs.archivePending);
  removeActiveIds(scheduledMountIDs.archiveActive);
  removeActiveIds(scheduledMountIDs.retrievePending);
  removeActiveIds(scheduledMountIDs.retrieveActive);
  removeActiveIds(scheduledMountIDs.archiveRepackPending);
  removeActiveIds(scheduledMountIDs.archiveRepackActive);
  removeActiveIds(scheduledMountIDs.retrieveRepackPending);
  removeActiveIds(scheduledMountIDs.retrieveRepackActive);
  params.add("archivePendingDeadMountIdsCount", scheduledMountIDs.archivePending.size());
  params.add("archiveActiveDeadMountIdsCount", scheduledMountIDs.archiveActive.size());
  params.add("retrievePendingDeadMountIdsCount", scheduledMountIDs.retrievePending.size());
  params.add("retrieveActiveDeadMountIdsCount", scheduledMountIDs.retrieveActive.size());
  params.add("archiveRepackPendingDeadMountIdsCount", scheduledMountIDs.archiveRepackPending.size());
  params.add("archiveRepackActiveDeadMountIdsCount", scheduledMountIDs.archiveRepackActive.size());
  params.add("retrieveRepackPendingDeadMountIdsCount", scheduledMountIDs.retrieveRepackPending.size());
  params.add("retrieveRepackActiveDeadMountIdsCount", scheduledMountIDs.retrieveRepackActive.size());
  lc.log(cta::log::INFO, "In RelationalDB::getDeadMounts(): Found dead mounts which need job rescheduling.");
  return scheduledMountIDs;
}

std::string RelationalDB::getQueueTypePrefix(bool isArchive, bool isRepack) {
  std::string prefix;

  if (isRepack) {
    prefix += "REPACK_";
  }

  prefix += isArchive ? "ARCHIVE_" : "RETRIEVE_";

  return prefix;
}

uint64_t RelationalDB::handleInactiveMountPendingQueues(const std::vector<uint64_t>& deadMountIds,
                                                        size_t batchSize,
                                                        bool isArchive,
                                                        bool isRepack,
                                                        log::LogContext& lc) {
  uint64_t njobs = 0;
  if (deadMountIds.empty()) {
    return njobs;
  }
  std::string queueTypePrefix = getQueueTypePrefix(isArchive, isRepack);
  // Cleaning up the PENDING tables
  // TO-DO look at the ARCHIVE_PENDING_QUEUE for
  // any jobs with assigned MOUNT_ID for which there are no active MOUNTS and sets their
  // MOUNT_ID to NULL freeing them to be requeued to new drive queues
  // Getting the JOB IDs of dead mounts to requeue from ACTIVE to PENDING TABLE
  schedulerdb::Transaction txn(m_connPool, lc);
  txn.takeNamedLock(queueTypePrefix + "handleInactiveMountPendingQueues");
  std::string mount_ids_array = "";
  for (size_t i = 0; i < deadMountIds.size(); ++i) {
    const auto& deadMountId = deadMountIds[i];
    mount_ids_array += std::to_string(deadMountId);
    if (i < deadMountIds.size() - 1) {
      mount_ids_array += ", ";
    }
  }
  try {
    std::string sql = R"SQL(
      WITH TMP_INACTIVE_MOUNT_IDS(MOUNT_ID) AS (
        SELECT unnest(ARRAY[
    )SQL";
    sql += mount_ids_array + R"SQL( ]::BIGINT[]) AS MOUNT_ID
      ),
      JOB_IDS_FOR_UPDATE AS ( SELECT a.job_id AS JOB_ID
          FROM
    )SQL";
    sql += queueTypePrefix + R"SQL(PENDING_QUEUE a
          JOIN TMP_INACTIVE_MOUNT_IDS t
            ON a.mount_id = t.mount_id
          ORDER BY a.job_id
          LIMIT :LIMIT
          FOR UPDATE SKIP LOCKED
      ) UPDATE
    )SQL";
    sql += queueTypePrefix + R"SQL(PENDING_QUEUE pq SET MOUNT_ID = NULL
      FROM JOB_IDS_FOR_UPDATE jid WHERE pq.JOB_ID = jid.JOB_ID
    )SQL";
    auto stmt = txn.getConn().createStmt(sql);
    stmt.bindUint64(":LIMIT", batchSize);
    stmt.executeQuery();
    njobs = stmt.getNbAffectedRows();
    txn.commit();

    cta::log::ScopedParamContainer(lc)
      .add("nrows", njobs)
      .add("queueTypePrefix", queueTypePrefix)
      .log(cta::log::INFO, "In RelationalDB::handleInactiveMountQueues(): Cleaned up PENDING table.");
    return njobs;
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(log::ERR, "In RelationalDB::handleInactiveMountQueues(): Failed cleaned up PENDING table.");
    txn.abort();
  }
  return njobs;
}

uint64_t RelationalDB::handleInactiveMountActiveQueues(const std::vector<uint64_t>& deadMountIds,
                                                       size_t batchSize,
                                                       bool isArchive,
                                                       bool isRepack,
                                                       log::LogContext& lc) {
  uint64_t njobs = 0;
  if (deadMountIds.empty()) {
    return njobs;
  }
  std::string queueTypePrefix = getQueueTypePrefix(isArchive, isRepack);
  // Getting the JOB IDs of dead mounts to requeue from ACTIVE to PENDING TABLE
  std::string mount_ids_array = "";
  for (size_t i = 0; i < deadMountIds.size(); ++i) {
    const auto& deadMountId = deadMountIds[i];
    mount_ids_array += std::to_string(deadMountId);
    if (i < deadMountIds.size() - 1) {
      mount_ids_array += ", ";
    }
  }
  schedulerdb::Transaction txn(m_connPool, lc);
  txn.takeNamedLock(queueTypePrefix + "handleInactiveMountActiveQueues");
  try {
    std::string sql = R"SQL(
      WITH TMP_INACTIVE_MOUNT_IDS(MOUNT_ID) AS (
        SELECT unnest(ARRAY[
    )SQL";
    sql += mount_ids_array + R"SQL( ]::BIGINT[]) AS MOUNT_ID
      ) SELECT a.job_id AS JOB_ID
          FROM
    )SQL";
    sql += queueTypePrefix + R"SQL(ACTIVE_QUEUE a
        JOIN TMP_INACTIVE_MOUNT_IDS t
          ON a.mount_id = t.mount_id
        WHERE a.IS_REPORTING IS FALSE
          AND a.STATUS = :STATUS
        ORDER BY a.job_id
        LIMIT :LIMIT
        FOR UPDATE SKIP LOCKED
    )SQL";
    auto stmt = txn.getConn().createStmt(sql);
    if (isArchive) {
      auto status = isRepack ? schedulerdb::ArchiveJobStatus::AJS_ToTransferForRepack :
                               schedulerdb::ArchiveJobStatus::AJS_ToTransferForUser;
      stmt.bindString(":STATUS", to_string(status));
    } else {
      auto status = schedulerdb::RetrieveJobStatus::RJS_ToTransfer;
      stmt.bindString(":STATUS", to_string(status));
    }
    stmt.bindUint64(":LIMIT", batchSize);
    auto rset = stmt.executeQuery();
    std::list<std::string> jobIDsList;
    while (rset.next()) {
      jobIDsList.emplace_back(std::to_string(rset.columnUint64("JOB_ID")));
      ++njobs;
    }
    cta::log::ScopedParamContainer(lc)
      .add("njobs_found", njobs)
      .add("queueTypePrefix", queueTypePrefix)
      .log(cta::log::INFO, "In RelationalDB::handleInactiveMountQueues(): Selected rows for cleaning up ACTIVE table.");
    uint64_t nrows = 0;
    if (isArchive) {
      auto status = isRepack ? schedulerdb::ArchiveJobStatus::AJS_ToTransferForRepack :
                               schedulerdb::ArchiveJobStatus::AJS_ToTransferForUser;
      nrows = schedulerdb::postgres::ArchiveJobQueueRow::requeueJobBatch(txn, status, jobIDsList, isRepack);
    } else {
      auto status = schedulerdb::RetrieveJobStatus::RJS_ToTransfer;
      nrows = schedulerdb::postgres::RetrieveJobQueueRow::requeueJobBatch(txn, status, jobIDsList, isRepack);
    }
    txn.commit();
    cta::log::ScopedParamContainer(lc)
      .add("nrows_requeued", nrows)
      .add("queueTypePrefix", queueTypePrefix)
      .log(cta::log::INFO,
           "In RelationalDB::handleInactiveMountActiveQueues(): Requeued rows from ACTIVE to PENDING queue.");
    if (nrows != njobs) {
      lc.log(cta::log::ERR,
             "In RelationalDB::handleInactiveMountActiveQueues(): Number of jobs selected for requeueing does not "
             "correspond to number of jobs requeued !");
    }
    return njobs;
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(log::ERR,
           "In RelationalDB::handleInactiveMountActiveQueues(): failed to requeue rows from ACTIVE to PENDING queue.");
    txn.abort();
  }
  return njobs;
}

void RelationalDB::deleteOldFailedQueues(uint64_t deletionAge, uint64_t batchSize, log::LogContext& lc) {
  std::vector<std::string> failedTables = {"ARCHIVE_FAILED_QUEUE",
                                           "REPACK_ARCHIVE_FAILED_QUEUE",
                                           "REPACK_RETRIEVE_FAILED_QUEUE",
                                           "RETRIEVE_FAILED_QUEUE"};

  std::string sql;
  uint64_t olderThanTimestamp = (uint64_t) cta::utils::getCurrentEpochTime() - deletionAge;
  for (const auto& tbl : failedTables) {
    schedulerdb::Transaction txn(m_connPool, lc);
    txn.takeNamedLock(tbl + "deleteOldFailedQueues");
    try {
      sql = R"SQL(
        DELETE FROM )SQL"
            + tbl;
      sql += R"SQL(
          WHERE JOB_ID IN ( SELECT JOB_ID FROM )SQL"
             + tbl;
      sql += R"SQL( WHERE LAST_UPDATE_TIME < :OLDER_THAN_TIMESTAMP
                     ORDER BY PRIORITY DESC, JOB_ID
                     LIMIT :LIMIT
                     FOR UPDATE SKIP LOCKED )
       )SQL";
      auto stmt = txn.getConn().createStmt(sql);
      stmt.bindUint64(":OLDER_THAN_TIMESTAMP", olderThanTimestamp);
      stmt.bindUint64(":LIMIT", batchSize);
      stmt.executeNonQuery();
      auto nrows = stmt.getNbAffectedRows();
      txn.commit();
      cta::log::ScopedParamContainer(lc)
        .add("deletedRowsFromTable", tbl)
        .add("deletedRows", nrows)
        .add("olderThanTimestamp", olderThanTimestamp)
        .log(cta::log::INFO,
             "In RelationalDB::deleteOldFailedQueues(): Deleted old rows from failed queue tables successfully.");
    } catch (exception::Exception& ex) {
      log::ScopedParamContainer(lc)
        .add("exceptionMessage", ex.getMessageValue())
        .log(log::ERR,
             "In RelationalDB::deleteOldFailedQueues(): Failed to delete old rows from failed queue tables: ");
      txn.abort();
    }
  }
}

void RelationalDB::cleanOldMountLastFetchTimes(uint64_t deletionAge, uint64_t batchSize, log::LogContext& lc) {
  std::string sql;
  uint64_t olderThanTimestamp = (uint64_t) cta::utils::getCurrentEpochTime() - deletionAge;
  schedulerdb::Transaction txn(m_connPool, lc);
  txn.takeNamedLock("MOUNT_QUEUE_LAST_FETCH_cleanOldMountLastFetchTimes");
  try {
    sql = R"SQL(
        WITH ROWS_TO_DELETE AS (
          SELECT MOUNT_ID, QUEUE_TYPE
          FROM MOUNT_QUEUE_LAST_FETCH
          WHERE LAST_UPDATE_TIME < :OLDER_THAN_TIMESTAMP
          ORDER BY LAST_UPDATE_TIME
          LIMIT :LIMIT
          FOR UPDATE SKIP LOCKED
        )
        DELETE FROM MOUNT_QUEUE_LAST_FETCH mh
        USING ROWS_TO_DELETE r
        WHERE (mh.MOUNT_ID, mh.QUEUE_TYPE) = (r.MOUNT_ID, r.QUEUE_TYPE)
       )SQL";
    auto stmt = txn.getConn().createStmt(sql);
    stmt.bindUint64(":OLDER_THAN_TIMESTAMP", olderThanTimestamp);
    stmt.bindUint64(":LIMIT", batchSize);
    stmt.executeNonQuery();
    auto nrows = stmt.getNbAffectedRows();
    txn.commit();
    cta::log::ScopedParamContainer(lc)
      .add("deletedRows", nrows)
      .add("olderThanTimestamp", olderThanTimestamp)
      .log(cta::log::INFO,
           "In RelationalDB::cleanOldMountLastFetchTimes(): Deleted old rows from mount last fetch time table.");
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(
        log::ERR,
        "In RelationalDB::cleanOldMountLastFetchTimes(): Failed to delete old rows from mount last fetch time table.");
    txn.abort();
  }
}

void RelationalDB::cleanMountLastFetchTimes(std::vector<uint64_t> deadMountIds,
                                            bool isArchive,
                                            bool isRepack,
                                            bool isPending,
                                            log::LogContext& lc) {
  std::string queue_type = getQueueTypePrefix(isArchive, isRepack);
  if (isPending) {
    queue_type += "_PENDING";
  } else {
    queue_type += "_ACTIVE";
  }
  std::string mount_ids_array = "";
  for (size_t i = 0; i < deadMountIds.size(); ++i) {
    const auto& deadMountId = deadMountIds[i];
    mount_ids_array += std::to_string(deadMountId);
    if (i < deadMountIds.size() - 1) {
      mount_ids_array += ", ";
    }
  }
  std::string sql;
  schedulerdb::Transaction txn(m_connPool, lc);
  txn.takeNamedLock("MOUNT_QUEUE_LAST_FETCH_cleanMountLastFetchTimes");
  try {
    sql = R"SQL(
      WITH TMP_MOUNT_IDS(MOUNT_ID) AS (
        SELECT unnest(ARRAY[
    )SQL";
    sql += mount_ids_array + R"SQL( ]::BIGINT[]) AS MOUNT_ID
      ) DELETE FROM MOUNT_QUEUE_LAST_FETCH mf
          USING TMP_MOUNT_IDS tm
          WHERE mf.MOUNT_ID = tm.MOUNT_ID
            AND mf.QUEUE_TYPE = :QUEUE_TYPE
       )SQL";
    auto stmt = txn.getConn().createStmt(sql);
    stmt.bindString(":QUEUE_TYPE", queue_type);
    stmt.executeNonQuery();
    auto nrows = stmt.getNbAffectedRows();
    txn.commit();
    cta::log::ScopedParamContainer(lc)
      .add("deletedRows", nrows)
      .add("mountIDs", mount_ids_array)
      .log(cta::log::INFO,
           "In RelationalDB::cleanMountLastFetchTimes(): Deleted rows from mount last fetch time table.");
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(lc)
      .add("exceptionMessage", ex.getMessageValue())
      .log(log::ERR,
           "In RelationalDB::cleanMountLastFetchTimes(): Failed to delete old rows from mount last fetch time table.");
    txn.abort();
  }
}

}  // namespace cta
