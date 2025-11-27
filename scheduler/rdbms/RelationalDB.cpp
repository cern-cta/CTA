/*
 * @project	 The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license	 This program is free software, distributed under the terms of the GNU General Public
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

#include "scheduler/rdbms/RelationalDB.hpp"
#include "scheduler/Scheduler.hpp"
#include "catalogue/Catalogue.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"
#include "common/log/TimingList.hpp"
#include "common/utils/utils.hpp"
#include "common/threading/MutexLocker.hpp"
#include "scheduler/rdbms/postgres/ArchiveJobSummary.hpp"
#include "scheduler/rdbms/postgres/RetrieveJobSummary.hpp"
#include "scheduler/rdbms/postgres/RepackRequestTracker.hpp"
#include "scheduler/rdbms/ArchiveRdbJob.hpp"
#include "scheduler/rdbms/ArchiveRequest.hpp"
#include "scheduler/rdbms/TapeMountDecisionInfo.hpp"
#include "scheduler/rdbms/Helpers.hpp"
#include "scheduler/rdbms/RetrieveRdbJob.hpp"
#include "scheduler/rdbms/RetrieveRequest.hpp"
#include "scheduler/rdbms/RepackRequest.hpp"
#include "rdbms/NullDbValue.hpp"

#include <vector>
#include <chrono>
#include <thread>
#include <mutex>

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

std::optional<common::dataStructures::VirtualOrganization> RelationalDB::getDefaultRepackVo(){
    return m_catalogue.VO()->getDefaultVirtualOrganizationForRepack();
}

std::string RelationalDB::queueArchive(const std::string& instanceName,
                                       const cta::common::dataStructures::ArchiveRequest& request,
                                       const cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId& criteria,
                                       log::LogContext& logContext) {
  // Construct the archive request object
  utils::Timer timeTotal;
  utils::Timer timeGetConn;
  auto sqlconn = m_connPool.getConn();
  log::ScopedParamContainer params(logContext);
  params.add("connCountOnLoad", m_connPool.getNbConnsOnLoan());
  params.add("getConnTime", timeGetConn.secs());
  schedulerdb::ArchiveRequest aReq(sqlconn, logContext);

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
  logContext.log(log::INFO, "In RelationalDB::queueArchive(): Finished enqueueing request.");
  return aReq.getIdStr();
}

std::map<std::string, std::list<common::dataStructures::ArchiveJob>, std::less<>> RelationalDB::getArchiveJobs() const {
  throw cta::exception::NotImplementedException();
}

std::list<cta::common::dataStructures::ArchiveJob> RelationalDB::getArchiveJobs(const std::string& tapePoolName) const {
  throw cta::exception::NotImplementedException();
}

std::unique_ptr<SchedulerDatabase::IArchiveJobQueueItor>
RelationalDB::getArchiveJobQueueItor(const std::string& tapePoolName,
                                     common::dataStructures::JobQueueType queueType) const {
  throw cta::exception::NotImplementedException();
}

std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>
RelationalDB::getNextArchiveJobsToReportBatch(uint64_t filesRequested, log::LogContext& logContext) {
  // This method is not being used for repack which has a separate workflow !
  log::TimingList timings;
  cta::utils::Timer t;
  cta::log::ScopedParamContainer logParams(logContext);
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ret;
  schedulerdb::Transaction txn(m_connPool);
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
      logContext.log(cta::log::INFO, "In RelationalDB::getNextArchiveJobsToReportBatch(): nothing to report.");
      return ret;
    }
    while (resultSet.next()) {
      // last parameter is false = signaling that this is not a repack workflow
      ret.emplace_back(std::make_unique<schedulerdb::ArchiveRdbJob>(m_connPool, resultSet, false));
    }
    txn.commit();
    timings.insertAndReset("fetchedArchiveJobs", t);
    timings.addToLog(logParams);
    logContext.log(cta::log::INFO, "Successfully flagged jobs for reporting.");
  } catch (exception::Exception& ex) {
    logContext.log(cta::log::ERR,
                   "In RelationalDB::getNextArchiveJobsToReportBatch(): failed to flagReportingJobsByStatus: " +
                     ex.getMessageValue());
    txn.abort();
    return ret;
  }
  logContext.log(log::INFO,
                 "In RelationalDB::getNextArchiveJobsToReportBatch(): Finished getting archive jobs for reporting.");
  return ret;
}

SchedulerDatabase::JobsFailedSummary RelationalDB::getArchiveJobsFailedSummary(log::LogContext& logContext) {
  SchedulerDatabase::JobsFailedSummary ret;
  // Get the jobs from DB
  cta::schedulerdb::Transaction txn(m_connPool);
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
    logContext.log(log::ERR, "In RelationalDB::getNextArchiveJobsToReportBatch(): Exception thrown: " + bt);
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

void RelationalDB::unblockRetrieveQueueForCleanup(const std::string& vid){
  throw cta::exception::NotImplementedException();
};

bool RelationalDB::trimEmptyToReportQueue(const std::string& queueName, log::LogContext& lc) {
  throw cta::exception::NotImplementedException();
}

void RelationalDB::setArchiveJobBatchReported(std::list<SchedulerDatabase::ArchiveJob*>& jobsBatch,
                                              log::TimingList& timingList,
                                              utils::Timer& t,
                                              log::LogContext& lc) {
  // This method is not being used for repack which has a separate workflow !
  // If job is done we will delete it (if the full request was served) - to be implemented !
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
      .add("archiveFileID", (*jobsBatchItor)->archiveFile.archiveFileID)
      .add("diskInstance", (*jobsBatchItor)->archiveFile.diskInstance)
      .log(log::INFO,
           "In schedulerdb::RelationalDB::setArchiveJobBatchReported(): received a reported job for a "
           "status change to Failed or Completed.");
    jobsBatchItor++;
  }
  schedulerdb::Transaction txn(m_connPool);
  try {
    cta::utils::Timer t2;
    uint64_t deletionCount = 0;
    // false in the updateJobStatus calls below =  this is not repack workflow
    if (!jobIDsList_success.empty()) {
      uint64_t nrows =
        schedulerdb::postgres::ArchiveJobQueueRow::updateJobStatus(txn,
                                                                   cta::schedulerdb::ArchiveJobStatus::ReadyForDeletion, false,
                                                                   jobIDsList_success);
      deletionCount += nrows;
      if (nrows != jobIDsList_success.size()) {
        log::ScopedParamContainer(lc)
          .add("updatedRows", nrows)
          .add("jobListSize", jobIDsList_success.size())
          .log(log::ERR,
               "In RelationalDB::setArchiveJobBatchReported: Failed to ArchiveJobQueueRow::updateJobStatus() "
               "for entire job list provided.");
      }
    }
    if (!jobIDsList_failure.empty()) {
      uint64_t nrows =
        schedulerdb::postgres::ArchiveJobQueueRow::updateJobStatus(txn,
                                                                   cta::schedulerdb::ArchiveJobStatus::AJS_Failed, false,
                                                                   jobIDsList_failure);
      if (nrows != jobIDsList_failure.size()) {
        log::ScopedParamContainer(lc)
          .add("updatedRows", nrows)
          .add("jobListSize", jobIDsList_failure.size())
          .log(log::ERR,
               "In RelationalDB::setArchiveJobBatchReported: Failed to ArchiveJobQueueRow::updateJobStatus() "
               "for entire job list provided.");
      }
    }
    txn.commit();
    log::ScopedParamContainer(lc)
      .add("rowDeletionTime", t2.secs())
      .add("rowDeletionCount", deletionCount)
      .log(log::INFO, "RelationalDB::setArchiveJobBatchReported(): deleted job.");

  } catch (exception::Exception& ex) {
    lc.log(cta::log::ERR,
           "In schedulerdb::RelationalDB::setArchiveJobBatchReported(): failed to update job status. "
           "Aborting the transaction." +
             ex.getMessageValue());
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
                            const std::optional<std::string> diskSystemName,
                            log::LogContext& logContext) {
  utils::Timer timeTotal;
  auto rreqMutex = std::make_unique<cta::threading::Mutex>();
  cta::threading::MutexLocker rreqMutexLock(*rreqMutex);
  SchedulerDatabase::RetrieveRequestInfo ret;
  log::ScopedParamContainer(logContext)
      .add("diskSystemName", diskSystemName);
  try {

    // Get the best vid from the cache
    std::set<std::string, std::less<>> candidateVids;
    for (auto& tf : criteria.archiveFile.tapeFiles) {
      candidateVids.insert(tf.vid);
    }
    logContext.log(cta::log::INFO, "In RelationalDB::queueRetrieve(): before sqlconn selection. ");

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
    schedulerdb::RetrieveRequest rReq(sqlconn, logContext);
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
    log::ScopedParamContainer(logContext)
      .add("totalTime", timeTotal.secs())
      .log(cta::log::INFO, "In RelationalDB::queueRetrieve(): Finished enqueueing request.");
    return ret;
  } catch (exception::Exception& ex) {
    logContext.log(cta::log::ERR,
                   "In schedulerdb::RelationalDB::queueRetrieve(): failed to queue retrieve." + ex.getMessageValue());
    return ret;
  }
}

void RelationalDB::cancelRetrieve(const std::string& instanceName,
                                  const cta::common::dataStructures::CancelRetrieveRequest& request,
                                  log::LogContext& lc) {
  schedulerdb::Transaction txn(m_connPool);
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
    lc.log(cta::log::ERR,
           "In RelationalDB::cancelRetrieve(): failed to cancel retrieve job. Aborting the transaction." +
             ex.getMessageValue());
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
  schedulerdb::Transaction txn(m_connPool);
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
    lc.log(cta::log::ERR,
           "In RelationalDB::cancelArchive(): failed to cancel archive job. Aborting the transaction." +
             ex.getMessageValue());
    txn.abort();
    throw;
  }
  return;
}

void RelationalDB::deleteFailed(const std::string& objectId, log::LogContext& lc) {
  throw cta::exception::NotImplementedException();
}

std::map<std::string, std::list<common::dataStructures::RetrieveJob>, std::less<>>
RelationalDB::getRetrieveJobs() const {
  throw cta::exception::NotImplementedException();
}

std::list<cta::common::dataStructures::RetrieveJob> RelationalDB::getRetrieveJobs(const std::string& vid) const {
  throw cta::exception::NotImplementedException();
}

std::unique_ptr<SchedulerDatabase::IRetrieveJobQueueItor>
RelationalDB::getRetrieveJobQueueItor(const std::string& vid, common::dataStructures::JobQueueType queueType) const {
  throw cta::exception::NotImplementedException();
}

std::string RelationalDB::queueRepack(const SchedulerDatabase::QueueRepackRequest& repackRequest,
                                      log::LogContext& logContext) {

  cta::utils::Timer t;
  log::LogContext lc(m_logger);
  auto rr = std::make_unique<cta::schedulerdb::RepackRequest>(m_connPool, m_catalogue, lc);
  rr->setVid(repackRequest.m_vid);
  rr->setType(repackRequest.m_repackType);
  rr->setBufferURL(repackRequest.m_repackBufferURL);
  rr->setMountPolicy(repackRequest.m_mountPolicy);
  rr->setNoRecall(repackRequest.m_noRecall);
  rr->setCreationLog(repackRequest.m_creationLog);
  rr->setMaxFilesToSelect(repackRequest.m_maxFilesToSelect);
  rr->insert();
  logContext.log(log::INFO, "RelationalDB::queueRepack() successfully queued request.");
  return rr->getIdStr();
}

//------------------------------------------------------------------------------
// RelationalDB::repackExists()
//------------------------------------------------------------------------------
bool RelationalDB::repackExists() {
  std::list <common::dataStructures::RepackInfo> repack_list = RelationalDB::fetchRepackInfo("all");
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
  try{
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
      } catch (cta::rdbms::NullDbValue& ex) { // pass, the string was Null as there is no destination info
      }
    }
    for (auto& kv : repackMap) {
      ret.emplace_back(std::move(kv.second));
    }
  } catch (exception::Exception& ex) {
    lc.log(cta::log::ERR,
           "In RelationalDB::getRepackInfo(): failed to get repack info." +
             ex.getMessageValue());
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
void RelationalDB::cancelRepack(const std::string &vid, log::LogContext &lc) {
  schedulerdb::Transaction txn(m_connPool);
  try {
    uint64_t cancelledRepackRequests =
            schedulerdb::postgres::RepackRequestTrackingRow::cancelRepack(txn, vid);
    log::ScopedParamContainer(lc)
            .add("VID", vid);
    if (cancelledRepackRequests > 1) {
      lc.log(cta::log::WARNING,
             "In RelationalDB::cancelRepack(): deleted more than 1 request (in any state except RRS_Running), please check if that is expected !");
    } else if (cancelledRepackRequests == 1) {
      lc.log(log::INFO, "In RelationalDB::cancelRepack(): deleted repack request.");
    } else if (cancelledRepackRequests == 0) {
      lc.log(cta::log::WARNING,
             "In RelationalDB::cancelRepack(): nothing to cancel, failed to find request in ToExpand or Pending state.");
    }
    txn.commit();
  } catch (exception::Exception &ex) {
    lc.log(cta::log::ERR,
           "In RelationalDB::cancelRepack(): failed to cancel repack request. Aborting the transaction." +
           ex.getMessageValue());
    txn.abort();
    throw;
  } catch (std::exception &ex) {
    lc.log(cta::log::ERR,
           std::string("In RelationalDB::cancelRepack(): unexpected std::exception: ") + ex.what());
    txn.abort();
    throw;
} catch (...) {
    lc.log(cta::log::ERR,
           "In RelationalDB::cancelRepack(): unknown exception occurred. Aborting the transaction.");
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
RelationalDB::RepackRequestPromotionStatistics::RepackRequestPromotionStatistics(RelationalDB &parent) : m_parentdb(parent) {}

//------------------------------------------------------------------------------
// RelationalDB::RepackRequestPromotionStatistics::promotePendingRequestsForExpansion()
//------------------------------------------------------------------------------
  auto RelationalDB::RepackRequestPromotionStatistics::promotePendingRequestsForExpansion(
          size_t requestCount, log::LogContext &lc) -> PromotionToToExpandResult {
    lc.log(log::INFO,
           "RelationalDB::RepackRequestPromotionStatistics::promotePendingRequestsForExpansion() updating request to ToExpand state.");
    PromotionToToExpandResult ret;
    using Status = common::dataStructures::RepackInfo::Status;
    ret.pendingBefore = at(Status::Pending);
    ret.toExpandBefore = at(Status::ToExpand);
    schedulerdb::Transaction txn(m_parentdb.m_connPool);
    try {
      txn.takeNamedLock("promotePendingRequestsForExpansion");
      auto nrows = schedulerdb::postgres::RepackRequestTrackingRow::updateRepackRequestForExpansion(txn, requestCount);
      log::ScopedParamContainer(lc)
              .add("updatedRows", nrows)
              .add("requestCount", requestCount);
      if (nrows != requestCount) {
        lc.log(log::WARNING,
               "In RelationalDB::RepackRequestPromotionStatistics::promotePendingRequestsForExpansion(): updated less rows than specified requestCount. Check if this is expected.");
      }
      ret.promotedRequests = nrows;
      ret.pendingAfter = ret.pendingBefore - nrows;
      ret.toExpandAfter = ret.toExpandBefore + nrows;
    } catch (exception::Exception &ex) {
      lc.log(cta::log::ERR,
             "In RelationalDB::RepackRequestPromotionStatistics::promotePendingRequestsForExpansion(): failed to update rows." +
             ex.getMessageValue());
      txn.abort();
    }
    txn.commit();
    return ret;
  }

//------------------------------------------------------------------------------
// RelationalDB::populateRepackRequestsStatistics()
//------------------------------------------------------------------------------
void RelationalDB::populateRepackRequestsStatistics(SchedulerDatabase::RepackRequestStatistics &stats) {
  log::LogContext lc(m_logger);
  lc.log(log::INFO, "RelationalDB::populateRepackRequestsStatistics(): fetching repack tracking summary from Scheduler DB.");
  auto sqlconn = m_connPool.getConn();
  auto rSet = schedulerdb::postgres::RepackRequestTrackingRow::getRepackRequestStatistics(sqlconn);
  // Ensure existence of stats for important statuses
  using Status = common::dataStructures::RepackInfo::Status;
  for (auto s: {Status::Pending, Status::ToExpand, Status::Starting, Status::Running}) {
    stats[s] = 0;
  }
  while (rSet.next()) {
    auto &row = rSet;
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

auto RelationalDB::getRepackStatisticsNoLock()
    -> std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> {
  log::LogContext lc(m_logger);
  lc.log(log::INFO,
         "RelationalDB::getRepackStatisticsNoLock(): calling populateRepackRequestsStatistics() select call to the DB.");

  //auto typedRet = std::make_unique<RelationalDB::RepackRequestPromotionStatisticsNoLock>();
  auto ret = std::make_unique<RelationalDB::RepackRequestPromotionStatistics>(*this);
  populateRepackRequestsStatistics(*ret);
  return ret;
}

auto RelationalDB::getRepackStatistics() -> std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> {
  log::LogContext lc(m_logger);
  lc.log(log::WARNING, "RelationalDB::getRepackStatistics() calling getRepackStatisticsNoLock as no lock is taken anyway !");
  return getRepackStatisticsNoLock();
}

  std::unique_ptr <SchedulerDatabase::RepackRequest>
  RelationalDB::getNextRepackJobToExpand() {
   log::LogContext lc(m_logger);
    lc.log(log::INFO,
           "In RelationalDB::getNextRepackJobToExpand(): marking one request as IS_EXPAND_STARTED.");
    schedulerdb::Transaction txn(m_connPool);
    txn.takeNamedLock("getNextRepackJobToExpand");
    cta::schedulerdb::postgres::RepackRequestTrackingRow rrjtr;
    bool found = false;
    try {
      auto rset  = schedulerdb::postgres::RepackRequestTrackingRow::markStartOfExpansion(txn);
      while (rset.next()) {
        found = true;
        rrjtr = cta::schedulerdb::postgres::RepackRequestTrackingRow(rset);
        log::ScopedParamContainer params(lc);
        params.add("row.vid", rrjtr.vid);
        params.add("row.bufferUrl", rrjtr.bufferUrl);
        lc.log(log::DEBUG,
           "In RelationalDB::getNextRepackJobToExpand(): RepackRequest row constructed.");
      }
      txn.commit();
    } catch (exception::Exception &ex) {
      lc.log(cta::log::ERR,
             "In RelationalDB::getNextRepackJobToExpand(): failed to mark IS_EXPAND_STARTED: " +
             ex.getMessageValue());
      txn.abort();
      return nullptr;
    }
    if (!found) {
      lc.log(log::DEBUG,
             "In RelationalDB::getNextRepackJobToExpand(): no repack request found, returning nullptr.");
      return nullptr;
    }
    lc.log(log::DEBUG,
           "In RelationalDB::getNextRepackJobToExpand(): finished marking IS_EXPAND_STARTED.");
    auto rreq = std::make_unique<schedulerdb::RepackRequest>(m_connPool, m_catalogue, lc, rrjtr);
    return rreq;
  }

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>
RelationalDB::getNextRetrieveJobsToReportBatch(uint64_t filesRequested, log::LogContext& logContext) {
  log::TimingList timings;
  cta::utils::Timer t;
  cta::log::ScopedParamContainer logParams(logContext);
  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> ret;
  schedulerdb::Transaction txn(m_connPool);
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
      logContext.log(cta::log::INFO, "In RelationalDB::getNextRetrieveJobsToReportBatch(): nothing to report.");
      return ret;
    }
    while (resultSet.next()) {
      ret.emplace_back(std::make_unique<schedulerdb::RetrieveRdbJob>(m_connPool, resultSet, false));
    }
    txn.commit();
    timings.insertAndReset("fetchedRetrieveJobs", t);
    timings.addToLog(logParams);
    logContext.log(cta::log::INFO, "Successfully flagged jobs for reporting.");
  } catch (exception::Exception& ex) {
    logContext.log(cta::log::ERR,
                   "In RelationalDB::getNextRetrieveJobsToReportBatch(): failed to flagReportingJobsByStatus: " +
                     ex.getMessageValue());
    txn.abort();
    return ret;
  }
  logContext.log(log::INFO,
                 "In RelationalDB::getNextRetrieveJobsToReportBatch(): Finished getting retrieve jobs for reporting.");
  return ret;
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>
RelationalDB::getNextRetrieveJobsFailedBatch(uint64_t filesRequested, log::LogContext& logContext) {
  throw cta::exception::NotImplementedException();
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> RelationalDB::getNextRepackReportBatch(log::LogContext& lc) {
  lc.log(log::WARNING, "RelationalDB::getNextRepackReportBatch() dummy implementation !");
  return nullptr;
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch>
RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(log::LogContext& lc) {
  lc.log(log::INFO, "RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(): starting job transformation and reporting.");
  cta::utils::Timer t;
  log::TimingList timings;
  std::unique_ptr<SchedulerDatabase::RepackReportBatch> ret;
  std::vector <schedulerdb::postgres::RepackRequestProgress> statUpdates;
  schedulerdb::Transaction txn(m_connPool);
  // move all finished REPACK_RETRIEVE_ACTIVE_QUEUE to the REPACK_ARCHIVE_PENDING_QUEUE
  // return back statistics for:
  //   1) all retrieve rows moved to archive table
  //   2) all rearchive copies inserted additionally
  // 1)+2) = the total number of columns being queued to the REPACK_ARCHIVE_PENDING_QUEUE
  try {
    auto count_rset  =
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
      lc.log(cta::log::INFO, "In RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(): Successfully transformed repack retrieve rows to archive queue table.");
      statUpdates.emplace_back(update);
    }
    txn.commit();
    timings.insertAndReset("movedRetrieveRepackJobs", t);
    log::ScopedParamContainer params(lc);
    timings.addToLog(params);
  } catch (exception::Exception& ex) {
    lc.log(cta::log::ERR,
                   "In RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(): failed to transform retrieve jobs to archive jobs: " +
                     ex.getMessageValue());
    txn.abort();
  }
  if (statUpdates.empty()) {
    lc.log(cta::log::INFO, "In RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(): no Repack Retrieve jobs finished, nothing to archive.");
    // return empty report batch since the rest of the original OStoreDB logic is not needed here
    return ret;
  }
  schedulerdb::Transaction txn2(m_connPool);
  // report back to the REPACK_REQUEST_TRACKING table
  try {
   auto vidrset = schedulerdb::postgres::RepackRequestTrackingRow::updateRepackRequestsProgress(txn2, statUpdates);
   uint64_t nrepreq = 0;
   while (vidrset.next()) {
     nrepreq++;
   }
   log::ScopedParamContainer params(lc);
   params.add("updatedRepackRequests", nrepreq);
   lc.log(cta::log::INFO, "In RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(): no Repack Retrieve jobs finished, nothing to archive.");
   txn2.commit();
  } catch (exception::Exception& ex) {
    lc.log(cta::log::ERR,
                   "In RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(): failed to updateRepackRequestsProgress(): " +
                     ex.getMessageValue());
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
  for (const auto &jobUrl: jobSrcUrls) {
    // async delete the file from the disk
    try {
      cta::disk::AsyncDiskFileRemoverFactory asyncDiskFileRemoverFactory;
      std::unique_ptr <cta::disk::AsyncDiskFileRemover> asyncRemover(
              asyncDiskFileRemoverFactory.createAsyncDiskFileRemover(jobUrl));
      deletersList.emplace_back(DiskFileRemovers{std::move(asyncRemover), jobUrl});
      deletersList.back().asyncRemover->asyncDelete();
    } catch (const cta::exception::Exception &ex) {
      if (ex.getMessageValue().find("No such file or directory") != std::string::npos) {
        log::ScopedParamContainer(lc)
        .add("jobUrl", jobUrl)
        .log(log::WARNING,
             "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): async deletion of disk file failed, file not found." +
             ex.getMessageValue());
      } else {
        log::ScopedParamContainer(lc)
        .add("jobUrl", jobUrl)
        .log(log::ERR,
             "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): async deletion of disk file failed." +
             ex.getMessageValue());
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
        lc.log(log::WARNING,
                     "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): async file not found anymore." + ex.getMessageValue());
      } else {
        lc.log(log::ERR, "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): async file not deleted. Exception thrown: " +
             ex.getMessageValue());
        return false;
      }
    }
  }
  return true;
}

// Candidate for renaming in the future to e.g. processNextSuccessfulArchiveRepackReportBatch
std::unique_ptr<SchedulerDatabase::RepackReportBatch>
RelationalDB::getNextSuccessfulArchiveRepackReportBatch(log::LogContext& lc) {
  lc.log(log::INFO, "RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): deleting successfully processed job rows and collecting statistics.");
  cta::utils::Timer t;
  log::TimingList timings;
  std::unique_ptr<SchedulerDatabase::RepackReportBatch> ret;
  schedulerdb::Transaction txn(m_connPool);
  //txn.takeNamedLock("getNextSuccessfulArchiveRepackReportBatch");
  std::vector<std::string> jobIDs;
  std::unordered_set<std::string> jobSrcUrls;
  try {
    // We pick up only jobs which are in ReadyForDeletion status (i.e. all copies were moved to tape).
    auto batch_rset = schedulerdb::postgres::ArchiveJobQueueRow::getNextSuccessfulArchiveRepackReportBatch(txn,
                                                                                                           c_repackArchiveReportBatchSize);
    while (batch_rset.next()) {
      jobIDs.emplace_back(batch_rset.columnString("JOB_ID"));
      jobSrcUrls.insert(batch_rset.columnString("SRC_URL"));
    }
  } catch (exception::Exception &ex) {
    lc.log(cta::log::ERR,
           "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): Failed to get jobs: " +
           ex.getMessageValue());
    txn.abort();
    return ret;
  }
  // ------------------------------------------
  // calling the deletion for the jobSrcUrls
  // ------------------------------------------
  bool deletionOk = deleteDiskFiles(jobSrcUrls, lc);
  if (!deletionOk){
    txn.abort();
    return ret;
  }
  std::vector <schedulerdb::postgres::RepackRequestProgress> statUpdates;
  if (!jobIDs.empty()) {
    try {
      auto count_rset =
              schedulerdb::postgres::ArchiveJobQueueRow::deleteSuccessfulRepackArchiveJobBatch(txn, jobIDs);
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
        lc.log(cta::log::INFO,
               "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): Successfully deleted finished archive jobs.");
        statUpdates.emplace_back(update);
      }
      txn.commit();
      timings.insertAndReset("deletedArchiveRepackJobs", t);
    } catch (exception::Exception &ex) {
      lc.log(cta::log::ERR,
             "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): Failed to delete jobs: " +
             ex.getMessageValue());
      txn.abort();
      return ret;
    }
  }
  txn.commit();
  if (statUpdates.empty()) {
    lc.log(cta::log::INFO, "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): No Repack progress statistics collected.");
    // return empty report batch since the rest of the OStoreDB machinery is not needed here
    return ret;
  }

  schedulerdb::Transaction txn3(m_connPool);
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
     if(isCompleteStatusChanged){
       repackBufferUrlsToDelete.push_back(repack_buffer_path);
       log::ScopedParamContainer(lc)
     .add("isCompleteStatusChanged", isCompleteStatusChanged)
     .add("repackBufferUrlsToDelete", repack_buffer_path)
       .log(cta::log::DEBUG, "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): pushing back repackBufferUrlsToDelete.");
     }
   }
   log::ScopedParamContainer params(lc);
   params.add("updatedRepackRequests", nrepreq);
   lc.log(cta::log::INFO, "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): Updated Repack progress statistics.");
   txn3.commit();
  } catch (exception::Exception& ex) {
    lc.log(cta::log::ERR,
                   "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): failed to update  Repack progress statistics: " +
                     ex.getMessageValue());
    txn3.abort();
  }
  // check if status is Failed or Complete and delete the disk directory for this repack in such a case
  for(auto& bufferURL: repackBufferUrlsToDelete){
      //Repack Request is complete, delete the directory in the buffer
      cta::disk::DirectoryFactory directoryFactory;
      // std::string directoryPath = cta::utils::getEnclosingPath(bufferURL);
      std::unique_ptr<cta::disk::Directory> directory;
      try {
        directory.reset(directoryFactory.createDirectory(bufferURL));
        directory->rmdir();
        log::ScopedParamContainer(lc)
          .add("bufferURL", bufferURL)
          .log(log::INFO,
               "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): deleted the repack buffer directory");
      } catch (const cta::exception::Exception& ex) {
        log::ScopedParamContainer(lc)
          .add("bufferURL", bufferURL)
          .log(log::WARNING,
               "In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): failed to remove the repack buffer directory");
      }
  }
  // return empty report batch since the rest of the OStoreDB machinery is not needed here
  return ret;
}

std::unique_ptr <SchedulerDatabase::RepackReportBatch>
RelationalDB::getNextFailedRetrieveRepackReportBatch(log::LogContext &lc) {
  std::unique_ptr<SchedulerDatabase::RepackReportBatch> ret;
  schedulerdb::Transaction txn(m_connPool);
  std::vector <uint64_t> rrIDs;
  std::vector <uint64_t> flcnts;
  std::vector <uint64_t> flbytes;
  try {
    auto batch_rset = schedulerdb::postgres::RetrieveJobQueueRow::moveFailedRepackJobBatchToFailedQueueTable(txn,
                                                                                                             c_repackRetrieveReportBatchSize);
    while (batch_rset.next()) {
      rrIDs.emplace_back(batch_rset.columnUint64NoOpt("REPACK_REQUEST_ID"));
      flcnts.emplace_back(batch_rset.columnUint64NoOpt("FILE_COUNT"));
      flbytes.emplace_back(batch_rset.columnUint64NoOpt("FILE_BYTES"));
    }
  } catch (exception::Exception &ex) {
    lc.log(cta::log::ERR,
           "In RelationalDB::getNextFailedRetrieveRepackReportBatch(): Failed to move failed jobs: " +
           ex.getMessageValue());
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
            .log(log::INFO,
                 "In RelationalDB::getNextFailedRetrieveRepackReportBatch(): updated repack request statistics.");
  } catch (exception::Exception &ex) {
    lc.log(cta::log::ERR,
           "In RelationalDB::getNextFailedRetrieveRepackReportBatch(): Failed to updateRepackRequestFailuresBatch() aborting the entire transaction: " +
           ex.getMessageValue());
    txn.abort();
    return ret;
  }
  return ret;
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch>
RelationalDB::getNextFailedArchiveRepackReportBatch(log::LogContext& lc) {
  std::unique_ptr<SchedulerDatabase::RepackReportBatch> ret;
  schedulerdb::Transaction txn(m_connPool);
  std::unordered_map<uint64_t, uint64_t> summaryFlCountMap;
  std::unordered_map<uint64_t, uint64_t> summaryFlBytesMap;
  std::vector <uint64_t> rrIDs;
  std::vector <uint64_t> flcnts;
  std::vector <uint64_t> flbytes;
  std::unordered_set<std::string> jobSrcUrls;
  try {
    auto batch_rset = schedulerdb::postgres::ArchiveJobQueueRow::moveFailedRepackJobBatchToFailedQueueTable(txn,
                                                                                                            c_repackArchiveReportBatchSize);
    while (batch_rset.next()) {
      uint64_t reqId = batch_rset.columnUint64NoOpt("REPACK_REQUEST_ID");
      // Increment count and total bytes
      summaryFlCountMap[reqId] += 1;
      summaryFlBytesMap[reqId] += batch_rset.columnUint64NoOpt("SIZE_IN_BYTES");
      jobSrcUrls.insert(batch_rset.columnStringNoOpt("SRC_URL"));
    }
    for (const auto &[reqId, count]: summaryFlCountMap) {
      rrIDs.emplace_back(reqId);
      flcnts.emplace_back(count);
      flbytes.emplace_back(summaryFlBytesMap[reqId]);  // safe lookup
    }
  } catch (exception::Exception &ex) {
    lc.log(cta::log::ERR,
           "In RelationalDB::getNextFailedArchiveRepackReportBatch(): Failed to move failed jobs: " +
           ex.getMessageValue());
    txn.abort();
    return ret;
  }
  // ------------------------------------------
  // calling the deletion for the jobSrcUrls
  // ------------------------------------------
  bool deletionOk = deleteDiskFiles(jobSrcUrls, lc);
  if (!deletionOk){
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
            .log(log::INFO,
                 "In RelationalDB::getNextFailedRetrieveRepackReportBatch(): updated repack request statistics.");
  } catch (exception::Exception &ex) {
    lc.log(cta::log::ERR,
           "In RelationalDB::getNextFailedArchiveRepackReportBatch(): Failed to updateRepackRequestFailuresBatch() aborting the entire transaction: " +
           ex.getMessageValue());
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
  schedulerdb::Transaction txn(m_connPool);
  try {
    cta::utils::Timer t2;
    uint64_t deletionCount = 0;
    if (!jobIDsList_success.empty()) {
      uint64_t nrows = schedulerdb::postgres::RetrieveJobQueueRow::updateJobStatus(
        txn,
        cta::schedulerdb::RetrieveJobStatus::ReadyForDeletion,
        jobIDsList_success, false);
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
                                                                    jobIDsList_failure, false);
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
    lc.log(cta::log::ERR,
           "In schedulerdb::RelationalDB::setRetrieveJobBatchReportedToUser(): failed to update job status. "
           "Aborting the transaction." +
             ex.getMessageValue());
    txn.abort();
  }
  return;
}

void RelationalDB::trimEmptyQueues(log::LogContext& lc) {
  throw cta::exception::NotImplementedException();
}

SchedulerDatabase::JobsFailedSummary RelationalDB::getRetrieveJobsFailedSummary(log::LogContext& logContext) {
  throw cta::exception::NotImplementedException();
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> RelationalDB::getMountInfo(log::LogContext& logContext) {
  return RelationalDB::getMountInfo(logContext, 0);
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> RelationalDB::getMountInfo(log::LogContext& logContext,
                                                                                     uint64_t timeout_us) {
  return RelationalDB::getMountInfo("all", logContext, timeout_us);
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo>
RelationalDB::getMountInfo(std::string_view logicalLibraryName, log::LogContext& logContext, uint64_t timeout_us) {
  utils::Timer t;
  // Allocate the getMountInfostructure to return.
  auto privateRet =
    std::make_unique<schedulerdb::TapeMountDecisionInfo>(*this, m_ownerId, m_tapeDrivesState.get(), m_logger);
  TapeMountDecisionInfo& tmdi = *privateRet;

  privateRet->lock(logicalLibraryName);

  // Get all the tape pools and tapes with queues (potential mounts)
  auto lockSchedGlobalTime = t.secs(utils::Timer::resetCounter);
  fetchMountInfo(tmdi, SchedulerDatabase::PurposeGetMountInfo::GET_NEXT_MOUNT, logContext);

  auto fetchMountInfoTime = t.secs(utils::Timer::resetCounter);
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> ret(std::move(privateRet));
  log::ScopedParamContainer params(logContext);
  params.add("lockSchedGlobalTime", lockSchedGlobalTime).add("fetchMountInfoTime", fetchMountInfoTime);
  logContext.log(log::INFO, "In RelationalDB::getMountInfo(): success.");

  return ret;
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo>
RelationalDB::getMountInfoNoLock(PurposeGetMountInfo purpose, log::LogContext& logContext) {
  utils::Timer t;

  // Allocate the getMountInfostructure to return
  auto privateRet =
    std::make_unique<schedulerdb::TapeMountDecisionInfo>(*this, m_ownerId, m_tapeDrivesState.get(), m_logger);
  TapeMountDecisionInfo& tmdi = *privateRet;

  // Get all the tape pools and tapes with queues (potential mounts)
  auto fetchNoLockTime = t.secs(utils::Timer::resetCounter);
  fetchMountInfo(tmdi, purpose, logContext);
  auto fetchMountInfoTime = t.secs(utils::Timer::resetCounter);
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> ret(std::move(privateRet));
  log::ScopedParamContainer params(logContext);
  params.add("fetchNoLockTime", fetchNoLockTime).add("fetchMountInfoTime", fetchMountInfoTime);
  logContext.log(log::INFO, "In RelationalDB::getMountInfoNoLock(): success.");
  return ret;
}

void RelationalDB::fetchMountInfo(SchedulerDatabase::TapeMountDecisionInfo& tmdi,
                                  SchedulerDatabase::PurposeGetMountInfo purpose,
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
  std::unordered_map<std::string, RelationalDB::DiskSleepEntry> diskSystemSleepMap = getActiveSleepDiskSystemNamesToFilter(lc);
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
        const auto &entry = it->second;
        auto now = static_cast<uint64_t>(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

        if (now < (entry.timestamp + entry.sleepTime)) {
          m.sleepingMount = true;
          m.sleepStartTime = entry.timestamp;
          m.diskSystemSleptFor = now - entry.timestamp;
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

std::list<SchedulerDatabase::RetrieveQueueCleanupInfo>
RelationalDB::getRetrieveQueuesCleanupInfo(log::LogContext& logContext) {
  throw cta::exception::NotImplementedException();
}

void RelationalDB::cleanRetrieveQueueForVid(const std::string& vid, log::LogContext& logContext) {
  schedulerdb::Transaction txn(m_connPool);
  try {
    uint64_t movedJobsForReporting =
      schedulerdb::postgres::RetrieveJobQueueRow::handlePendingRetrieveJobsAfterTapeStateChange(txn, vid);
    log::ScopedParamContainer(logContext)
      .add("VID", vid)
      .add("movedJobsForReporting", movedJobsForReporting)
      .log(log::INFO,
           "In RelationalDB::cleanRetrieveQueueForVid(): removed all retrieve jobs of the specified VID from the "
           "pending queue. If error report URL was available, it moved jobs to the reporting workflow.");
    txn.commit();
  } catch (exception::Exception& ex) {
    logContext.log(
      cta::log::ERR,
      "In RelationalDB::cleanRetrieveQueueForVid(): failed to remove retrieve jobs from the pending queue of the "
      "specified tape VID. Aborting the transaction." +
        ex.getMessageValue());
    txn.abort();
    throw;
  }
  return;
}

uint64_t RelationalDB::insertOrUpdateDiskSleepEntry(
          schedulerdb::Transaction &txn,
          const std::string &diskSystemName,
          const RelationalDB::DiskSleepEntry &entry)
{
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

std::unordered_map<std::string, RelationalDB::DiskSleepEntry> RelationalDB::getDiskSystemSleepStatus(rdbms::Conn &conn) {
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

uint64_t RelationalDB::removeDiskSystemSleepEntries(schedulerdb::Transaction &txn,
                                                    const std::vector <std::string> &expiredDiskSystemNames) {
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

std::unordered_map<std::string, RelationalDB::DiskSleepEntry> RelationalDB::getActiveSleepDiskSystemNamesToFilter(log::LogContext& logContext) {
  cta::threading::MutexLocker ml(m_diskSystemSleepMutex);
  auto conn = m_connPool.getConn();
  std::unordered_map<std::string, RelationalDB::DiskSleepEntry> diskSystemSleepMap = getDiskSystemSleepStatus(conn);
  conn.commit();
  if (diskSystemSleepMap.empty()) {
    return diskSystemSleepMap;
  }
  uint64_t currentTime = static_cast<uint64_t>(std::chrono::system_clock::to_time_t(
    std::chrono::system_clock::now()));

  std::vector<std::string> expiredDiskSystems;
  auto it = diskSystemSleepMap.begin();
  while (it != diskSystemSleepMap.end()) {
    const std::string& diskName = it->first;
    const RelationalDB::DiskSleepEntry& entry = it->second;
    cta::log::ScopedParamContainer(logContext)
              .add("currentTime", currentTime)
              .add("entry.timestamp", entry.timestamp)
              .add("entry.sleepTime", entry.sleepTime)
              .add("currentTime-entry.timestamp", currentTime - entry.timestamp)
              .log(cta::log::DEBUG,
                   "In RelationalDB::getActiveSleepDiskSystemNamesToFilter(): Checking sleeping disk systems.");
    if (currentTime - entry.timestamp > entry.sleepTime) {
      // erase; iterator is invalidated, but the next one is returned
      it = diskSystemSleepMap.erase(it);
      expiredDiskSystems.push_back(diskName);
    } else {
      ++it;
    }
  }
  if(!expiredDiskSystems.empty()){
    schedulerdb::Transaction txn(m_connPool);
    try{
      uint64_t nrows = removeDiskSystemSleepEntries(txn, expiredDiskSystems);
      txn.commit();
      cta::log::ScopedParamContainer(logContext)
              .add("nrows", nrows)
              .log(cta::log::INFO,
                   "In RelationalDB::getActiveSleepDiskSystemNamesToFilter(): Removed disk system sleep entries from the DB.");
     } catch (const std::exception& ex) {
      cta::log::ScopedParamContainer(logContext)
          .add("exceptionWhat", ex.what())
          .log(cta::log::ERR, "In RelationalDB::getActiveSleepDiskSystemNamesToFilter(): Failed to remove disk system sleep entries from DB.");
      txn.abort();
    }
  }

  return diskSystemSleepMap;
}

}  // namespace cta
