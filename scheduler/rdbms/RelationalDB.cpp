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
#include "scheduler/rdbms/postgres/Transaction.hpp"
#include "scheduler/rdbms/postgres/ArchiveJobSummary.hpp"
#include "scheduler/rdbms/postgres/ArchiveJobQueue.hpp"
#include "scheduler/rdbms/ArchiveJob.hpp"
#include "scheduler/rdbms/ArchiveRequest.hpp"
#include "scheduler/rdbms/TapeMountDecisionInfo.hpp"
#include "scheduler/rdbms/Helpers.hpp"
#include "scheduler/rdbms/RetrieveRequest.hpp"
#include "scheduler/rdbms/RepackRequest.hpp"

namespace cta {

RelationalDB::RelationalDB( const std::string &ownerId,
                                  log::Logger &logger,
                                  catalogue::Catalogue &catalogue,
                                  const rdbms::Login &login,
                                  const uint64_t nbConns) :
   m_ownerId(ownerId),
   m_connPool(login, nbConns),
   m_catalogue(catalogue),
   m_logger(logger)
{
  m_tapeDrivesState = std::make_unique<TapeDrivesCatalogueState>(m_catalogue);
}

RelationalDB::~RelationalDB() = default;

void RelationalDB::waitSubthreadsComplete()
{
   throw cta::exception::Exception("Not implemented");
}

void RelationalDB::ping()
{
  try {
    // TO-DO: we might prefer to check schema version instead
    auto conn = m_connPool.getConn();
    const auto names = conn.getTableNames();
    bool found_scheddb = false;
    for(auto &name : names) {
      if ("CTA_SCHEDULER" == name) {
        found_scheddb = true;
      }
    }
    if(!found_scheddb) {
      throw cta::exception::Exception("Did not find CTA_SCHEDULER table in the Postgres Scheduler DB.");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::string RelationalDB::queueArchive(const std::string &instanceName, const cta::common::dataStructures::ArchiveRequest &request,
    const cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId &criteria, log::LogContext &logContext)
{
  utils::Timer timer;

  // Construct the archive request object
  auto aReq = std::make_unique<schedulerdb::ArchiveRequest>(m_connPool, logContext);

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
  aReq->setArchiveFile(aFile);
  aReq->setMountPolicy(criteria.mountPolicy);
  aReq->setArchiveReportURL(request.archiveReportURL);
  aReq->setArchiveErrorReportURL(request.archiveErrorReportURL);
  aReq->setRequester(request.requester);
  aReq->setSrcURL(request.srcURL);
  aReq->setEntryLog(request.creationLog);

  std::list<schedulerdb::ArchiveRequest::JobDump> jl;
  for(auto & [key, value]:criteria.copyToPoolMap) {
    const uint32_t hardcodedRetriesWithinMount = 2;
    const uint32_t hardcodedTotalRetries = 2;
    const uint32_t hardcodedReportRetries = 2;
    aReq->addJob(key, value, hardcodedRetriesWithinMount, hardcodedTotalRetries, hardcodedReportRetries);
    jl.emplace_back();
    jl.back().copyNb = key;
    jl.back().tapePool = value;
  }

  if(jl.empty()) {
    throw schedulerdb::ArchiveRequestHasNoCopies("In RelationalDB::queueArchive: the archive request has no copies");
  }

  utils::Timer timerinsert;
  // Insert the object into the DB
  aReq->insert();
  // Commit the transaction
  aReq->commit();
  log::ScopedParamContainer params(logContext);
  params.add("InsertCommitTimeSec", timerinsert.secs());
  logContext.log(log::DEBUG, "In RelationalDB::queueArchive(): insert() and commit() done.");

  return aReq->getIdStr();
}

std::map<std::string, std::list<common::dataStructures::ArchiveJob>, std::less<>> RelationalDB::getArchiveJobs() const
{
   throw cta::exception::Exception("Not implemented");
}

std::list<cta::common::dataStructures::ArchiveJob> RelationalDB::getArchiveJobs(const std::string& tapePoolName) const
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::IArchiveJobQueueItor> RelationalDB::getArchiveJobQueueItor(const std::string &tapePoolName,
    common::dataStructures::JobQueueType queueType) const
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > RelationalDB::getNextArchiveJobsToReportBatch(uint64_t filesRequested,
     log::LogContext & logContext)
{
  rdbms::Rset resultSet_ForTransfer;
  rdbms::Rset resultSet_ForFailure;
  auto sqlconn_fortransfer = m_connPool.getConn();
  //auto sqlconn_forfailure = m_connPool.getConn();
  logContext.log(log::DEBUG, "In RelationalDB::getNextArchiveJobsToReportBatch(): Before getting archive row.");
  // retrieve batch up to file limit
  resultSet_ForTransfer = cta::schedulerdb::postgres::ArchiveJobQueueRow::selectJobsByStatus(
          sqlconn_fortransfer, schedulerdb::ArchiveJobStatus::AJS_ToReportToUserForTransfer, filesRequested);
  logContext.log(log::DEBUG, "In RelationalDB::getNextArchiveJobsToReportBatch(): After getting archive row AJS_ToReportToUserForTransfer.");
  //resultSet_ForFailure = cta::schedulerdb::postgres::ArchiveJobQueueRow::selectJobsByStatus(
  //        sqlconn_forfailure, schedulerdb::ArchiveJobStatus::AJS_ToReportToUserForFailure, filesRequested);
  //logContext.log(log::DEBUG, "In RelationalDB::getNextArchiveJobsToReportBatch(): After getting archive row AJS_ToReportToUserForFailure.");
  std::list<cta::schedulerdb::postgres::ArchiveJobQueueRow> jobs;
  logContext.log(log::DEBUG, "In RelationalDB::getNextArchiveJobsToReportBatch(): Before Next Result is fetched.");
  if(!resultSet_ForTransfer.isEmpty()){
    try {
      while(resultSet_ForTransfer.next()) {
        logContext.log(log::DEBUG,
                       "In RelationalDB::getNextArchiveJobsToReportBatch(): After Next resultSet_ForTransfer is fetched.");
        jobs.emplace_back(resultSet_ForTransfer);
      }
    } catch (cta::exception::Exception & e) {
      std::string bt = e.backtrace();
      logContext.log(log::DEBUG, "In RelationalDB::getNextArchiveJobsToReportBatch(): Exception thrown: " + bt);
    }
    logContext.log(log::DEBUG, "In RelationalDB::getNextArchiveJobsToReportBatch(): After emplace_back resultSet_ForTransfer.");
  }
  logContext.log(log::DEBUG, "In RelationalDB::getNextArchiveJobsToReportBatch(): Before Archive Jobs filled.");
  // Construct the return value
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ret;
  for (const auto &j : jobs) {
    auto aj = std::make_unique<schedulerdb::ArchiveJob>(true, j.mountId.value(), j.jobId, j.tapePool);
    aj->tapeFile.copyNb = j.copyNb;
    aj->archiveFile = j.archiveFile;
    aj->archiveReportURL = j.archiveReportUrl;
    aj->errorReportURL = j.archiveErrorReportUrl;
    aj->srcURL = j.srcUrl;
    ret.emplace_back(std::move(aj));
  }
  logContext.log(log::DEBUG, "In RelationalDB::getNextArchiveJobsToReportBatch(): After Archive Jobs filled, before return.");

  return ret;
}

SchedulerDatabase::JobsFailedSummary RelationalDB::getArchiveJobsFailedSummary(log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> RelationalDB::getNextRetrieveJobsToTransferBatch(const std::string & vid, uint64_t filesRequested, log::LogContext &lc)
{
  throw cta::exception::Exception("Not implemented");
}

void RelationalDB::requeueRetrieveRequestJobs(std::list<cta::SchedulerDatabase::RetrieveJob *> &jobs, log::LogContext &lc)
{
throw cta::exception::Exception("Not implemented");
}

void RelationalDB::reserveRetrieveQueueForCleanup(const std::string & vid, std::optional<uint64_t> cleanupHeartBeatValue)
{
  throw cta::exception::Exception("Not implemented");
}

void RelationalDB::tickRetrieveQueueCleanupHeartbeat(const std::string & vid)
{
  throw cta::exception::Exception("Not implemented");
}

void RelationalDB::setArchiveJobBatchReported(std::list<SchedulerDatabase::ArchiveJob*> & jobsBatch,
     log::TimingList & timingList, utils::Timer & t, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::list<SchedulerDatabase::RetrieveQueueStatistics> RelationalDB::getRetrieveQueueStatistics(
    const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria, const std::set<std::string>& vidsToConsider)
{
   throw cta::exception::Exception("Not implemented");
}

void RelationalDB::clearStatisticsCache(const std::string & vid)
{
  schedulerdb::Helpers::flushStatisticsCacheForVid(vid);
}

void RelationalDB::setStatisticsCacheConfig(const StatisticsCacheConfig & conf) {
  if (conf.retrieveQueueCacheMaxAgeSecs.has_value()) {
    schedulerdb::Helpers::setRetrieveQueueCacheMaxAgeSecs(conf.retrieveQueueCacheMaxAgeSecs.value());
  }
  if (conf.tapeCacheMaxAgeSecs.has_value()) {
    schedulerdb::Helpers::setTapeCacheMaxAgeSecs(conf.tapeCacheMaxAgeSecs.value());
  }
}

SchedulerDatabase::RetrieveRequestInfo RelationalDB::queueRetrieve(cta::common::dataStructures::RetrieveRequest& rqst,
    const cta::common::dataStructures::RetrieveFileQueueCriteria &criteria, const std::optional<std::string> diskSystemName,
    log::LogContext &logContext)
{
  utils::Timer timer;
  schedulerdb::Transaction txn(m_connPool);

   // Get the best vid from the cache
  std::set<std::string, std::less<>> candidateVids;
  for (auto & tf:criteria.archiveFile.tapeFiles) candidateVids.insert(tf.vid);

  SchedulerDatabase::RetrieveRequestInfo ret;
  ret.selectedVid=cta::schedulerdb::Helpers::selectBestVid4Retrieve(candidateVids, m_catalogue, txn, false);

  uint8_t bestCopyNb=0;
  for(auto & tf: criteria.archiveFile.tapeFiles) {
    if (tf.vid == ret.selectedVid) {
      bestCopyNb = tf.copyNb;
      // Appending the file size to the dstURL so that
      // XrootD will fail to retrieve if there is not enough free space
      // in the eos disk
      rqst.appendFileSizeToDstURL(tf.fileSize);
      break;
    }
  }

  // In order to post the job, construct it first in memory.
  auto rReq = std::make_unique<cta::schedulerdb::RetrieveRequest>(m_connPool,logContext);
  ret.requestId = rReq->getIdStr();
  rReq->setSchedulerRequest(rqst);
  rReq->setRetrieveFileQueueCriteria(criteria);
  rReq->setActivityIfNeeded(rqst, criteria);
  rReq->setCreationTime(rqst.creationLog.time);
  rReq->setIsVerifyOnly(rqst.isVerifyOnly);
  if (diskSystemName) rReq->setDiskSystemName(diskSystemName.value());

  rReq->setActiveCopyNumber(bestCopyNb);
  rReq->insert();

  // Commit the transaction
  rReq->commit();

  return ret;
}

void RelationalDB::cancelRetrieve(const std::string& instanceName, const cta::common::dataStructures::CancelRetrieveRequest& rqst,
    log::LogContext& lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::map<std::string, std::list<RetrieveRequestDump> > RelationalDB::getRetrieveRequests() const
{
   throw cta::exception::Exception("Not implemented");
}

std::list<RetrieveRequestDump> RelationalDB::getRetrieveRequestsByVid(const std::string& vid) const
{
   throw cta::exception::Exception("Not implemented");
}

std::list<RetrieveRequestDump> RelationalDB::getRetrieveRequestsByRequester(const std::string& vid) const
{
   throw cta::exception::Exception("Not implemented");
}

void RelationalDB::deleteRetrieveRequest(const common::dataStructures::SecurityIdentity& requester,
    const std::string& remoteFile)
{
   throw cta::exception::Exception("Not implemented");
}

void RelationalDB::cancelArchive(const common::dataStructures::DeleteArchiveRequest& request, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

void RelationalDB::deleteFailed(const std::string &objectId, log::LogContext &lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::map<std::string, std::list<common::dataStructures::RetrieveJob>, std::less<>> RelationalDB::getRetrieveJobs() const
{
   throw cta::exception::Exception("Not implemented");
}

std::list<cta::common::dataStructures::RetrieveJob> RelationalDB::getRetrieveJobs(const std::string &vid) const
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::IRetrieveJobQueueItor> RelationalDB::getRetrieveJobQueueItor(const std::string &vid,
    common::dataStructures::JobQueueType queueType) const
{
   throw cta::exception::Exception("Not implemented");
}

std::string RelationalDB::queueRepack(const SchedulerDatabase::QueueRepackRequest & repackRequest, log::LogContext &logContext)
{
  std::string vid = repackRequest.m_vid;
  common::dataStructures::RepackInfo::Type repackType = repackRequest.m_repackType;

  std::string bufferURL = repackRequest.m_repackBufferURL;
  common::dataStructures::MountPolicy mountPolicy = repackRequest.m_mountPolicy;

  // Prepare the repack request object in memory.
  cta::utils::Timer t;
  auto rr=std::make_unique<cta::schedulerdb::RepackRequest>(m_connPool,m_catalogue,logContext);
  rr->setVid(vid);
  rr->setType(repackType);
  rr->setBufferURL(bufferURL);
  rr->setMountPolicy(mountPolicy);
  rr->setNoRecall(repackRequest.m_noRecall);
  rr->setCreationLog(repackRequest.m_creationLog);
  rr->insert();
  rr->commit();

  return rr->getIdStr();
}

//------------------------------------------------------------------------------
// RelationalDB::repackExists()
//------------------------------------------------------------------------------
bool RelationalDB::repackExists() {
    throw cta::exception::Exception("Not implemented");
}

std::list<common::dataStructures::RepackInfo> RelationalDB::getRepackInfo()
{
  log::LogContext lc(m_logger);
  lc.log(log::WARNING, "RelationalDB::getRepackInfo() dummy implementation !");
  std::list<common::dataStructures::RepackInfo> ret;
  return ret;
}

common::dataStructures::RepackInfo RelationalDB::getRepackInfo(const std::string& vid)
{
  log::LogContext lc(m_logger);
  lc.log(log::WARNING, "RelationalDB::getRepackInfo() dummy implementation !");
  common::dataStructures::RepackInfo ret;
  return ret;
}

void RelationalDB::cancelRepack(const std::string& vid, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

//------------------------------------------------------------------------------
// RelationalDB::RepackRequestPromotionStatistics::RepackRequestPromotionStatistics()
//------------------------------------------------------------------------------
RelationalDB::RepackRequestPromotionStatistics::RepackRequestPromotionStatistics() = default;

//------------------------------------------------------------------------------
// RelationalDB::RepackRequestPromotionStatistics::promotePendingRequestsForExpansion()
//------------------------------------------------------------------------------
auto RelationalDB::RepackRequestPromotionStatistics::promotePendingRequestsForExpansion(
        size_t requestCount, log::LogContext& lc) -> PromotionToToExpandResult {
  lc.log(log::WARNING, "RelationalDB::RepackRequestPromotionStatistics::promotePendingRequestsForExpansion() dummy implementation !");
  PromotionToToExpandResult ret;
  using Status = common::dataStructures::RepackInfo::Status;
  ret.pendingBefore = at(Status::Pending);
  ret.toEnpandBefore = at(Status::ToExpand);
  return ret;
}

//------------------------------------------------------------------------------
// RelationalDB::populateRepackRequestsStatistics()
//------------------------------------------------------------------------------
void RelationalDB::populateRepackRequestsStatistics(SchedulerDatabase::RepackRequestStatistics& stats) {
  log::LogContext lc(m_logger);
  lc.log(log::WARNING, "RelationalDB::populateRepackRequestsStatistics() dummy implementation !");
  // Ensure existence of stats for important statuses
  using Status = common::dataStructures::RepackInfo::Status;
  for (auto s : {Status::Pending, Status::ToExpand, Status::Starting, Status::Running}) {
    stats[s] = 0;
  }
}

auto RelationalDB::getRepackStatisticsNoLock() -> std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> {
  log::LogContext lc(m_logger);
  lc.log(log::WARNING, "RelationalDB::getRepackStatisticsNoLock() dummy implementation !");
  auto typedRet = std::make_unique<RelationalDB::RepackRequestPromotionStatisticsNoLock>();
  populateRepackRequestsStatistics(*typedRet);
  std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> ret(typedRet.release());
  return ret;
}

auto RelationalDB::getRepackStatistics() -> std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> {
  log::LogContext lc(m_logger);
  lc.log(log::WARNING, "RelationalDB::getRepackStatistics() dummy implementation !");
  return getRepackStatisticsNoLock();
}

std::unique_ptr<SchedulerDatabase::RepackRequest> RelationalDB::getNextRepackJobToExpand()
{
  log::LogContext lc(m_logger);
  lc.log(log::WARNING, "RelationalDB::getNextRepackJobToExpand() dummy implementation !");
  std::unique_ptr<SchedulerDatabase::RepackRequest> ret;
  return ret;
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> RelationalDB::getNextRetrieveJobsToReportBatch(
    uint64_t filesRequested, log::LogContext &logContext)
{
  log::LogContext lc(m_logger);
  lc.log(log::WARNING, "RelationalDB::getNextRetrieveJobsToReportBatch() dummy implementation !");
  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> ret;
  return ret;
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> RelationalDB::getNextRetrieveJobsFailedBatch(
    uint64_t filesRequested, log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> RelationalDB::getNextRepackReportBatch(log::LogContext& lc)
{
  lc.log(log::WARNING, "RelationalDB::getNextRepackReportBatch() dummy implementation !");
  return nullptr;
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(log::LogContext& lc)
{
  lc.log(log::WARNING, "RelationalDB::getNextSuccessfulRetrieveRepackReportBatch() dummy implementation !");
  throw NoRepackReportBatchFound("In RelationalDB::getNextSuccessfulRetrieveRepackReportBatch(): no report found.");
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> RelationalDB::getNextSuccessfulArchiveRepackReportBatch(log::LogContext& lc)
{
  lc.log(log::WARNING, "RelationalDB::getNextSuccessfulArchiveRepackReportBatch() dummy implementation !");
  throw NoRepackReportBatchFound("In RelationalDB::getNextSuccessfulArchiveRepackReportBatch(): no report found.");
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> RelationalDB::getNextFailedRetrieveRepackReportBatch(log::LogContext& lc)
{
  lc.log(log::WARNING, "RelationalDB::getNextFailedRetrieveRepackReportBatch() dummy implementation !");
  throw NoRepackReportBatchFound("In RelationalDB::getNextFailedRetrieveRepackReportBatch(): no report found.");
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> RelationalDB::getNextFailedArchiveRepackReportBatch(log::LogContext &lc)
{
  lc.log(log::WARNING, "RelationalDB::getNextFailedArchiveRepackReportBatch() dummy implementation !");
  throw NoRepackReportBatchFound("In RelationalDB::getNextFailedArchiveRepackReportBatch(): no report found.");
}

std::list<std::unique_ptr<SchedulerDatabase::RepackReportBatch>> RelationalDB::getRepackReportBatches(log::LogContext &lc)
{
  lc.log(log::WARNING, "RelationalDB::getRepackReportBatches() dummy implementation !");
  std::list<std::unique_ptr<SchedulerDatabase::RepackReportBatch>> ret;
  return ret;
}

void RelationalDB::setRetrieveJobBatchReportedToUser(std::list<SchedulerDatabase::RetrieveJob*> & jobsBatch,
     log::TimingList & timingList, utils::Timer & t, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

SchedulerDatabase::JobsFailedSummary RelationalDB::getRetrieveJobsFailedSummary(log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> RelationalDB::getMountInfo(log::LogContext& logContext)
{
  return RelationalDB::getMountInfo(logContext, 0);
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> RelationalDB::getMountInfo(log::LogContext& logContext, uint64_t timeout_us)
{
  utils::Timer t;

  // Allocate the getMountInfostructure to return.
  auto privateRet = std::make_unique<schedulerdb::TapeMountDecisionInfo>(*this, m_ownerId, m_tapeDrivesState.get(), m_logger);
  TapeMountDecisionInfo& tmdi = *privateRet;

  // Take an exclusive lock on the scheduling
  privateRet->lock();

  // Get all the tape pools and tapes with queues (potential mounts)
  auto lockSchedGlobalTime = t.secs(utils::Timer::resetCounter);
  fetchMountInfo(tmdi, SchedulerDatabase::PurposeGetMountInfo::GET_NEXT_MOUNT, logContext);

  auto fetchMountInfoTime = t.secs(utils::Timer::resetCounter);
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> ret(std::move(privateRet));
  log::ScopedParamContainer params(logContext);
  params.add("lockSchedGlobalTime", lockSchedGlobalTime)
        .add("fetchMountInfoTime", fetchMountInfoTime);
  logContext.log(log::INFO, "In RelationalDB::getMountInfo(): success.");

  return ret;
}

void RelationalDB::trimEmptyQueues(log::LogContext& lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> RelationalDB::getMountInfoNoLock(PurposeGetMountInfo purpose,
    log::LogContext& logContext)
{
  utils::Timer t;

  // Allocate the getMountInfostructure to return
  auto privateRet = std::make_unique<schedulerdb::TapeMountDecisionInfo>(*this, m_ownerId, m_tapeDrivesState.get(), m_logger);
  TapeMountDecisionInfo& tmdi = *privateRet;


  // Get all the tape pools and tapes with queues (potential mounts)
  auto fetchNoLockTime = t.secs(utils::Timer::resetCounter);
  fetchMountInfo(tmdi, purpose, logContext);

  auto fetchMountInfoTime = t.secs(utils::Timer::resetCounter);
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> ret(std::move(privateRet));
  log::ScopedParamContainer params(logContext);
  params.add("fetchNoLockTime", fetchNoLockTime)
        .add("fetchMountInfoTime", fetchMountInfoTime);
  logContext.log(log::INFO, "In RelationalDB::getMountInfoNoLock(): success.");
  return ret;
}

void RelationalDB::fetchMountInfo(SchedulerDatabase::TapeMountDecisionInfo& tmdi, SchedulerDatabase::PurposeGetMountInfo purpose, log::LogContext& lc)
{
  utils::Timer t;
  utils::Timer t2;
  lc.log(log::DEBUG, "In RelationalDB::fetchMountInfo(): starting to fetch mount info.");
  // Get a reference to the transaction, which may or may not be holding the scheduler global lock

  auto &txn = static_cast<schedulerdb::TapeMountDecisionInfo*>(&tmdi)->m_txn;

  // Map of mount policies. getCachedMountPolicies() should be refactored to return a map instead of a list. In the meantime, copy the values into a local map.
  std::map<std::string,common::dataStructures::MountPolicy, std::less<>> cachedMountPolicies;
  for(const auto &mp : m_catalogue.MountPolicy()->getCachedMountPolicies()) {
    cachedMountPolicies[mp.name] = mp;
  }

  // Map of (mount type, tapepool/vid) -> PotentialMount to aggregate queue info
  std::map<std::pair<common::dataStructures::MountType, std::string>,SchedulerDatabase::PotentialMount> potentialMounts;

  // Iterate over all archive queues
  auto rset = cta::schedulerdb::postgres::ArchiveJobSummaryRow::selectNotOwned(txn);
  while(rset.next()) {
    cta::schedulerdb::postgres::ArchiveJobSummaryRow ajsr(rset);
    // Set the queue type
    common::dataStructures::MountType mountType;
    switch(ajsr.status) {
      case schedulerdb::ArchiveJobStatus::AJS_ToTransferForUser:
        mountType = common::dataStructures::MountType::ArchiveForUser; break;
      case schedulerdb::ArchiveJobStatus::AJS_ToTransferForRepack:
        mountType = common::dataStructures::MountType::ArchiveForRepack; break;
      default:
	continue;
    }
    // Get statistics for User and Repack archive queues
    auto &m = potentialMounts[std::make_pair(mountType, ajsr.tapePool)];
    m.type = mountType;
    m.tapePool = ajsr.tapePool;
    m.bytesQueued += ajsr.jobsTotalSize;
    m.filesQueued += ajsr.jobsCount;
    m.oldestJobStartTime = ajsr.oldestJobStartTime < m.oldestJobStartTime ? ajsr.oldestJobStartTime : m.oldestJobStartTime;
    // The cached mount policies take priority. If the mount policy has been deleted from the catalogue,
    // we fall back to the mount policy values cached in the queue.
    uint64_t priority;
    time_t minRequestAge;
    if(auto mpIt = cachedMountPolicies.find(ajsr.mountPolicy); mpIt != cachedMountPolicies.end()) {
      priority = mpIt->second.archivePriority;
      minRequestAge = mpIt->second.archiveMinRequestAge;
    } else {
      priority = ajsr.archivePriority;
      minRequestAge = ajsr.archiveMinRequestAge;
    }
    m.priority = priority > m.priority ? priority : m.priority;
    m.minRequestAge = minRequestAge < m.minRequestAge ? minRequestAge : m.minRequestAge;
    m.logicalLibrary = "";
  }

  // Copy the aggregated Potential Mounts into the TapeMountDecisionInfo
  for(const auto &[mt, pm] : potentialMounts) {
    lc.log(log::DEBUG, "In RelationalDB::fetchMountInfo(): pushing back potential mount to the vector.");
    tmdi.potentialMounts.push_back(pm);
  }

  lc.log(log::DEBUG, "In RelationalDB::fetchMountInfo(): getting drive state.");
  // Collect information about existing and next mounts. If a next mount exists the drive "counts double",
  // but the corresponding drive is either about to mount, or about to replace its current mount.
  const auto driveStates = m_catalogue.DriveState()->getTapeDrives();
  auto registerFetchTime = t.secs(utils::Timer::resetCounter);

  for(const auto& driveState : driveStates) {
    switch(driveState.driveStatus) {
      case common::dataStructures::DriveStatus::Starting:
      case common::dataStructures::DriveStatus::Mounting:
      case common::dataStructures::DriveStatus::Transferring:
      case common::dataStructures::DriveStatus::Unloading:
      case common::dataStructures::DriveStatus::Unmounting:
      case common::dataStructures::DriveStatus::DrainingToDisk:
      case common::dataStructures::DriveStatus::CleaningUp: {
        tmdi.existingOrNextMounts.emplace_back(ExistingMount());
        auto& existingMount = tmdi.existingOrNextMounts.back();
        existingMount.type = driveState.mountType;
        existingMount.tapePool = driveState.currentTapePool ? driveState.currentTapePool.value() : "";
        existingMount.vo = driveState.currentVo ? driveState.currentVo.value() : "";
        existingMount.driveName = driveState.driveName;
        existingMount.vid = driveState.currentVid ? driveState.currentVid.value() : "";
        existingMount.currentMount = true;
        existingMount.bytesTransferred = driveState.bytesTransferedInSession ? driveState.bytesTransferedInSession.value() : 0;
        existingMount.filesTransferred = driveState.filesTransferedInSession ? driveState.filesTransferedInSession.value() : 0;
        if(driveState.filesTransferedInSession && driveState.sessionElapsedTime && driveState.sessionElapsedTime.value() > 0) {
          existingMount.averageBandwidth = driveState.filesTransferedInSession.value() / driveState.sessionElapsedTime.value();
        } else {
          existingMount.averageBandwidth = 0.0;
        }
        existingMount.activity = driveState.currentActivity ? driveState.currentActivity.value() : "";
      }
      default: break;
    }

    if(driveState.nextMountType == common::dataStructures::MountType::NoMount) continue;
    switch(driveState.nextMountType) {
      case common::dataStructures::MountType::ArchiveForUser:
      case common::dataStructures::MountType::ArchiveForRepack:
      case common::dataStructures::MountType::Retrieve:
      case common::dataStructures::MountType::Label: {
        tmdi.existingOrNextMounts.emplace_back(ExistingMount());
        auto& nextMount = tmdi.existingOrNextMounts.back();
        nextMount.type = driveState.nextMountType;
        nextMount.tapePool = driveState.nextTapePool ? driveState.nextTapePool.value() : "";
        nextMount.vo = driveState.nextVo ? driveState.nextVo.value() : "";
        nextMount.driveName = driveState.driveName;
        nextMount.vid = driveState.nextVid ? driveState.nextVid.value() : "";
        nextMount.currentMount = false;
        nextMount.bytesTransferred = 0;
        nextMount.filesTransferred = 0;
        nextMount.averageBandwidth = 0;
        nextMount.activity = driveState.nextActivity ? driveState.nextActivity.value() : "";
      }
      default: break;
    }
  }
  auto registerProcessingTime = t.secs(utils::Timer::resetCounter);
  log::ScopedParamContainer params(lc);
  params.add("queueFetchTime", registerFetchTime)
        .add("processingTime", registerProcessingTime);
  lc.log(log::DEBUG, "In RelationalDB::fetchMountInfo(): fetched the drive register.");
}

std::list<SchedulerDatabase::RetrieveQueueCleanupInfo> RelationalDB::getRetrieveQueuesCleanupInfo(log::LogContext& logContext)
{
   throw cta::exception::Exception("Not implemented");
}

void RelationalDB::setRetrieveQueueCleanupFlag(const std::string&vid, bool val, log::LogContext& logContext)
{
   throw cta::exception::Exception("Not implemented");
}


} // namespace cta
