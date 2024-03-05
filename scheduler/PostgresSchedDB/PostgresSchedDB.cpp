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

#include "scheduler/PostgresSchedDB/PostgresSchedDB.hpp"
#include "scheduler/Scheduler.hpp"
#include "catalogue/Catalogue.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "common/exception/Exception.hpp"
#include "scheduler/PostgresSchedDB/sql/Transaction.hpp"
#include "scheduler/PostgresSchedDB/sql/ArchiveJobSummary.hpp"
#include "scheduler/PostgresSchedDB/ArchiveRequest.hpp"
#include "scheduler/PostgresSchedDB/TapeMountDecisionInfo.hpp"
#include "scheduler/PostgresSchedDB/Helpers.hpp"
#include "scheduler/PostgresSchedDB/RetrieveRequest.hpp"
#include "scheduler/PostgresSchedDB/RepackRequest.hpp"

namespace cta {

PostgresSchedDB::PostgresSchedDB( const std::string &ownerId,
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

PostgresSchedDB::~PostgresSchedDB() = default;

void PostgresSchedDB::waitSubthreadsComplete()
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::ping()
{
  try {
    // we might prefer to check schema version instead
    auto conn = m_connPool.getConn();
    const auto names = conn.getTableNames();
    for(auto &name : names) {
      if("cta_scheduler" == name) {
        break;
      } else {
        throw cta::exception::Exception("Did not find cta_scheduler table in the PostgresDB. Found: " + name);
      }
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::string PostgresSchedDB::queueArchive(const std::string &instanceName, const cta::common::dataStructures::ArchiveRequest &request,
    const cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId &criteria, log::LogContext &logContext)
{
  utils::Timer timer;

  // Construct the archive request object
  auto aReq = std::make_unique<postgresscheddb::ArchiveRequest>(m_connPool, logContext);

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

  std::list<postgresscheddb::ArchiveRequest::JobDump> jl;
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
    throw postgresscheddb::ArchiveRequestHasNoCopies("In PostgresSchedDB::queueArchive: the archive request has no copies");
  }

  // Insert the object into the DB
  aReq->insert();

  // Commit the transaction
  aReq->commit();

  return aReq->getIdStr();
}

std::map<std::string, std::list<common::dataStructures::ArchiveJob>, std::less<>> PostgresSchedDB::getArchiveJobs() const
{
   throw cta::exception::Exception("Not implemented");
}

std::list<cta::common::dataStructures::ArchiveJob> PostgresSchedDB::getArchiveJobs(const std::string& tapePoolName) const
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::IArchiveJobQueueItor> PostgresSchedDB::getArchiveJobQueueItor(const std::string &tapePoolName,
    common::dataStructures::JobQueueType queueType) const
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > PostgresSchedDB::getNextArchiveJobsToReportBatch(uint64_t filesRequested,
     log::LogContext & logContext)
{
   throw cta::exception::Exception("Not implemented");
}

SchedulerDatabase::JobsFailedSummary PostgresSchedDB::getArchiveJobsFailedSummary(log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> PostgresSchedDB::getNextRetrieveJobsToTransferBatch(const std::string & vid, uint64_t filesRequested, log::LogContext &lc)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::requeueRetrieveRequestJobs(std::list<cta::SchedulerDatabase::RetrieveJob *> &jobs, log::LogContext &lc)
{
throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::reserveRetrieveQueueForCleanup(const std::string & vid, std::optional<uint64_t> cleanupHeartBeatValue)
{
  throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::tickRetrieveQueueCleanupHeartbeat(const std::string & vid)
{
  throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::setArchiveJobBatchReported(std::list<SchedulerDatabase::ArchiveJob*> & jobsBatch,
     log::TimingList & timingList, utils::Timer & t, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::list<SchedulerDatabase::RetrieveQueueStatistics> PostgresSchedDB::getRetrieveQueueStatistics(
    const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria, const std::set<std::string>& vidsToConsider)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::clearRetrieveQueueStatisticsCache(const std::string & vid)
{
  postgresscheddb::Helpers::flushRetrieveQueueStatisticsCacheForVid(vid);
}

SchedulerDatabase::RetrieveRequestInfo PostgresSchedDB::queueRetrieve(cta::common::dataStructures::RetrieveRequest& rqst,
    const cta::common::dataStructures::RetrieveFileQueueCriteria &criteria, const std::optional<std::string> diskSystemName,
    log::LogContext &logContext)
{
  utils::Timer timer;
  postgresscheddb::Transaction txn(m_connPool);

   // Get the best vid from the cache
  std::set<std::string> candidateVids;
  for (auto & tf:criteria.archiveFile.tapeFiles) candidateVids.insert(tf.vid);

  SchedulerDatabase::RetrieveRequestInfo ret;
  ret.selectedVid=cta::postgresscheddb::Helpers::selectBestVid4Retrieve(candidateVids, m_catalogue, txn, false);

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
  auto rReq = std::make_unique<cta::postgresscheddb::RetrieveRequest>(m_connPool,logContext);
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

void PostgresSchedDB::cancelRetrieve(const std::string& instanceName, const cta::common::dataStructures::CancelRetrieveRequest& rqst,
    log::LogContext& lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::map<std::string, std::list<RetrieveRequestDump> > PostgresSchedDB::getRetrieveRequests() const
{
   throw cta::exception::Exception("Not implemented");
}

std::list<RetrieveRequestDump> PostgresSchedDB::getRetrieveRequestsByVid(const std::string& vid) const
{
   throw cta::exception::Exception("Not implemented");
}

std::list<RetrieveRequestDump> PostgresSchedDB::getRetrieveRequestsByRequester(const std::string& vid) const
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::deleteRetrieveRequest(const common::dataStructures::SecurityIdentity& requester,
    const std::string& remoteFile)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::cancelArchive(const common::dataStructures::DeleteArchiveRequest& request, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::deleteFailed(const std::string &objectId, log::LogContext &lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::map<std::string, std::list<common::dataStructures::RetrieveJob>, std::less<>> PostgresSchedDB::getRetrieveJobs() const
{
   throw cta::exception::Exception("Not implemented");
}

std::list<cta::common::dataStructures::RetrieveJob> PostgresSchedDB::getRetrieveJobs(const std::string &vid) const
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::IRetrieveJobQueueItor> PostgresSchedDB::getRetrieveJobQueueItor(const std::string &vid,
    common::dataStructures::JobQueueType queueType) const
{
   throw cta::exception::Exception("Not implemented");
}

std::string PostgresSchedDB::queueRepack(const SchedulerDatabase::QueueRepackRequest & repackRequest, log::LogContext &logContext)
{
  std::string vid = repackRequest.m_vid;
  common::dataStructures::RepackInfo::Type repackType = repackRequest.m_repackType;

  std::string bufferURL = repackRequest.m_repackBufferURL;
  common::dataStructures::MountPolicy mountPolicy = repackRequest.m_mountPolicy;

  // Prepare the repack request object in memory.
  cta::utils::Timer t;
  auto rr=std::make_unique<cta::postgresscheddb::RepackRequest>(m_connPool,m_catalogue,logContext);
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
// PostgresSchedDB::repackExists()
//------------------------------------------------------------------------------
bool PostgresSchedDB::repackExists() {
    throw cta::exception::Exception("Not implemented");
}

std::list<common::dataStructures::RepackInfo> PostgresSchedDB::getRepackInfo()
{
   throw cta::exception::Exception("Not implemented");
}

common::dataStructures::RepackInfo PostgresSchedDB::getRepackInfo(const std::string& vid)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::cancelRepack(const std::string& vid, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> PostgresSchedDB::getRepackStatistics()
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> PostgresSchedDB::getRepackStatisticsNoLock()
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackRequest> PostgresSchedDB::getNextRepackJobToExpand()
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> PostgresSchedDB::getNextRetrieveJobsToReportBatch(
    uint64_t filesRequested, log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> PostgresSchedDB::getNextRetrieveJobsFailedBatch(
    uint64_t filesRequested, log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> PostgresSchedDB::getNextRepackReportBatch(log::LogContext& lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> PostgresSchedDB::getNextSuccessfulRetrieveRepackReportBatch(log::LogContext& lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> PostgresSchedDB::getNextSuccessfulArchiveRepackReportBatch(log::LogContext& lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> PostgresSchedDB::getNextFailedRetrieveRepackReportBatch(log::LogContext& lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> PostgresSchedDB::getNextFailedArchiveRepackReportBatch(log::LogContext &lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<SchedulerDatabase::RepackReportBatch>> PostgresSchedDB::getRepackReportBatches(log::LogContext &lc)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::setRetrieveJobBatchReportedToUser(std::list<SchedulerDatabase::RetrieveJob*> & jobsBatch,
     log::TimingList & timingList, utils::Timer & t, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

SchedulerDatabase::JobsFailedSummary PostgresSchedDB::getRetrieveJobsFailedSummary(log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> PostgresSchedDB::getMountInfo(log::LogContext& logContext)
{
  return PostgresSchedDB::getMountInfo(logContext, 0);
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> PostgresSchedDB::getMountInfo(log::LogContext& logContext, uint64_t timeout_us)
{
  utils::Timer t;

  // Allocate the getMountInfostructure to return.
  auto privateRet = std::make_unique<postgresscheddb::TapeMountDecisionInfo>(*this, m_connPool, m_ownerId, m_tapeDrivesState.get(), m_logger);
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
  logContext.log(log::INFO, "In PostgresSchedDB::getMountInfo(): success.");

  return ret;
}

void PostgresSchedDB::trimEmptyQueues(log::LogContext& lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> PostgresSchedDB::getMountInfoNoLock(PurposeGetMountInfo purpose,
    log::LogContext& logContext)
{
  utils::Timer t;

  // Allocate the getMountInfostructure to return
  auto privateRet = std::make_unique<postgresscheddb::TapeMountDecisionInfo>(*this, m_connPool, m_ownerId, m_tapeDrivesState.get(), m_logger);
  TapeMountDecisionInfo& tmdi = *privateRet;


  // Get all the tape pools and tapes with queues (potential mounts)
  auto fetchNoLockTime = t.secs(utils::Timer::resetCounter);
  fetchMountInfo(tmdi, purpose, logContext);

  auto fetchMountInfoTime = t.secs(utils::Timer::resetCounter);
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> ret(std::move(privateRet));
  log::ScopedParamContainer params(logContext);
  params.add("fetchNoLockTime", fetchNoLockTime)
        .add("fetchMountInfoTime", fetchMountInfoTime);
  logContext.log(log::INFO, "In PostgresSchedDB::getMountInfoNoLock(): success.");
  return ret;
}

void PostgresSchedDB::fetchMountInfo(SchedulerDatabase::TapeMountDecisionInfo& tmdi, SchedulerDatabase::PurposeGetMountInfo purpose, log::LogContext& lc)
{
  utils::Timer t;
  utils::Timer t2;
  lc.log(log::INFO, "In PostgresSchedDB::fetchMountInfo(): starting to fetch mount info.");
  // Get a reference to the transaction, which may or may not be holding the scheduler global lock

  auto &txn = static_cast<postgresscheddb::TapeMountDecisionInfo*>(&tmdi)->m_txn;

  // Map of mount policies. getCachedMountPolicies() should be refactored to return a map instead of a list. In the meantime, copy the values into a local map.
  std::map<std::string,common::dataStructures::MountPolicy, std::less<>> cachedMountPolicies;
  for(const auto &mp : m_catalogue.MountPolicy()->getCachedMountPolicies()) {
    cachedMountPolicies[mp.name] = mp;
  }

  // Map of (mount type, tapepool/vid) -> PotentialMount to aggregate queue info
  std::map<std::pair<common::dataStructures::MountType, std::string>,SchedulerDatabase::PotentialMount> potentialMounts;

  // Iterate over all archive queues
  auto rset = cta::postgresscheddb::sql::ArchiveJobSummaryRow::selectNotOwned(txn);
  while(rset.next()) {
    cta::postgresscheddb::sql::ArchiveJobSummaryRow ajsr(rset);
    // Set the queue type
    common::dataStructures::MountType mountType;
    switch(ajsr.status) {
      case postgresscheddb::ArchiveJobStatus::AJS_ToTransferForUser:
        mountType = common::dataStructures::MountType::ArchiveForUser; break;
      case postgresscheddb::ArchiveJobStatus::AJS_ToTransferForRepack:
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
    lc.log(log::INFO, "In PostgresSchedDB::fetchMountInfo(): pushing back potential mount to the vector.");
    tmdi.potentialMounts.push_back(pm);
  }

  lc.log(log::INFO, "In PostgresSchedDB::fetchMountInfo(): getting drive state.");
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
  lc.log(log::INFO, "In PostgresSchedDB::fetchMountInfo(): fetched the drive register.");
}

std::list<SchedulerDatabase::RetrieveQueueCleanupInfo> PostgresSchedDB::getRetrieveQueuesCleanupInfo(log::LogContext& logContext)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::setRetrieveQueueCleanupFlag(const std::string&vid, bool val, log::LogContext& logContext)
{
   throw cta::exception::Exception("Not implemented");
}


} // namespace cta
