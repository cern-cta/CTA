/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "OStoreDB.hpp"
#include "MemQueues.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "objectstore/RootEntry.hpp"
#include "objectstore/ArchiveQueue.hpp"
#include "objectstore/RetrieveQueue.hpp"
#include "objectstore/DriveRegister.hpp"
#include "objectstore/DriveState.hpp"
#include "objectstore/ArchiveRequest.hpp"
#include "objectstore/RetrieveRequest.hpp"
#include "objectstore/Helpers.hpp"
#include "common/exception/Exception.hpp"
#include "common/admin/AdminHost.hpp"
#include "common/admin/AdminUser.hpp"
#include "common/archiveRoutes/ArchiveRoute.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "common/TapePool.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/TapeSessionStats.hpp"
#include <algorithm>
#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */
#include <stdexcept>
#include <set>
#include <iostream>

namespace cta {  
using namespace objectstore;

//------------------------------------------------------------------------------
// OStoreDB::OStoreDB()
//------------------------------------------------------------------------------
OStoreDB::OStoreDB(objectstore::Backend& be, catalogue::Catalogue & catalogue, log::Logger &logger):
  m_objectStore(be), m_catalogue(catalogue), m_logger(logger) {}

//------------------------------------------------------------------------------
// OStoreDB::~OStoreDB()
//------------------------------------------------------------------------------
OStoreDB::~OStoreDB() throw() {}

//------------------------------------------------------------------------------
// OStoreDB::setAgentReference()
//------------------------------------------------------------------------------
void OStoreDB::setAgentReference(objectstore::AgentReference *agentReference) {
  m_agentReference = agentReference;
}

//------------------------------------------------------------------------------
// OStoreDB::assertAgentAddressSet()
//------------------------------------------------------------------------------
void OStoreDB::assertAgentAddressSet() {
  if (!m_agentReference)
    throw AgentNotSet("In OStoreDB::assertAgentSet: Agent address not set");
}

//------------------------------------------------------------------------------
// OStoreDB::ping()
//------------------------------------------------------------------------------
void OStoreDB::ping() {
  // Validate we can lock and fetch the root entry.
  objectstore::RootEntry re(m_objectStore);
  re.fetchNoLock();
}

//------------------------------------------------------------------------------
// OStoreDB::fetchMountInfo()
//------------------------------------------------------------------------------
void OStoreDB::fetchMountInfo(SchedulerDatabase::TapeMountDecisionInfo& tmdi, RootEntry& re, 
    log::LogContext & logContext) {
  utils::Timer t, t2;
  // Walk the archive queues for statistics
  for (auto & aqp: re.dumpArchiveQueues()) {
    objectstore::ArchiveQueue aqueue(aqp.address, m_objectStore);
    // debug utility variable
    std::string __attribute__((__unused__)) poolName = aqp.tapePool;
    objectstore::ScopedSharedLock aqlock;
    double queueLockTime = 0;
    double queueFetchTime = 0;
    try {
      aqueue.fetchNoLock();
      queueFetchTime = t.secs(utils::Timer::resetCounter);
    } catch (cta::exception::Exception &ex) {
      log::ScopedParamContainer params (logContext);
      params.add("queueObject", aqp.address)
            .add("tapepool", aqp.tapePool)
            .add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::WARNING, "In OStoreDB::fetchMountInfo(): failed to lock/fetch an archive queue. Skipping it.");
      continue;
    }
    // If there are files queued, we create an entry for this tape pool in the
    // mount candidates list.
    if (aqueue.getJobsSummary().jobs) {
      tmdi.potentialMounts.push_back(SchedulerDatabase::PotentialMount());
      auto & m = tmdi.potentialMounts.back();
      m.tapePool = aqp.tapePool;
      m.type = cta::common::dataStructures::MountType::Archive;
      m.bytesQueued = aqueue.getJobsSummary().bytes;
      m.filesQueued = aqueue.getJobsSummary().jobs;      
      m.oldestJobStartTime = aqueue.getJobsSummary().oldestJobStartTime;
      m.priority = aqueue.getJobsSummary().priority;
      m.maxDrivesAllowed = aqueue.getJobsSummary().maxDrivesAllowed;
      m.minRequestAge = aqueue.getJobsSummary().minArchiveRequestAge;
      m.logicalLibrary = "";
    } else {
      tmdi.queueTrimRequired = true;
    }
    auto processingTime = t.secs(utils::Timer::resetCounter);
    log::ScopedParamContainer params (logContext);
    params.add("queueObject", aqp.address)
          .add("tapepool", aqp.tapePool)
          .add("queueLockTime", queueLockTime)
          .add("queueFetchTime", queueFetchTime)
          .add("processingTime", processingTime);
    logContext.log(log::INFO, "In OStoreDB::fetchMountInfo(): fetched an archive queue.");
  }
  // Walk the retrieve queues for statistics
  for (auto & rqp: re.dumpRetrieveQueues()) {
    RetrieveQueue rqueue(rqp.address, m_objectStore);
    // debug utility variable
    std::string __attribute__((__unused__)) vid = rqp.vid;
    ScopedSharedLock rqlock;
    double queueLockTime = 0;
    double queueFetchTime = 0;
    try {
      rqueue.fetchNoLock();
      queueFetchTime = t.secs(utils::Timer::resetCounter);
    } catch (cta::exception::Exception &ex) {
      log::LogContext lc(m_logger);
      log::ScopedParamContainer params (lc);
      params.add("queueObject", rqp.address)
            .add("vid", rqp.vid)
            .add("exceptionMessage", ex.getMessageValue());
      lc.log(log::WARNING, "In OStoreDB::fetchMountInfo(): failed to lock/fetch a retrieve queue. Skipping it.");
      continue;
    }
    // If there are files queued, we create an entry for this retrieve queue in the
    // mount candidates list.
    if (rqueue.getJobsSummary().files) {
      tmdi.potentialMounts.push_back(SchedulerDatabase::PotentialMount());
      auto & m = tmdi.potentialMounts.back();
      m.vid = rqp.vid;
      m.type = cta::common::dataStructures::MountType::Retrieve;
      m.bytesQueued = rqueue.getJobsSummary().bytes;
      m.filesQueued = rqueue.getJobsSummary().files;      
      m.oldestJobStartTime = rqueue.getJobsSummary().oldestJobStartTime;
      m.priority = rqueue.getJobsSummary().priority;
      m.maxDrivesAllowed = rqueue.getJobsSummary().maxDrivesAllowed;
      m.minRequestAge = rqueue.getJobsSummary().minRetrieveRequestAge;
      m.logicalLibrary = ""; // The logical library is not known here, and will be determined by the caller.
    } else {
      tmdi.queueTrimRequired = true;
    }
    auto processingTime = t.secs(utils::Timer::resetCounter);
    log::ScopedParamContainer params (logContext);
    params.add("queueObject", rqp.address)
          .add("vid", rqp.vid)
          .add("queueLockTime", queueLockTime)
          .add("queueFetchTime", queueFetchTime)
          .add("processingTime", processingTime);
    logContext.log(log::INFO, "In OStoreDB::fetchMountInfo(): fetched a retrieve queue.");
  }
  // Collect information about the existing and next mounts
  // If a next mount exists the drive "counts double", but the corresponding drive
  // is either about to mount, or about to replace its current mount.
  double registerFetchTime = 0;
  auto driveStates = Helpers::getAllDriveStates(m_objectStore, logContext);
  registerFetchTime = t.secs(utils::Timer::resetCounter);
  using common::dataStructures::DriveStatus;
  std::set<int> activeDriveStatuses = {
    (int)cta::common::dataStructures::DriveStatus::Starting,
    (int)cta::common::dataStructures::DriveStatus::Mounting,
    (int)cta::common::dataStructures::DriveStatus::Transferring,
    (int)cta::common::dataStructures::DriveStatus::Unloading,
    (int)cta::common::dataStructures::DriveStatus::Unmounting,
    (int)cta::common::dataStructures::DriveStatus::DrainingToDisk,
    (int)cta::common::dataStructures::DriveStatus::CleaningUp };
  std::set<int> activeMountTypes = {
    (int)cta::common::dataStructures::MountType::Archive,
    (int)cta::common::dataStructures::MountType::Retrieve,
    (int)cta::common::dataStructures::MountType::Label };
  for (const auto &d : driveStates) {
    if (activeDriveStatuses.count((int)d.driveStatus)) {
      tmdi.existingOrNextMounts.push_back(ExistingMount());
      tmdi.existingOrNextMounts.back().type = d.mountType;
      tmdi.existingOrNextMounts.back().tapePool = d.currentTapePool;
      tmdi.existingOrNextMounts.back().driveName = d.driveName;
      tmdi.existingOrNextMounts.back().vid = d.currentVid;
      tmdi.existingOrNextMounts.back().currentMount = true;
      tmdi.existingOrNextMounts.back().bytesTransferred = d.bytesTransferredInSession;
      tmdi.existingOrNextMounts.back().filesTransferred = d.filesTransferredInSession;
      tmdi.existingOrNextMounts.back().latestBandwidth = d.latestBandwidth;
    }
    if (activeMountTypes.count((int)d.nextMountType)) {
      tmdi.existingOrNextMounts.push_back(ExistingMount());
      tmdi.existingOrNextMounts.back().type = d.nextMountType;
      tmdi.existingOrNextMounts.back().tapePool = d.nextTapepool;
      tmdi.existingOrNextMounts.back().driveName = d.driveName;
      tmdi.existingOrNextMounts.back().vid = d.nextVid;
      tmdi.existingOrNextMounts.back().currentMount = false;
      tmdi.existingOrNextMounts.back().bytesTransferred = 0;
      tmdi.existingOrNextMounts.back().filesTransferred = 0;
      tmdi.existingOrNextMounts.back().latestBandwidth = 0;
    }
  }
  auto registerProcessingTime = t.secs(utils::Timer::resetCounter);
  log::ScopedParamContainer params (logContext);
  params.add("queueFetchTime", registerFetchTime)
        .add("processingTime", registerProcessingTime);
  logContext.log(log::INFO, "In OStoreDB::fetchMountInfo(): fetched the drive register.");
}

//------------------------------------------------------------------------------
// OStoreDB::getMountInfo()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> 
  OStoreDB::getMountInfo(log::LogContext& logContext) {
  utils::Timer t;
  //Allocate the getMountInfostructure to return.
  assertAgentAddressSet();
  std::unique_ptr<OStoreDB::TapeMountDecisionInfo> privateRet (new OStoreDB::TapeMountDecisionInfo(*this));
  TapeMountDecisionInfo & tmdi=*privateRet;
  // Get all the tape pools and tapes with queues (potential mounts)
  objectstore::RootEntry re(m_objectStore);
  re.fetchNoLock();
  auto rootFetchNoLockTime = t.secs(utils::Timer::resetCounter);
  // Take an exclusive lock on the scheduling and fetch it.
  tmdi.m_schedulerGlobalLock.reset(
    new SchedulerGlobalLock(re.getSchedulerGlobalLock(), m_objectStore));
  tmdi.m_lockOnSchedulerGlobalLock.lock(*tmdi.m_schedulerGlobalLock);
  auto lockSchedGlobalTime = t.secs(utils::Timer::resetCounter);
  tmdi.m_lockTaken = true;
  tmdi.m_schedulerGlobalLock->fetch();
  auto fetchSchedGlobalTime = t.secs(utils::Timer::resetCounter);;
  fetchMountInfo(tmdi, re, logContext);
  auto fetchMountInfoTime = t.secs(utils::Timer::resetCounter);
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> ret(std::move(privateRet));
  {
    log::ScopedParamContainer params(logContext);
    params.add("rootFetchNoLockTime", rootFetchNoLockTime)
          .add("lockSchedGlobalTime", lockSchedGlobalTime)
          .add("fetchSchedGlobalTime", fetchSchedGlobalTime)
          .add("fetchMountInfoTime", fetchMountInfoTime);
    logContext.log(log::INFO, "In OStoreDB::getMountInfo(): success.");
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getMountInfoNoLock()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> OStoreDB::getMountInfoNoLock(log::LogContext & logContext) {
  utils::Timer t;
  //Allocate the getMountInfostructure to return.
  assertAgentAddressSet();
  std::unique_ptr<OStoreDB::TapeMountDecisionInfoNoLock> privateRet (new OStoreDB::TapeMountDecisionInfoNoLock);
  // Get all the tape pools and tapes with queues (potential mounts)
  objectstore::RootEntry re(m_objectStore);
  re.fetchNoLock();
  auto rootFetchNoLockTime = t.secs(utils::Timer::resetCounter);
  TapeMountDecisionInfoNoLock & tmdi=*privateRet;
  fetchMountInfo(tmdi, re, logContext);
  auto fetchMountInfoTime = t.secs(utils::Timer::resetCounter);
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> ret(std::move(privateRet));
  {
    log::ScopedParamContainer params(logContext);
    params.add("rootFetchNoLockTime", rootFetchNoLockTime)
          .add("fetchMountInfoTime", fetchMountInfoTime);
    logContext.log(log::INFO, "In OStoreDB::getMountInfoNoLock(): success.");
  }
  return ret;
}
//------------------------------------------------------------------------------
// OStoreDB::trimEmptyQueues()
//------------------------------------------------------------------------------
void OStoreDB::trimEmptyQueues(log::LogContext& lc) {
  // We will trim empty queues from the root entry.
  lc.log(log::INFO, "In OStoreDB::trimEmptyQueues(): will start trimming empty queues");
  // Get an exclusive lock on the root entry, we have good chances to need it.
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  try {
    auto archiveQueueList = re.dumpArchiveQueues();
    for (auto & a: archiveQueueList) {
      ArchiveQueue aq(a.address, m_objectStore);
      ScopedSharedLock aql(aq);
      aq.fetch();
      if (!aq.getJobsSummary().jobs) {
        aql.release();
        re.removeArchiveQueueAndCommit(a.tapePool, lc);
        log::ScopedParamContainer params(lc);
        params.add("tapePool", a.tapePool)
              .add("queueObject", a.address);
        lc.log(log::INFO, "In OStoreDB::trimEmptyQueues(): deleted empty archive queue.");
      }
    }
    auto retrieveQeueueList = re.dumpRetrieveQueues();
    for (auto & r:retrieveQeueueList) {
      RetrieveQueue rq(r.address, m_objectStore);
      ScopedSharedLock rql(rq);
      rq.fetch();
      if (!rq.getJobsSummary().files) {
        rql.release();
        re.removeRetrieveQueueAndCommit(r.vid, lc);
        log::ScopedParamContainer params(lc);
        params.add("vid", r.vid)
              .add("queueObject", r.address);
        lc.log(log::INFO, "In OStoreDB::trimEmptyQueues(): deleted empty retrieve queue.");
      }
    }
  } catch (cta::exception::Exception & ex) {
    log::ScopedParamContainer params(lc);
    params.add("exceptionMessage", ex.getMessageValue());
    lc.log(log::ERR, "In OStoreDB::trimEmptyQueues(): got an exception. Stack trace follows.");
    lc.logBacktrace(log::ERR, ex.backtrace());
  }
}

//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfoNoLock::createArchiveMount()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::ArchiveMount> OStoreDB::TapeMountDecisionInfoNoLock::createArchiveMount(const catalogue::TapeForWriting& tape, const std::string driveName, const std::string& logicalLibrary, const std::string& hostName, time_t startTime) {
  throw cta::exception::Exception("In OStoreDB::TapeMountDecisionInfoNoLock::createArchiveMount(): This function should not be called");
}

//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfoNoLock::createRetrieveMount()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::RetrieveMount> OStoreDB::TapeMountDecisionInfoNoLock::createRetrieveMount(const std::string& vid, const std::string& tapePool, const std::string driveName, const std::string& logicalLibrary, const std::string& hostName, time_t startTime) {
  throw cta::exception::Exception("In OStoreDB::TapeMountDecisionInfoNoLock::createRetrieveMount(): This function should not be called");
}

//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfoNoLock::~TapeMountDecisionInfoNoLock()
//------------------------------------------------------------------------------
OStoreDB::TapeMountDecisionInfoNoLock::~TapeMountDecisionInfoNoLock() {}

//------------------------------------------------------------------------------
// OStoreDB::queueArchive()
//------------------------------------------------------------------------------
void OStoreDB::queueArchive(const std::string &instanceName, const cta::common::dataStructures::ArchiveRequest &request, 
        const cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId &criteria, log::LogContext &logContext) {
  assertAgentAddressSet();
  cta::utils::Timer timer;
  // Construct the archive request object in memory
  cta::objectstore::ArchiveRequest aReq(m_agentReference->nextId("ArchiveRequest"), m_objectStore);
  aReq.initialize();
  // Summarize all as an archiveFile
  cta::common::dataStructures::ArchiveFile aFile;
  aFile.archiveFileID = criteria.fileId;
  aFile.checksumType = request.checksumType;
  aFile.checksumValue = request.checksumValue;
  // TODO: fully fledged archive file should not be the representation of the request.
  aFile.creationTime = std::numeric_limits<decltype(aFile.creationTime)>::min();
  aFile.reconciliationTime = 0;
  aFile.diskFileId = request.diskFileID;
  aFile.diskFileInfo = request.diskFileInfo;
  aFile.diskInstance = instanceName;
  aFile.fileSize = request.fileSize;
  aFile.storageClass = request.storageClass;
  aReq.setArchiveFile(aFile);
  aReq.setMountPolicy(criteria.mountPolicy);
  aReq.setArchiveReportURL(request.archiveReportURL);
  aReq.setRequester(request.requester);
  aReq.setSrcURL(request.srcURL);
  aReq.setEntryLog(request.creationLog);
  std::list<cta::objectstore::ArchiveRequest::JobDump> jl;
  for (auto & copy:criteria.copyToPoolMap) {
    const uint32_t hardcodedRetriesWithinMount = 3;
    const uint32_t hardcodedTotalRetries = 6;
    aReq.addJob(copy.first, copy.second, "archive queue address to be set later", hardcodedRetriesWithinMount, hardcodedTotalRetries);
    jl.push_back(cta::objectstore::ArchiveRequest::JobDump());
    jl.back().copyNb = copy.first;
    jl.back().tapePool = copy.second;
  }
  if (jl.empty()) {
    throw ArchiveRequestHasNoCopies("In OStoreDB::queue: the archive to file request has no copy");
  }
  // We create the object here
  m_agentReference->addToOwnership(aReq.getAddressIfSet(), m_objectStore);
  aReq.setOwner(m_agentReference->getAgentAddress());
  aReq.insert();
  ScopedExclusiveLock arl(aReq);
  double arCreationAndRelock = timer.secs(cta::utils::Timer::reset_t::resetCounter);
  double arTotalQueueingTime = 0;
  double arTotalCommitTime = 0;
  double arTotalQueueUnlockTime = 0;
  // We can now enqueue the requests
  std::list<std::string> linkedTapePools;
  std::string currentTapepool;
  try {
    for (auto &j: aReq.dumpJobs()) {
      currentTapepool = j.tapePool;
      // The shared lock will be released automatically at the end of this scope.
      // The queueing implicitly sets the job owner as the queue (as should be). The queue should not
      // be unlocked before we commit the archive request (otherwise, the pointer could be seen as
      // stale and the job would be dereferenced from the queue.
      auto shareLock = ostoredb::MemArchiveQueue::sharedAddToQueue(j, j.tapePool, aReq, *this, logContext);
      double qTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
      arTotalQueueingTime += qTime;
      aReq.commit();
      double cTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
      arTotalCommitTime += cTime;
      // Now we can let go off the queue.
      shareLock.reset();
      double qUnlockTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
      arTotalQueueUnlockTime += qUnlockTime;
      linkedTapePools.push_back(j.owner);
      log::ScopedParamContainer params(logContext);
      params.add("tapepool", j.tapePool)
            .add("queueObject", j.owner)
            .add("jobObject", aReq.getAddressIfSet())
            .add("queueingTime", qTime)
            .add("commitTime", cTime)
            .add("queueUnlockTime", qUnlockTime);
      logContext.log(log::INFO, "In OStoreDB::queueArchive(): added job to queue");
    }
  } catch (NoSuchArchiveQueue &ex) {
    // Unlink the request from already connected tape pools
    for (auto tpa=linkedTapePools.begin(); tpa!=linkedTapePools.end(); tpa++) {
      objectstore::ArchiveQueue aq(*tpa, m_objectStore);
      ScopedExclusiveLock aql(aq);
      aq.fetch();
      aq.removeJobsAndCommit({aReq.getAddressIfSet()});
    }
    aReq.remove();
    log::ScopedParamContainer params(logContext);
    params.add("tapepool", currentTapepool)
          .add("archiveRequestObject", aReq.getAddressIfSet())
          .add("exceptionMessage", ex.getMessageValue())
          .add("jobObject", aReq.getAddressIfSet());
    logContext.log(log::INFO, "In OStoreDB::queueArchive(): failed to enqueue job");
    throw;
  }
  // The request is now fully set. As it's multi-owned, we do not set the owner,
  // just to disown it from the agent.
  aReq.setOwner("");
  aReq.commit();
  double arOwnerResetTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
  arl.release();
  double arLockRelease = timer.secs(cta::utils::Timer::reset_t::resetCounter);
  // And remove reference from the agent
  m_agentReference->removeFromOwnership(aReq.getAddressIfSet(), m_objectStore);
  log::ScopedParamContainer params(logContext);
  params.add("jobObject", aReq.getAddressIfSet())
    .add("creationAndRelockTime", arCreationAndRelock)
    .add("totalQueueingTime", arTotalQueueingTime)
    .add("totalCommitTime", arTotalCommitTime)
    .add("totalQueueUnlockTime", arTotalQueueUnlockTime)
    .add("ownerResetTime", arOwnerResetTime)
    .add("lockReleaseTime", arLockRelease)
    .add("agentOwnershipResetTime", timer.secs());
  logContext.log(log::INFO, "In OStoreDB::queueArchive(): Finished enqueuing request.");
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveToFileRequestCancelation::complete()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveToFileRequestCancelation::complete(log::LogContext & lc) {
  if (m_closed)
    throw ArchiveRequestAlreadyDeleted("OStoreDB::ArchiveToFileRequestCancelation::complete(): called twice");
  // We just need to delete the object and forget it
  m_request.remove();
  log::ScopedParamContainer params(lc);
  params.add("archiveRequestObject", m_request.getAddressIfSet());
  lc.log(log::INFO, "In ArchiveToFileRequestCancelation::complete(): removed archive request.");
  m_agentReference.removeFromOwnership(m_request.getAddressIfSet(), m_objectStore);
  m_closed = true;
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveToFileRequestCancelation::~ArchiveToFileRequestCancelation()
//------------------------------------------------------------------------------
OStoreDB::ArchiveToFileRequestCancelation::~ArchiveToFileRequestCancelation() {
  if (!m_closed) {
    try {
      log::LogContext lc(m_logger);
      m_request.garbageCollect(m_agentReference.getAgentAddress(), m_agentReference, lc, m_catalogue);
      m_agentReference.removeFromOwnership(m_request.getAddressIfSet(), m_objectStore);
    } catch (...) {}
  }
}

//------------------------------------------------------------------------------
// OStoreDB::getArchiveJobs()
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveJob>
  OStoreDB::getArchiveJobs(const std::string& tapePoolName) const {
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto tpl = re.dumpArchiveQueues();
  rel.release();
  for (auto & tpp:tpl) {
    if (tpp.tapePool != tapePoolName) continue;
    std::list<cta::common::dataStructures::ArchiveJob> ret;
    objectstore::ArchiveQueue osaq(tpp.address, m_objectStore);
    ScopedSharedLock ostpl(osaq);
    osaq.fetch();
    auto arl = osaq.dumpJobs();
    ostpl.release();
    for (auto ar=arl.begin(); ar!=arl.end(); ar++) {
      objectstore::ArchiveRequest osar(ar->address, m_objectStore);
      ScopedSharedLock osarl(osar);
      osar.fetch();
      // Find which copy number is for this tape pool.
      // skip the request if not found
      uint16_t copynb;
      bool copyndFound=false;
      for (auto & j:osar.dumpJobs()) {
        if (j.tapePool == tpp.tapePool) {
          copynb = j.copyNb;
          copyndFound = true;
          break;
        }
      }
      if (!copyndFound) continue;
      ret.push_back(cta::common::dataStructures::ArchiveJob());
      ret.back().archiveFileID = osar.getArchiveFile().archiveFileID;
      ret.back().copyNumber = copynb;
      ret.back().tapePool = tpp.tapePool;
      ret.back().request.checksumType = osar.getArchiveFile().checksumType;
      ret.back().request.checksumValue = osar.getArchiveFile().checksumValue;
      ret.back().request.creationLog = osar.getEntryLog();
      ret.back().request.diskFileID = osar.getArchiveFile().diskFileId;
      ret.back().request.diskFileInfo = osar.getArchiveFile().diskFileInfo;
      ret.back().request.fileSize = osar.getArchiveFile().fileSize;
      ret.back().instanceName = osar.getArchiveFile().diskInstance;
      ret.back().request.requester = osar.getRequester();
      ret.back().request.srcURL = osar.getSrcURL();
      ret.back().request.archiveReportURL = osar.getArchiveReportURL();
      ret.back().request.storageClass = osar.getArchiveFile().storageClass;
    }
    return ret;
  }
  return std::list<cta::common::dataStructures::ArchiveJob>();
}

//------------------------------------------------------------------------------
// OStoreDB::getArchiveJobs()
//------------------------------------------------------------------------------
std::map<std::string, std::list<common::dataStructures::ArchiveJob> >
  OStoreDB::getArchiveJobs() const {
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto tpl = re.dumpArchiveQueues();
  rel.release();
  std::map<std::string, std::list<common::dataStructures::ArchiveJob> > ret;
  for (auto & tpp:tpl) {
    objectstore::ArchiveQueue osaq(tpp.address, m_objectStore);
    ScopedSharedLock ostpl(osaq);
    osaq.fetch();
    auto arl = osaq.dumpJobs();
    ostpl.release();
    for (auto ar=arl.begin(); ar!=arl.end(); ar++) {
      objectstore::ArchiveRequest osar(ar->address, m_objectStore);
      ScopedSharedLock osarl(osar);
      osar.fetch();
      // Find which copy number is for this tape pool.
      // skip the request if not found
      uint16_t copynb;
      bool copyndFound=false;
      for (auto & j:osar.dumpJobs()) {
        if (j.tapePool == tpp.tapePool) {
          copynb = j.copyNb;
          copyndFound = true;
          break;
        }
      }
      if (!copyndFound) continue;
      ret[tpp.tapePool].push_back(cta::common::dataStructures::ArchiveJob());
      ret[tpp.tapePool].back().archiveFileID = osar.getArchiveFile().archiveFileID;
      ret[tpp.tapePool].back().copyNumber = copynb;
      ret[tpp.tapePool].back().tapePool = tpp.tapePool;
      ret[tpp.tapePool].back().request.checksumType = osar.getArchiveFile().checksumType;
      ret[tpp.tapePool].back().request.checksumValue = osar.getArchiveFile().checksumValue;
      ret[tpp.tapePool].back().request.creationLog = osar.getEntryLog();
      ret[tpp.tapePool].back().request.diskFileID = osar.getArchiveFile().diskFileId;
      ret[tpp.tapePool].back().request.diskFileInfo = osar.getArchiveFile().diskFileInfo;
      ret[tpp.tapePool].back().request.fileSize = osar.getArchiveFile().fileSize;
      ret[tpp.tapePool].back().instanceName = osar.getArchiveFile().diskInstance;
      ret[tpp.tapePool].back().request.requester = osar.getRequester();
      ret[tpp.tapePool].back().request.srcURL = osar.getSrcURL();
      ret[tpp.tapePool].back().request.archiveReportURL = osar.getArchiveReportURL();
      ret[tpp.tapePool].back().request.storageClass = osar.getArchiveFile().storageClass;
    }
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveQueueStatistics()
//------------------------------------------------------------------------------
std::list<SchedulerDatabase::RetrieveQueueStatistics> OStoreDB::getRetrieveQueueStatistics(
  const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria,
  const std::set<std::string> & vidsToConsider) {
  return Helpers::getRetrieveQueueStatistics(criteria, vidsToConsider, m_objectStore);
}

//------------------------------------------------------------------------------
// OStoreDB::queueRetrieve()
//------------------------------------------------------------------------------
std::string OStoreDB::queueRetrieve(const cta::common::dataStructures::RetrieveRequest& rqst,
  const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria, log::LogContext &logContext) {
  assertAgentAddressSet();
  // Get the best vid from the cache
  std::set<std::string> candidateVids;
  for (auto & tf:criteria.archiveFile.tapeFiles) candidateVids.insert(tf.second.vid);
  std::string bestVid=Helpers::selectBestRetrieveQueue(candidateVids, m_catalogue, m_objectStore);
  // Check that the requested retrieve job (for the provided vid) exists, and record the copynb.
  uint64_t bestCopyNb;
  for (auto & tf: criteria.archiveFile.tapeFiles) {
    if (tf.second.vid == bestVid) {
      bestCopyNb = tf.second.copyNb;
      goto vidFound;
    }
  }
  {
    std::stringstream err;
    err << "In OStoreDB::queueRetrieve(): no tape file for requested vid. archiveId=" << criteria.archiveFile.archiveFileID
        << " vid=" << bestVid;
    throw RetrieveRequestHasNoCopies(err.str());
  }
  vidFound:
  // In order to post the job, construct it first in memory.
  objectstore::RetrieveRequest rReq(m_agentReference->nextId("RetrieveRequest"), m_objectStore);
  rReq.initialize();
  rReq.setSchedulerRequest(rqst);
  rReq.setRetrieveFileQueueCriteria(criteria);
  // Point to the request in the agent
  m_agentReference->addToOwnership(rReq.getAddressIfSet(), m_objectStore);
  // Set an arbitrary copy number so we can serialize. Garbage collection we re-choose 
  // the tape file number and override it in case of problem (and we will set it further).
  rReq.setActiveCopyNumber(1);
  rReq.insert();
  ScopedExclusiveLock rrl(rReq);
  // Find the job corresponding to the vid (and check we indeed have one).
  auto jobs = rReq.getJobs();
  objectstore::RetrieveRequest::JobDump * job = nullptr;
  for (auto & j:jobs) {
    if (j.copyNb == bestCopyNb) {
      job = &j;
      goto jobFound;
    }
  }
  {
    std::stringstream err;
    err << "In OStoreDB::queueRetrieve(): no job for requested copyNb. archiveId=" << criteria.archiveFile.archiveFileID
        << " vid=" << bestVid << " copyNb=" << bestCopyNb;
    throw RetrieveRequestHasNoCopies(err.str());
  }
  jobFound:
  {
    // Add the request to the queue (with a shared access).
    auto sharedLock = ostoredb::MemRetrieveQueue::sharedAddToQueue(*job, bestVid, rReq, *this, logContext);
    // The object ownership was set in SharedAdd.
    rReq.commit();
    // The lock on the queue is released here (has to be after the request commit for consistency.
  }
  rrl.release();
  // And relinquish ownership form agent
  m_agentReference->removeFromOwnership(rReq.getAddressIfSet(), m_objectStore);
  return bestVid;
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveRequestsByVid()
//------------------------------------------------------------------------------
std::list<RetrieveRequestDump> OStoreDB::getRetrieveRequestsByVid(const std::string& vid) const {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveRequestsByRequester()
//------------------------------------------------------------------------------
std::list<RetrieveRequestDump> OStoreDB::getRetrieveRequestsByRequester(const std::string& vid) const {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveRequests()
//------------------------------------------------------------------------------
std::map<std::string, std::list<RetrieveRequestDump> > OStoreDB::getRetrieveRequests() const {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
//  std::map<cta::Tape, std::list<RetrieveRequestDump> > ret;
//  // Get list of tape pools and then tapes
//  objectstore::RootEntry re(m_objectStore);
//  objectstore::ScopedSharedLock rel(re);
//  re.fetch();
//  auto tpl=re.dumpTapePools();
//  rel.release();
//  for (auto tpp = tpl.begin(); tpp != tpl.end(); tpp++) {
//    // Get the list of tapes for the tape pool
//    objectstore::TapePool tp(tpp->address, m_objectStore);
//    objectstore::ScopedSharedLock tplock(tp);
//    tp.fetch();
//    auto tl = tp.dumpTapes();
//    for (auto tptr = tl.begin(); tptr!= tl.end(); tptr++) {
//      // Get the list of retrieve requests for the tape.
//      objectstore::Tape t(tptr->address, m_objectStore);
//      objectstore::ScopedSharedLock tlock(t);
//      t.fetch();
//      auto jobs = t.dumpAndFetchRetrieveRequests();
//      // If the list is not empty, add to the map.
//      if (jobs.size()) {
//        cta::Tape tkey;
//        // TODO tkey.capacityInBytes;
//        tkey.creationLog = t.getCreationLog();
//        // TODO tkey.dataOnTapeInBytes;
//        tkey.logicalLibraryName = t.getLogicalLibrary();
//        tkey.nbFiles = t.getLastFseq();
//        // TODO tkey.status
//        tkey.tapePoolName = tp.getName();
//        tkey.vid = t.getVid();
//        ret[tkey] = std::move(jobs);
//      }
//    }
//  }
//  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::deleteRetrieveRequest()
//------------------------------------------------------------------------------
void OStoreDB::deleteRetrieveRequest(const common::dataStructures::SecurityIdentity& requester, 
  const std::string& remoteFile) {
  throw exception::Exception("Not Implemented");
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveJobs()
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::RetrieveJob> OStoreDB::getRetrieveJobs(const std::string& tapePoolName) const {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveJobs()
//------------------------------------------------------------------------------
std::map<std::string, std::list<common::dataStructures::RetrieveJob> > OStoreDB::getRetrieveJobs() const {
  // We will walk all the tapes to get the jobs.
  std::map<std::string, std::list<common::dataStructures::RetrieveJob> > ret;
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto rql=re.dumpRetrieveQueues();
  rel.release();
  for (auto & rqp: rql) {
    // This implementation gives imperfect consistency. Do we need better? (If so, TODO: improve).
    // This implementation is racy, so we tolerate that the queue is gone.
    try {
      RetrieveQueue rq(rqp.address, m_objectStore);
      ScopedSharedLock rql(rq);
      rq.fetch();
      for (auto & j: rq.dumpJobs()) {
        ret[rqp.vid].push_back(common::dataStructures::RetrieveJob());
        try {
          auto & jd=ret[rqp.vid].back();
          RetrieveRequest rr(j.address, m_objectStore);
          ScopedSharedLock rrl(rr);
          rr.fetch();
          jd.request=rr.getSchedulerRequest();
          for (auto & tf: rr.getArchiveFile().tapeFiles) {
            jd.tapeCopies[tf.second.vid].first=tf.second.copyNb;
            jd.tapeCopies[tf.second.vid].second=tf.second;
          }
        } catch (...) {
          ret[rqp.vid].pop_back();
        }
            
      }
    } catch (...) {}
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getDriveStates()
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::DriveState> OStoreDB::getDriveStates(log::LogContext & lc) const {
  return Helpers::getAllDriveStates(m_objectStore, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::setDesiredDriveState()
//------------------------------------------------------------------------------
void OStoreDB::setDesiredDriveState(const std::string& drive, const common::dataStructures::DesiredDriveState & desiredState, log::LogContext &lc) {
  objectstore::DriveState ds(m_objectStore);
  ScopedExclusiveLock dsl;
  Helpers::getLockedAndFetchedDriveState(ds, dsl, *m_agentReference, drive, lc);
  auto driveState = ds.getState();
  driveState.desiredDriveState = desiredState;
  ds.setState(driveState);
  ds.commit();
}

//------------------------------------------------------------------------------
// OStoreDB::removeDrive()
//------------------------------------------------------------------------------
void OStoreDB::removeDrive(const std::string& drive, log::LogContext &lc) {
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  auto driveRegisterAddress = re.getDriveRegisterAddress();
  objectstore::DriveRegister dr(driveRegisterAddress, m_objectStore);
  objectstore::ScopedExclusiveLock drl(dr);
  dr.fetch();
  // If the driveStatus exists, take ownership before deleting and finally dereferencing.
  auto driveAddresses = dr.getDriveAddresses();
  // If the drive is present, find status and lock it. If not, nothing to do.
  auto di = std::find_if(driveAddresses.begin(), driveAddresses.end(),
      [&](const DriveRegister::DriveAddress & da){ return da.driveName == drive; });
  if (di!=driveAddresses.end()) {
    // Try and take an exclusive lock on the object
    log::ScopedParamContainer params(lc);
    params.add("driveName", drive)
          .add("driveRegisterObject", dr.getAddressIfSet())
          .add("driveStateObject", di->driveStateAddress);
    try {
      objectstore::DriveState ds(di->driveStateAddress, m_objectStore);
      objectstore::ScopedExclusiveLock dsl(ds);
      ds.fetch();
      params.add("driveName", drive)
            .add("driveRegisterObject", dr.getAddressIfSet())
            .add("driveStateObject", ds.getAddressIfSet());
      if (ds.getOwner() == dr.getAddressIfSet()) {
        // The drive state is owned as expected, delete it and then de-reference it.
        ds.remove();
        dr.removeDrive(drive);
        dr.commit();
        log::ScopedParamContainer params(lc);
        params.add("driveStateObject", ds.getAddressIfSet());
        lc.log(log::INFO, "In OStoreDB::removeDrive(): removed and dereferenced drive state object.");
      } else {
        dr.removeDrive(drive);
        dr.commit();
        lc.log(log::WARNING, "In OStoreDB::removeDrive(): just dereferenced drive state object not owned by drive register.");
      }
    } catch (cta::exception::Exception & ex) {
      // The drive might not exist anymore, in which case we can proceed to
      // dereferencing from DriveRegister.
      if (!m_objectStore.exists(di->driveStateAddress)) {
        dr.removeDrive(drive);
        dr.commit();
        lc.log(log::WARNING, "In OStoreDB::removeDrive(): dereferenced non-existing drive state object.");
      } else {
        params.add("exceptionMessage", ex.getMessageValue());
        lc.log(log::ERR, "In OStoreDB::removeDrive(): unexpected error dereferencing drive state. Doing nothing.");
      }
    }
  } else {
    log::ScopedParamContainer params (lc);
    params.add("driveName", drive);
    lc.log(log::INFO, "In OStoreDB::removeDrive(): no such drive reference in register.");
  }
}
 
//------------------------------------------------------------------------------
// OStoreDB::reportDriveStatus()
//------------------------------------------------------------------------------
void OStoreDB::reportDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
  cta::common::dataStructures::MountType mountType, common::dataStructures::DriveStatus status,
  time_t reportTime, log::LogContext & lc, uint64_t mountSessionId, uint64_t byteTransferred,
  uint64_t filesTransferred, double latestBandwidth, const std::string& vid, 
  const std::string& tapepool) {
  using common::dataStructures::DriveStatus;
  // Wrap all the parameters together for easier manipulation by sub-functions
  ReportDriveStatusInputs inputs;
  inputs.mountType = mountType;
  inputs.byteTransferred = byteTransferred;
  inputs.filesTransferred = filesTransferred;
  inputs.latestBandwidth = latestBandwidth;
  inputs.mountSessionId = mountSessionId;
  inputs.reportTime = reportTime;
  inputs.status = status;
  inputs.vid = vid;
  inputs.tapepool = tapepool;
  updateDriveStatus(driveInfo, inputs, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::updateDriveStatus()
//------------------------------------------------------------------------------
void OStoreDB::updateDriveStatus(const common::dataStructures::DriveInfo& driveInfo, const ReportDriveStatusInputs& inputs,
  log::LogContext &lc) {
  using common::dataStructures::DriveStatus;
  // First, get the drive state.
  objectstore::DriveState ds(m_objectStore);
  objectstore::ScopedExclusiveLock dsl;
  Helpers::getLockedAndFetchedDriveState(ds, dsl, *m_agentReference, driveInfo.driveName, lc);
  // The drive state might not be present, in which case we have to fill it up with default values.
  cta::common::dataStructures::DriveState driveState = ds.getState();
  // Set the parameters that we always set
  driveState.host = driveInfo.host;
  driveState.logicalLibrary = driveInfo.logicalLibrary;
  // Set the status
  switch (inputs.status) {
    case DriveStatus::Down:
      setDriveDown(driveState, inputs);
      break;
    case DriveStatus::Up:
      setDriveUpOrMaybeDown(driveState, inputs);
      break;
    case DriveStatus::Probing:
      setDriveProbing(driveState, inputs);
      break;
    case DriveStatus::Starting:
      setDriveStarting(driveState, inputs);
      break;
    case DriveStatus::Mounting:
      setDriveMounting(driveState, inputs);
      break;
    case DriveStatus::Transferring:
      setDriveTransferring(driveState, inputs);
      break;
    case DriveStatus::Unloading:
      setDriveUnloading(driveState, inputs);
      break;
    case DriveStatus::Unmounting:
      setDriveUnmounting(driveState, inputs);
      break;
    case DriveStatus::DrainingToDisk:
      setDriveDrainingToDisk(driveState, inputs);
      break;
    case DriveStatus::CleaningUp:
      setDriveCleaningUp(driveState, inputs);
      break;
    case DriveStatus::Shutdown:
      setDriveShutdown(driveState, inputs);
      break;
    default:
      throw exception::Exception("Unexpected status in DriveRegister::reportDriveStatus");
  }
  ds.setState(driveState);
  ds.commit();
}

//------------------------------------------------------------------------------
// OStoreDB::updateDriveStatsInRegitry()
//------------------------------------------------------------------------------
void OStoreDB::updateDriveStatistics(const common::dataStructures::DriveInfo& driveInfo, const ReportDriveStatsInputs& inputs, log::LogContext & lc) {
  using common::dataStructures::DriveStatus;
  // The drive state might not be present, in which case we do nothing.
  cta::common::dataStructures::DriveState driveState;
  objectstore::DriveState ds(m_objectStore);
  objectstore::ScopedExclusiveLock dsl;
  try { 
    Helpers::getLockedAndFetchedDriveState(ds, dsl, *m_agentReference, driveInfo.driveName,
      lc, Helpers::CreateIfNeeded::doNotCreate);
  } catch (cta::exception::Exception & ex) {
    // The drive is missing in the registry. Nothing to update
    return;    
  }
  driveState = ds.getState();
  // Set the parameters that we always set
  driveState.host = driveInfo.host;
  driveState.logicalLibrary = driveInfo.logicalLibrary;
  
  switch (driveState.driveStatus) {    
    case DriveStatus::Transferring:
    {
      const time_t timeDifference = inputs.reportTime -  driveState.lastUpdateTime;
      const uint64_t bytesDifference = inputs.bytesTransferred - driveState.bytesTransferredInSession;   
      driveState.lastUpdateTime=inputs.reportTime;
      driveState.bytesTransferredInSession=inputs.bytesTransferred;
      driveState.filesTransferredInSession=inputs.filesTransferred;
      driveState.latestBandwidth = timeDifference?1.0*bytesDifference/timeDifference:0.0;
      break;
    }
    default:
      return;
  }
  ds.setState(driveState);
  ds.commit();
}  

//------------------------------------------------------------------------------
// OStoreDB::setDriveDown()
//------------------------------------------------------------------------------
void OStoreDB::setDriveDown(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we were already down, then we only update the last update time.
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Down) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset.
  driveState.sessionId=0;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=inputs.reportTime;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=common::dataStructures::MountType::NoMount;
  driveState.driveStatus=common::dataStructures::DriveStatus::Down;
  driveState.desiredDriveState.up=false;
  driveState.desiredDriveState.forceDown=false;
  driveState.currentVid="";
  driveState.currentTapePool="";
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveUp()
//------------------------------------------------------------------------------
void OStoreDB::setDriveUpOrMaybeDown(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  using common::dataStructures::DriveStatus;
  // Decide whether we should be up or down
  DriveStatus  targetStatus=DriveStatus::Up;
  if (!driveState.desiredDriveState.up) {
    driveState.driveStatus = common::dataStructures::DriveStatus::Down;
  }
  // If we were already up (or down), then we only update the last update time.
  if (driveState.driveStatus == targetStatus) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset.
  driveState.sessionId=0;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=inputs.reportTime;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=common::dataStructures::MountType::NoMount;
  driveState.driveStatus=targetStatus;
  driveState.currentVid="";
  driveState.currentTapePool="";
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveUp()
//------------------------------------------------------------------------------
void OStoreDB::setDriveProbing(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  using common::dataStructures::DriveStatus;
  // If we were already up (or down), then we only update the last update time.
  if (driveState.driveStatus == inputs.status) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset.
  driveState.sessionId=0;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=inputs.reportTime;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=common::dataStructures::MountType::NoMount;
  driveState.driveStatus=inputs.status;
  driveState.currentVid="";
  driveState.currentTapePool="";
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveStarting()
//------------------------------------------------------------------------------
void OStoreDB::setDriveStarting(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we were already starting, then we only update the last update time.
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Starting) {
    driveState.lastUpdateTime = inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId=inputs.mountSessionId;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=inputs.reportTime;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.startStartTime=inputs.reportTime;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::Starting;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveMounting()
//------------------------------------------------------------------------------
void OStoreDB::setDriveMounting(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we were already starting, then we only update the last update time.
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Mounting) {
    driveState.lastUpdateTime = inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId=inputs.mountSessionId;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  //driveState.sessionstarttime=inputs.reportTime;
  driveState.mountStartTime=inputs.reportTime;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::Mounting;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveTransferring()
//------------------------------------------------------------------------------
void OStoreDB::setDriveTransferring(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we were already transferring, we update the full statistics
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Transferring) {
    driveState.lastUpdateTime=inputs.reportTime;
    driveState.bytesTransferredInSession=inputs.byteTransferred;
    driveState.filesTransferredInSession=inputs.filesTransferred;
    driveState.latestBandwidth=inputs.latestBandwidth;
    return;
  }
  driveState.sessionId=inputs.mountSessionId;
  driveState.bytesTransferredInSession=inputs.byteTransferred;
  driveState.filesTransferredInSession=inputs.filesTransferred;
  driveState.latestBandwidth=inputs.latestBandwidth;
  //driveState.sessionstarttime=inputs.reportTime;
  //driveState.mountstarttime=inputs.reportTime;
  driveState.transferStartTime=inputs.reportTime;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::Transferring;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveUnloading()
//------------------------------------------------------------------------------
void OStoreDB::setDriveUnloading(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Unloading) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId=inputs.mountSessionId;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=inputs.reportTime;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::Unloading;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveUnmounting()
//------------------------------------------------------------------------------
void OStoreDB::setDriveUnmounting(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Unmounting) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId=inputs.mountSessionId;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=inputs.reportTime;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::Unmounting;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveDrainingToDisk()
//------------------------------------------------------------------------------
void OStoreDB::setDriveDrainingToDisk(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  if (driveState.driveStatus == common::dataStructures::DriveStatus::DrainingToDisk) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId=inputs.mountSessionId;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=inputs.reportTime;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::DrainingToDisk;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveCleaningUp()
//------------------------------------------------------------------------------
void OStoreDB::setDriveCleaningUp(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  if (driveState.driveStatus == common::dataStructures::DriveStatus::CleaningUp) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId=inputs.mountSessionId;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=inputs.reportTime;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::CleaningUp;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveShutdown()
//------------------------------------------------------------------------------
void OStoreDB::setDriveShutdown(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Shutdown) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId=0;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=inputs.reportTime;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::CleaningUp;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}
//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfo::createArchiveMount()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::ArchiveMount> 
  OStoreDB::TapeMountDecisionInfo::createArchiveMount(
    const catalogue::TapeForWriting & tape, const std::string driveName,
    const std::string& logicalLibrary, const std::string& hostName, time_t startTime) {
  // In order to create the mount, we have to:
  // Check we actually hold the scheduling lock
  // Set the drive status to up, and indicate which tape we use.
  std::unique_ptr<OStoreDB::ArchiveMount> privateRet(
    new OStoreDB::ArchiveMount(m_oStoreDB));
  auto &am = *privateRet;
  // Check we hold the scheduling lock
  if (!m_lockTaken)
    throw SchedulingLockNotHeld("In OStoreDB::TapeMountDecisionInfo::createArchiveMount: "
      "cannot create mount without holding scheduling lock");
  objectstore::RootEntry re(m_oStoreDB.m_objectStore);
  re.fetchNoLock();
  auto driveRegisterAddress = re.getDriveRegisterAddress();
  am.nbFilesCurrentlyOnTape = tape.lastFSeq;
  am.mountInfo.vid = tape.vid;
  // Fill up the mount info
  am.mountInfo.drive = driveName;
  am.mountInfo.host = hostName;
  am.mountInfo.mountId = m_schedulerGlobalLock->getIncreaseCommitMountId();
  m_schedulerGlobalLock->commit();
  am.mountInfo.tapePool = tape.tapePool;
  am.mountInfo.logicalLibrary = logicalLibrary;
  // Update the status of the drive in the registry
  {
    // The drive is already in-session, to prevent double scheduling before it 
    // goes to mount state. If the work to be done gets depleted in the mean time,
    // we will switch back to up.
    common::dataStructures::DriveInfo driveInfo;
    driveInfo.driveName=driveName;
    driveInfo.logicalLibrary=logicalLibrary;
    driveInfo.host=hostName;
    ReportDriveStatusInputs inputs;
    inputs.mountType = common::dataStructures::MountType::Archive;
    inputs.byteTransferred = 0;
    inputs.filesTransferred = 0;
    inputs.latestBandwidth = 0;
    inputs.mountSessionId = am.mountInfo.mountId;
    inputs.reportTime = startTime;
    inputs.status = common::dataStructures::DriveStatus::Mounting;
    inputs.vid = tape.vid;
    inputs.tapepool = tape.tapePool;
    log::LogContext lc(m_oStoreDB.m_logger);
    m_oStoreDB.updateDriveStatus(driveInfo, inputs, lc);
  }
  // We committed the scheduling decision. We can now release the scheduling lock.
  m_lockOnSchedulerGlobalLock.release();
  m_lockTaken = false;
  // We can now return the mount session object to the user.
  std::unique_ptr<SchedulerDatabase::ArchiveMount> ret(privateRet.release());
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfo::TapeMountDecisionInfo()
//------------------------------------------------------------------------------
OStoreDB::TapeMountDecisionInfo::TapeMountDecisionInfo(OStoreDB & oStroeDb): m_lockTaken(false), m_oStoreDB(oStroeDb) {}

//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfo::createArchiveMount()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::RetrieveMount> 
  OStoreDB::TapeMountDecisionInfo::createRetrieveMount(
    const std::string& vid, const std::string & tapePool, const std::string driveName, 
    const std::string& logicalLibrary, const std::string& hostName, time_t startTime) {
  // In order to create the mount, we have to:
  // Check we actually hold the scheduling lock
  // Check the tape exists, add it to ownership and set its activity status to 
  // busy, with the current agent pointing to it for unbusying
  // Set the drive status to up, but do not commit anything to the drive register
  // the drive register does not need garbage collection as it should reflect the
  // latest known state of the drive (and its absence of updating if needed)
  // Prepare the return value
  std::unique_ptr<OStoreDB::RetrieveMount> privateRet(
    new OStoreDB::RetrieveMount(m_oStoreDB));
  auto &rm = *privateRet;
  // Check we hold the scheduling lock
  if (!m_lockTaken)
    throw SchedulingLockNotHeld("In OStoreDB::TapeMountDecisionInfo::createRetrieveMount: "
      "cannot create mount without holding scheduling lock");
  // Find the tape and update it
  objectstore::RootEntry re(m_oStoreDB.m_objectStore);
  re.fetchNoLock();
  auto driveRegisterAddress = re.getDriveRegisterAddress();
  // Fill up the mount info
  rm.mountInfo.vid = vid;
  rm.mountInfo.drive = driveName;
  rm.mountInfo.host = hostName;
  rm.mountInfo.mountId = m_schedulerGlobalLock->getIncreaseCommitMountId();
  m_schedulerGlobalLock->commit();
  rm.mountInfo.tapePool = tapePool;
  rm.mountInfo.logicalLibrary = logicalLibrary;
  // Update the status of the drive in the registry
  {
    // Get hold of the drive registry
    // The drive is already in-session, to prevent double scheduling before it 
    // goes to mount state. If the work to be done gets depleted in the mean time,
    // we will switch back to up.
    common::dataStructures::DriveInfo driveInfo;
    driveInfo.driveName=driveName;
    driveInfo.logicalLibrary=logicalLibrary;
    driveInfo.host=hostName;
    ReportDriveStatusInputs inputs;
    inputs.mountType = common::dataStructures::MountType::Retrieve;
    inputs.mountSessionId = rm.mountInfo.mountId;
    inputs.reportTime = startTime;
    inputs.status = common::dataStructures::DriveStatus::Mounting;
    inputs.vid = rm.mountInfo.vid;
    inputs.tapepool = rm.mountInfo.tapePool;
    log::LogContext lc(m_oStoreDB.m_logger);
    m_oStoreDB.updateDriveStatus(driveInfo, inputs, lc);
  }
  // We committed the scheduling decision. We can now release the scheduling lock.
  m_lockOnSchedulerGlobalLock.release();
  m_lockTaken = false;
  // We can now return the mount session object to the user.
  std::unique_ptr<SchedulerDatabase::RetrieveMount> ret(privateRet.release());
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfo::~TapeMountDecisionInfo()
//------------------------------------------------------------------------------
OStoreDB::TapeMountDecisionInfo::~TapeMountDecisionInfo() {
  // The lock should be released before the objectstore object 
  // m_schedulerGlobalLock gets destroyed. We explicitely release the lock,
  // and then destroy the object
  if (m_lockTaken)
    m_lockOnSchedulerGlobalLock.release();
  m_schedulerGlobalLock.reset(NULL);
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::ArchiveMount()
//------------------------------------------------------------------------------
OStoreDB::ArchiveMount::ArchiveMount(OStoreDB & oStoreDB): m_oStoreDB(oStoreDB) {}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::getMountInfo()
//------------------------------------------------------------------------------
const SchedulerDatabase::ArchiveMount::MountInfo& OStoreDB::ArchiveMount::getMountInfo() {
  return mountInfo;
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::getNextJobBatch()
//------------------------------------------------------------------------------
std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > OStoreDB::ArchiveMount::getNextJobBatch(uint64_t filesRequested,
    uint64_t bytesRequested, log::LogContext& logContext) {
  utils::Timer t, totalTime;
  double driveRegisterCheckTime = 0;
  double findQueueTime = 0;
  double lockFetchQueueTime = 0;
  double emptyQueueCleanupTime = 0;
  double jobSelectionTime = 0;
  double ownershipAdditionTime = 0;
  double asyncUpdateLaunchTime = 0;
  double jobsUpdateTime = 0;
  double queueProcessAndCommitTime = 0;
  double ownershipRemovalTime = 0;
  // Find the next files to archive
  // First, check we should not forcibly go down. In such an occasion, we just find noting to do.
  // Get drive register
  {
    // Get the archive queue. Failure is non-fatal. We will carry on.
    try { 
      objectstore::RootEntry re(m_oStoreDB.m_objectStore);
      re.fetchNoLock();
      objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_oStoreDB.m_objectStore);
      dr.fetchNoLock();
      objectstore::DriveState ds(dr.getDriveAddress(mountInfo.drive), m_oStoreDB.m_objectStore);
      ds.fetchNoLock();
      auto driveStateValue = ds.getState();
      if (!driveStateValue.desiredDriveState.up && driveStateValue.desiredDriveState.forceDown) {
        logContext.log(log::INFO, "In OStoreDB::ArchiveMount::getNextJobBatch(): returning no job as we are forcibly going down.");
        return std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> >();
      }
    } catch (cta::exception::Exception & ex) {
      log::ScopedParamContainer params (logContext);
      params.add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::INFO, "In OStoreDB::ArchiveMount::getNextJobBatch(): failed to check up/down status.");
    }
    driveRegisterCheckTime = t.secs(utils::Timer::resetCounter);
  }
  // Now, we should repeatedly fetch jobs from the queue until we fulfilled the request or there is nothing to get form the
  // queue anymore.
  // Requests are left as-is on errors. We will keep a list of them to avoid re-accessing them in the same batch.
  std::set<std::string> archiveRequestsToSkip;
  // Prepare the returned jobs that we might accumulate in several rounds.
  std::list<std::unique_ptr<OStoreDB::ArchiveJob>> privateRet;
  uint64_t currentBytes=0;
  uint64_t currentFiles=0;
  size_t iterationCount=0;
  while (true) {
    iterationCount++;
    uint64_t beforeBytes=currentBytes;
    uint64_t beforeFiles=currentFiles;
    // Try and get access to a queue.
    objectstore::RootEntry re(m_oStoreDB.m_objectStore);
    re.fetchNoLock();
    std::string aqAddress;
    auto aql = re.dumpArchiveQueues();
    for (auto & aqp : aql) {
    if (aqp.tapePool == mountInfo.tapePool)
      aqAddress = aqp.address;
    }
    if (!aqAddress.size()) break;
    // try and lock the archive queue. Any failure from here on means the end of the getting jobs.
    objectstore::ArchiveQueue aq(aqAddress, m_oStoreDB.m_objectStore);
    objectstore::ScopedExclusiveLock aqlock;
    findQueueTime += t.secs(utils::Timer::resetCounter);
    try {
      try {
        aqlock.lock(aq);
        aq.fetch();
        lockFetchQueueTime += t.secs(utils::Timer::resetCounter);
      } catch (cta::exception::Exception & ex) {
        // The queue is now absent. We can remove its reference in the root entry.
        // A new queue could have been added in the mean time, and be non-empty.
        // We will then fail to remove from the RootEntry (non-fatal).
        ScopedExclusiveLock rexl(re);
        re.fetch();
        try {
          re.removeArchiveQueueAndCommit(mountInfo.tapePool, logContext);
          log::ScopedParamContainer params(logContext);
          params.add("tapepool", mountInfo.tapePool)
                .add("queueObject", aq.getAddressIfSet());
          logContext.log(log::INFO, "In ArchiveMount::getNextJobBatch(): de-referenced missing queue from root entry");
        } catch (RootEntry::ArchiveQueueNotEmpty & ex) {
          // TODO: improve: if we fail here we could retry to fetch a job.
          log::ScopedParamContainer params(logContext);
          params.add("tapepool", mountInfo.tapePool)
                .add("queueObject", aq.getAddressIfSet())
                .add("Message", ex.getMessageValue());
          logContext.log(log::INFO, "In ArchiveMount::getNextJobBatch(): could not de-referenced missing queue from root entry");
        } catch (RootEntry::NoSuchArchiveQueue & ex) {
          // Somebody removed the queue in the mean time. Barely worth mentioning.
          log::ScopedParamContainer params(logContext);
          params.add("tapepool", mountInfo.tapePool)
                .add("queueObject", aq.getAddressIfSet());
          logContext.log(log::DEBUG, "In ArchiveMount::getNextJobBatch(): could not de-referenced missing queue from root entry: already done.");
        }
        emptyQueueCleanupTime += t.secs(utils::Timer::resetCounter);
        continue;
      }
      // We now have the queue.
      {
        log::ScopedParamContainer params(logContext);
        params.add("tapepool", mountInfo.tapePool)
              .add("queueObject", aq.getAddressIfSet())
              .add("queueSize", aq.getJobsSummary().jobs);
        logContext.log(log::INFO, "In ArchiveMount::getNextJobBatch(): archive queue found.");
      }
      // The queue will give us a list of files to try and grab. We will attempt to 
      // dequeue them in one go, updating jobs in parallel. If some jobs turn out
      // to not be there really, we will have to do several passes.
      // We build directly the return value in the process.
      auto candidateJobsFromQueue=aq.getCandidateList(bytesRequested, filesRequested, archiveRequestsToSkip);
      std::list<std::unique_ptr<OStoreDB::ArchiveJob>> candidateJobs;
      // If we fail to find jobs in one round, we will exit.
      for (auto & cj: candidateJobsFromQueue.candidates) {
        currentFiles++;
        currentBytes+=cj.size;
        candidateJobs.emplace_back(new OStoreDB::ArchiveJob(cj.address, m_oStoreDB, *this));
        candidateJobs.back()->tapeFile.copyNb = cj.copyNb;
      }
      {
        log::ScopedParamContainer params(logContext);
        params.add("tapepool", mountInfo.tapePool)
              .add("queueObject", aq.getAddressIfSet())
              .add("candidatesCount", candidateJobs.size())
              .add("currentFiles", currentFiles)
              .add("currentBytes", currentBytes)
              .add("requestedFiles", filesRequested)
              .add("requestedBytes", bytesRequested);
        logContext.log(log::INFO, "In ArchiveMount::getNextJobBatch(): will process a set of candidate jobs.");
      }
      jobSelectionTime += t.secs(utils::Timer::resetCounter);
      // We now have a batch of jobs to try and dequeue. Should not be empty.
      // First add the jobs to the owned list of the agent.
      std::list<std::string> addedJobs;
      for (const auto &j: candidateJobs) addedJobs.emplace_back(j->m_archiveRequest.getAddressIfSet());
      m_oStoreDB.m_agentReference->addBatchToOwnership(addedJobs, m_oStoreDB.m_objectStore);
      ownershipAdditionTime += t.secs(utils::Timer::resetCounter);
      // We can now attempt to switch the ownership of the jobs. Depending on the type of failure (if any) we
      // will adapt the rest.
      // First, start the parallel updates of jobs
      std::list<std::unique_ptr<objectstore::ArchiveRequest::AsyncJobOwnerUpdater>> jobUpdates;
      for (const auto &j: candidateJobs) jobUpdates.emplace_back(
        j->m_archiveRequest.asyncUpdateJobOwner(j->tapeFile.copyNb, m_oStoreDB.m_agentReference->getAgentAddress(), aqAddress));
      asyncUpdateLaunchTime += t.secs(utils::Timer::resetCounter);
      // Now run through the results of the asynchronous updates. Non-success results come in the form of exceptions.
      std::list<std::string> jobsToForget; // The jobs either absent or not owned, for which we should just remove references (agent).
      std::list<std::string> jobsToDequeue; // The jobs that should not be queued anymore. All of them indeed (invalid or successfully poped).
      std::list<std::unique_ptr<OStoreDB::ArchiveJob>> validatedJobs; // The jobs we successfully validated.
      auto j=candidateJobs.begin(); // We will iterate on 2 lists...
      auto ju=jobUpdates.begin();
      while (ju!=jobUpdates.end()) {
        // Get the processing status of update
        try {
          (*ju)->wait();
          // Getting here means the update went through... We can proceed with removing the 
          // job from the queue, and populating the job to report in memory.
          jobsToDequeue.emplace_back((*j)->m_archiveRequest.getAddressIfSet());
          (*j)->archiveFile = (*ju)->getArchiveFile();
          (*j)->srcURL = (*ju)->getSrcURL();
          (*j)->archiveReportURL = (*ju)->getArchiveReportURL();
          (*j)->tapeFile.fSeq = ++nbFilesCurrentlyOnTape;
          (*j)->tapeFile.vid = mountInfo.vid;
          (*j)->tapeFile.blockId =
              std::numeric_limits<decltype((*j)->tapeFile.blockId)>::max();
          (*j)->m_jobOwned = true;
          (*j)->m_mountId = mountInfo.mountId;
          (*j)->m_tapePool = mountInfo.tapePool;
          log::ScopedParamContainer params(logContext);
          auto timingsReport = (*ju)->getTimeingsReport();
          params.add("tapepool", mountInfo.tapePool)
                .add("queueObject", aq.getAddressIfSet())
                .add("requestObject", (*j)->m_archiveRequest.getAddressIfSet())
                .add("fileId", (*j)->archiveFile.archiveFileID)
                .add("lockFetchTime", timingsReport.lockFetchTime)
                .add("processTime", timingsReport.processTime)
                .add("commitUnlockTime", timingsReport.commitUnlockTime);
          logContext.log(log::INFO, "In ArchiveMount::getNextJobBatch(): popped one job");
          validatedJobs.emplace_back(std::move(*j));
        } catch (cta::exception::Exception & e) {
          std::string debugType=typeid(e).name();
          if (typeid(e) == typeid(Backend::NoSuchObject) ||
              typeid(e) == typeid(objectstore::ArchiveRequest::WrongPreviousOwner)) {
            // The object was not present or not owned, so we skip it. It should be removed from
            // the queue.
            jobsToDequeue.emplace_back((*j)->m_archiveRequest.getAddressIfSet());
            // Log the event.
            log::ScopedParamContainer params(logContext);
            params.add("tapepool", mountInfo.tapePool)
                  .add("queueObject", aq.getAddressIfSet())
                  .add("requestObject", (*j)->m_archiveRequest.getAddressIfSet());
            logContext.log(log::WARNING, "In ArchiveMount::getNextJobBatch(): skipped job not owned or not present.");
          } else if (typeid(e) == typeid(Backend::CouldNotUnlock)) {
            // We failed to unlock the object. The request was successfully updated, so we do own it. This is a non-fatal
            // situation, so we just issue a warning. Removing the request from our agent ownership would 
            // orphan it.
            log::ScopedParamContainer params(logContext);
            int demangleStatus;
            char * exceptionTypeStr = abi::__cxa_demangle(typeid(e).name(), nullptr, nullptr, &demangleStatus);
            params.add("tapepool", mountInfo.tapePool)
                  .add("queueObject", aq.getAddressIfSet())
                  .add("requestObject", (*j)->m_archiveRequest.getAddressIfSet());
            if (!demangleStatus) {
              params.add("exceptionType", exceptionTypeStr);
            } else {
              params.add("exceptionType", typeid(e).name());
            }
            free(exceptionTypeStr);
            exceptionTypeStr = nullptr;
            params.add("message", e.getMessageValue());
            logContext.log(log::WARNING, "In ArchiveMount::getNextJobBatch(): Failed to unlock the request (lock expiration). Request remains selected.");
            validatedJobs.emplace_back(std::move(*j));
          } else {
            // This is not a success, yet we could not confirm the job status due to an unexpected error.
            // We leave the queue as is. We forget about owning this job. This is an error.
            log::ScopedParamContainer params(logContext);
            int demangleStatus;
            char * exceptionTypeStr = abi::__cxa_demangle(typeid(e).name(), nullptr, nullptr, &demangleStatus);
            params.add("tapepool", mountInfo.tapePool)
                  .add("queueObject", aq.getAddressIfSet())
                  .add("requestObject", (*j)->m_archiveRequest.getAddressIfSet());
            if (!demangleStatus) {
              params.add("exceptionType", exceptionTypeStr);
            } else {
              params.add("exceptionType", typeid(e).name());
            }
            free(exceptionTypeStr);
            exceptionTypeStr = nullptr;
            params.add("message", e.getMessageValue());
            logContext.log(log::ERR, "In ArchiveMount::getNextJobBatch(): unexpected error. Leaving the job queued.");
            jobsToForget.emplace_back((*j)->m_archiveRequest.getAddressIfSet());
            archiveRequestsToSkip.insert((*j)->m_archiveRequest.getAddressIfSet());
          }
          // This job is not for us.
          jobsToForget.emplace_back((*j)->m_archiveRequest.getAddressIfSet());
          // We also need to update the counts.
          currentFiles--;
          currentBytes-=(*j)->archiveFile.fileSize;
        }
        jobsUpdateTime += t.secs(utils::Timer::resetCounter);
        // In all cases: move to the nexts.
        ju=jobUpdates.erase(ju);
        j=candidateJobs.erase(j);
      }
      // All (most) jobs are now officially owned by our agent. We can hence remove them from the queue.
      aq.removeJobsAndCommit(jobsToDequeue);
      queueProcessAndCommitTime += t.secs(utils::Timer::resetCounter);
      if (jobsToForget.size()) {
        m_oStoreDB.m_agentReference->removeBatchFromOwnership(jobsToForget, m_oStoreDB.m_objectStore);
        ownershipRemovalTime += t.secs(utils::Timer::resetCounter);
      }
      // We can now add the validated jobs to the return value.
      auto vj = validatedJobs.begin();
      while (vj != validatedJobs.end()) {
        privateRet.emplace_back(std::move(*vj));
        vj=validatedJobs.erase(vj);
      }
      // Before going for another round, we can release the queue and delete it if we emptied it.
      auto remainingJobs=aq.dumpJobs().size();
      aqlock.release();
      // If the queue is empty, we can get rid of it.
      if (!remainingJobs) {
        try {
          // The queue should be removed as it is empty.
          ScopedExclusiveLock rexl(re);
          re.fetch();
          re.removeArchiveQueueAndCommit(mountInfo.tapePool, logContext);
          log::ScopedParamContainer params(logContext);
          params.add("tapepool", mountInfo.tapePool)
                .add("queueObject", aq.getAddressIfSet());
          logContext.log(log::INFO, "In ArchiveMount::getNextJobBatch(): deleted empty queue");
        } catch (cta::exception::Exception &ex) {
          log::ScopedParamContainer params(logContext);
          params.add("tapepool", mountInfo.tapePool)
                .add("queueObject", aq.getAddressIfSet())
                .add("Message", ex.getMessageValue());
          logContext.log(log::INFO, "In ArchiveMount::getNextJobBatch(): could not delete a presumably empty queue");
        }
      }
      // We can now summarize this round
      {
        log::ScopedParamContainer params(logContext);
        params.add("tapepool", mountInfo.tapePool)
              .add("queueObject", aq.getAddressIfSet())
              .add("filesAdded", currentFiles - beforeFiles)
              .add("bytesAdded", currentBytes - beforeBytes)
              .add("filesBefore", beforeFiles)
              .add("bytesBefore", beforeBytes)
              .add("filesAfter", currentFiles)
              .add("bytesAfter", currentBytes)
              .add("iterationCount", iterationCount);
        logContext.log(log::INFO, "In ArchiveMount::getNextJobBatch(): did one round of jobs retrieval.");
      }
      // We could be done now.
      if (currentBytes >= bytesRequested || currentFiles >= filesRequested)
        break;
      // If we had exhausted the queue while selecting the jobs, we stop here, else we can go for another
      // round.
      if (!candidateJobsFromQueue.remainingFilesAfterCandidates)
        break;
    } catch (cta::exception::Exception & ex) {
      log::ScopedParamContainer params (logContext);
      params.add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::ERR, "In OStoreDB::ArchiveMount::getNextJobBatch(): error (CTA exception) getting more jobs. Backtrace follows.");
      logContext.logBacktrace(log::ERR, ex.backtrace());
      break;
    } catch (std::exception & e) {
      log::ScopedParamContainer params (logContext);
      params.add("exceptionWhat", e.what());
      logContext.log(log::ERR, "In OStoreDB::ArchiveMount::getNextJobBatch(): error (std exception) getting more jobs.");
      break;
    } catch (...) {
      logContext.log(log::ERR, "In OStoreDB::ArchiveMount::getNextJobBatch(): error (unknown exception) getting more jobs.");
      break;
    }
  }
  // We either ran out of jobs or fulfilled the requirements. Time to build up the reply.
  // Log the outcome.
  uint64_t nFiles=privateRet.size();
  uint64_t nBytes=0;
  for (auto & j: privateRet) {
    nBytes+=j->archiveFile.fileSize;
  }
  {
    log::ScopedParamContainer params(logContext);
    params.add("tapepool", mountInfo.tapePool)
          .add("files", nFiles)
          .add("bytes", nBytes)
          .add("driveRegisterCheckTime", driveRegisterCheckTime)
          .add("findQueueTime", findQueueTime)
          .add("lockFetchQueueTime", lockFetchQueueTime)
          .add("emptyQueueCleanupTime", emptyQueueCleanupTime)
          .add("jobSelectionTime", jobSelectionTime)
          .add("ownershipAdditionTime", ownershipAdditionTime)
          .add("asyncUpdateLaunchTime", asyncUpdateLaunchTime)
          .add("jobsUpdateTime", jobsUpdateTime)
          .add("queueProcessAndCommitTime", queueProcessAndCommitTime)
          .add("ownershipRemovalTime", ownershipRemovalTime)
          .add("schedulerDbTime", totalTime.secs());
    logContext.log(log::INFO, "In ArchiveMount::getNextJobBatch(): jobs retrieval complete.");
  }
  // We can construct the return value.
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > ret;
  for (auto & j: privateRet) ret.emplace_back(std::move(j));
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::complete()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveMount::complete(time_t completionTime) {
  // When the session is complete, we can reset the status of the drive.
  // Tape will be implicitly released
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=mountInfo.drive;
  driveInfo.logicalLibrary=mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;
  ReportDriveStatusInputs inputs;
  inputs.mountType = common::dataStructures::MountType::NoMount;
  inputs.mountSessionId = mountInfo.mountId;
  inputs.reportTime = completionTime;
  inputs.status = common::dataStructures::DriveStatus::Up;
  inputs.vid = mountInfo.vid;
  inputs.tapepool = mountInfo.tapePool;
  log::LogContext lc(m_oStoreDB.m_logger);
  m_oStoreDB.updateDriveStatus(driveInfo, inputs, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::ArchiveJob()
//------------------------------------------------------------------------------
OStoreDB::ArchiveJob::ArchiveJob(const std::string& jobAddress, OStoreDB& oStoreDB, ArchiveMount& am): m_jobOwned(false),
  m_oStoreDB(oStoreDB), m_archiveRequest(jobAddress, m_oStoreDB.m_objectStore), m_archiveMount(am) {}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::RetrieveMount()
//------------------------------------------------------------------------------
OStoreDB::RetrieveMount::RetrieveMount(OStoreDB & oStoreDB): m_oStoreDB(oStoreDB) {}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::getMountInfo()
//------------------------------------------------------------------------------
const OStoreDB::RetrieveMount::MountInfo& OStoreDB::RetrieveMount::getMountInfo() {
  return mountInfo;
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::getNextJobBatch()
//------------------------------------------------------------------------------
std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob> > OStoreDB::RetrieveMount::getNextJobBatch(uint64_t filesRequested, 
    uint64_t bytesRequested, log::LogContext& logContext) {
  utils::Timer t, totalTime;
  double driveRegisterCheckTime = 0;
  double findQueueTime = 0;
  double lockFetchQueueTime = 0;
  double emptyQueueCleanupTime = 0;
  double jobSelectionTime = 0;
  double ownershipAdditionTime = 0;
  double asyncUpdateLaunchTime = 0;
  double jobsUpdateTime = 0;
  double queueProcessAndCommitTime = 0;
  double ownershipRemovalTime = 0;
  // Find the next files to retrieve
  // First, check we should not forcibly go down. In such an occasion, we just find noting to do.
  // Get drive register
  {
    // Get the drive status. Failure is non-fatal. We will carry on.
    try { 
      objectstore::RootEntry re(m_oStoreDB.m_objectStore);
      re.fetchNoLock();
      objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_oStoreDB.m_objectStore);
      dr.fetchNoLock();
      objectstore::DriveState ds(dr.getDriveAddress(mountInfo.drive), m_oStoreDB.m_objectStore);
      ds.fetchNoLock();
      auto driveStateValue = ds.getState();
      if (!driveStateValue.desiredDriveState.up && driveStateValue.desiredDriveState.forceDown) {
        logContext.log(log::INFO, "In OStoreDB::RetrieveMount::getNextJobBatch(): returning no job as we are forcibly going down.");
        return std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob> >();
      }
    } catch (cta::exception::Exception & ex) {
      log::ScopedParamContainer params (logContext);
      params.add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::INFO, "In OStoreDB::RetrieveMount::getNextJobBatch(): failed to check up/down status.");
    }
    driveRegisterCheckTime = t.secs(utils::Timer::resetCounter);
  }
  // Now, we should repeatedly fetch jobs from the queue until we fulfilled the request or there is nothing to get form the
  // queue anymore.
  // Requests are left as-is on errors. We will keep a list of them to avoid re-accessing them in the same batch.
  std::set<std::string> retrieveRequestsToSkip;
  // Prepare the returned jobs that we might accumulate in several rounds.
  std::list<std::unique_ptr<OStoreDB::RetrieveJob>> privateRet;
  uint64_t currentBytes=0;
  uint64_t currentFiles=0;
  size_t iterationCount=0;
  while (true) {
    iterationCount++;
    uint64_t beforeBytes=currentBytes;
    uint64_t beforeFiles=currentFiles;
    // Try and get access to a queue.
    objectstore::RootEntry re(m_oStoreDB.m_objectStore);
    re.fetchNoLock();
    std::string rqAddress;
    auto rql = re.dumpRetrieveQueues();
    for (auto & rqp : rql) {
    if (rqp.vid == mountInfo.vid)
      rqAddress = rqp.address;
    }
    if (!rqAddress.size()) break;
    // try and lock the archive queue. Any failure from here on means the end of the getting jobs.
    objectstore::RetrieveQueue rq(rqAddress, m_oStoreDB.m_objectStore);
    objectstore::ScopedExclusiveLock rqLock;
    findQueueTime += t.secs(utils::Timer::resetCounter);
    try {
      try {
        rqLock.lock(rq);
        rq.fetch();
        lockFetchQueueTime += t.secs(utils::Timer::resetCounter);
      } catch (cta::exception::Exception & ex) {
        // The queue is now absent. We can remove its reference in the root entry.
        // A new queue could have been added in the mean time, and be non-empty.
        // We will then fail to remove from the RootEntry (non-fatal).
        ScopedExclusiveLock rexl(re);
        re.fetch();
        try {
          re.removeRetrieveQueueAndCommit(mountInfo.vid, logContext);
          log::ScopedParamContainer params(logContext);
          params.add("vid", mountInfo.vid)
                .add("queueObject", rq.getAddressIfSet());
          logContext.log(log::INFO, "In RetrieveMount::getNextJobBatch(): de-referenced missing queue from root entry");
        } catch (RootEntry::ArchiveQueueNotEmpty & ex) {
          // TODO: improve: if we fail here we could retry to fetch a job.
          log::ScopedParamContainer params(logContext);
          params.add("vid", mountInfo.vid)
                .add("queueObject", rq.getAddressIfSet())
                .add("Message", ex.getMessageValue());
          logContext.log(log::INFO, "In RetrieveMount::getNextJobBatch(): could not de-referenced missing queue from root entry");
        }
        emptyQueueCleanupTime += t.secs(utils::Timer::resetCounter);
        continue;
      }
      // We now have the queue.
      {
        log::ScopedParamContainer params(logContext);
        params.add("vid", mountInfo.vid)
              .add("queueObject", rq.getAddressIfSet())
              .add("queueSize", rq.getJobsSummary().files);
        logContext.log(log::INFO, "In RetrieveMount::getNextJobBatch(): retrieve queue found.");
      }
      // We should build the list of jobs we intend to grab. We will attempt to 
      // dequeue them in one go, updating jobs in parallel. If some jobs turn out
      // to not be there really, we will have to do several passes.
      // We build directly the return value in the process.
      auto candidateJobsFromQueue=rq.getCandidateList(bytesRequested, filesRequested, retrieveRequestsToSkip);
      std::list<std::unique_ptr<OStoreDB::RetrieveJob>> candidateJobs;
      // If we fail to find jobs in one round, we will exit.
      for (auto & cj: candidateJobsFromQueue.candidates) {
        currentFiles++;
        currentBytes+=cj.size;
        candidateJobs.emplace_back(new OStoreDB::RetrieveJob(cj.address, m_oStoreDB, *this));
        candidateJobs.back()->selectedCopyNb = cj.copyNb;
      }
      {
        log::ScopedParamContainer params(logContext);
        params.add("vid", mountInfo.vid)
              .add("queueObject", rq.getAddressIfSet())
              .add("candidatesCount", candidateJobs.size())
              .add("currentFiles", currentFiles)
              .add("currentBytes", currentBytes)
              .add("requestedFiles", filesRequested)
              .add("requestedBytes", bytesRequested);
        logContext.log(log::INFO, "In RetrieveMount::getNextJobBatch(): will process a set of candidate jobs.");
      }
      jobSelectionTime += t.secs(utils::Timer::resetCounter);
      // We now have a batch of jobs to try and dequeue. Should not be empty.
      // First add the jobs to the owned list of the agent.
      std::list<std::string> addedJobs;
      for (const auto &j: candidateJobs) addedJobs.emplace_back(j->m_retrieveRequest.getAddressIfSet());
      m_oStoreDB.m_agentReference->addBatchToOwnership(addedJobs, m_oStoreDB.m_objectStore);
      ownershipAdditionTime += t.secs(utils::Timer::resetCounter);
      // We can now attempt to switch the ownership of the jobs. Depending on the type of failure (if any) we
      // will adapt the rest.
      // First, start the parallel updates of jobs
      std::list<std::unique_ptr<objectstore::RetrieveRequest::AsyncOwnerUpdater>> jobUpdates;
      for (const auto &j: candidateJobs) jobUpdates.emplace_back(
        j->m_retrieveRequest.asyncUpdateOwner(j->selectedCopyNb, m_oStoreDB.m_agentReference->getAgentAddress(), rqAddress));
      asyncUpdateLaunchTime += t.secs(utils::Timer::resetCounter);
      // Now run through the results of the asynchronous updates. Non-sucess results come in the form of exceptions.
      std::list<std::string> jobsToForget; // The jobs either absent or not owned, for which we should just remove references (agent).
      std::list<std::string> jobsToDequeue; // The jobs that should not be queued anymore. All of them indeed (invalid or successfully poped).
      std::list<std::unique_ptr<OStoreDB::RetrieveJob>> validatedJobs; // The jobs we successfully validated.
      auto j=candidateJobs.begin(); // We will iterate on 2 lists...
      auto ju=jobUpdates.begin();
      while (ju!=jobUpdates.end()) {
        // Get the processing status of update
        try {
          (*ju)->wait();
          // Getting here means the update went through... We can proceed with removing the 
          // job from the queue, and populating the job to report in memory.
          jobsToDequeue.emplace_back((*j)->m_retrieveRequest.getAddressIfSet());
          (*j)->archiveFile = (*ju)->getArchiveFile();
          (*j)->retrieveRequest = (*ju)->getRetrieveRequest();
          (*j)->m_jobOwned = true;
          (*j)->m_mountId = mountInfo.mountId;
          log::ScopedParamContainer params(logContext);
          params.add("vid", mountInfo.vid)
                .add("queueObject", rq.getAddressIfSet())
                .add("requestObject", (*j)->m_retrieveRequest.getAddressIfSet())
                .add("fileId", (*j)->archiveFile.archiveFileID);
          logContext.log(log::INFO, "In RetrieveMount::getNextJobBatch(): popped one job");
          validatedJobs.emplace_back(std::move(*j));
        } catch (cta::exception::Exception & e) {
          std::string debugType=typeid(e).name();
          if (typeid(e) == typeid(Backend::NoSuchObject) ||
              typeid(e) == typeid(objectstore::RetrieveRequest::WrongPreviousOwner)) {
            // The object was not present or not owned, so we skip it. It should be removed from
            // the queue.
            jobsToDequeue.emplace_back((*j)->m_retrieveRequest.getAddressIfSet());
            // Log the event.
            log::ScopedParamContainer params(logContext);
            params.add("vid", mountInfo.vid)
                  .add("queueObject", rq.getAddressIfSet())
                  .add("requestObject", (*j)->m_retrieveRequest.getAddressIfSet());
            logContext.log(log::WARNING, "In RetrieveMount::getNextJobBatch(): skipped job not owned or not present.");
          } else if (typeid(e) == typeid(Backend::CouldNotUnlock)) {
            // We failed to unlock the object. The request was successfully updated, so we do own it. This is a non-fatal
            // situation, so we just issue a warning. Removing the request from our agent ownership would 
            // orphan it.
            log::ScopedParamContainer params(logContext);
            int demangleStatus;
            char * exceptionTypeStr = abi::__cxa_demangle(typeid(e).name(), nullptr, nullptr, &demangleStatus);
            params.add("vid", mountInfo.vid)
                  .add("queueObject", rq.getAddressIfSet())
                  .add("requestObject", (*j)->m_retrieveRequest.getAddressIfSet());
            if (!demangleStatus) {
              params.add("exceptionType", exceptionTypeStr);
            } else {
              params.add("exceptionType", typeid(e).name());
            }
            free(exceptionTypeStr);
            exceptionTypeStr = nullptr;
            params.add("message", e.getMessageValue());
            logContext.log(log::WARNING, "In RetrieveMount::getNextJobBatch(): Failed to unlock the request (lock expiration). Request remains selected.");
            validatedJobs.emplace_back(std::move(*j));
          } else {
            // This is not a success, yet we could not confirm the job status due to an unexpected error.
            // We leave the queue as is. We forget about owning this job. This is an error.
            log::ScopedParamContainer params(logContext);
            int demangleStatus;
            char * exceptionTypeStr = abi::__cxa_demangle(typeid(e).name(), nullptr, nullptr, &demangleStatus);
            params.add("vid", mountInfo.vid)
                  .add("queueObject", rq.getAddressIfSet())
                  .add("requestObject", (*j)->m_retrieveRequest.getAddressIfSet());
            if (!demangleStatus) {
              params.add("exceptionType", exceptionTypeStr);
            } else {
              params.add("exceptionType", typeid(e).name());
            }
            free(exceptionTypeStr);
            exceptionTypeStr = nullptr;
            params.add("message", e.getMessageValue());
            logContext.log(log::ERR, "In RetrieveMount::getNextJobBatch(): unexpected error. Leaving the job queued.");
            jobsToForget.emplace_back((*j)->m_retrieveRequest.getAddressIfSet());
            retrieveRequestsToSkip.insert((*j)->m_retrieveRequest.getAddressIfSet());
          }
          // This job is not for us.
          jobsToForget.emplace_back((*j)->m_retrieveRequest.getAddressIfSet());
          // We also need to update the counts.
          currentFiles--;
          currentBytes-=(*j)->archiveFile.fileSize;
        }
        jobsUpdateTime += t.secs(utils::Timer::resetCounter);
        // In all cases: move to the nexts.
        ju=jobUpdates.erase(ju);
        j=candidateJobs.erase(j);
      }
      // All (most) jobs are now officially owned by our agent. We can hence remove them from the queue.
      rq.removeJobsAndCommit(jobsToDequeue);
      ownershipRemovalTime += t.secs(utils::Timer::resetCounter);
      if (jobsToForget.size()) {
        m_oStoreDB.m_agentReference->removeBatchFromOwnership(jobsToForget, m_oStoreDB.m_objectStore);
        ownershipRemovalTime += t.secs(utils::Timer::resetCounter);
      }
      // We can now add the validated jobs to the return value.
      auto vj = validatedJobs.begin();
      while (vj != validatedJobs.end()) {
        privateRet.emplace_back(std::move(*vj));
        vj=validatedJobs.erase(vj);
      }
      // Before going for another round, we can release the queue and delete it if we emptied it.
      auto remainingJobs=rq.getJobsSummary().files;
      rqLock.release();
      // If the queue is empty, we can get rid of it.
      if (!remainingJobs) {
        try {
          // The queue should be removed as it is empty.
          ScopedExclusiveLock rexl(re);
          re.fetch();
          re.removeRetrieveQueueAndCommit(mountInfo.vid, logContext);
          log::ScopedParamContainer params(logContext);
          params.add("vid", mountInfo.vid)
                .add("queueObject", rq.getAddressIfSet());
          logContext.log(log::INFO, "In RetrieveMount::getNextJobBatch(): deleted empty queue");
        } catch (cta::exception::Exception &ex) {
          log::ScopedParamContainer params(logContext);
          params.add("vid", mountInfo.vid)
                .add("queueObject", rq.getAddressIfSet())
                .add("Message", ex.getMessageValue());
          logContext.log(log::INFO, "In RetrieveMount::getNextJobBatch(): could not delete a presumably empty queue");
        }
      }
      // We can now summarize this round
      {
        log::ScopedParamContainer params(logContext);
        params.add("tapepool", mountInfo.tapePool)
              .add("queueObject", rq.getAddressIfSet())
              .add("filesAdded", currentFiles - beforeFiles)
              .add("bytesAdded", currentBytes - beforeBytes)
              .add("filesBefore", beforeFiles)
              .add("bytesBefore", beforeBytes)
              .add("filesAfter", currentFiles)
              .add("bytesAfter", currentBytes)
              .add("iterationCount", iterationCount);
        logContext.log(log::INFO, "In RetrieveMount::getNextJobBatch(): did one round of jobs retrieval.");
      }
      // We could be done now.
      if (currentBytes >= bytesRequested || currentFiles >= filesRequested)
        break;
      // If we had exhausted the queue while selecting the jobs, we stop here, else we can go for another
      // round.
      if (!candidateJobsFromQueue.remainingFilesAfterCandidates)
        break;
    } catch (cta::exception::Exception & ex) {
      log::ScopedParamContainer params (logContext);
      params.add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::ERR, "In OStoreDB::RetrieveMount::getNextJobBatch(): error (CTA exception) getting more jobs. Backtrace follows.");
      logContext.logBacktrace(log::ERR, ex.backtrace());
      break;
    } catch (std::exception & e) {
      log::ScopedParamContainer params (logContext);
      params.add("exceptionWhat", e.what());
      logContext.log(log::ERR, "In OStoreDB::RetrieveMount::getNextJobBatch(): error (std exception) getting more jobs.");
      break;
    } catch (...) {
      logContext.log(log::ERR, "In OStoreDB::RetrieveMount::getNextJobBatch(): error (unknown exception) getting more jobs.");
      break;
    }
  }
  // We either ran out of jobs or fulfilled the requirements. Time to build up the reply.
  // Log the outcome.
  uint64_t nFiles=privateRet.size();
  uint64_t nBytes=0;
  for (auto & j: privateRet) {
    nBytes+=j->archiveFile.fileSize;
  }
  {
    log::ScopedParamContainer params(logContext);
    params.add("tapepool", mountInfo.tapePool)
          .add("files", nFiles)
          .add("bytes", nBytes)
          .add("driveRegisterCheckTime", driveRegisterCheckTime)
          .add("findQueueTime", findQueueTime)
          .add("lockFetchQueueTime", lockFetchQueueTime)
          .add("emptyQueueCleanupTime", emptyQueueCleanupTime)
          .add("jobSelectionTime", jobSelectionTime)
          .add("ownershipAdditionTime", ownershipAdditionTime)
          .add("asyncUpdateLaunchTime", asyncUpdateLaunchTime)
          .add("jobsUpdateTime", jobsUpdateTime)
          .add("queueProcessAndCommitTime", queueProcessAndCommitTime)
          .add("ownershipRemovalTime", ownershipRemovalTime)
          .add("schedulerDbTime", totalTime.secs());
    logContext.log(log::INFO, "In RetrieveMount::getNextJobBatch(): jobs retrieval complete.");
  }
  // We can construct the return value.
  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob> > ret;
  for (auto & j: privateRet) ret.emplace_back(std::move(j));
  return ret;
}


//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::complete()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveMount::complete(time_t completionTime) {
  // When the session is complete, we can reset the status of the tape and the
  // drive
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=mountInfo.drive;
  driveInfo.logicalLibrary=mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;
  ReportDriveStatusInputs inputs;
  inputs.mountType = common::dataStructures::MountType::NoMount;
  inputs.mountSessionId = mountInfo.mountId;
  inputs.reportTime = completionTime;
  inputs.status = common::dataStructures::DriveStatus::Up;
  inputs.vid = mountInfo.vid;
  inputs.tapepool = mountInfo.tapePool;
  log::LogContext lc(m_oStoreDB.m_logger);
  m_oStoreDB.updateDriveStatus(driveInfo, inputs, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::setDriveStatus()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveMount::setDriveStatus(cta::common::dataStructures::DriveStatus status, time_t completionTime) {
  // We just report the drive status as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=mountInfo.drive;
  driveInfo.logicalLibrary=mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;
  ReportDriveStatusInputs inputs;
  inputs.mountType = common::dataStructures::MountType::Retrieve;
  inputs.mountSessionId = mountInfo.mountId;
  inputs.reportTime = completionTime;
  inputs.status = status;
  inputs.vid = mountInfo.vid;
  inputs.tapepool = mountInfo.tapePool;
  // TODO: statistics!
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  inputs.latestBandwidth = 0;
  log::LogContext lc(m_oStoreDB.m_logger);
  m_oStoreDB.updateDriveStatus(driveInfo, inputs, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::setTapeSessionStats()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) {
  // We just report tthe tape session statistics as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=mountInfo.drive;
  driveInfo.logicalLibrary=mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;
  
  ReportDriveStatsInputs inputs;
  inputs.reportTime = time(nullptr); 
  inputs.bytesTransferred = stats.dataVolume;
  inputs.filesTransferred = stats.filesCount;
  
  log::LogContext lc(m_oStoreDB.m_logger);
  m_oStoreDB.updateDriveStatistics(driveInfo, inputs, lc);
}


//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::castFromSchedDBJob()
//------------------------------------------------------------------------------
OStoreDB::RetrieveJob * OStoreDB::RetrieveMount::castFromSchedDBJob(SchedulerDatabase::RetrieveJob * job) {
  OStoreDB::RetrieveJob * ret = dynamic_cast<OStoreDB::RetrieveJob *> (job);
  if (!ret) {
    std::string unexpectedType = typeid(*job).name();
    throw cta::exception::Exception(std::string("In OStoreDB::RetrieveMount::castFromSchedDBJob(): unexpected archive job type while casting: ")+
        unexpectedType);
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::waitAndFinishSettingJobsBatchSuccessful()
//------------------------------------------------------------------------------
std::set<cta::SchedulerDatabase::RetrieveJob*> OStoreDB::RetrieveMount::finishSettingJobsBatchSuccessful(
  std::list<cta::SchedulerDatabase::RetrieveJob*>& jobsBatch, log::LogContext& lc) {
  std::set<cta::SchedulerDatabase::RetrieveJob*> ret;
  std::list<std::string> rjToUnown;
  // We will wait on the asynchronously started reports of jobs and remove them from
  // ownership.
  for (auto & sDBJob: jobsBatch) {
    auto osdbJob = castFromSchedDBJob(sDBJob);
    rjToUnown.push_back(osdbJob->m_retrieveRequest.getAddressIfSet());
    ret.insert(sDBJob);
  }
  m_oStoreDB.m_agentReference->removeBatchFromOwnership(rjToUnown, m_oStoreDB.m_objectStore);
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::setDriveStatus()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveMount::setDriveStatus(cta::common::dataStructures::DriveStatus status, time_t completionTime) {
  // We just report the drive status as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=mountInfo.drive;
  driveInfo.logicalLibrary=mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;
  ReportDriveStatusInputs inputs;
  inputs.mountType = common::dataStructures::MountType::Archive;
  inputs.mountSessionId = mountInfo.mountId;
  inputs.reportTime = completionTime;
  inputs.status = status;
  inputs.vid = mountInfo.vid;
  inputs.tapepool = mountInfo.tapePool;
  // TODO: statistics!
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  inputs.latestBandwidth = 0;
  log::LogContext lc(m_oStoreDB.m_logger);
  m_oStoreDB.updateDriveStatus(driveInfo, inputs, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::setTapeSessionStats()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) {
  // We just report the tape session statistics as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=mountInfo.drive;
  driveInfo.logicalLibrary=mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;
  
  ReportDriveStatsInputs inputs;
  inputs.reportTime = time(nullptr); 
  inputs.bytesTransferred = stats.dataVolume;
  inputs.filesTransferred = stats.filesCount;
  
  log::LogContext lc(m_oStoreDB.m_logger);
  m_oStoreDB.updateDriveStatistics(driveInfo, inputs, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::castFromSchedDBJob()
//------------------------------------------------------------------------------
OStoreDB::ArchiveJob * OStoreDB::ArchiveMount::castFromSchedDBJob(SchedulerDatabase::ArchiveJob * job) {
  OStoreDB::ArchiveJob * ret = dynamic_cast<OStoreDB::ArchiveJob *> (job);
  if (!ret) {
    std::string unexpectedType = typeid(*job).name();
    throw cta::exception::Exception(std::string("In OStoreDB::ArchiveMount::castFromSchedDBJob(): unexpected archive job type while casting: ")+
        unexpectedType);
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::asyncSetJobBatchSuccessful()
//------------------------------------------------------------------------------
std::set<cta::SchedulerDatabase::ArchiveJob*> OStoreDB::ArchiveMount::setJobBatchSuccessful(
  std::list<cta::SchedulerDatabase::ArchiveJob*>& jobsBatch, log::LogContext & lc) {
  std::set<cta::SchedulerDatabase::ArchiveJob*> ret;
  std::list<std::string> ajToUnown;
  // We will asynchronously report the archive jobs (which MUST be OStoreDBJobs).
  // We let the exceptions through as failing to report is fatal.
  for (auto & sDBJob: jobsBatch) {
    castFromSchedDBJob(sDBJob)->asyncSucceed();
  }
  for (auto & sDBJob: jobsBatch) {
    if (castFromSchedDBJob(sDBJob)->waitAsyncSucceed())
      ret.insert(sDBJob);
    ajToUnown.push_back(castFromSchedDBJob(sDBJob)->m_archiveRequest.getAddressIfSet());
  }
  m_oStoreDB.m_agentReference->removeBatchFromOwnership(ajToUnown, m_oStoreDB.m_objectStore);
  return ret;
}


//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::fail()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveJob::fail(log::LogContext & lc) {
  if (!m_jobOwned)
    throw JobNowOwned("In OStoreDB::ArchiveJob::fail: cannot fail a job not owned");
  // Lock the archive request. Fail the job.
  objectstore::ScopedExclusiveLock arl(m_archiveRequest);
  m_archiveRequest.fetch();
  // Add a job failure. If the job is failed, we will delete it.
  if (m_archiveRequest.addJobFailure(tapeFile.copyNb, m_mountId, lc)) {
    // The job will not be retried. Either another jobs for the same request is 
    // queued and keeps the request referenced or the request has been deleted.
    // In any case, we can forget it.
    m_oStoreDB.m_agentReference->removeFromOwnership(m_archiveRequest.getAddressIfSet(), m_oStoreDB.m_objectStore);
    m_jobOwned = false;
    return;
  }
  // The job still has a chance, return it to its original tape pool's queue
  objectstore::ArchiveQueue aq(m_oStoreDB.m_objectStore);
  objectstore::ScopedExclusiveLock aqlock;
  objectstore::Helpers::getLockedAndFetchedQueue<ArchiveQueue>(aq, aqlock, *m_oStoreDB.m_agentReference, m_tapePool, lc);
  // Find the right job
  auto jl = m_archiveRequest.dumpJobs();
  for (auto & j:jl) {
    if (j.copyNb == tapeFile.copyNb) {
      std::list<objectstore::ArchiveQueue::JobToAdd> jta;
      jta.push_back({j, m_archiveRequest.getAddressIfSet(), m_archiveRequest.getArchiveFile().archiveFileID,
          m_archiveRequest.getArchiveFile().fileSize, m_archiveRequest.getMountPolicy(), m_archiveRequest.getEntryLog().time});
      aq.addJobsIfNecessaryAndCommit(jta, *m_oStoreDB.m_agentReference, lc);
      aqlock.release();
      // We have a pointer to the job, we can change the job ownership
      m_archiveRequest.setJobOwner(tapeFile.copyNb, aq.getAddressIfSet());
      m_archiveRequest.commit();
      arl.release();
      // We just have to remove the ownership from the agent and we're done.
      m_oStoreDB.m_agentReference->removeFromOwnership(m_archiveRequest.getAddressIfSet(), m_oStoreDB.m_objectStore);
      m_jobOwned = false;
      return;
    }
  }
  throw NoSuchJob("In OStoreDB::ArchiveJob::fail(): could not find the job in the request object");
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::bumpUpTapeFileCount()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveJob::bumpUpTapeFileCount(uint64_t newFileCount) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
//  m_archiveMount.setTapeMaxFileCount(newFileCount);
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::asyncSucceed()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveJob::asyncSucceed() {
  log::LogContext lc(m_oStoreDB.m_logger);
  log::ScopedParamContainer params(lc);
  params.add("requestObject", m_archiveRequest.getAddressIfSet());
  lc.log(log::DEBUG, "Will start async update archiveRequest for success");
  m_jobUpdate.reset(m_archiveRequest.asyncUpdateJobSuccessful(tapeFile.copyNb));
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::checkSucceed()
//------------------------------------------------------------------------------
bool OStoreDB::ArchiveJob::waitAsyncSucceed() {  
  m_jobUpdate->wait();
  log::LogContext lc(m_oStoreDB.m_logger);
  log::ScopedParamContainer params(lc);
  params.add("requestObject", m_archiveRequest.getAddressIfSet());
  lc.log(log::DEBUG, "Async update of archiveRequest for success complete");
  if (m_jobUpdate->m_isLastJob) {
    m_archiveRequest.resetValues();
  }
  // We no more own the job (which could be gone)
  m_jobOwned = false;
  // Ownership removal will be done globally by the caller.
  return m_jobUpdate->m_isLastJob;
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::~ArchiveJob()
//------------------------------------------------------------------------------
OStoreDB::ArchiveJob::~ArchiveJob() {
  if (m_jobOwned) {
    log::LogContext lc(m_oStoreDB.m_logger);
    // Just log that the object will be left in agent's ownership.
    log::ScopedParamContainer params(lc);
    params.add("agentObject", m_oStoreDB.m_agentReference->getAgentAddress())
          .add("jobObject", m_archiveRequest.getAddressIfSet());
    lc.log(log::INFO, "In OStoreDB::ArchiveJob::~ArchiveJob(): will leave the job owned after destruction.");
  }
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::RetrieveJob()
//------------------------------------------------------------------------------
OStoreDB::RetrieveJob::RetrieveJob(const std::string& jobAddress, OStoreDB & oStoreDB, OStoreDB::RetrieveMount& rm): 
  m_jobOwned(false), m_oStoreDB(oStoreDB), m_retrieveRequest(jobAddress, m_oStoreDB.m_objectStore), m_retrieveMount(rm) { }

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::fail()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveJob::fail(log::LogContext &logContext) {
  if (!m_jobOwned)
    throw JobNowOwned("In OStoreDB::RetrieveJob::fail: cannot fail a job not owned");
  // Lock the retrieve request. Fail the job.
  objectstore::ScopedExclusiveLock rrl(m_retrieveRequest);
  m_retrieveRequest.fetch();
  // Add a job failure. If the job is failed, we will delete it.
  if (m_retrieveRequest.addJobFailure(selectedCopyNb, m_mountId, logContext)) {
    // The job will not be retried. Either another jobs for the same request is 
    // queued and keeps the request referenced or the request has been deleted.
    // In any case, we can forget it.
    m_oStoreDB.m_agentReference->removeFromOwnership(m_retrieveRequest.getAddressIfSet(), m_oStoreDB.m_objectStore);
    m_jobOwned = false;
    log::ScopedParamContainer params(logContext);
    params.add("object", m_retrieveRequest.getAddressIfSet());
    logContext.log(log::ERR, "In OStoreDB::RetrieveJob::fail(): request was definitely failed and deleted.");
    return;
  }
  // The job still has a chance, requeue is to the best tape.
  // Get the best vid from the cache
  std::set<std::string> candidateVids;
  using serializers::RetrieveJobStatus;
  std::set<serializers::RetrieveJobStatus> finishedStatuses(
    {RetrieveJobStatus::RJS_Complete, RetrieveJobStatus::RJS_Failed});
  for (auto & tf: m_retrieveRequest.getRetrieveFileQueueCriteria().archiveFile.tapeFiles)
    if (!finishedStatuses.count(m_retrieveRequest.getJobStatus(tf.second.copyNb)))
      candidateVids.insert(tf.second.vid);
  if (candidateVids.empty())
    throw cta::exception::Exception("In OStoreDB::RetrieveJob::fail(): no active job after addJobFailure() returned false.");
  std::string bestVid=Helpers::selectBestRetrieveQueue(candidateVids, m_oStoreDB.m_catalogue, m_oStoreDB.m_objectStore);
  // Check that the requested retrieve job (for the provided vid) exists, and record the copynb.
  uint64_t bestCopyNb;
  for (auto & tf: m_retrieveRequest.getRetrieveFileQueueCriteria().archiveFile.tapeFiles) {
    if (tf.second.vid == bestVid) {
      bestCopyNb = tf.second.copyNb;
      goto vidFound;
    }
  }
  {
    std::stringstream err;
    err << "In OStoreDB::RetrieveJob::fail(): no tape file for requested vid. archiveId="
        << m_retrieveRequest.getRetrieveFileQueueCriteria().archiveFile.archiveFileID
        << " vid=" << bestVid;
    throw RetrieveRequestHasNoCopies(err.str());
  }
  vidFound:
  {
    // Add the request to the queue.
    objectstore::RetrieveQueue rq(m_oStoreDB.m_objectStore);
    objectstore::ScopedExclusiveLock rql;
    objectstore::Helpers::getLockedAndFetchedQueue<RetrieveQueue>(rq, rql, *m_oStoreDB.m_agentReference, bestVid, logContext);
    auto rfqc = m_retrieveRequest.getRetrieveFileQueueCriteria();
    auto & af=rfqc.archiveFile;
    auto & tf = af.tapeFiles.at(bestCopyNb);
    auto sr = m_retrieveRequest.getSchedulerRequest();
    std::list<objectstore::RetrieveQueue::JobToAdd> jta;
    jta.push_back({bestCopyNb, tf.fSeq, m_retrieveRequest.getAddressIfSet(), af.fileSize, rfqc.mountPolicy, sr.creationLog.time});
    rq.addJobsIfNecessaryAndCommit(jta, *m_oStoreDB.m_agentReference, logContext);
    m_retrieveRequest.setOwner(rq.getAddressIfSet());
    m_retrieveRequest.commit();
    // We do not own the request anymore
    m_jobOwned = false;
    // The lock on the queue is released here (has to be after the request commit for consistency.
  }
  rrl.release();
  // And relinquish ownership form agent
  m_oStoreDB.m_agentReference->removeFromOwnership(m_retrieveRequest.getAddressIfSet(), m_oStoreDB.m_objectStore);
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::~RetrieveJob()
//------------------------------------------------------------------------------
OStoreDB::RetrieveJob::~RetrieveJob() {
  if (m_jobOwned) {
    log::LogContext lc(m_oStoreDB.m_logger);
    // Just log that the object will be left in agent's ownership.
    log::ScopedParamContainer params(lc);
    params.add("agentObject", m_oStoreDB.m_agentReference->getAgentAddress())
          .add("jobObject", m_retrieveRequest.getAddressIfSet());
    lc.log(log::INFO, "In OStoreDB::RetrieveJob::~RetrieveJob(): will leave the job owned after destruction.");
  }
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::asyncSucceed()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveJob::asyncSucceed() {
  // set the request as successful (delete it).
  m_jobDelete.reset(m_retrieveRequest.asyncDeleteJob());
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::checkSucceed()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveJob::checkSucceed() {
  m_jobDelete->wait();
  m_retrieveRequest.resetValues();
  // We no more own the job (which could be gone)
  m_jobOwned = false;
  // Ownership will ber removed from agent by caller through retrieve mount object.
}

} // namespace cta

