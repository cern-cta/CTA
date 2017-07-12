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
OStoreDB::OStoreDB(objectstore::Backend& be):
  m_objectStore(be) {}

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
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
}

//------------------------------------------------------------------------------
// OStoreDB::fetchMountInfo()
//------------------------------------------------------------------------------
void OStoreDB::fetchMountInfo(SchedulerDatabase::TapeMountDecisionInfo& tmdi, RootEntry& re) {
  // Walk the archive queues for statistics
  for (auto & aqp: re.dumpArchiveQueues()) {
    objectstore::ArchiveQueue aqueue(aqp.address, m_objectStore);
    // debug utility variable
    std::string __attribute__((__unused__)) poolName = aqp.tapePool;
    objectstore::ScopedSharedLock aqlock(aqueue);
    aqueue.fetch();
    // If there are files queued, we create an entry for this tape pool in the
    // mount candidates list.
    if (aqueue.getJobsSummary().files) {
      tmdi.potentialMounts.push_back(SchedulerDatabase::PotentialMount());
      auto & m = tmdi.potentialMounts.back();
      m.tapePool = aqp.tapePool;
      m.type = cta::common::dataStructures::MountType::Archive;
      m.bytesQueued = aqueue.getJobsSummary().bytes;
      m.filesQueued = aqueue.getJobsSummary().files;      
      m.oldestJobStartTime = aqueue.getJobsSummary().oldestJobStartTime;
      m.priority = aqueue.getJobsSummary().priority;
      m.maxDrivesAllowed = aqueue.getJobsSummary().maxDrivesAllowed;
      m.minArchiveRequestAge = aqueue.getJobsSummary().minArchiveRequestAge;
      m.logicalLibrary = "";
    }
  }
  // Walk the retrieve queues for statistics
  for (auto & rqp: re.dumpRetrieveQueues()) {
    RetrieveQueue rqueue(rqp.address, m_objectStore);
    // debug utility variable
    std::string __attribute__((__unused__)) vid = rqp.vid;
    ScopedSharedLock rqlock(rqueue);
    rqueue.fetch();
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
      m.minArchiveRequestAge = rqueue.getJobsSummary().minArchiveRequestAge;
      m.logicalLibrary = ""; // The logical library is not known here, and will be determined by the caller.
    }
  }
  // Collect information about the existing and next mounts
  // If a next mount exists the drive "counts double", but the corresponding drive
  // is either about to mount, or about to replace its current mount.
  objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_objectStore);
  objectstore::ScopedSharedLock drl(dr);
  dr.fetch();
  auto dl = dr.getAllDrivesState();
  using common::dataStructures::DriveStatus;
  std::set<int> activeDriveStatuses = {
    (int)cta::common::dataStructures::DriveStatus::Starting,
    (int)cta::common::dataStructures::DriveStatus::Mounting,
    (int)cta::common::dataStructures::DriveStatus::Transferring,
    (int)cta::common::dataStructures::DriveStatus::Unloading,
    (int)cta::common::dataStructures::DriveStatus::Unmounting,
    (int)cta::common::dataStructures::DriveStatus::DrainingToDisk };
  std::set<int> activeMountTypes = {
    (int)cta::common::dataStructures::MountType::Archive,
    (int)cta::common::dataStructures::MountType::Retrieve,
    (int)cta::common::dataStructures::MountType::Label };
  for (auto d=dl.begin(); d!= dl.end(); d++) {
    if (activeDriveStatuses.count((int)d->driveStatus)) {
      tmdi.existingOrNextMounts.push_back(ExistingMount());
      tmdi.existingOrNextMounts.back().type = d->mountType;
      tmdi.existingOrNextMounts.back().tapePool = d->currentTapePool;
      tmdi.existingOrNextMounts.back().driveName = d->driveName;
      tmdi.existingOrNextMounts.back().vid = d->currentVid;
      tmdi.existingOrNextMounts.back().currentMount = true;
      tmdi.existingOrNextMounts.back().bytesTransferred = d->bytesTransferredInSession;
      tmdi.existingOrNextMounts.back().filesTransferred = d->filesTransferredInSession;
      tmdi.existingOrNextMounts.back().latestBandwidth = d->latestBandwidth;
    }
    if (activeMountTypes.count((int)d->nextMountType)) {
      tmdi.existingOrNextMounts.push_back(ExistingMount());
      tmdi.existingOrNextMounts.back().type = d->nextMountType;
      tmdi.existingOrNextMounts.back().tapePool = d->nextTapepool;
      tmdi.existingOrNextMounts.back().driveName = d->driveName;
      tmdi.existingOrNextMounts.back().vid = d->nextVid;
      tmdi.existingOrNextMounts.back().currentMount = false;
      tmdi.existingOrNextMounts.back().bytesTransferred = 0;
      tmdi.existingOrNextMounts.back().filesTransferred = 0;
      tmdi.existingOrNextMounts.back().latestBandwidth = 0;
    }
  }
}

//------------------------------------------------------------------------------
// OStoreDB::getMountInfo()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> 
  OStoreDB::getMountInfo() {
  //Allocate the getMountInfostructure to return.
  assertAgentAddressSet();
  std::unique_ptr<OStoreDB::TapeMountDecisionInfo> privateRet (new OStoreDB::TapeMountDecisionInfo(
    m_objectStore, *m_agentReference));
  TapeMountDecisionInfo & tmdi=*privateRet;
  // Get all the tape pools and tapes with queues (potential mounts)
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  // Take an exclusive lock on the scheduling and fetch it.
  tmdi.m_schedulerGlobalLock.reset(
    new SchedulerGlobalLock(re.getSchedulerGlobalLock(), m_objectStore));
  tmdi.m_lockOnSchedulerGlobalLock.lock(*tmdi.m_schedulerGlobalLock);
  tmdi.m_lockTaken = true;
  tmdi.m_schedulerGlobalLock->fetch();
  fetchMountInfo(tmdi, re);
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> ret(std::move(privateRet));
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getMountInfoNoLock()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> OStoreDB::getMountInfoNoLock() {
  //Allocate the getMountInfostructure to return.
  assertAgentAddressSet();
  std::unique_ptr<OStoreDB::TapeMountDecisionInfoNoLock> privateRet (new OStoreDB::TapeMountDecisionInfoNoLock);
  // Get all the tape pools and tapes with queues (potential mounts)
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  TapeMountDecisionInfoNoLock & tmdi=*privateRet;
  fetchMountInfo(tmdi, re);
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> ret(std::move(privateRet));
  return ret;
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


/* Old getMountInfo
//------------------------------------------------------------------------------
// OStoreDB::getMountInfo()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> 
  OStoreDB::getMountInfo() {
  //Allocate the getMountInfostructure to return.
  assertAgentSet();
  std::unique_ptr<TapeMountDecisionInfo> privateRet (new TapeMountDecisionInfo(
    m_objectStore, *m_agent));
  TapeMountDecisionInfo & tmdi=*privateRet;
  // Get all the tape pools and tapes with queues (potential mounts)
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  // Take an exclusive lock on the scheduling and fetch it.
  tmdi.m_schedulerGlobalLock.reset(
    new SchedulerGlobalLock(re.getSchedulerGlobalLock(), m_objectStore));
  tmdi.m_lockOnSchedulerGlobalLock.lock(*tmdi.m_schedulerGlobalLock);
  tmdi.m_lockTaken = true;
  tmdi.m_schedulerGlobalLock->fetch();
  auto tpl = re.dumpTapePools();
  for (auto tpp=tpl.begin(); tpp!=tpl.end(); tpp++) {
    // Get the tape pool object
    objectstore::TapePool tpool(tpp->address, m_objectStore);
    // debug utility variable
    std::string __attribute__((__unused__)) poolName = tpp->tapePool;
    objectstore::ScopedSharedLock tpl(tpool);
    tpool.fetch();
    // If there are files queued, we create an entry for this tape pool in the
    // mount candidates list.
    if (tpool.getJobsSummary().files) {
      tmdi.potentialMounts.push_back(SchedulerDatabase::PotentialMount());
      auto & m = tmdi.potentialMounts.back();
      m.tapePool = tpp->tapePool;
      m.type = cta::MountType::ARCHIVE;
      m.bytesQueued = tpool.getJobsSummary().bytes;
      m.filesQueued = tpool.getJobsSummary().files;      
      m.oldestJobStartTime = tpool.getJobsSummary().oldestJobStartTime;
      m.priority = tpool.getJobsSummary().priority;
      
      m.mountCriteria.maxFilesQueued = 
          tpool.getMountCriteriaByDirection().archive.maxFilesQueued;
      m.mountCriteria.maxBytesQueued = 
          tpool.getMountCriteriaByDirection().archive.maxBytesQueued;
      m.mountCriteria.maxAge = 
          tpool.getMountCriteriaByDirection().archive.maxAge;
      m.mountCriteria.quota = 
          tpool.getMountCriteriaByDirection().archive.quota;
      m.logicalLibrary = "";

    }
    // For each tape in the pool, list the tapes with work
    auto tl = tpool.dumpTapesAndFetchStatus();
    for (auto tp = tl.begin(); tp!= tl.end(); tp++) {
      objectstore::Tape t(tp->address, m_objectStore);
      objectstore::ScopedSharedLock tl(t);
      t.fetch();
      if (t.getJobsSummary().files) {
        tmdi.potentialMounts.push_back(PotentialMount());
        auto & m = tmdi.potentialMounts.back();
        m.type = cta::MountType::RETRIEVE;
        m.bytesQueued = t.getJobsSummary().bytes;
        m.filesQueued = t.getJobsSummary().files;
        m.oldestJobStartTime = t.getJobsSummary().oldestJobStartTime;
        m.priority = t.getJobsSummary().priority;
        m.vid = t.getVid();
        m.logicalLibrary = t.getLogicalLibrary();
        
        m.mountCriteria.maxFilesQueued = 
            tpool.getMountCriteriaByDirection().retrieve.maxFilesQueued;
        m.mountCriteria.maxBytesQueued = 
            tpool.getMountCriteriaByDirection().retrieve.maxBytesQueued;
        m.mountCriteria.maxAge = 
            tpool.getMountCriteriaByDirection().retrieve.maxAge;
        m.mountCriteria.quota = 
            tpool.getMountCriteriaByDirection().retrieve.quota;
        m.logicalLibrary = t.getLogicalLibrary();
      }
    }
  }
  // Dedication information comes here
  // TODO
  // 
  // Collect information about the existing mounts
  objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_objectStore);
  objectstore::ScopedSharedLock drl(dr);
  dr.fetch();
  auto dl = dr.dumpDrives();
  using common::DriveStatus;
  std::set<int> activeDriveStatuses = {
    (int)DriveStatus::Starting,
    (int)DriveStatus::Mounting,
    (int)DriveStatus::Transferring,
    (int)DriveStatus::Unloading,
    (int)DriveStatus::Unmounting,
    (int)DriveStatus::DrainingToDisk };
  for (auto d=dl.begin(); d!= dl.end(); d++) {
    if (activeDriveStatuses.count((int)d->status)) {
      tmdi.existingMounts.push_back(ExistingMount());
      tmdi.existingMounts.back().type = d->mountType;
      tmdi.existingMounts.back().tapePool = d->currentTapePool;
    }
  }
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> ret(std::move(privateRet));
  return ret;
}
*/

//------------------------------------------------------------------------------
// OStoreDB::queueArchive()
//------------------------------------------------------------------------------
void OStoreDB::queueArchive(const std::string &instanceName, const cta::common::dataStructures::ArchiveRequest &request, 
        const cta::common::dataStructures::ArchiveFileQueueCriteria &criteria, log::LogContext &logContext) {
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
      auto shareLock = ostoredb::MemArchiveQueue::sharedAddToArchiveQueue(j, aReq, *this, logContext);
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
  } catch (NoSuchArchiveQueue &) {
    // Unlink the request from already connected tape pools
    for (auto tpa=linkedTapePools.begin(); tpa!=linkedTapePools.end(); tpa++) {
      objectstore::ArchiveQueue aq(*tpa, m_objectStore);
      ScopedExclusiveLock aql(aq);
      aq.fetch();
      aq.removeJob(aReq.getAddressIfSet());
      aq.commit();
      aReq.remove();
    }
    log::ScopedParamContainer params(logContext);
    params.add("tapepool", currentTapepool)
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
// OStoreDB::deleteArchiveRequest()
//------------------------------------------------------------------------------
void OStoreDB::deleteArchiveRequest(const std::string &diskInstanceName, 
  uint64_t fileId) {
  // First of, find the archive request from all the tape pools.
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto aql = re.dumpArchiveQueues();
  rel.release();
  for (auto & aqp: aql) {
    objectstore::ArchiveQueue aq(aqp.address, m_objectStore);
    ScopedSharedLock aqlock(aq);
    aq.fetch();
    auto ajl=aq.dumpJobs();
    aqlock.release();
    for (auto & ajp: ajl) {
      objectstore::ArchiveRequest ar(ajp.address, m_objectStore);
      ScopedSharedLock arl(ar);
      ar.fetch();
      if (ar.getArchiveFile().archiveFileID == fileId) {
        // We found a job for the right file Id.
        // We now need to dequeue it from all it archive queues (one per job).
        // Upgrade the lock to an exclusive one.
        arl.release();
        ScopedExclusiveLock arxl(ar);
        m_agentReference->addToOwnership(ar.getAddressIfSet(), m_objectStore);
        ar.fetch();
        ar.setAllJobsFailed();
        for (auto j:ar.dumpJobs()) {
          // Dequeue the job from the queue.
          // The owner might not be a queue, in which case the fetch will fail (and it's fine)
          try {
            // The queue on which we found the job is not locked anymore, so we can re-lock it.
            ArchiveQueue aq2(j.owner, m_objectStore);
            ScopedExclusiveLock aq2xl(aq2);
            aq2.fetch();
            aq2.removeJob(ar.getAddressIfSet());
            aq2.commit();
          } catch (...) {}
          ar.setJobOwner(j.copyNb, m_agentReference->getAgentAddress());
        }
        ar.remove();
        m_agentReference->removeFromOwnership(ar.getAddressIfSet(), m_objectStore);
        // We found and deleted the job: return.
        return;
      }
    }
  }
  throw NoSuchArchiveRequest("In OStoreDB::deleteArchiveRequest: ArchiveToFileRequest not found");
}

//------------------------------------------------------------------------------
// OStoreDB::markArchiveRequestForDeletion()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::ArchiveToFileRequestCancelation>
  OStoreDB::markArchiveRequestForDeletion(const common::dataStructures::SecurityIdentity& requester,
  uint64_t fileId) {
  assertAgentAddressSet();
  // Construct the return value immediately
  std::unique_ptr<cta::OStoreDB::ArchiveToFileRequestCancelation>
    internalRet(new cta::OStoreDB::ArchiveToFileRequestCancelation(*m_agentReference, m_objectStore));
  cta::objectstore::ArchiveRequest & ar = internalRet->m_request;
  cta::objectstore::ScopedExclusiveLock & atfrl = internalRet->m_lock;
  // Attempt to find the request
  objectstore::RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto tpl=re.dumpArchiveQueues();
  rel.release();
  for (auto tpp=tpl.begin(); tpp!=tpl.end(); tpp++) {
    try {
      objectstore::ArchiveQueue aq(tpp->address, m_objectStore);
      ScopedSharedLock tpl(aq);
      aq.fetch();
      auto arl = aq.dumpJobs();
      tpl.release();
      for (auto arp=arl.begin(); arp!=arl.end(); arp++) {
        objectstore::ArchiveRequest tar(arp->address, m_objectStore);
        objectstore::ScopedSharedLock tatfrl(tar);
        tar.fetch();
        if (tar.getArchiveFile().archiveFileID == fileId) {
          // Point the agent to the request
          m_agentReference->addToOwnership(arp->address, m_objectStore);
          // Mark all jobs are being pending NS deletion (for being deleted them selves) 
          tatfrl.release();
          ar.setAddress(arp->address);
          atfrl.lock(ar);
          ar.fetch();
          ar.setAllJobsPendingNSdeletion();
          ar.commit();
          // Unlink the jobs from the tape pools (it is safely referenced in the agent)
          auto arJobs=ar.dumpJobs();
          for (auto atpp=arJobs.begin(); atpp!=arJobs.end(); atpp++) {
            objectstore::ArchiveQueue aqp(atpp->owner, m_objectStore);
            objectstore::ScopedExclusiveLock atpl(aqp);
            aqp.fetch();
            aqp.removeJob(arp->address);
            aqp.commit();
          }
          // Return the object to the caller, so complete() can be called later.
          std::unique_ptr<cta::SchedulerDatabase::ArchiveToFileRequestCancelation> ret;
          ret.reset(internalRet.release());
          return ret;
        }
      }
    } catch (...) {}
  }
  throw NoSuchArchiveRequest("In OStoreDB::markArchiveRequestForDeletion: ArchiveToFileRequest no found");
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveToFileRequestCancelation::complete()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveToFileRequestCancelation::complete() {
  if (m_closed)
    throw ArchiveRequestAlreadyDeleted("OStoreDB::ArchiveToFileRequestCancelation::complete(): called twice");
  // We just need to delete the object and forget it
  m_request.remove();
  m_agentReference.removeFromOwnership(m_request.getAddressIfSet(), m_objectStore);
  m_closed = true;
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveToFileRequestCancelation::~ArchiveToFileRequestCancelation()
//------------------------------------------------------------------------------
OStoreDB::ArchiveToFileRequestCancelation::~ArchiveToFileRequestCancelation() {
  if (!m_closed) {
    try {
      m_request.garbageCollect(m_agentReference.getAgentAddress(), m_agentReference);
      m_agentReference.removeFromOwnership(m_request.getAddressIfSet(), m_objectStore);
    } catch (...) {}
  }
}


//------------------------------------------------------------------------------
// OStoreDB::getArchiveRequests()
//------------------------------------------------------------------------------
//std::map<std::string, std::list<ArchiveToTapeCopyRequest> >
//  OStoreDB::getArchiveRequests() const {
//  objectstore::RootEntry re(m_objectStore);
//  objectstore::ScopedSharedLock rel(re);
//  re.fetch();
//  std::map<std::string, std::list<ArchiveToTapeCopyRequest> > ret;
//  auto aql = re.dumpArchiveQueues();
//  rel.release();
//  for (auto & aqp:aql) {
//    objectstore::ArchiveQueue osaq(aqp.address, m_objectStore);
//    ScopedSharedLock osaql(osaq);
//    osaq.fetch();  
//    auto arl = osaq.dumpJobs();
//    osaql.release();
//    for (auto & ar: arl) {
//      objectstore::ArchiveRequest osar(ar.address, m_objectStore);
//      ScopedSharedLock osarl(osar);
//      osar.fetch();
//      // Find which copy number is for this tape pool.
//      // skip the request if not found
//      auto jl = osar.dumpJobs();
//      uint16_t copynb;
//      bool copyndFound=false;
//      for (auto & j:jl) {
//        if (j.tapePool == aqp.tapePool) {
//          copynb = j.copyNb;
//          copyndFound = true;
//          break;
//        }
//      }
//      if (!copyndFound) continue;
//      ret[aqp.tapePool].push_back(cta::ArchiveToTapeCopyRequest(
//        osar.getDiskFileID(),
//        osar.getArchiveFileID(),
//        copynb,
//        aqp.tapePool,
//        osar.getMountPolicy().archivePriority,
//        osar.getCreationLog()));
//    }
//  }
//  return ret;
//}

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
  std::list<RetrieveQueueStatistics> ret;
  // Find the retrieve queues for each vid if they exist (absence is possible).
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  rel.release();
  for (auto &tf:criteria.archiveFile.tapeFiles) {
    if (!vidsToConsider.count(tf.second.vid))
      continue;
    std::string rqAddr;
    try {
      std::string rqAddr = re.getRetrieveQueue(tf.second.vid);
    } catch (cta::exception::Exception &) {
      ret.push_back(RetrieveQueueStatistics());
      ret.back().vid=tf.second.vid;
      ret.back().bytesQueued=0;
      ret.back().currentPriority=0;
      ret.back().filesQueued=0;
      continue;
    }
    RetrieveQueue rq(rqAddr, m_objectStore);
    ScopedSharedLock rql(rq);
    rq.fetch();
    rql.release();
    if (rq.getVid() != tf.second.vid)
      throw cta::exception::Exception("In OStoreDB::getRetrieveQueueStatistics(): unexpected vid for retrieve queue");
    ret.push_back(RetrieveQueueStatistics());
    ret.back().vid=rq.getVid();
    ret.back().currentPriority=rq.getJobsSummary().priority;
    ret.back().bytesQueued=rq.getJobsSummary().bytes;
    ret.back().filesQueued=rq.getJobsSummary().files;
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::queueRetrieve()
//------------------------------------------------------------------------------
void OStoreDB::queueRetrieve(const cta::common::dataStructures::RetrieveRequest& rqst,
  const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria,
  const std::string &vid) {
  assertAgentAddressSet();
  // Check that the requested retrieve job (for the provided vid) exists.
  if (!std::count_if(criteria.archiveFile.tapeFiles.cbegin(), 
                     criteria.archiveFile.tapeFiles.end(),
                     [vid](decltype(*criteria.archiveFile.tapeFiles.cbegin()) & tf){ return tf.second.vid == vid; }))
    throw RetrieveRequestHasNoCopies("In OStoreDB::queueRetrieve(): no tape file for requested vid.");
  // In order to post the job, construct it first in memory.
  objectstore::RetrieveRequest rReq(m_agentReference->nextId("RetrieveToFileRequest"), m_objectStore);
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
  // Find the retrieve queue (or create it if necessary)
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  auto rqAddr=re.addOrGetRetrieveQueueAndCommit(vid, *m_agentReference);
  // Create the request.
  rel.release();
  RetrieveQueue rq(rqAddr, m_objectStore);
  ScopedExclusiveLock rql(rq);
  rq.fetch();
  // We need to find the job corresponding to the vid
  for (auto & j: rReq.getArchiveFile().tapeFiles) {
    if (j.second.vid == vid) {
      rq.addJob(j.second.copyNb, j.second.fSeq, rReq.getAddressIfSet(), criteria.archiveFile.fileSize, 
          criteria.mountPolicy, rReq.getEntryLog().time);
      rReq.setActiveCopyNumber(j.second.copyNb);
      goto jobAdded;
    }
  }
  // Getting here means the expected job was not found. This is an internal error.
  throw cta::exception::Exception("In OStoreDB::queueRetrieve(): job not found for this vid");
  jobAdded:;
  // We can now commit the queue.
  rq.commit();
  rql.release();
  // Set the request ownership.
  rReq.setOwner(rqAddr);
  rReq.commit();
  rrl.release();
  // And relinquish ownership form agent
  m_agentReference->removeFromOwnership(rReq.getAddressIfSet(), m_objectStore);
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
std::list<cta::common::dataStructures::DriveState> OStoreDB::getDriveStates() const {
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto driveRegisterAddress = re.getDriveRegisterAddress();
  objectstore::DriveRegister dr(driveRegisterAddress, m_objectStore);
  objectstore::ScopedExclusiveLock drl(dr);
  dr.fetch();
  return dr.getAllDrivesState();
}

//------------------------------------------------------------------------------
// OStoreDB::setDesiredDriveState()
//------------------------------------------------------------------------------
void OStoreDB::setDesiredDriveState(const std::string& drive, const common::dataStructures::DesiredDriveState & desiredState) {
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto driveRegisterAddress = re.getDriveRegisterAddress();
  objectstore::DriveRegister dr(driveRegisterAddress, m_objectStore);
  objectstore::ScopedExclusiveLock drl(dr);
  dr.fetch();
  auto driveState = dr.getDriveState(drive);
  driveState.desiredDriveState = desiredState;
  dr.setDriveState(driveState);
  dr.commit();
}

//------------------------------------------------------------------------------
// OStoreDB::reportDriveStatus()
//------------------------------------------------------------------------------
void OStoreDB::reportDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
  cta::common::dataStructures::MountType mountType, common::dataStructures::DriveStatus status,
  time_t reportTime, uint64_t mountSessionId, uint64_t byteTransferred,
  uint64_t filesTransferred, double latestBandwidth, const std::string& vid, 
  const std::string& tapepool) {
  using common::dataStructures::DriveStatus;
  // Lock the drive register and try to find the drive entry
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto driveRegisterAddress = re.getDriveRegisterAddress();
  objectstore::DriveRegister dr(driveRegisterAddress, m_objectStore);
  objectstore::ScopedExclusiveLock drl(dr);
  dr.fetch();
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
  updateDriveStatusInRegitry(dr, driveInfo, inputs);
  dr.commit();
}

void OStoreDB::updateDriveStatusInRegitry(objectstore::DriveRegister& dr, 
  const common::dataStructures::DriveInfo& driveInfo, const ReportDriveStatusInputs& inputs) {
  using common::dataStructures::DriveStatus;
  // The drive state might not be present, in which case we have to fill it up with default values.
  cta::common::dataStructures::DriveState driveState;
  try { 
    driveState = dr.getDriveState(driveInfo.driveName);
  } catch (cta::exception::Exception & ex) {
    // The drive is missing in the registry. We have to fill the state with default
    // values, for what that will not be covered later.
    driveState.driveName = driveInfo.driveName;
    // host will be reset anyway.
    // logical library will be reset anyway.
    driveState.mountType = common::dataStructures::MountType::NoMount;
    driveState.driveStatus = common::dataStructures::DriveStatus::Unknown;
    driveState.sessionId = 0;
    driveState.bytesTransferredInSession = 0;
    driveState.filesTransferredInSession = 0;
    driveState.latestBandwidth = 0;
    driveState.sessionStartTime = 0;
    driveState.mountStartTime = 0;
    driveState.transferStartTime = 0;
    driveState.unloadStartTime = 0;
    driveState.unmountStartTime = 0;
    driveState.drainingStartTime = 0;
    driveState.downOrUpStartTime = 0;
    driveState.cleanupStartTime = 0;
    driveState.lastUpdateTime = 0;
    driveState.startStartTime = 0;
    driveState.desiredDriveState.up = (inputs.status==DriveStatus::Down?false:true);
    driveState.desiredDriveState.forceDown = false;
    driveState.currentTapePool = "";
    driveState.currentVid = "";
  }
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
    default:
      throw exception::Exception("Unexpected status in DriveRegister::reportDriveStatus");
  }
  dr.setDriveState(driveState);
}

void OStoreDB::updateDriveStatsInRegitry(objectstore::DriveRegister& dr, 
  const common::dataStructures::DriveInfo& driveInfo, const ReportDriveStatsInputs& inputs) {
  using common::dataStructures::DriveStatus;
  // The drive state might not be present, in which case we do nothing.
  cta::common::dataStructures::DriveState driveState;
  try { 
    driveState = dr.getDriveState(driveInfo.driveName);
  } catch (cta::exception::Exception & ex) {
    // The drive is missing in the registry. Nothing to update
      return;    
  }
  // Set the parameters that we always set
  driveState.host = driveInfo.host;
  driveState.logicalLibrary = driveInfo.logicalLibrary;

  switch (driveState.driveStatus) {    
    case DriveStatus::Transferring:
      driveState.lastUpdateTime=inputs.reportTime;
      driveState.bytesTransferredInSession=inputs.byteTransferred;
      driveState.filesTransferredInSession=inputs.filesTransferred;
      driveState.latestBandwidth=inputs.latestBandwidth;
      break;    
    default:
      return ;
  }
  dr.setDriveState(driveState);
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
  driveState.cleanupStartTime=0;
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
  driveState.cleanupStartTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=common::dataStructures::MountType::NoMount;
  driveState.driveStatus=targetStatus;
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
  driveState.cleanupStartTime=0;
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
  driveState.cleanupStartTime=0;
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
  driveState.cleanupStartTime=0;
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
  driveState.cleanupStartTime=0;
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
  driveState.cleanupStartTime=0;
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
  driveState.cleanupStartTime=0;
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
  driveState.cleanupStartTime=inputs.reportTime;
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
    new OStoreDB::ArchiveMount(m_objectStore, m_agentReference));
  auto &am = *privateRet;
  // Check we hold the scheduling lock
  if (!m_lockTaken)
    throw SchedulingLockNotHeld("In OStoreDB::TapeMountDecisionInfo::createArchiveMount: "
      "cannot create mount without holding scheduling lock");
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto driveRegisterAddress = re.getDriveRegisterAddress();
  rel.release();
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
    // Get hold of the drive registry
    objectstore::DriveRegister dr(driveRegisterAddress, m_objectStore);
    objectstore::ScopedExclusiveLock drl(dr);
    dr.fetch();
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
    OStoreDB::updateDriveStatusInRegitry(dr, driveInfo, inputs);
    dr.commit();
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
OStoreDB::TapeMountDecisionInfo::TapeMountDecisionInfo(
  objectstore::Backend& os, objectstore::AgentReference& a):
   m_lockTaken(false), m_objectStore(os), m_agentReference(a) {}

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
    new OStoreDB::RetrieveMount(m_objectStore, m_agentReference));
  auto &rm = *privateRet;
  // Check we hold the scheduling lock
  if (!m_lockTaken)
    throw SchedulingLockNotHeld("In OStoreDB::TapeMountDecisionInfo::createRetrieveMount: "
      "cannot create mount without holding scheduling lock");
  // Find the tape and update it
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto driveRegisterAddress = re.getDriveRegisterAddress();
  rel.release();
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
    objectstore::DriveRegister dr(driveRegisterAddress, m_objectStore);
    objectstore::ScopedExclusiveLock drl(dr);
    dr.fetch();
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
    OStoreDB::updateDriveStatusInRegitry(dr, driveInfo, inputs);
    dr.commit();
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
OStoreDB::ArchiveMount::ArchiveMount(objectstore::Backend& os, objectstore::AgentReference& a):
  m_objectStore(os), m_agentReference(a) {}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::getMountInfo()
//------------------------------------------------------------------------------
const SchedulerDatabase::ArchiveMount::MountInfo& OStoreDB::ArchiveMount::getMountInfo() {
  return mountInfo;
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::getNextJob()
//------------------------------------------------------------------------------
auto OStoreDB::ArchiveMount::getNextJob(log::LogContext &logContext) -> std::unique_ptr<SchedulerDatabase::ArchiveJob> {
  // Find the next file to archive
  // Get the archive queue
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  // First, check we should not forcibly go down. In such an occasion, we just find noting to do.
  // Get drive register
  {
    objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_objectStore);
    ScopedSharedLock drl(dr);
    dr.fetch();
    auto drs = dr.getDriveState(mountInfo.drive);
    if (!drs.desiredDriveState.up && drs.desiredDriveState.forceDown) {
      logContext.log(log::INFO, "In OStoreDB::ArchiveMount::getNextJob(): returning no job as we are forcibly going down.");
      return nullptr;
    }
  }
  auto aql = re.dumpArchiveQueues();
  rel.release();
  std::string aqAddress;
  for (auto & aqp : aql) {
    if (aqp.tapePool == mountInfo.tapePool)
      aqAddress = aqp.address;
  }
  // The archive queue is gone, there is no more job
  if (!aqAddress.size())
    return nullptr;
  // Try and open the archive queue. It could be gone by now.
  try {
    objectstore::ArchiveQueue aq(aqAddress, m_objectStore);
    objectstore::ScopedExclusiveLock aqlock;
    try {
      aqlock.lock(aq);
      aq.fetch();
    } catch (cta::exception::Exception & ex) {
      // The queue is now absent. We can remove its reference in the root entry.
      // A new queue could have been added in the mean time, and be non-empty.
      // We will then fail to remove from the RootEntry (non-fatal).
      // TODO: We still conclude that the queue is empty on this unlikely event.
      // (cont'd): A better approach would be to restart the process of this function
      // from scratch.
      rel.lock(re);
      re.fetch();
      try {
        re.removeArchiveQueueAndCommit(mountInfo.tapePool);
        log::ScopedParamContainer params(logContext);
        params.add("tapepool", mountInfo.tapePool)
              .add("queueObject", aq.getAddressIfSet());
        logContext.log(log::INFO, "In ArchiveMount::getNextJob(): de-referenced missing queue from root entry");
      } catch (RootEntry::ArchivelQueueNotEmpty & ex) {
        // TODO: improve: if we fail here we could retry to fetch a job.
        log::ScopedParamContainer params(logContext);
        params.add("tapepool", mountInfo.tapePool)
              .add("queueObject", aq.getAddressIfSet())
              .add("Message", ex.getMessageValue());
        logContext.log(log::INFO, "In ArchiveMount::getNextJob(): could not de-referenced missing queue from root entry");
      }
      return nullptr;
    }
    // Pop jobs until we find one actually belonging to the queue.
    // Any job not really belonging is an uncommitted pop, which we will
    // re-do here.
    bool aqUpdated = false;
    while (aq.dumpJobs().size()) {
      // First take a lock on and download the job
      // If the request is not around anymore, we will just move the the next
      // Prepare the return value
      auto job=aq.dumpJobs().front();
      std::unique_ptr<OStoreDB::ArchiveJob> privateRet(new OStoreDB::ArchiveJob(
        job.address, m_objectStore, m_agentReference, *this));
      privateRet->tapeFile.copyNb = job.copyNb;
      objectstore::ScopedExclusiveLock arl;
      try {
        arl.lock(privateRet->m_archiveRequest);
        privateRet->m_archiveRequest.fetch();
        // If the archive job does not belong to the queue, it's again a missed pop
        if (privateRet->m_archiveRequest.getJobOwner(job.copyNb) != aq.getAddressIfSet()) {
          aq.removeJob(privateRet->m_archiveRequest.getAddressIfSet());
          log::ScopedParamContainer params(logContext);
          params.add("tapepool", mountInfo.tapePool)
                .add("queueObject", aq.getAddressIfSet())
                .add("jobObject", privateRet->m_archiveRequest.getAddressIfSet());
          logContext.log(log::INFO, "In ArchiveMount::getNextJob(): skipped orphaned job from the queue.");
          aqUpdated=true;
          continue;
        }
      } catch (cta::exception::Exception &) {
        // we failed to access the object. It might be missing.
        // Just pop this job from the pool and move to the next.
        aq.removeJob(privateRet->m_archiveRequest.getAddressIfSet());
        // Commit in case we do not pass by again.
        aq.commit();
        continue;
      }
      // Take ownership of the job
      // Add to ownership
      m_agentReference.addToOwnership(privateRet->m_archiveRequest.getAddressIfSet(), m_objectStore);
      // Make the ownership official (for this job within the request)
      privateRet->m_archiveRequest.setJobOwner(job.copyNb, m_agentReference.getAgentAddress());
      privateRet->m_archiveRequest.commit();
      // Remove the job from the archive queue
      aq.removeJob(privateRet->m_archiveRequest.getAddressIfSet());
      // We can commit and release the archive queue lock, we will only fill up
      // memory structure from here on.
      aq.commit();
      aqlock.release();
      privateRet->archiveFile = privateRet->m_archiveRequest.getArchiveFile();
      privateRet->srcURL = privateRet->m_archiveRequest.getSrcURL();
      privateRet->archiveReportURL = privateRet->m_archiveRequest.getArchiveReportURL();
      privateRet->tapeFile.fSeq = ++nbFilesCurrentlyOnTape;
      privateRet->tapeFile.copyNb = job.copyNb;
      privateRet->tapeFile.vid = mountInfo.vid;
      privateRet->tapeFile.blockId =
        std::numeric_limits<decltype(privateRet->tapeFile.blockId)>::max();
      privateRet->m_jobOwned = true;
      privateRet->m_mountId = mountInfo.mountId;
      privateRet->m_tapePool = mountInfo.tapePool;
      log::ScopedParamContainer params(logContext);
      params.add("tapepool", mountInfo.tapePool)
            .add("queueObject", aq.getAddressIfSet())
            .add("jobObject", privateRet->m_archiveRequest.getAddressIfSet())
            .add("fileId", privateRet->m_archiveRequest.getArchiveFile().archiveFileID);
      logContext.log(log::INFO, "In ArchiveMount::getNextJob(): popped job from queue");
      return std::unique_ptr<SchedulerDatabase::ArchiveJob> (privateRet.release());
    }
    if (aqUpdated) aq.commit();
    // If we get here, we exhausted the queue. We can now remove it. 
    // removeArchiveQueueAndCommit is safe, as it checks whether the queue is empty 
    // before deleting it. It will throw an exception in such a case (allowing us to
    // log such instance.) We need to release the lock here
    aqlock.release();
    try {
      objectstore::RootEntry re(m_objectStore);
      objectstore::ScopedExclusiveLock rel (re);
      re.fetch();
      re.removeArchiveQueueAndCommit(mountInfo.tapePool);
      log::ScopedParamContainer params(logContext);
      params.add("tapepool", mountInfo.tapePool)
            .add("queueObject", aq.getAddressIfSet());
      logContext.log(log::INFO, "In ArchiveMount::getNextJob(): deleted empty queue");
    } catch (cta::exception::Exception &ex) {
      log::ScopedParamContainer params(logContext);
      params.add("tapepool", mountInfo.tapePool)
            .add("queueObject", aq.getAddressIfSet())
            .add("Message", ex.getMessageValue());
      logContext.log(log::INFO, "In ArchiveMount::getNextJob(): could not delete a presumably empty queue");
    }
    return nullptr;
  } catch (cta::exception::Exception & ex){
    return nullptr;
  }
}
  // Open the archive queue
//  objectstore::TapePool tp(tpAddress, m_objectStore);
//  objectstore::ScopedExclusiveLock tplock(tp);
//  tp.fetch();
//  // Loop until we find a job which is actually belonging to the tape pool
//  while (tp.dumpJobs().size()) {
//    // Get the tape pool's jobs list, and pop the first
//    auto jl = tp.dumpJobs();
//    // First take a lock on and download the job
//    // If the request is not around anymore, we will just move the the next
//    // Prepare the return value
//    std::unique_ptr<OStoreDB::ArchiveJob> privateRet(new OStoreDB::ArchiveJob(
//      jl.front().address, m_objectStore, m_agent, *this));
//    privateRet->m_copyNb = jl.front().copyNb;
//    objectstore::ScopedExclusiveLock atfrl;
//    try {
//      atfrl.lock(privateRet->m_atfr);
//      privateRet->m_atfr.fetch();
//    } catch (cta::exception::Exception &) {
//      // we failed to access the object. It might be missing.
//      // Just pop this job from the pool and move to the next.
//      tp.removeJob(privateRet->m_atfr.getAddressIfSet());
//      // Commit in case we do not pass by again.
//      tp.commit();
//      continue;
//    }
//    // Take ownership of the job
//    // Add to ownership
//    objectstore::ScopedExclusiveLock al(m_agent);
//    m_agent.fetch();
//    m_agent.addToOwnership(privateRet->m_atfr.getAddressIfSet());
//    m_agent.commit();
//    al.release();
//    // Make the ownership official (for this job within the request)
//    privateRet->m_atfr.setJobOwner(privateRet->m_copyNb, m_agent.getAddressIfSet());
//    privateRet->m_atfr.commit();
//    // Remove the job from the tape pool queue
//    tp.removeJob(privateRet->m_atfr.getAddressIfSet());
//    // We can commit and release the tape pool lock, we will only fill up
//    // memory structure from here on.
//    tp.commit();
//    privateRet->archiveFile = privateRet->m_atfr.getArchiveFile();
//    privateRet->remoteFile = privateRet->m_atfr.getRemoteFile();
//    privateRet->nameServerTapeFile.tapeFileLocation.fSeq = ++nbFilesCurrentlyOnTape;
//    privateRet->nameServerTapeFile.tapeFileLocation.copyNb = privateRet->m_copyNb;
//    privateRet->nameServerTapeFile.tapeFileLocation.vid = mountInfo.vid;
//    privateRet->nameServerTapeFile.tapeFileLocation.blockId =
//      std::numeric_limits<decltype(privateRet->nameServerTapeFile.tapeFileLocation.blockId)>::max();
//    privateRet->m_jobOwned = true;
//    privateRet->m_mountId = mountInfo.mountId;
//    privateRet->m_tapePool = mountInfo.tapePool;
//    return std::unique_ptr<SchedulerDatabase::ArchiveJob> (privateRet.release());
//  }
//  return std::unique_ptr<SchedulerDatabase::ArchiveJob>();

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::getNextJobBatch()
//------------------------------------------------------------------------------
std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > OStoreDB::ArchiveMount::getNextJobBatch(uint64_t filesRequested,
    uint64_t bytesRequested, log::LogContext& logContext) {
  // Find the next files to archive
  // Get the archive queue
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto aql = re.dumpArchiveQueues();
  rel.release();
  std::string aqAddress;
  for (auto & aqp : aql) {
    if (aqp.tapePool == mountInfo.tapePool)
      aqAddress = aqp.address;
  }
  // The archive queue is gone, there is no more job
  if (!aqAddress.size())
    return std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> >();
  // Try and open the archive queue. It could be gone by now.
  try {
    objectstore::ArchiveQueue aq(aqAddress, m_objectStore);
    objectstore::ScopedExclusiveLock aqlock;
    try {
      aqlock.lock(aq);
      aq.fetch();
    } catch (cta::exception::Exception & ex) {
      // The queue is now absent. We can remove its reference in the root entry.
      // A new queue could have been added in the mean time, and be non-empty.
      // We will then fail to remove from the RootEntry (non-fatal).
      // TODO: We still conclude that the queue is empty on this unlikely event.
      // (cont'd): A better approach would be to restart the process of this function
      // from scratch.
      rel.lock(re);
      re.fetch();
      try {
        re.removeArchiveQueueAndCommit(mountInfo.tapePool);
        log::ScopedParamContainer params(logContext);
        params.add("tapepool", mountInfo.tapePool)
              .add("queueObject", aq.getAddressIfSet());
        logContext.log(log::INFO, "In ArchiveMount::getNextJobBatch(): de-referenced missing queue from root entry");
      } catch (RootEntry::ArchivelQueueNotEmpty & ex) {
        // TODO: improve: if we fail here we could retry to fetch a job.
        log::ScopedParamContainer params(logContext);
        params.add("tapepool", mountInfo.tapePool)
              .add("queueObject", aq.getAddressIfSet())
              .add("Message", ex.getMessageValue());
        logContext.log(log::INFO, "In ArchiveMount::getNextJobBatch(): could not de-referenced missing queue from root entry");
      }
      return std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> >();
    }
    // Pop jobs by batch until we find a sufficient number actually belonging to the queue.
    // Any job not really belonging is an uncommitted pop, which we will
    // re-do here.
    std::list<std::unique_ptr<OStoreDB::ArchiveJob>> privateRet;
    uint64_t currentBytes=0;
    uint64_t currentFiles=0;
    {
      log::ScopedParamContainer params(logContext);
      params.add("tapepool", mountInfo.tapePool)
            .add("queueObject", aq.getAddressIfSet())
            .add("queueSize", aq.dumpJobs().size());
      logContext.log(log::INFO, "In ArchiveMount::getNextJobBatch(): archive queue found.");
    }
    while (aq.dumpJobs().size()) {
      // We should build the list of jobs we intend to grab. We will attempt to 
      // dequeue them in one go, updating jobs in parallel. If some jobs turn out
      // to not be there really, we will have to do several passes.
      // We build directly the return value in the process.
      auto candidateDumps=aq.dumpJobs();
      std::list<std::unique_ptr<OStoreDB::ArchiveJob>> candidateJobs;
      // If we fail to find jobs in one round, we will exit.
      size_t jobsInThisRound=0;
      while (candidateDumps.size() && currentBytes < bytesRequested && currentFiles < filesRequested) {
        auto job=candidateDumps.front();
        candidateDumps.pop_front();
        currentFiles++;
        currentBytes+=job.size;
        candidateJobs.emplace_back(new OStoreDB::ArchiveJob(job.address, m_objectStore, m_agentReference, *this));
        candidateJobs.back()->tapeFile.copyNb = job.copyNb;
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
      // We now have a batch of jobs to try and dequeue. Should not be empty.
      // First add the jobs to the owned list of the agent.
      std::list<std::string> addedJobs;
      for (const auto &j: candidateJobs) addedJobs.emplace_back(j->m_archiveRequest.getAddressIfSet());
      m_agentReference.addBatchToOwnership(addedJobs, m_objectStore);
      // We can now attempt to switch the ownership of the jobs. Depending on the type of failure (if any) we
      // will adapt the rest.
      // First, start the parallel updates of jobs
      std::list<std::unique_ptr<objectstore::ArchiveRequest::AsyncJobOwnerUpdater>> jobUpdates;
      for (const auto &j: candidateJobs) jobUpdates.emplace_back(
        j->m_archiveRequest.asyncUpdateJobOwner(j->tapeFile.copyNb, m_agentReference.getAgentAddress(), aqAddress));
      // Now run through the results of the asynchronous updates. Non-sucess results come in the form of exceptions.
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
          params.add("tapepool", mountInfo.tapePool)
                .add("queueObject", aq.getAddressIfSet())
                .add("archiveRequest", (*j)->m_archiveRequest.getAddressIfSet())
                .add("fileId", (*j)->archiveFile.archiveFileID);
          logContext.log(log::INFO, "In ArchiveMount::getNextJobBatch(): popped one job");
          validatedJobs.emplace_back(std::move(*j));
          jobsInThisRound++;
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
                  .add("archiveRequest", (*j)->m_archiveRequest.getAddressIfSet());
            logContext.log(log::WARNING, "In ArchiveMount::getNextJobBatch(): skipped job not owned or not present.");
          } else {
            // This is not a success, yet we could not confirm the job status due to an unexpected error.
            // We leave the queue as is. We forget about owning this job. This is an error.
            log::ScopedParamContainer params(logContext);
            int demangleStatus;
            char * exceptionTypeStr = abi::__cxa_demangle(typeid(e).name(), nullptr, nullptr, &demangleStatus);
            params.add("tapepool", mountInfo.tapePool)
                  .add("queueObject", aq.getAddressIfSet())
                  .add("archiveRequest", (*j)->m_archiveRequest.getAddressIfSet());
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
          }
          // This job is not for us.
          jobsToForget.emplace_back((*j)->m_archiveRequest.getAddressIfSet());
          // We also need to update the counts.
          currentFiles--;
          currentBytes-=(*j)->archiveFile.fileSize;
        }
        // In all cases: move to the nexts.
        ju=jobUpdates.erase(ju);
        j=candidateJobs.erase(j);
      }
      // All (most) jobs are now officially owned by our agent. We can hence remove them from the queue.
      for (const auto &j: jobsToDequeue) aq.removeJob(j);
      if (jobsToForget.size()) m_agentReference.removeBatchFromOwnership(addedJobs, m_objectStore);
      // (Possibly intermediate) commit of the queue. We keep the lock for the moment.
      aq.commit();
      // We can now add the validated jobs to the return value.
      auto vj = validatedJobs.begin();
      while (vj != validatedJobs.end()) {
        privateRet.emplace_back(std::move(*vj));
        vj=validatedJobs.erase(vj);
      }
      // We could be done now.
      if (currentBytes >= bytesRequested || currentFiles >= filesRequested)
        break;
      // If this round was not fruitful, just break.
      if (!jobsInThisRound)
        break;
    }
    // We either ran out of jobs or fulfilled the requirements. Time to build up the reply.
    // Before this, we can release the queue and delete it if we emptied it.
    auto remainingJobs=aq.dumpJobs().size();
    aqlock.release();
    // If the queue is empty, we can get rid of it.
    if (!remainingJobs) {
      try {
        // The queue should be removed as it is empty.
        ScopedExclusiveLock reel(re);
        re.fetch();
        re.removeArchiveQueueAndCommit(mountInfo.tapePool);
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
            .add("bytes", nBytes);
      logContext.log(log::INFO, "In ArchiveMount::getNextJobBatch(): jobs retrieval complete.");
    }
    // We can construct the return value.
    std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > ret;
    for (auto & j: privateRet) ret.emplace_back(std::move(j));
    return ret;
  } catch (cta::exception::Exception & ex) {
    log::ScopedParamContainer params(logContext);
    params.add("tapepool", mountInfo.tapePool)
          .add("message", ex.getMessageValue());
    logContext.log(log::ERR, "In ArchiveMount::getNextJobBatch(): got exception. Returning empty");
    return std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> >();
  }
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::complete()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveMount::complete(time_t completionTime) {
  // When the session is complete, we can reset the status of the drive.
  // Tape will be implicitly released
  // Reset the drive
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_objectStore);
  objectstore::ScopedExclusiveLock drl(dr);
  dr.fetch();
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
  OStoreDB::updateDriveStatusInRegitry(dr, driveInfo, inputs);
  dr.commit();
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::ArchiveJob()
//------------------------------------------------------------------------------
OStoreDB::ArchiveJob::ArchiveJob(const std::string& jobAddress, 
  objectstore::Backend& os, objectstore::AgentReference& ar, ArchiveMount & am): m_jobOwned(false),
  m_objectStore(os), m_agentReference(ar), m_archiveRequest(jobAddress, os), m_archiveMount(am) {}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::RetrieveMount()
//------------------------------------------------------------------------------
OStoreDB::RetrieveMount::RetrieveMount(objectstore::Backend& os, objectstore::AgentReference& ar):
  m_objectStore(os), m_agentReference(ar) { }

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::getMountInfo()
//------------------------------------------------------------------------------
const OStoreDB::RetrieveMount::MountInfo& OStoreDB::RetrieveMount::getMountInfo() {
  return mountInfo;
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::getNextJob()
//------------------------------------------------------------------------------
auto OStoreDB::RetrieveMount::getNextJob(log::LogContext & logContext) -> std::unique_ptr<SchedulerDatabase::RetrieveJob> {
  // Find the next file to retrieve
  // Get the tape pool and then tape
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  // First, check we should not forcibly go down. In such an occasion, we just find noting to do.
  // Get drive register
  {
    objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_objectStore);
    ScopedSharedLock drl(dr);
    dr.fetch();
    auto drs = dr.getDriveState(mountInfo.drive);
    if (!drs.desiredDriveState.up && drs.desiredDriveState.forceDown) {
      logContext.log(log::INFO, "In OStoreDB::RetrieveMount::getNextJob(): returning no job as we are forcibly going down.");
      return nullptr;
    }
  }
  auto rql = re.dumpRetrieveQueues();
  rel.release();
  std::string rqAddress;
  for (auto & rqp: rql) {
    if (rqp.vid == mountInfo.vid)
      rqAddress = rqp.address;
  }
  // The retrieve queue is gone. There is no more job.
  if (!rqAddress.size())
    return nullptr;
  // Try and open the retrieve queue. It could be gone by now.
  try {
    objectstore::RetrieveQueue rq(rqAddress, m_objectStore);
    objectstore::ScopedExclusiveLock rqlock;
    try {
      rqlock.lock(rq);
      rq.fetch();
    } catch (cta::exception::Exception & ex) {
      // The queue is now absent. We can remove its reference in the root entry.
      // A new queue could have been added in the mean time, and be non-empty.
      // We will then fail to remove from the RootEntry (non-fatal).
      // TODO: We still conclude that the queue is empty on this unlikely event.
      // (cont'd): A better approach would be to retart the process of this function
      // from scratch.
      rel.lock(re);
      re.fetch();
      try {
        re.removeRetrieveQueueAndCommit(mountInfo.vid);
      } catch (RootEntry::RetrieveQueueNotEmpty & ex) {
        // TODO: improve: if we fail here we could retry to fetch a job.
        return nullptr;
      }
    }
    // Pop jobs until we find one actually belonging to the queue.
    // Any job not really belonging is an uncommitted pop, which we will
    // re-do here.
    while (rq.dumpJobs().size()) {
      // First take a lock on and download the job
      // If the request is not around anymore, we will just move the the next
      // Prepare the return value
      auto job=rq.dumpJobs().front();
      std::unique_ptr<OStoreDB::RetrieveJob> privateRet(new OStoreDB::RetrieveJob(
        job.address, m_objectStore, m_agentReference, *this));
      privateRet->selectedCopyNb = job.copyNb;
      objectstore::ScopedExclusiveLock rrl;
      try {
        rrl.lock(privateRet->m_retrieveRequest);
        privateRet->m_retrieveRequest.fetch();
        if(privateRet->m_retrieveRequest.getOwner() != rq.getAddressIfSet()) {
          rq.removeJob(privateRet->m_retrieveRequest.getAddressIfSet());
          continue;
        }
      } catch (cta::exception::Exception &) {
        // we failed to access the object. It might be missing.
        // Just pop this job from the queue and move to the next.
        rq.removeJob(privateRet->m_retrieveRequest.getAddressIfSet());
        // Commit in case we do not pass by again.
        rq.commit();
        continue;
      }
      // Take ownership of the job
      // Add to ownership
      m_agentReference.addToOwnership(privateRet->m_retrieveRequest.getAddressIfSet(), m_objectStore);
      // Make the ownership official
      privateRet->m_retrieveRequest.setOwner(m_agentReference.getAgentAddress());
      privateRet->m_retrieveRequest.commit();
      // Remove the job from the archive queue
      rq.removeJob(privateRet->m_retrieveRequest.getAddressIfSet());
      // We can commit and release the retrieve queue lock, we will only fill up
      // memory structure from here on.
      rq.commit();
      rqlock.release();
      privateRet->retrieveRequest = privateRet->m_retrieveRequest.getSchedulerRequest();
      privateRet->archiveFile = privateRet->m_retrieveRequest.getArchiveFile();
      privateRet->m_jobOwned = true;
      privateRet->m_mountId = mountInfo.mountId;
      return std::unique_ptr<SchedulerDatabase::RetrieveJob> (std::move(privateRet));
    }
    return std::unique_ptr<SchedulerDatabase::RetrieveJob>();  
  } catch (cta::exception::Exception & ex) {
    return nullptr;
  }
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::complete()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveMount::complete(time_t completionTime) {
  // When the session is complete, we can reset the status of the tape and the
  // drive
  // Reset the drive
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_objectStore);
  objectstore::ScopedExclusiveLock drl(dr);
  dr.fetch();
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
  OStoreDB::updateDriveStatusInRegitry(dr, driveInfo, inputs);
  dr.commit();
  rel.release();
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::setDriveStatus()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveMount::setDriveStatus(cta::common::dataStructures::DriveStatus status, time_t completionTime) {
  // We just report the drive status as instructed by the tape thread.
  // Get the drive register
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_objectStore);
  objectstore::ScopedExclusiveLock drl(dr);
  dr.fetch();
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
  OStoreDB::updateDriveStatusInRegitry(dr, driveInfo, inputs);
  dr.commit();
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::setTapeSessionStats()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveMount::setTapeSessionStats(castor::tape::tapeserver::daemon::TapeSessionStats stats) {
  // We just report tthe tape session statistics as instructed by the tape thread.
  // Get the drive register
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_objectStore);
  objectstore::ScopedExclusiveLock drl(dr);
  dr.fetch();
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=mountInfo.drive;
  driveInfo.logicalLibrary=mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;
  
  ReportDriveStatsInputs inputs;
  inputs.reportTime = time(nullptr); 
  inputs.byteTransferred = stats.dataVolume;
  inputs.filesTransferred = stats.filesCount;
  inputs.latestBandwidth = stats.totalTime?1.0*(stats.dataVolume+stats.headerVolume)/1000/1000/stats.totalTime:0.0;
  
  OStoreDB::updateDriveStatsInRegitry(dr, driveInfo, inputs);
  
  dr.commit();
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::setDriveStatus()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveMount::setDriveStatus(cta::common::dataStructures::DriveStatus status, time_t completionTime) {
  // We just report the drive status as instructed by the tape thread.
  // Get the drive register
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_objectStore);
  objectstore::ScopedExclusiveLock drl(dr);
  dr.fetch();
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
  OStoreDB::updateDriveStatusInRegitry(dr, driveInfo, inputs);
  dr.commit();
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::setTapeSessionStats()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) {
  // We just report the tape session statistics as instructed by the tape thread.
  // Get the drive register
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_objectStore);
  objectstore::ScopedExclusiveLock drl(dr);
  dr.fetch();
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=mountInfo.drive;
  driveInfo.logicalLibrary=mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;
  
  ReportDriveStatsInputs inputs;
  inputs.reportTime = time(nullptr); 
  inputs.byteTransferred = stats.dataVolume;
  inputs.filesTransferred = stats.filesCount;
  inputs.latestBandwidth = stats.totalTime?1.0*(stats.dataVolume+stats.headerVolume)/1000/1000/stats.totalTime:0.0;
  
  OStoreDB::updateDriveStatsInRegitry(dr, driveInfo, inputs);
  
  dr.commit();
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::fail()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveJob::fail() {
  if (!m_jobOwned)
    throw JobNowOwned("In OStoreDB::ArchiveJob::fail: cannot fail a job not owned");
  // Lock the archive request. Fail the job.
  objectstore::ScopedExclusiveLock arl(m_archiveRequest);
  m_archiveRequest.fetch();
  // Add a job failure. If the job is failed, we will delete it.
  if (m_archiveRequest.addJobFailure(tapeFile.copyNb, m_mountId)) {
    // The job will not be retried. Either another jobs for the same request is 
    // queued and keeps the request referenced or the request has been deleted.
    // In any case, we can forget it.
    m_agentReference.removeFromOwnership(m_archiveRequest.getAddressIfSet(), m_objectStore);
    m_jobOwned = false;
    return;
  }
  // The job still has a chance, return it to its original tape pool's queue
  objectstore::ArchiveQueue aq(m_objectStore);
  objectstore::ScopedExclusiveLock aqlock;
  objectstore::Helpers::getLockedAndFetchedArchiveQueue(aq, aqlock, m_agentReference, m_tapePool);
  // Find the right job
  auto jl = m_archiveRequest.dumpJobs();
  for (auto & j:jl) {
    if (j.copyNb == tapeFile.copyNb) {
      aq.addJobIfNecessary(j, m_archiveRequest.getAddressIfSet(), m_archiveRequest.getArchiveFile().archiveFileID,
          m_archiveRequest.getArchiveFile().fileSize, m_archiveRequest.getMountPolicy(), m_archiveRequest.getEntryLog().time);
      aq.commit();
      aqlock.release();
      // We have a pointer to the job, we can change the job ownership
      m_archiveRequest.setJobOwner(tapeFile.copyNb, aq.getAddressIfSet());
      m_archiveRequest.commit();
      arl.release();
      // We just have to remove the ownership from the agent and we're done.
      m_agentReference.removeFromOwnership(m_archiveRequest.getAddressIfSet(), m_objectStore);
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
// OStoreDB::ArchiveJob::succeed()
//------------------------------------------------------------------------------
bool OStoreDB::ArchiveJob::succeed() {
  // Lock the request and set the job as successful.
  objectstore::ScopedExclusiveLock atfrl(m_archiveRequest);
  m_archiveRequest.fetch();
  std::string atfrAddress = m_archiveRequest.getAddressIfSet();
  bool lastJob=m_archiveRequest.setJobSuccessful(tapeFile.copyNb);
  if (lastJob) {
    m_archiveRequest.remove();
  } else {
    m_archiveRequest.commit();
  }
  // We no more own the job (which could be gone)
  m_jobOwned = false;
  // Remove ownership from agent
  m_agentReference.removeFromOwnership(atfrAddress, m_objectStore);
  return lastJob;
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::~ArchiveJob()
//------------------------------------------------------------------------------
OStoreDB::ArchiveJob::~ArchiveJob() {
  if (m_jobOwned) {
    // Return the job to the pot if we failed to handle it.
    objectstore::ScopedExclusiveLock atfrl(m_archiveRequest);
    m_archiveRequest.fetch();
    m_archiveRequest.garbageCollect(m_agentReference.getAgentAddress(), m_agentReference);
    atfrl.release();
    // Remove ownership from agent
    m_agentReference.removeFromOwnership(m_archiveRequest.getAddressIfSet(), m_objectStore);
  }
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::RetrieveJob()
//------------------------------------------------------------------------------
OStoreDB::RetrieveJob::RetrieveJob(const std::string& jobAddress, 
    objectstore::Backend& os, objectstore::AgentReference& ar, 
    OStoreDB::RetrieveMount& rm): m_jobOwned(false),
  m_objectStore(os), m_agentReference(ar), m_retrieveRequest(jobAddress, os), 
  m_retrieveMount(rm) { }

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::fail()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveJob::fail() {
  throw NotImplemented("");
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::~RetrieveJob()
//------------------------------------------------------------------------------
OStoreDB::RetrieveJob::~RetrieveJob() {
//  if (m_jobOwned) {
//    // Re-queue the job entirely if we failed to handle it.
//    try {
//      // We now need to select the tape from which we will migrate next. This should
//      // be the tape with the most jobs already queued.
//      // TODO: this will have to look at tape statuses on the long run as well
//      uint16_t selectedCopyNumber;
//      uint64_t bestTapeQueuedBytes;
//      std::string selectedVid;
//      std::string selectedTapeAddress;
//      objectstore::ScopedExclusiveLock rtfrl(m_rtfr);
//      m_rtfr.fetch();
//      auto jl=m_rtfr.dumpJobs();
//      {
//        // First tape copy is always better than nothing. 
//        auto tc=jl.begin();
//        selectedCopyNumber = tc->copyNb;
//        selectedVid = tc->tape;
//        selectedTapeAddress = tc->tapeAddress;
//        // Get info for the tape.
//        {
//          objectstore::Tape t(tc->tapeAddress, m_objectStore);
//          objectstore::ScopedSharedLock tl(t);
//          t.fetch();
//          bestTapeQueuedBytes = t.getJobsSummary().bytes;
//        }
//        tc++;
//        // Compare with the next ones
//        for (;tc!=jl.end(); tc++) {
//          objectstore::Tape t(tc->tapeAddress, m_objectStore);
//          objectstore::ScopedSharedLock tl(t);
//          t.fetch();
//          if (t.getJobsSummary().bytes > bestTapeQueuedBytes) {
//            bestTapeQueuedBytes = t.getJobsSummary().bytes;
//            selectedCopyNumber = tc->copyNb;
//            selectedVid = tc->tape;
//            selectedTapeAddress = tc->tapeAddress;
//          }
//        }
//      }
//      // We now can enqueue the request on this most promising tape.
//      {
//        objectstore::Tape tp(selectedTapeAddress, m_objectStore);
//        ScopedExclusiveLock tpl(tp);
//        tp.fetch();
//        objectstore::RetrieveToFileRequest::JobDump jd;
//        jd.copyNb = selectedCopyNumber;
//        jd.tape = selectedVid;
//        jd.tapeAddress = selectedTapeAddress;
//        tp.addJob(jd, m_rtfr.getAddressIfSet(), m_rtfr.getSize(), m_rtfr.getPriority(), m_rtfr.getEntryLog().time);
//        tp.commit();
//      }
//      // The request is now fully set. It belongs to the tape.
//      std::string previousOwner = m_rtfr.getOwner();
//      m_rtfr.setOwner(selectedTapeAddress);
//      m_rtfr.commit();
//      // And remove reference from the agent (if it was owned by an agent)
//      try {
//        if (!previousOwner.size())
//          return;
//        objectstore::Agent agent(previousOwner, m_objectStore);
//        objectstore::ScopedExclusiveLock al(agent);
//        agent.fetch();
//        agent.removeFromOwnership(m_rtfr.getAddressIfSet());
//        agent.commit();
//      } catch (...) {}
//    } catch (...) {}
//  }
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::succeed()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveJob::succeed() {
  // Lock the request and set the job as successful.
  objectstore::ScopedExclusiveLock rtfrl(m_retrieveRequest);
  m_retrieveRequest.fetch();
  std::string rtfrAddress = m_retrieveRequest.getAddressIfSet();
  if (m_retrieveRequest.setJobSuccessful(selectedCopyNb)) {
    m_retrieveRequest.remove();
  } else {
    m_retrieveRequest.commit();
  }
  // We no more own the job (which could be gone)
  m_jobOwned = false;
  // Remove ownership form the agent
  m_agentReference.removeFromOwnership(rtfrAddress, m_objectStore);
}


} // namespace cta


