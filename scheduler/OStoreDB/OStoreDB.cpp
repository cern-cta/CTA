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
#include "common/SecurityIdentity.hpp"
#include "objectstore/RootEntry.hpp"
#include "objectstore/ArchiveQueue.hpp"
#include "objectstore/RetrieveQueue.hpp"
#include "objectstore/DriveRegister.hpp"
#include "objectstore/ArchiveRequest.hpp"
#include "objectstore/RetrieveToFileRequest.hpp"
#include "common/exception/Exception.hpp"
#include "common/admin/AdminHost.hpp"
#include "common/admin/AdminUser.hpp"
#include "common/archiveRoutes/ArchiveRoute.hpp"
#include "scheduler/UserArchiveRequest.hpp"
#include "scheduler/ArchiveRequest.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/RetrieveToFileRequest.hpp"
#include "common/archiveNS/StorageClass.hpp"
#include "common/TapePool.hpp"
#include "common/archiveNS/Tape.hpp"
#include "RetrieveToFileRequest.hpp"
#include "common/archiveNS/TapeFileLocation.hpp"
#include "common/archiveNS/ArchiveFile.hpp"
#include "objectstore/ArchiveRequest.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include <algorithm>
#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */
#include <stdexcept>
#include <set>
#include <iostream>

namespace cta {
  
using namespace objectstore;

OStoreDB::OStoreDB(objectstore::Backend& be):
  m_objectStore(be), m_agent(NULL) {}


OStoreDB::~OStoreDB() throw() {}

void OStoreDB::setAgent(objectstore::Agent& agent) {
  m_agent = & agent;
  }

void OStoreDB::assertAgentSet() {
  if (!m_agent)
    throw AgentNotSet("In OStoreDB::assertAgentSet: Agent pointer not set");
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> 
  OStoreDB::getMountInfo() {
  //Allocate the getMountInfostructure to return.
  assertAgentSet();
  std::unique_ptr<OStoreDB::TapeMountDecisionInfo> privateRet (new OStoreDB::TapeMountDecisionInfo(
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
  auto tpl = re.dumpArchiveQueues();
  for (auto tpp=tpl.begin(); tpp!=tpl.end(); tpp++) {
    // Get the tape pool object
    objectstore::ArchiveQueue aqueue(tpp->address, m_objectStore);
    // debug utility variable
    std::string __attribute__((__unused__)) poolName = tpp->tapePool;
    objectstore::ScopedSharedLock tpl(aqueue);
    aqueue.fetch();
    // If there are files queued, we create an entry for this tape pool in the
    // mount candidates list.
    if (aqueue.getJobsSummary().files) {
      tmdi.potentialMounts.push_back(SchedulerDatabase::PotentialMount());
      auto & m = tmdi.potentialMounts.back();
      m.tapePool = tpp->tapePool;
      m.type = cta::MountType::ARCHIVE;
      m.bytesQueued = aqueue.getJobsSummary().bytes;
      m.filesQueued = aqueue.getJobsSummary().files;      
      m.oldestJobStartTime = aqueue.getJobsSummary().oldestJobStartTime;
      m.priority = aqueue.getJobsSummary().priority;
      m.maxDrivesAllowed = aqueue.getJobsSummary().maxDrivesAllowed;
      m.minArchiveRequestAge = aqueue.getJobsSummary().minArchiveRequestAge;
      m.logicalLibrary = "";

    }
    // TODO TODO: cover the retrieve mounts as well
    // For each tape in the pool, list the tapes with work
//    auto tl = aqueue.dumpTapesAndFetchStatus();
//    for (auto tp = tl.begin(); tp!= tl.end(); tp++) {
//      objectstore::Tape t(tp->address, m_objectStore);
//      objectstore::ScopedSharedLock tl(t);
//      t.fetch();
//      if (t.getJobsSummary().files) {
//        tmdi.potentialMounts.push_back(PotentialMount());
//        auto & m = tmdi.potentialMounts.back();
//        m.type = cta::MountType::RETRIEVE;
//        m.bytesQueued = t.getJobsSummary().bytes;
//        m.filesQueued = t.getJobsSummary().files;
//        m.oldestJobStartTime = t.getJobsSummary().oldestJobStartTime;
//        m.priority = t.getJobsSummary().priority;
//        m.vid = t.getVid();
//        m.logicalLibrary = t.getLogicalLibrary();
//        
//        m.mountPolicy.maxFilesQueued = 
//            aqueue.getMountCriteriaByDirection().retrieve.maxFilesQueued;
//        m.mountPolicy.maxBytesQueued = 
//            aqueue.getMountCriteriaByDirection().retrieve.maxBytesQueued;
//        m.mountPolicy.maxAge = 
//            aqueue.getMountCriteriaByDirection().retrieve.maxAge;
//        m.mountPolicy.quota = 
//            aqueue.getMountCriteriaByDirection().retrieve.quota;
//        m.logicalLibrary = t.getLogicalLibrary();
//      }
//    }
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
    (int)DriveStatus::Transfering,
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

/* Old getMountInfo
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
    (int)DriveStatus::Transfering,
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

void OStoreDB::queue(const cta::common::dataStructures::ArchiveRequest &request, 
        const cta::common::dataStructures::ArchiveFileQueueCriteria &criteria) {
  assertAgentSet();
  // Construct the return value immediately
  cta::objectstore::ArchiveRequest ar(m_agent->nextId("ArchiveRequest"), m_objectStore);
  ar.initialize();
  ar.setArchiveFileID(criteria.fileId);
  ar.setChecksumType(request.checksumType);
  ar.setChecksumValue(request.checksumValue);
  ar.setCreationLog(request.creationLog);
  ar.setDiskpoolName(request.diskpoolName);
  ar.setDiskpoolThroughput(request.diskpoolThroughput);
  ar.setDrData(request.drData);
  ar.setDiskFileID(request.diskFileID);
  ar.setFileSize(request.fileSize);
  ar.setInstance(request.instance);
  ar.setMountPolicy(criteria.mountPolicy);
  ar.setRequester(request.requester);
  ar.setSrcURL(request.srcURL);
  ar.setStorageClass(request.storageClass);
  // We will need to identity tapepools is order to construct the request
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  auto & cl = criteria.copyToPoolMap;
  std::list<cta::objectstore::ArchiveRequest::JobDump> jl;
  for (auto copy=cl.begin(); copy != cl.end(); copy++) {
    std::string aqaddr = re.addOrGetArchiveQueueAndCommit(copy->second, *m_agent);
    ar.addJob(copy->first, copy->second, aqaddr);
    jl.push_back(cta::objectstore::ArchiveRequest::JobDump());
    jl.back().copyNb = copy->first;
    jl.back().tapePool = copy->second;
    jl.back().ArchiveQueueAddress = aqaddr;
  }
  if (!jl.size()) {
    throw ArchiveRequestHasNoCopies("In OStoreDB::queue: the archive to file request has no copy");
  }
  // We create the object here
  {
    objectstore::ScopedExclusiveLock al(*m_agent);
    m_agent->fetch();
    m_agent->addToOwnership(ar.getAddressIfSet());
    m_agent->commit();
  }
  ar.setOwner(m_agent->getAddressIfSet());
  ar.insert();
  ScopedExclusiveLock arl(ar);
  // We can now enqueue the requests
  std::list<std::string> linkedTapePools;
  try {
    for (auto &j: ar.dumpJobs()) {
      objectstore::ArchiveQueue aq(j.ArchiveQueueAddress, m_objectStore);
      ScopedExclusiveLock aql(aq);
      aq.fetch();
      if (aq.getOwner() != re.getAddressIfSet())
          throw NoSuchArchiveQueue("In OStoreDB::queue: non-existing archive queue found "
              "(dangling pointer): canceling request creation.");
      aq.addJob(j, ar.getAddressIfSet(), ar.getArchiveFileID(), 
        ar.getFileSize(), ar.getMountPolicy(),
        ar.getCreationLog().time);
      // Now that we have the tape pool handy, get the retry limits from it and 
      // assign them to the job
      ar.setJobFailureLimits(j.copyNb, criteria.mountPolicy.maxRetriesWithinMount, 
        criteria.mountPolicy.maxTotalRetries);
      aq.commit();
      linkedTapePools.push_back(j.ArchiveQueueAddress);
    }
  } catch (NoSuchArchiveQueue &) {
    // Unlink the request from already connected tape pools
    for (auto tpa=linkedTapePools.begin(); tpa!=linkedTapePools.end(); tpa++) {
      objectstore::ArchiveQueue aq(*tpa, m_objectStore);
      ScopedExclusiveLock aql(aq);
      aq.fetch();
      aq.removeJob(ar.getAddressIfSet());
      aq.commit();
      ar.remove();
    }
    throw;
  }
  // The request is now fully set. As it's multi-owned, we do not set the owner,
  // just to disown it from the agent.
  ar.setOwner("");
  ar.commit();
  arl.release();
  // And remove reference from the agent
  {
    objectstore::ScopedExclusiveLock al(*m_agent);
    m_agent->fetch();
    m_agent->removeFromOwnership(ar.getAddressIfSet());
    m_agent->commit();
  }
}

void OStoreDB::deleteArchiveRequest(const SecurityIdentity& requester, 
  uint64_t fileId) {
  // First of, find the archive request form all the tape pools.
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto tpl = re.dumpArchiveQueues();
  rel.release();
  for (auto tpp=tpl.begin(); tpp!= tpl.end(); tpp++) {
    objectstore::ArchiveQueue aq(tpp->address, m_objectStore);
    ScopedSharedLock tplock(aq);
    aq.fetch();
    auto ajl=aq.dumpJobs();
    tplock.release();
    for (auto ajp=ajl.begin(); ajp!=ajl.end(); ajp++) {
      objectstore::ArchiveRequest ar(ajp->address, m_objectStore);
      ScopedSharedLock atfrl(ar);
      ar.fetch();
      if (ar.getArchiveFileID() == fileId) {
        atfrl.release();
        objectstore::ScopedExclusiveLock al(*m_agent);
        m_agent->fetch();
        m_agent->addToOwnership(ar.getAddressIfSet());
        m_agent->commit();
        ScopedExclusiveLock atfrxl(ar);
        ar.fetch();
        ar.setAllJobsFailed();
        ar.setOwner(m_agent->getAddressIfSet());
        ar.commit();
        auto jl = ar.dumpJobs();
        for (auto j=jl.begin(); j!=jl.end(); j++) {
          try {
            objectstore::ArchiveQueue aq(j->ArchiveQueueAddress, m_objectStore);
            ScopedExclusiveLock tpl(aq);
            aq.fetch();
            aq.removeJob(ar.getAddressIfSet());
            aq.commit();
          } catch (...) {}
        }
        ar.remove();
        m_agent->removeFromOwnership(ar.getAddressIfSet());
        m_agent->commit();
      }
    }
  }
  throw NoSuchArchiveRequest("In OStoreDB::deleteArchiveRequest: ArchiveToFileRequest not found");
}

std::unique_ptr<SchedulerDatabase::ArchiveToFileRequestCancelation>
  OStoreDB::markArchiveRequestForDeletion(const SecurityIdentity& requester,
  uint64_t fileId) {
  assertAgentSet();
  // Construct the return value immediately
  std::unique_ptr<cta::OStoreDB::ArchiveToFileRequestCancelation>
    internalRet(new cta::OStoreDB::ArchiveToFileRequestCancelation(m_agent, m_objectStore));
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
        if (tar.getArchiveFileID() == fileId) {
          // Point the agent to the request
          ScopedExclusiveLock agl(*m_agent);
          m_agent->fetch();
          m_agent->addToOwnership(arp->address);
          m_agent->commit();
          agl.release();
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
            objectstore::ArchiveQueue aqp(atpp->ArchiveQueueAddress, m_objectStore);
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

void OStoreDB::ArchiveToFileRequestCancelation::complete() {
  if (m_closed)
    throw ArchiveRequestAlreadyDeleted("OStoreDB::ArchiveToFileRequestCancelation::complete(): called twice");
  // We just need to delete the object and forget it
  m_request.remove();
  objectstore::ScopedExclusiveLock al (*m_agent);
  m_agent->fetch();
  m_agent->removeFromOwnership(m_request.getAddressIfSet());
  m_agent->commit();
  m_closed = true;
  }

OStoreDB::ArchiveToFileRequestCancelation::~ArchiveToFileRequestCancelation() {
  if (!m_closed) {
    try {
      m_request.garbageCollect(m_agent->getAddressIfSet());
      objectstore::ScopedExclusiveLock al (*m_agent);
      m_agent->fetch();
      m_agent->removeFromOwnership(m_request.getAddressIfSet());
      m_agent->commit();
    } catch (...) {}
  }
}



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
      ret.back().archiveFileID = osar.getArchiveFileID();
      ret.back().copyNumber = copynb;
      ret.back().tapePool = tpp.tapePool;
      ret.back().request.checksumType = osar.getChecksumType();
      ret.back().request.checksumValue = osar.getChecksumValue();
      ret.back().request.creationLog = osar.getCreationLog();
      ret.back().request.diskFileID = osar.getDiskFileID();
      ret.back().request.diskpoolName = osar.getDiskpoolName();
      ret.back().request.diskpoolThroughput = osar.getDiskpoolThroughput();
      ret.back().request.drData = osar.getDrData();
      ret.back().request.fileSize = osar.getFileSize();
      ret.back().request.instance = osar.getInstance();
      ret.back().request.requester = osar.getRequester();
      ret.back().request.srcURL = osar.getSrcURL();
      ret.back().request.storageClass = osar.getStorageClass();
    }
    return ret;
  }
  throw NoSuchArchiveQueue("In OStoreDB::getArchiveRequests: tape pool not found");
}

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
      ret[tpp.tapePool].back().archiveFileID = osar.getArchiveFileID();
      ret[tpp.tapePool].back().copyNumber = copynb;
      ret[tpp.tapePool].back().tapePool = tpp.tapePool;
      ret[tpp.tapePool].back().request.checksumType = osar.getChecksumType();
      ret[tpp.tapePool].back().request.checksumValue = osar.getChecksumValue();
      ret[tpp.tapePool].back().request.creationLog = osar.getCreationLog();
      ret[tpp.tapePool].back().request.diskFileID = osar.getDiskFileID();
      ret[tpp.tapePool].back().request.diskpoolName = osar.getDiskpoolName();
      ret[tpp.tapePool].back().request.diskpoolThroughput = osar.getDiskpoolThroughput();
      ret[tpp.tapePool].back().request.drData = osar.getDrData();
      ret[tpp.tapePool].back().request.fileSize = osar.getFileSize();
      ret[tpp.tapePool].back().request.instance = osar.getInstance();
      ret[tpp.tapePool].back().request.requester = osar.getRequester();
      ret[tpp.tapePool].back().request.srcURL = osar.getSrcURL();
      ret[tpp.tapePool].back().request.storageClass = osar.getStorageClass();
    }
  }
  return ret;
}


void OStoreDB::queue(const cta::common::dataStructures::RetrieveRequest& rqst,
  const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
//  assertAgentSet();
//  // Check at least one potential tape copy is provided.
//  // In order to post the job, construct it first.
//  objectstore::RetrieveToFileRequest rtfr(m_agent->nextId("RetrieveToFileRequest"), m_objectStore);
//  rtfr.initialize();
//  rtfr.setArchiveFile(rqst.getArchiveFile());
//  rtfr.setRemoteFile(rqst.getRemoteFile());
//  rtfr.setPriority(rqst.priority);
//  rtfr.setEntryLog(rqst.entryLog);
//  rtfr.setSize(rqst.getArchiveFile().size);
//  // We will need to identity tapes is order to construct the request.
//  // First load all the tapes information in a memory map
//  std::map<std::string, std::string> vidToAddress;
//  RootEntry re(m_objectStore);
//  ScopedSharedLock rel(re);
//  re.fetch();
//  auto archiveQueues = re.dumpArchiveQueues();
//  for(auto pool=archiveQueues.begin(); pool!=archiveQueues.end(); pool++) {
//    objectstore::ArchiveQueue aq(pool->address, m_objectStore);
//    objectstore::ScopedSharedLock tpl(aq);
//    aq.fetch();
//    auto tapes = aq.dumpTapesAndFetchStatus();
//    for(auto tape=tapes.begin(); tape!=tapes.end(); tape++)
//      vidToAddress[tape->vid] = tape->address;
//  }
//  // Now add all the candidate tape copies to the request. With validation
//  for (auto tc=rqst.getTapeCopies().begin(); tc!=rqst.getTapeCopies().end(); tc++) {
//    // Check the tape copy copynumber (range = [1 - copyCount] )
//    if (tc->copyNb > rqst.getTapeCopies().size() || tc->copyNb < 1) {
//      throw TapeCopyNumberOutOfRange("In OStoreDB::queue(RetrieveToFile): copy number out of range");
//    }
//  }
//  // Add all the tape copies to the request
//  try {
//    for (auto tc=rqst.getTapeCopies().begin(); tc!=rqst.getTapeCopies().end(); tc++) {
//      rtfr.addJob(*tc, vidToAddress.at(tc->vid));
//    }
//  } catch (std::out_of_range &) {
//    throw NoSuchTape("In OStoreDB::queue(RetrieveToFile): tape not found");
//  }
//  // We now need to select the tape from which we will migrate next. This should
//  // be the tape with the most jobs already queued.
//  // TODO: this will have to look at tape statuses on the long run as well
//  uint16_t selectedCopyNumber;
//  uint64_t bestTapeQueuedBytes;
//  std::string selectedVid;
//  uint64_t selectedFseq;
//  uint64_t selectedBlockid;
//  {
//    // First tape copy is always better than nothing. 
//    auto tc=rqst.getTapeCopies().begin();
//    selectedCopyNumber = tc->copyNb;
//    selectedVid = tc->vid;
//    selectedFseq = tc->fSeq;
//    selectedBlockid = tc->blockId;
//    // Get info for the tape.
//    {
//      objectstore::Tape t(vidToAddress.at(tc->vid), m_objectStore);
//      objectstore::ScopedSharedLock tl(t);
//      t.fetch();
//      bestTapeQueuedBytes = t.getJobsSummary().bytes;
//    }
//    tc++;
//    // Compare with the next ones
//    for (;tc!=rqst.getTapeCopies().end(); tc++) {
//      objectstore::Tape t(vidToAddress.at(tc->vid), m_objectStore);
//      objectstore::ScopedSharedLock tl(t);
//      t.fetch();
//      if (t.getJobsSummary().bytes > bestTapeQueuedBytes) {
//        bestTapeQueuedBytes = t.getJobsSummary().bytes;
//        selectedCopyNumber = tc->copyNb;
//        selectedVid = tc->vid;
//        selectedFseq = tc->fSeq;
//        selectedBlockid = tc->blockId;
//      }
//    }
//  }
//  // We now can enqueue the request on this most promising tape.
//  {
//    objectstore::Tape tp(vidToAddress.at(selectedVid), m_objectStore);
//    ScopedExclusiveLock tpl(tp);
//    tp.fetch();
//    objectstore::RetrieveToFileRequest::JobDump jd;
//    jd.copyNb = selectedCopyNumber;
//    jd.tape = selectedVid;
//    jd.tapeAddress = vidToAddress.at(selectedVid);
//    jd.fseq = selectedFseq;
//    jd.blockid = selectedBlockid;
//    tp.addJob(jd, rtfr.getAddressIfSet(), rqst.getArchiveFile().size, rqst.priority, rqst.creationLog.time);
//    tp.commit();
//  }
//  // The request is now fully set. It belongs to the tape.
//  rtfr.setOwner(vidToAddress.at(selectedVid));
//  rtfr.insert();
//  // And remove reference from the agent
//  {
//    objectstore::ScopedExclusiveLock al(*m_agent);
//    m_agent->fetch();
//    m_agent->removeFromOwnership(rtfr.getAddressIfSet());
//    m_agent->commit();
//  }
}

std::list<RetrieveRequestDump> OStoreDB::getRetrieveRequestsByVid(const std::string& vid) const {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

std::list<RetrieveRequestDump> OStoreDB::getRetrieveRequestsByRequester(const std::string& vid) const {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}




std::map<cta::Tape, std::list<RetrieveRequestDump> > OStoreDB::getRetrieveRequests() const {
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

void OStoreDB::deleteRetrieveRequest(const SecurityIdentity& requester, 
  const std::string& remoteFile) {
  throw exception::Exception("Not Implemented");
  }

std::list<cta::common::DriveState> OStoreDB::getDriveStates() const {
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto driveRegisterAddress = re.getDriveRegisterAddress();
  objectstore::DriveRegister dr(driveRegisterAddress, m_objectStore);
  objectstore::ScopedExclusiveLock drl(dr);
  dr.fetch();
  return dr.dumpDrives();
}

std::unique_ptr<SchedulerDatabase::ArchiveMount> 
  OStoreDB::TapeMountDecisionInfo::createArchiveMount(
    const std::string& vid, const std::string & tapePool, const std::string driveName,
    const std::string& logicalLibrary, const std::string& hostName, time_t startTime) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
//  // In order to create the mount, we have to:
//  // Check we actually hold the scheduling lock
//  // Check the tape exists, add it to ownership and set its activity status to 
//  // busy, with the current agent pointing to it for unbusying
//  // Set the drive status to up, but do not commit anything to the drive register
//  // the drive register does not need garbage collection as it should reflect the
//  // latest known state of the drive (and its absence of updating if needed)
//  // Prepare the return value
//  std::unique_ptr<OStoreDB::ArchiveMount> privateRet(
//    new OStoreDB::ArchiveMount(m_objectStore, m_agent));
//  auto &am = *privateRet;
//  // Check we hold the scheduling lock
//  if (!m_lockTaken)
//    throw SchedulingLockNotHeld("In OStoreDB::TapeMountDecisionInfo::createArchiveMount: "
//      "cannot create mount without holding scheduling lock");
//  // Find the tape and update it
//  objectstore::RootEntry re(m_objectStore);
//  objectstore::ScopedSharedLock rel(re);
//  re.fetch();
//  auto tplist = re.dumpTapePools();
//  auto driveRegisterAddress = re.getDriveRegisterAddress();
//  rel.release();
//  {
//    std::string tpAdress;
//    for (auto tpp=tplist.begin(); tpp!=tplist.end(); tpp++)
//      if (tpp->tapePool == tapePool)
//        tpAdress = tpp->address;
//    if (!tpAdress.size())
//      throw NoSuchArchiveQueue("In OStoreDB::TapeMountDecisionInfo::createArchiveMount:"
//        " tape pool not found");
//    objectstore::ArchiveQueue aq(tpAdress, m_objectStore);
//    objectstore::ScopedSharedLock aql(aq);
//    aq.fetch();
//    auto tlist = aq.dumpTapesAndFetchStatus();
//    std::string tAddress;
//    for (auto tptr = tlist.begin(); tptr!=tlist.end(); tptr++) {
//      if (tptr->vid == vid)
//        tAddress = tptr->address;
//    }
//    if (!tAddress.size())
//      throw NoSuchTape("In OStoreDB::TapeMountDecisionInfo::createArchiveMount:"
//        " tape not found");
//    objectstore::Tape t(tAddress, m_objectStore);
//    objectstore::ScopedExclusiveLock tlock(t);
//    t.fetch();
//    if (t.isFull())
//      throw TapeNotWritable("In OStoreDB::TapeMountDecisionInfo::createArchiveMount:"
//        " the tape is not writable (full)");
//    if (t.isArchived())
//      throw TapeNotWritable("In OStoreDB::TapeMountDecisionInfo::createArchiveMount:"
//        " the tape is not writable (archived)");
//    if (t.isReadOnly())
//      throw TapeNotWritable("In OStoreDB::TapeMountDecisionInfo::createArchiveMount:"
//        " the tape is not writable (readonly)");
//    if (t.isDisabled())
//      throw TapeNotWritable("In OStoreDB::TapeMountDecisionInfo::createArchiveMount:"
//        " the tape is not writable (disabled)");
//    if (t.isBusy())
//      throw TapeIsBusy("In OStoreDB::TapeMountDecisionInfo::createArchiveMount:"
//        " the tape is busy");
//    // This tape seems fine for our purposes. We will set it as an owned object
//    // so that garbage collection can unbusy the tape in case of a session crash
//    {
//      objectstore::ScopedExclusiveLock al(m_agent);
//      m_agent.fetch();
//      m_agent.addToOwnership(t.getAddressIfSet());
//      m_agent.commit();
//    }
//    am.nbFilesCurrentlyOnTape = t.getLastFseq();
//    am.mountInfo.vid = t.getVid();
//    t.setBusy(driveName, objectstore::Tape::MountType::Archive, hostName, startTime, 
//      m_agent.getAddressIfSet());
//    t.commit();
//  }
//  // Fill up the mount info
//  am.mountInfo.drive = driveName;
//  am.mountInfo.mountId = m_schedulerGlobalLock->getIncreaseCommitMountId();
//  m_schedulerGlobalLock->commit();
//  am.mountInfo.tapePool = tapePool;
//  am.mountInfo.tapePool = tapePool;
//  am.mountInfo.logicalLibrary = logicalLibrary;
//  // Update the status of the drive in the registry
//  {
//    // Get hold of the drive registry
//    objectstore::DriveRegister dr(driveRegisterAddress, m_objectStore);
//    objectstore::ScopedExclusiveLock drl(dr);
//    dr.fetch();
//    // The drive is already in-session, to prevent double scheduling before it 
//    // goes to mount state. If the work to be done gets depleted in the mean time,
//    // we will switch back to up.
//    dr.reportDriveStatus(driveName, logicalLibrary, 
//      cta::common::DriveStatus::Starting, startTime, 
//      cta::MountType::ARCHIVE, privateRet->mountInfo.mountId,
//      0, 0, 0, vid, tapePool);
//    dr.commit();
//  }
//  // We committed the scheduling decision. We can now release the scheduling lock.
//  m_lockOnSchedulerGlobalLock.release();
//  m_lockTaken = false;
//  // We can now return the mount session object to the user.
//  std::unique_ptr<SchedulerDatabase::ArchiveMount> ret(privateRet.release());
//  return ret;
}

OStoreDB::TapeMountDecisionInfo::TapeMountDecisionInfo(
  objectstore::Backend& os, objectstore::Agent& a):
   m_lockTaken(false), m_objectStore(os), m_agent(a) {}


std::unique_ptr<SchedulerDatabase::RetrieveMount> 
  OStoreDB::TapeMountDecisionInfo::createRetrieveMount(
    const std::string& vid, const std::string & tapePool, const std::string driveName, 
    const std::string& logicalLibrary, const std::string& hostName, time_t startTime) {
    throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
//  // In order to create the mount, we have to:
//  // Check we actually hold the scheduling lock
//  // Check the tape exists, add it to ownership and set its activity status to 
//  // busy, with the current agent pointing to it for unbusying
//  // Set the drive status to up, but do not commit anything to the drive register
//  // the drive register does not need garbage collection as it should reflect the
//  // latest known state of the drive (and its absence of updating if needed)
//  // Prepare the return value
//  std::unique_ptr<OStoreDB::RetrieveMount> privateRet(
//    new OStoreDB::RetrieveMount(m_objectStore, m_agent));
//  auto &rm = *privateRet;
//  // Check we hold the scheduling lock
//  if (!m_lockTaken)
//    throw SchedulingLockNotHeld("In OStoreDB::TapeMountDecisionInfo::createRetrieveMount: "
//      "cannot create mount without holding scheduling lock");
//  // Find the tape and update it
//  objectstore::RootEntry re(m_objectStore);
//  objectstore::ScopedSharedLock rel(re);
//  re.fetch();
//  auto tplist = re.dumpTapePools();
//  auto driveRegisterAddress = re.getDriveRegisterAddress();
//  rel.release();
//  {
//    std::string tpAdress;
//    for (auto tpp=tplist.begin(); tpp!=tplist.end(); tpp++)
//      if (tpp->tapePool == tapePool)
//        tpAdress = tpp->address;
//    if (!tpAdress.size())
//      throw NoSuchArchiveQueue("In OStoreDB::TapeMountDecisionInfo::createRetrieveMount:"
//        " tape pool not found");
//    objectstore::TapePool tp(tpAdress, m_objectStore);
//    objectstore::ScopedSharedLock tpl(tp);
//    tp.fetch();
//    auto tlist = tp.dumpTapesAndFetchStatus();
//    std::string tAddress;
//    for (auto tptr = tlist.begin(); tptr!=tlist.end(); tptr++) {
//      if (tptr->vid == vid)
//        tAddress = tptr->address;
//    }
//    if (!tAddress.size())
//      throw NoSuchTape("In OStoreDB::TapeMountDecisionInfo::createRetrieveMount:"
//        " tape not found");
//    objectstore::Tape t(tAddress, m_objectStore);
//    objectstore::ScopedExclusiveLock tlock(t);
//    t.fetch();
//    if (t.isArchived())
//      throw TapeNotWritable("In OStoreDB::TapeMountDecisionInfo::createRetrieveMount:"
//        " the tape is not readable (archived)");
//    if (t.isDisabled())
//      throw TapeNotWritable("In OStoreDB::TapeMountDecisionInfo::createRetrieveMount:"
//        " the tape is not readable (disabled)");
//    if (t.isBusy())
//      throw TapeIsBusy("In OStoreDB::TapeMountDecisionInfo::createRetrieveMount:"
//        " the tape is busy");
//    // This tape seems fine for our purposes. We will set it as an owned object
//    // so that garbage collection can unbusy the tape in case of a session crash
//    {
//      objectstore::ScopedExclusiveLock al(m_agent);
//      m_agent.fetch();
//      m_agent.addToOwnership(t.getAddressIfSet());
//      m_agent.commit();
//    }
//    t.setBusy(driveName, objectstore::Tape::MountType::Archive, hostName, startTime, 
//      m_agent.getAddressIfSet());
//    t.commit();
//  }
//  // Fill up the mount info
//  rm.mountInfo.vid = vid;
//  rm.mountInfo.drive = driveName;
//  rm.mountInfo.logicalLibrary = logicalLibrary;
//  rm.mountInfo.mountId = m_schedulerGlobalLock->getIncreaseCommitMountId();
//  rm.mountInfo.tapePool = tapePool;
//  // Update the status of the drive in the registry
//  {
//    // Get hold of the drive registry
//    objectstore::DriveRegister dr(driveRegisterAddress, m_objectStore);
//    objectstore::ScopedExclusiveLock drl(dr);
//    dr.fetch();
//    // The drive is already in-session, to prevent double scheduling before it 
//    // goes to mount state. If the work to be done gets depleted in the mean time,
//    // we will switch back to up.
//    dr.reportDriveStatus(driveName, logicalLibrary, 
//      cta::common::DriveStatus::Starting, startTime, 
//      cta::MountType::RETRIEVE, privateRet->mountInfo.mountId,
//      0, 0, 0, vid, tapePool);
//    dr.commit();
//  }
//  // We committed the scheduling decision. We can now release the scheduling lock.
//  m_lockOnSchedulerGlobalLock.release();
//  m_lockTaken = false;
//  // We can now return the mount session object to the user.
//  std::unique_ptr<SchedulerDatabase::RetrieveMount> ret(privateRet.release());
//  return ret;
}
 
OStoreDB::TapeMountDecisionInfo::~TapeMountDecisionInfo() {
  // The lock should be released before the objectstore object 
  // m_schedulerGlobalLock gets destroyed. We explicitely release the lock,
  // and then destroy the object
  if (m_lockTaken)
    m_lockOnSchedulerGlobalLock.release();
  m_schedulerGlobalLock.reset(NULL);
}

OStoreDB::ArchiveMount::ArchiveMount(objectstore::Backend& os, objectstore::Agent& a):
  m_objectStore(os), m_agent(a) {}

const SchedulerDatabase::ArchiveMount::MountInfo& OStoreDB::ArchiveMount::getMountInfo() {
  return mountInfo;
}

auto OStoreDB::ArchiveMount::getNextJob() -> std::unique_ptr<SchedulerDatabase::ArchiveJob> {
  // Find the next file to archive
  // Get the tape pool
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto tpl = re.dumpArchiveQueues();
  rel.release();
  std::string tpAddress;
  for (auto tpp = tpl.begin(); tpp != tpl.end(); tpp++) {
    if (tpp->tapePool == mountInfo.tapePool)
      tpAddress = tpp->address;
  }
  if (!tpAddress.size())
    throw NoSuchArchiveQueue("In OStoreDB::ArchiveMount::getNextJob(): tape pool not found");
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
}

void OStoreDB::ArchiveMount::complete(time_t completionTime) {
  // When the session is complete, we can reset the status of the tape and the
  // drive
  // Reset the drive
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
//  objectstore::RootEntry re(m_objectStore);
//  objectstore::ScopedSharedLock rel(re);
//  re.fetch();
//  objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_objectStore);
//  objectstore::ScopedExclusiveLock drl(dr);
//  dr.fetch();
//  // Reset the drive state.
//  dr.reportDriveStatus(mountInfo.drive, mountInfo.logicalLibrary, 
//    cta::common::DriveStatus::Up, completionTime, 
//    cta::MountType::NONE, 0,
//    0, 0, 0, "", "");
//  dr.commit();
//  // Find the tape and unbusy it.
//  objectstore::TapePool tp (re.getTapePoolAddress(mountInfo.tapePool), m_objectStore);
//  rel.release();
//  objectstore::ScopedSharedLock tpl(tp);
//  tp.fetch();
//  objectstore::Tape t(tp.getTapeAddress(mountInfo.vid), m_objectStore);
//  objectstore::ScopedExclusiveLock tl(t);
//  tpl.release();
//  t.fetch();
//  t.releaseBusy();
//  t.commit();
//  objectstore::ScopedExclusiveLock agl(m_agent);
//  m_agent.fetch();
//  m_agent.removeFromOwnership(t.getAddressIfSet());
//  m_agent.commit();
}

OStoreDB::ArchiveJob::ArchiveJob(const std::string& jobAddress, 
  objectstore::Backend& os, objectstore::Agent& ag, ArchiveMount & am): m_jobOwned(false),
  m_objectStore(os), m_agent(ag), m_archiveRequest(jobAddress, os), m_archiveMount(am) {}

OStoreDB::RetrieveMount::RetrieveMount(objectstore::Backend& os, objectstore::Agent& a):
  m_objectStore(os), m_agent(a) { }

const OStoreDB::RetrieveMount::MountInfo& OStoreDB::RetrieveMount::getMountInfo() {
  return mountInfo;
}

auto OStoreDB::RetrieveMount::getNextJob() -> std::unique_ptr<SchedulerDatabase::RetrieveJob> {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
//  // Find the next file to retrieve
//  // Get the tape pool and then tape
//  objectstore::RootEntry re(m_objectStore);
//  objectstore::ScopedSharedLock rel(re);
//  re.fetch();
//  auto tpl = re.dumpTapePools();
//  rel.release();
//  std::string tpAddress;
//  for (auto tpp = tpl.begin(); tpp != tpl.end(); tpp++) {
//    if (tpp->tapePool == mountInfo.tapePool)
//      tpAddress = tpp->address;
//  }
//  if (!tpAddress.size())
//    throw NoSuchArchiveQueue("In OStoreDB::RetrieveMount::getNextJob(): tape pool not found");
//  objectstore::TapePool tp(tpAddress, m_objectStore);
//  objectstore::ScopedSharedLock tplock(tp);
//  tp.fetch();
//  auto tl = tp.dumpTapes();
//  tplock.release();
//  std::string tAddress;
//  for (auto tptr = tl.begin(); tptr != tl.end(); tptr++) {
//    if (tptr->vid == mountInfo.vid)
//      tAddress = tptr->address;
//  }
//  if (!tAddress.size())
//    throw NoSuchTape("In OStoreDB::RetrieveMount::getNextJob(): tape not found");
//  objectstore::Tape t(tAddress, m_objectStore);
//  objectstore::ScopedExclusiveLock tlock(t);
//  t.fetch();
//  while (t.dumpJobs().size()) {
//    // Get the tape pool's jobs list, and pop the first
//    auto jl=t.dumpJobs();
//    // First take a lock on and download the job
//    // If the request is not around anymore, we will just move the the next
//    // Prepare the return value
//    std::unique_ptr<OStoreDB::RetrieveJob> privateRet(new OStoreDB::RetrieveJob(
//      jl.front().address, m_objectStore, m_agent));
//    privateRet->m_copyNb = jl.front().copyNb;
//    objectstore::ScopedExclusiveLock rtfrl;
//    try {
//      rtfrl.lock(privateRet->m_rtfr);
//      privateRet->m_rtfr.fetch();
//    } catch (cta::exception::Exception &) {
//      // we failed to access the object. It might be missing.
//      // Just pop this job from the pool and move to the next.
//      t.removeJob(privateRet->m_rtfr.getAddressIfSet());
//      // Commit in case we do not pass by again.
//      t.commit();
//      continue;
//    }
//    // Take ownership of the job
//    // Add to ownership
//    objectstore::ScopedExclusiveLock al(m_agent);
//    m_agent.fetch();
//    m_agent.addToOwnership(privateRet->m_rtfr.getAddressIfSet());
//    m_agent.commit();
//    al.release();
//    // Make the ownership official (for the whole request in retrieves)
//    privateRet->m_rtfr.setOwner(m_agent.getAddressIfSet());
//    privateRet->m_rtfr.commit();
//    // Remove the job from the tape pool queue
//    t.removeJob(privateRet->m_rtfr.getAddressIfSet());
//    // We can commit and release the tape pool lock, we will only fill up
//    // memory structure from here on.
//    t.commit();
//    privateRet->archiveFile = privateRet->m_rtfr.getArchiveFile();
//    privateRet->remoteFile = privateRet->m_rtfr.getRemoteFile();
//    objectstore::RetrieveToFileRequest::JobDump jobDump = privateRet->m_rtfr.getJob(privateRet->m_copyNb);
//    privateRet->nameServerTapeFile.tapeFileLocation.fSeq = jobDump.fseq;
//    privateRet->nameServerTapeFile.tapeFileLocation.blockId = jobDump.blockid;
//    privateRet->nameServerTapeFile.tapeFileLocation.copyNb = privateRet->m_copyNb;
//    privateRet->nameServerTapeFile.tapeFileLocation.vid = mountInfo.vid;
//
//      std::numeric_limits<decltype(privateRet->nameServerTapeFile.tapeFileLocation.blockId)>::max();
//    privateRet->m_jobOwned = true;
//    return std::unique_ptr<SchedulerDatabase::RetrieveJob> (privateRet.release());
//  }
//  return std::unique_ptr<SchedulerDatabase::RetrieveJob>();
}

void OStoreDB::RetrieveMount::complete(time_t completionTime) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
//  // When the session is complete, we can reset the status of the tape and the
//  // drive
//  // Reset the drive
//  objectstore::RootEntry re(m_objectStore);
//  objectstore::ScopedSharedLock rel(re);
//  re.fetch();
//  objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_objectStore);
//  objectstore::ScopedExclusiveLock drl(dr);
//  dr.fetch();
//  // Reset the drive state.
//  dr.reportDriveStatus(mountInfo.drive, mountInfo.logicalLibrary, 
//    cta::common::DriveStatus::Up, completionTime, 
//    cta::MountType::NONE, 0,
//    0, 0, 0, "", "");
//  dr.commit();
//  // Find the tape and unbusy it.
//  objectstore::TapePool tp (re.getTapePoolAddress(mountInfo.tapePool), m_objectStore);
//  rel.release();
//  objectstore::ScopedSharedLock tpl(tp);
//  tp.fetch();
//  objectstore::Tape t(tp.getTapeAddress(mountInfo.vid), m_objectStore);
//  objectstore::ScopedExclusiveLock tl(t);
//  tpl.release();
//  t.fetch();
//  t.releaseBusy();
//  t.commit();
//  objectstore::ScopedExclusiveLock agl(m_agent);
//  m_agent.fetch();
//  m_agent.removeFromOwnership(t.getAddressIfSet());
//  m_agent.commit();
}

void OStoreDB::RetrieveMount::setDriveStatus(cta::common::DriveStatus status, time_t completionTime) {
  // We just report the drive status as instructed by the tape thread.
  // Get the drive register
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_objectStore);
  objectstore::ScopedExclusiveLock drl(dr);
  dr.fetch();
  // Reset the drive state.
  dr.reportDriveStatus(mountInfo.drive, mountInfo.logicalLibrary, 
    status, completionTime, 
    cta::MountType::RETRIEVE, 0,
    0, 0, 0, getMountInfo().vid, getMountInfo().tapePool);
  dr.commit();
}

void OStoreDB::ArchiveMount::setDriveStatus(cta::common::DriveStatus status, time_t completionTime) {
  // We just report the drive status as instructed by the tape thread.
  // Get the drive register
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  objectstore::DriveRegister dr(re.getDriveRegisterAddress(), m_objectStore);
  objectstore::ScopedExclusiveLock drl(dr);
  dr.fetch();
  // Reset the drive state.
  dr.reportDriveStatus(mountInfo.drive, mountInfo.logicalLibrary, 
    status, completionTime, 
    cta::MountType::ARCHIVE, 0,
    0, 0, 0, getMountInfo().vid, getMountInfo().tapePool);
  dr.commit();
}

void OStoreDB::ArchiveJob::fail() {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
//  if (!m_jobOwned)
//    throw JobNowOwned("In OStoreDB::ArchiveJob::fail: cannot fail a job not owned");
//  // Lock the archive request. Fail the job.
//  objectstore::ScopedExclusiveLock atfrl(m_archiveRequest);
//  m_archiveRequest.fetch();
//  // Add a job failure. If the job is failed, we will delete it.
//  if (m_archiveRequest.addJobFailure(m_copyNb, m_mountId)) {
//    // The job will not be retried. Either another jobs for the same request is 
//    // queued and keeps the request referenced or the request has been deleted.
//    // In any case, we can forget it.
//    objectstore::ScopedExclusiveLock al(m_agent);
//    m_agent.fetch();
//    m_agent.removeFromOwnership(m_archiveRequest.getAddressIfSet());
//    m_agent.commit();
//    m_jobOwned = false;
//    return;
//  }
//  // The job still has a chance, return it to its original tape pool's queue
//  objectstore::RootEntry re(m_objectStore);
//  objectstore::ScopedSharedLock rel(re);
//  re.fetch();
//  auto tpl = re.dumpTapePools();
//  rel.release();
//  for (auto tpp=tpl.begin(); tpp!=tpl.end(); tpp++) {
//    if (tpp->tapePool == m_tapePool) {
//      objectstore::TapePool tp(tpp->address, m_objectStore);
//      objectstore::ScopedExclusiveLock tplock(tp);
//      tp.fetch();
//      // Find the right job
//      auto jl = m_archiveRequest.dumpJobs();
//      for (auto j=jl.begin(); j!=jl.end(); j++) {
//        if (j->copyNb == m_copyNb) {
//          tp.addJobIfNecessary(*j, m_archiveRequest.getAddressIfSet(), m_archiveRequest.getArchiveFile().fileId, m_archiveRequest.getRemoteFile().status.size);
//          tp.commit();
//          tplock.release();
//          // We have a pointer to the job, we can change the job ownership
//          m_archiveRequest.setJobOwner(m_copyNb, tpp->address);
//          m_archiveRequest.commit();
//          atfrl.release();
//          // We just have to remove the ownership from the agent and we're done.
//          objectstore::ScopedExclusiveLock al(m_agent);
//          m_agent.fetch();
//          m_agent.removeFromOwnership(m_archiveRequest.getAddressIfSet());
//          m_agent.commit();
//          m_jobOwned = false;
//          return;
//        }
//      }
//      throw NoSuchJob("In OStoreDB::ArchiveJob::fail(): could not find the job in the request object");
//    }
//  }
//  throw NoSuchArchiveQueue("In OStoreDB::ArchiveJob::fail(): could not find the tape pool");
}

void OStoreDB::ArchiveJob::bumpUpTapeFileCount(uint64_t newFileCount) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
//  m_archiveMount.setTapeMaxFileCount(newFileCount);
}


void OStoreDB::ArchiveJob::succeed() {
  // Lock the request and set the job as successful.
  objectstore::ScopedExclusiveLock atfrl(m_archiveRequest);
  m_archiveRequest.fetch();
  std::string atfrAddress = m_archiveRequest.getAddressIfSet();
  if (m_archiveRequest.setJobSuccessful(m_copyNb)) {
    m_archiveRequest.remove();
  } else {
    m_archiveRequest.commit();
  }
  // We no more own the job (which could be gone)
  m_jobOwned = false;
  // Remove ownership from agent
  objectstore::ScopedExclusiveLock al(m_agent);
  m_agent.fetch();
  m_agent.removeFromOwnership(atfrAddress);
  m_agent.commit();
}

OStoreDB::ArchiveJob::~ArchiveJob() {
  if (m_jobOwned) {
    // Return the job to the pot if we failed to handle it.
    objectstore::ScopedExclusiveLock atfrl(m_archiveRequest);
    m_archiveRequest.fetch();
    m_archiveRequest.garbageCollect(m_agent.getAddressIfSet());
    atfrl.release();
    // Remove ownership from agent
    objectstore::ScopedExclusiveLock al(m_agent);
    m_agent.fetch();
    m_agent.removeFromOwnership(m_archiveRequest.getAddressIfSet());
    m_agent.commit();
  }
}

OStoreDB::RetrieveJob::RetrieveJob(const std::string& jobAddress, 
    objectstore::Backend& os, objectstore::Agent& ag): m_jobOwned(false),
  m_objectStore(os), m_agent(ag), m_rtfr(jobAddress, os) { }

void OStoreDB::RetrieveJob::fail() {
  throw NotImplemented("");
}

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

void OStoreDB::RetrieveJob::succeed() {
  // Lock the request and set the job as successful.
  objectstore::ScopedExclusiveLock rtfrl(m_rtfr);
  m_rtfr.fetch();
  std::string rtfrAddress = m_rtfr.getAddressIfSet();
  if (m_rtfr.setJobSuccessful(m_copyNb)) {
    m_rtfr.remove();
  } else {
    m_rtfr.commit();
  }
  // We no more own the job (which could be gone)
  m_jobOwned = false;
  // Remove ownership form the agent
  objectstore::ScopedExclusiveLock al(m_agent);
  m_agent.fetch();
  m_agent.removeFromOwnership(rtfrAddress);
  m_agent.commit();
}


}
