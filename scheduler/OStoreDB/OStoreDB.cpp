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
#include "objectstore/TapePool.hpp"
#include "objectstore/Tape.hpp"
#include "objectstore/DriveRegister.hpp"
#include "objectstore/ArchiveToFileRequest.hpp"
#include "objectstore/RetrieveToFileRequest.hpp"
#include "common/exception/Exception.hpp"
#include "common/admin/AdminHost.hpp"
#include "common/admin/AdminUser.hpp"
#include "common/archiveRoutes/ArchiveRoute.hpp"
#include "scheduler/UserArchiveRequest.hpp"
#include "scheduler/ArchiveToFileRequest.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/RetrieveToFileRequest.hpp"
#include "common/archiveNS/StorageClass.hpp"
#include "common/TapePool.hpp"
#include "common/archiveNS/Tape.hpp"
#include "RetrieveToFileRequest.hpp"
#include "common/archiveNS/TapeFileLocation.hpp"
#include "ArchiveToTapeCopyRequest.hpp"
#include "common/archiveNS/ArchiveFile.hpp"
#include "objectstore/ArchiveRequest.hpp"
#include "common/dataStructures/MountGroup.hpp"
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

void OStoreDB::createStorageClass(const std::string& name,
  const uint16_t nbCopies, const cta::CreationLog& creationLog) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  re.addStorageClass(name, nbCopies, creationLog);
  re.commit();
}

StorageClass OStoreDB::getStorageClass(const std::string& name) const {
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto sc = re.dumpStorageClass(name);
  return cta::StorageClass(name,
    sc.copyCount,
    sc.log);
}

std::list<StorageClass> OStoreDB::getStorageClasses() const {
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto scl = re.dumpStorageClasses();
  decltype (getStorageClasses()) ret;
  for (auto sc = scl.begin(); sc != scl.end(); sc++) {
    ret.push_back(StorageClass(sc->storageClass, 
        sc->copyCount, sc->log));
  }
  ret.sort();
  return ret;
}

void OStoreDB::deleteStorageClass(const SecurityIdentity& requester, 
  const std::string& name) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  re.removeStorageClass(name);
  re.commit();
}

void OStoreDB::createArchiveRoute(const std::string& storageClassName,
  const uint16_t copyNb, const std::string& tapePoolName,
  const cta::CreationLog& creationLog) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  re.addArchiveRoute(storageClassName, copyNb, tapePoolName, 
    objectstore::CreationLog(creationLog));
  re.commit();
}

std::list<common::archiveRoute::ArchiveRoute> 
  OStoreDB::getArchiveRoutes(const std::string& storageClassName) const {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  auto scd = re.dumpStorageClass(storageClassName);
  rel.release();
  if (scd.routes.size() != scd.copyCount) {
    throw IncompleteRouting("In OStoreDB::getArchiveRoutes: routes are incomplete");
  }
  std::list<common::archiveRoute::ArchiveRoute> ret;
  for (auto r=scd.routes.begin(); r!=scd.routes.end(); r++) {
    ret.push_back(common::archiveRoute::ArchiveRoute(storageClassName, 
      r->copyNumber, r->tapePool, r->log));
  }
  return ret;
}

std::list<common::archiveRoute::ArchiveRoute> OStoreDB::getArchiveRoutes() const {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  auto scd = re.dumpStorageClasses();
  rel.release();
  std::list<common::archiveRoute::ArchiveRoute> ret;
  for (auto sc=scd.begin(); sc!=scd.end(); sc++) {
    for (auto r=sc->routes.begin(); r!=sc->routes.end(); r++) {
      ret.push_back(common::archiveRoute::ArchiveRoute(sc->storageClass, 
        r->copyNumber, r->tapePool, r->log));
    }
  }
  return ret;
}

void OStoreDB::deleteArchiveRoute(const SecurityIdentity& requester, 
  const std::string& storageClassName, const uint16_t copyNb) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  re.removeArchiveRoute(storageClassName, copyNb);
  re.commit();
}

void OStoreDB::ArchiveToFileRequestCreation::complete() {
  // We inherited all the objects from the creation.
  // Lock is still here at that point.
  // First, record that we are fine for next step.
  m_request.setAllJobsLinkingToTapePool();
  m_request.commit();
  objectstore::RootEntry re(m_objectStore);
  // We can now plug the request onto its tape pools.
  // We can discover at that point that a tape pool is actually not
  // really owned by the root entry, and hence a dangling pointer
  // We should then unlink the jobs from that already connected
  // tape pools and abort the job creation.
  // The list of done tape pools is held here for this purpose
  // Reconstruct the job list
  auto jl = m_request.dumpJobs();
  std::list<std::string> linkedTapePools;
  try {
    for (auto j=jl.begin(); j!=jl.end(); j++) {
      objectstore::TapePool tp(j->tapePoolAddress, m_objectStore);
      ScopedExclusiveLock tpl(tp);
      tp.fetch();
      if (tp.getOwner() != re.getAddressIfSet())
        throw NoSuchTapePool("In OStoreDB::queue: non-existing tape pool found "
            "(dangling pointer): cancelling request creation.");
      tp.addJob(*j, m_request.getAddressIfSet(), m_request.getArchiveFile().path, 
        m_request.getRemoteFile().status.size, m_request.getPriority(),
        m_request.getCreationLog().time);
      // Now that we have the tape pool handy, get the retry limits from it and 
      // assign them to the job
      m_request.setJobFailureLimits(j->copyNb, tp.getMaxRetriesWithinMount(), 
        tp.getMaxTotalRetries());
      tp.commit();
      linkedTapePools.push_back(j->tapePoolAddress);
    }
  } catch (NoSuchTapePool &) {
    // Unlink the request from already connected tape pools
    for (auto tpa=linkedTapePools.begin(); tpa!=linkedTapePools.end(); tpa++) {
      objectstore::TapePool tp(*tpa, m_objectStore);
      ScopedExclusiveLock tpl(tp);
      tp.fetch();
      tp.removeJob(m_request.getAddressIfSet());
      tp.commit();
      m_request.remove();
    }
    throw;
  }
  // The request is now fully set. As it's multi-owned, we do not set the owner,
  // just to disown it from the agent.
  m_request.setOwner("");
  m_request.commit();
  m_lock.release();
  // And remove reference from the agent
  {
    objectstore::ScopedExclusiveLock al(*m_agent);
    m_agent->fetch();
    m_agent->removeFromOwnership(m_request.getAddressIfSet());
    m_agent->commit();
  }
  m_closed=true;
  return;
}

void OStoreDB::ArchiveToFileRequestCreation::cancel() {
  // We inherited everything from the creation, and all we have to
  // do here is to delete the request from storage and dereference it from
  // the agent's entry
  if (m_closed) {
    throw ArchiveRequestAlreadyCompleteOrCanceled(
      "In OStoreDB::ArchiveToFileRequestCreation::cancel: trying the close "
      "the request creation twice");
  }
  m_request.remove();
  {
    objectstore::ScopedExclusiveLock al(*m_agent);
    m_agent->fetch();
    m_agent->removeFromOwnership(m_request.getAddressIfSet());
    m_agent->commit();
  }
  m_closed=true;
  return;
}

OStoreDB::ArchiveToFileRequestCreation::~ArchiveToFileRequestCreation() {
  // We have to determine whether complete() or cancel() were called, in which
  // case there is nothing to do, or not, in which case we have to garbage
  // collect the archive to file request. This will queue it to the appropriate
  // tape pool(s) orphanesArchiveToFileCreations. The schedule will then 
  // determine its fate depending on the status of the NS entry creation
  // (no entry, just cancel, already created in NS, carry on).
  if (m_closed)
    return;
  try {
    m_request.garbageCollect(m_agent->getAddressIfSet());
    {
      objectstore::ScopedExclusiveLock al(*m_agent);
      m_agent->fetch();
      m_agent->removeFromOwnership(m_request.getAddressIfSet());
      m_agent->commit();
    }
    m_closed=true;
  } catch (...) {}
}

void OStoreDB::ArchiveRequestCreation::complete() {
  // We inherited all the objects from the creation.
  // Lock is still here at that point.
  // First, record that we are fine for next step.
  m_request.setAllJobsLinkingToTapePool();
  m_request.commit();
  objectstore::RootEntry re(m_objectStore);
  // We can now plug the request onto its tape pools.
  // We can discover at that point that a tape pool is actually not
  // really owned by the root entry, and hence a dangling pointer
  // We should then unlink the jobs from that already connected
  // tape pools and abort the job creation.
  // The list of done tape pools is held here for this purpose
  // Reconstruct the job list
  auto jl = m_request.dumpJobs();
  std::list<std::string> linkedTapePools;
  try {
    for (auto j=jl.begin(); j!=jl.end(); j++) {
      objectstore::TapePool tp(j->tapePoolAddress, m_objectStore);
      ScopedExclusiveLock tpl(tp);
      tp.fetch();
      if (tp.getOwner() != re.getAddressIfSet())
        throw NoSuchTapePool("In OStoreDB::queue: non-existing tape pool found "
            "(dangling pointer): cancelling request creation.");
      tp.addJob(*j, m_request.getAddressIfSet(), m_request.getDrData().drPath, 
        m_request.getFileSize(), 0, //TODO: fix priorities and mount criteria to come from usergroups
        m_request.getCreationLog().time);
      // Now that we have the tape pool handy, get the retry limits from it and 
      // assign them to the job
      m_request.setJobFailureLimits(j->copyNb, tp.getMaxRetriesWithinMount(), 
        tp.getMaxTotalRetries());
      tp.commit();
      linkedTapePools.push_back(j->tapePoolAddress);
    }
  } catch (NoSuchTapePool &) {
    // Unlink the request from already connected tape pools
    for (auto tpa=linkedTapePools.begin(); tpa!=linkedTapePools.end(); tpa++) {
      objectstore::TapePool tp(*tpa, m_objectStore);
      ScopedExclusiveLock tpl(tp);
      tp.fetch();
      tp.removeJob(m_request.getAddressIfSet());
      tp.commit();
      m_request.remove();
    }
    throw;
  }
  // The request is now fully set. As it's multi-owned, we do not set the owner,
  // just to disown it from the agent.
  m_request.setOwner("");
  m_request.commit();
  m_lock.release();
  // And remove reference from the agent
  {
    objectstore::ScopedExclusiveLock al(*m_agent);
    m_agent->fetch();
    m_agent->removeFromOwnership(m_request.getAddressIfSet());
    m_agent->commit();
  }
  m_closed=true;
  return;
}

void OStoreDB::ArchiveRequestCreation::cancel() {
  // We inherited everything from the creation, and all we have to
  // do here is to delete the request from storage and dereference it from
  // the agent's entry
  if (m_closed) {
    throw ArchiveRequestAlreadyCompleteOrCanceled(
      "In OStoreDB::ArchiveToFileRequestCreation::cancel: trying the close "
      "the request creation twice");
  }
  m_request.remove();
  {
    objectstore::ScopedExclusiveLock al(*m_agent);
    m_agent->fetch();
    m_agent->removeFromOwnership(m_request.getAddressIfSet());
    m_agent->commit();
  }
  m_closed=true;
  return;
}

OStoreDB::ArchiveRequestCreation::~ArchiveRequestCreation() {
  // We have to determine whether complete() or cancel() were called, in which
  // case there is nothing to do, or not, in which case we have to garbage
  // collect the archive to file request. This will queue it to the appropriate
  // tape pool(s) orphanesArchiveToFileCreations. The schedule will then 
  // determine its fate depending on the status of the NS entry creation
  // (no entry, just cancel, already created in NS, carry on).
  if (m_closed)
    return;
  try {
    m_request.garbageCollect(m_agent->getAddressIfSet());
    {
      objectstore::ScopedExclusiveLock al(*m_agent);
      m_agent->fetch();
      m_agent->removeFromOwnership(m_request.getAddressIfSet());
      m_agent->commit();
    }
    m_closed=true;
  } catch (...) {}
}

void OStoreDB::createTapePool(const std::string& name,
  const uint32_t nbPartialTapes, const cta::CreationLog &creationLog) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  assertAgentSet();
  re.addOrGetTapePoolAndCommit(name, nbPartialTapes, 5, 5, *m_agent, creationLog);
  re.commit();
  }

void OStoreDB::setTapePoolMountCriteria(const std::string& tapePool,
  const MountCriteriaByDirection& mountCriteriaByDirection) {
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  objectstore::TapePool tp(re.getTapePoolAddress(tapePool),m_objectStore);
  rel.release();
  ScopedExclusiveLock tplock(tp);
  tp.fetch();
  tp.setMountCriteriaByDirection(mountCriteriaByDirection);
  tp.commit();
}


std::list<cta::TapePool> OStoreDB::getTapePools() const {
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto tpd = re.dumpTapePools();
  rel.release();
  std::list<cta::TapePool> ret;
  for (auto tp=tpd.begin(); tp!=tpd.end(); tp++) {
    ret.push_back(cta::TapePool(tp->tapePool, tp->nbPartialTapes,
      tp->mountCriteriaByDirection, tp->log));
  }
  return ret;
}

void OStoreDB::deleteTapePool(const SecurityIdentity& requester, 
  const std::string& name) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  re.removeTapePoolAndCommit(name);
  re.commit();
}

void OStoreDB::createTape(const std::string& vid, 
  const std::string& logicalLibraryName, 
  const std::string& tapePoolName, const uint64_t capacityInBytes,
  const cta::CreationLog& creationLog) {
  // To create a tape, we have to
  // - Find the storage class and lock for write.
  // - Create the tape object.
  // - Connect the tape object to the tape pool.
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  // Check the library exists
  auto libs = re.dumpLibraries();
  for (auto l=libs.begin(); l!=libs.end(); l++) {
    if (l->library == logicalLibraryName)
      goto found;
  }
  throw NoSuchLibrary("In OStoreDB::createTape: trying to create a tape in a non-existing library");
  found:
  std::string tpAddress = re.getTapePoolAddress(tapePoolName);
  // Take hold of the tape pool
  objectstore::TapePool tp(tpAddress, m_objectStore);
  ScopedExclusiveLock tpl(tp);
  tp.fetch();
  // Check the tape pool is owned by the root entry. If not, it should be
  // considered as a dangling pointer.
  if (tp.getOwner() != re.getAddressIfSet())
    throw NoSuchTapePool("In OStoreDB::createTape: trying to create a tape in a"
      " non-existing tape pool (dangling pointer)");
  // Check that the tape exists and throw an exception if it does.
  // TODO: we should check in all tape pools (or have a central index)
  try {
    tp.getTapeAddress(vid);
    throw TapeAlreadyExists("In OStoreDB::createTape: trying to create an existing tape.");
  } catch (cta::exception::Exception &) {}
  // Create the tape. The tape pool method takes care of the gory details for us.
  tp.addOrGetTapeAndCommit(vid, logicalLibraryName, capacityInBytes, *m_agent, creationLog);
  tp.commit();
}

cta::Tape OStoreDB::getTape(const std::string &vid) const {
  // Got through all tape pools. Get the list of them
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto tpl = re.dumpTapePools();
  for (auto tpi=tpl.begin(); tpi!=tpl.end(); tpi++) {
    objectstore::TapePool tp(tpi->address, m_objectStore);
    ScopedSharedLock tpl(tp);
    tp.fetch();
    // Check the tape pool is owned by the root entry. If not, it should be
    // considered as a dangling pointer (and skip it)
    if (tp.getOwner() != re.getAddressIfSet())
      continue;
    auto tl=tp.dumpTapesAndFetchStatus();
    for (auto ti=tl.begin(); ti!=tl.end(); ti++) {
      if (vid == ti->vid) {
        objectstore::Tape t(ti->address, m_objectStore);
        ScopedSharedLock tl(t);
        t.fetch();
        const uint64_t nbFiles = 0; // TO BE DONE
        cta::Tape::Status status;
        return cta::Tape(ti->vid, nbFiles, ti->logicalLibraryName,
          tpi->tapePool, ti->capacityInBytes, t.getStoredData(), ti->log, status);
      }
    }
  }
  throw NoSuchTape("In OStoreDB::getTape: No such tape");
}

std::list<cta::Tape> OStoreDB::getTapes() const {
  std::list<cta::Tape> ret;
  // Got through all tape pools. Get the list of them
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto tpl = re.dumpTapePools();
  for (auto tpi=tpl.begin(); tpi!=tpl.end(); tpi++) {
    objectstore::TapePool tp(tpi->address, m_objectStore);
    ScopedSharedLock tpl(tp);
    tp.fetch();
    // Check the tape pool is owned by the root entry. If not, it should be
    // considered as a dangling pointer (and skip it)
    if (tp.getOwner() != re.getAddressIfSet())
      continue;
    auto tl=tp.dumpTapesAndFetchStatus();
    for (auto ti=tl.begin(); ti!=tl.end(); ti++) {
      objectstore::Tape t(ti->address, m_objectStore);
      ScopedSharedLock tl(t);
      t.fetch();
      const uint64_t nbFiles = 0; // TO BE DONE
      ret.push_back(cta::Tape(ti->vid, nbFiles, ti->logicalLibraryName,
        tpi->tapePool, ti->capacityInBytes, t.getStoredData(), ti->log,
        ti->status));
    }
  }
  return ret;
}

void OStoreDB::deleteTape(const SecurityIdentity& requester, const std::string& vid) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  // Check we are not deleting a non-empty library
  auto tapePoolList = re.dumpTapePools();
  for (auto tpi=tapePoolList.begin(); tpi!=tapePoolList.end(); tpi++) {
    objectstore::TapePool tp(tpi->address, m_objectStore);
    ScopedExclusiveLock tpl(tp);
    tp.fetch();
    // Check the tape pool is owned by the root entry. If not, it should be
    // considered as a dangling pointer (and skip it)
    if (tp.getOwner() != re.getAddressIfSet())
      continue;
    auto tl=tp.dumpTapesAndFetchStatus();
    for (auto ti=tl.begin(); ti!=tl.end(); ti++) {
      if(ti->vid==vid) {
        tp.removeTapeAndCommit(vid);
        return;
      }
    }
  }
  throw exception::Exception("Cannot delete the tape: it does not exist in any tape pool");
}

void OStoreDB::createLogicalLibrary(const std::string& name, 
  const cta::CreationLog& creationLog) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  re.addLibrary(name, creationLog);
  re.commit();
}

std::list<LogicalLibrary> OStoreDB::getLogicalLibraries() const {
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto ll = re.dumpLibraries();
  rel.release();
  std::list<LogicalLibrary> ret;
  for (auto l=ll.begin(); l!=ll.end(); l++) {
    ret.push_back(LogicalLibrary(l->library, l->log));
  }
  return ret;
}

void OStoreDB::deleteLogicalLibrary(const SecurityIdentity& requester, 
  const std::string& name) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  // Check we are not deleting a non-empty library
  auto tpl = re.dumpTapePools();
  for (auto tpp=tpl.begin(); tpp!=tpl.end(); tpp++) {
    objectstore::TapePool tp(tpp->address, m_objectStore);
    ScopedSharedLock tplock(tp);
    tp.fetch();
    // Check the tape pool is owned by the root entry. If not, it should be
    // considered as a dangling pointer.
    if (tp.getOwner() != re.getAddressIfSet())
      continue;
    auto tl=tp.dumpTapesAndFetchStatus();
    for (auto t=tl.begin(); t!=tl.end(); t++) {
      if (t->logicalLibraryName == name)
        throw LibraryInUse("In OStoreDB::deleteLogicalLibrary: trying to delete a library used by a tape.");
    }
  }
  re.removeLibrary(name);
  re.commit();
}

std::unique_ptr<cta::SchedulerDatabase::ArchiveToFileRequestCreation>
  OStoreDB::queue(const cta::ArchiveToFileRequest& rqst) {
  assertAgentSet();
  // Construct the return value immediately
  std::unique_ptr<cta::OStoreDB::ArchiveToFileRequestCreation> 
    internalRet(new cta::OStoreDB::ArchiveToFileRequestCreation(m_agent, m_objectStore));
  cta::objectstore::ArchiveToFileRequest & atfr = internalRet->m_request;
  atfr.setAddress(m_agent->nextId("ArchiveToFileRequest"));
  atfr.initialize();
  atfr.setArchiveFile(rqst.archiveFile);
  atfr.setRemoteFile(rqst.remoteFile);
  atfr.setPriority(rqst.priority);
  atfr.setCreationLog(rqst.creationLog);
  // We will need to identity tapepools is order to construct the request
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto & cl = rqst.copyNbToPoolMap;
  std::list<cta::objectstore::ArchiveToFileRequest::JobDump> jl;
  for (auto copy=cl.begin(); copy != cl.end(); copy++) {
    std::string tpaddr = re.getTapePoolAddress(copy->second);
    atfr.addJob(copy->first, copy->second, tpaddr);
    jl.push_back(cta::objectstore::ArchiveToFileRequest::JobDump());
    jl.back().copyNb = copy->first;
    jl.back().tapePool = copy->second;
    jl.back().tapePoolAddress = tpaddr;
  }
  if (!jl.size()) {
    throw ArchiveRequestHasNoCopies("In OStoreDB::queue: the archive to file request has no copy");
  }
  // We create the object here
  {
    objectstore::ScopedExclusiveLock al(*m_agent);
    m_agent->fetch();
    m_agent->addToOwnership(atfr.getAddressIfSet());
    m_agent->commit();
  }
  atfr.setOwner(m_agent->getAddressIfSet());
  atfr.insert();
  internalRet->m_lock.lock(atfr);  
  // We successfully prepared the object. It will remain attached to the agent 
  // entry for the time being and get plugged to the tape pools on a second
  // pass. 
  // TODO: this can be improved by passing an opaque set of data to the called
  // (including the lock) in order to optimise the acesses to the object store.
  // In the mean time, the step 2 of this insertion will be done by finding the
  // archiveRequest from the agent's owned object. Crude, but should not be too
  // bad as the agent is not supposed to own many objects in this place.
  std::unique_ptr<cta::SchedulerDatabase::ArchiveToFileRequestCreation> ret;
  ret.reset(internalRet.release());
  return ret;
}

std::unique_ptr<cta::SchedulerDatabase::ArchiveRequestCreation> OStoreDB::queue(const cta::common::dataStructures::ArchiveRequest &request, 
        const uint64_t archiveFileId, const std::map<uint64_t, std::string> &copyNbToPoolMap, const cta::common::dataStructures::MountPolicy &mountPolicy) {
  assertAgentSet();
  // Construct the return value immediately
  std::unique_ptr<cta::OStoreDB::ArchiveRequestCreation> internalRet(new cta::OStoreDB::ArchiveRequestCreation(m_agent, m_objectStore));
  cta::objectstore::ArchiveRequest & ar = internalRet->m_request;
  ar.setAddress(m_agent->nextId("ArchiveRequest"));
  ar.initialize();
  ar.setArchiveFileID(archiveFileId);
  ar.setChecksumType(request.checksumType);
  ar.setChecksumValue(request.checksumValue);
  ar.setCreationLog(request.creationLog);
  ar.setDiskpoolName(request.diskpoolName);
  ar.setDiskpoolThroughput(request.diskpoolThroughput);
  ar.setDrData(request.drData);
  ar.setEosFileID(request.eosFileID);
  ar.setFileSize(request.fileSize);
  ar.setMountPolicy(mountPolicy);
  ar.setRequester(request.requester);
  ar.setSrcURL(request.srcURL);
  ar.setStorageClass(request.storageClass);
  // We will need to identity tapepools is order to construct the request
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto & cl = copyNbToPoolMap;
  std::list<cta::objectstore::ArchiveRequest::JobDump> jl;
  for (auto copy=cl.begin(); copy != cl.end(); copy++) {
    std::string tpaddr = re.getTapePoolAddress(copy->second);
    ar.addJob(copy->first, copy->second, tpaddr);
    jl.push_back(cta::objectstore::ArchiveRequest::JobDump());
    jl.back().copyNb = copy->first;
    jl.back().tapePool = copy->second;
    jl.back().tapePoolAddress = tpaddr;
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
  internalRet->m_lock.lock(ar);  
  // We successfully prepared the object. It will remain attached to the agent 
  // entry for the time being and get plugged to the tape pools on a second
  // pass. 
  // TODO: this can be improved by passing an opaque set of data to the called
  // (including the lock) in order to optimise the acesses to the object store.
  // In the mean time, the step 2 of this insertion will be done by finding the
  // archiveRequest from the agent's owned object. Crude, but should not be too
  // bad as the agent is not supposed to own many objects in this place.
  std::unique_ptr<cta::SchedulerDatabase::ArchiveRequestCreation> ret;
  ret.reset(internalRet.release());
  return ret;
}

void OStoreDB::deleteArchiveRequest(const SecurityIdentity& requester, 
  const std::string& archiveFile) {
  // First of, find the archive request form all the tape pools.
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto tpl = re.dumpTapePools();
  rel.release();
  for (auto tpp=tpl.begin(); tpp!= tpl.end(); tpp++) {
    objectstore::TapePool tp(tpp->address, m_objectStore);
    ScopedSharedLock tplock(tp);
    tp.fetch();
    auto ajl=tp.dumpJobs();
    tplock.release();
    for (auto ajp=ajl.begin(); ajp!=ajl.end(); ajp++) {
      objectstore::ArchiveToFileRequest atfr(ajp->address, m_objectStore);
      ScopedSharedLock atfrl(atfr);
      atfr.fetch();
      if (atfr.getArchiveFile().path == archiveFile) {
        atfrl.release();
        objectstore::ScopedExclusiveLock al(*m_agent);
        m_agent->fetch();
        m_agent->addToOwnership(atfr.getAddressIfSet());
        m_agent->commit();
        ScopedExclusiveLock atfrxl(atfr);
        atfr.fetch();
        atfr.setAllJobsFailed();
        atfr.setOwner(m_agent->getAddressIfSet());
        atfr.commit();
        auto jl = atfr.dumpJobs();
        for (auto j=jl.begin(); j!=jl.end(); j++) {
          try {
            objectstore::TapePool tp(j->tapePoolAddress, m_objectStore);
            ScopedExclusiveLock tpl(tp);
            tp.fetch();
            tp.removeJob(atfr.getAddressIfSet());
            tp.commit();
          } catch (...) {}
        }
        atfr.remove();
        m_agent->removeFromOwnership(atfr.getAddressIfSet());
        m_agent->commit();
      }
    }
  }
  throw NoSuchArchiveRequest("In OStoreDB::deleteArchiveRequest: ArchiveToFileRequest not found");
}

std::unique_ptr<SchedulerDatabase::ArchiveToFileRequestCancelation>
  OStoreDB::markArchiveRequestForDeletion(const SecurityIdentity& requester,
  const std::string& archiveFile) {
  assertAgentSet();
  // Construct the return value immediately
  std::unique_ptr<cta::OStoreDB::ArchiveToFileRequestCancelation>
    internalRet(new cta::OStoreDB::ArchiveToFileRequestCancelation(m_agent, m_objectStore));
  cta::objectstore::ArchiveToFileRequest & atfr = internalRet->m_request;
  cta::objectstore::ScopedExclusiveLock & atfrl = internalRet->m_lock;
  // Attempt to find the request
  objectstore::RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto tpl=re.dumpTapePools();
  rel.release();
  for (auto tpp=tpl.begin(); tpp!=tpl.end(); tpp++) {
    try {
      objectstore::TapePool tp(tpp->address, m_objectStore);
      ScopedSharedLock tpl(tp);
      tp.fetch();
      auto arl = tp.dumpJobs();
      tpl.release();
      for (auto arp=arl.begin(); arp!=arl.end(); arp++) {
        objectstore::ArchiveToFileRequest tatfr(arp->address, m_objectStore);
        objectstore::ScopedSharedLock tatfrl(tatfr);
        tatfr.fetch();
        if (tatfr.getArchiveFile().path == archiveFile) {
          // Point the agent to the request
          ScopedExclusiveLock agl(*m_agent);
          m_agent->fetch();
          m_agent->addToOwnership(arp->address);
          m_agent->commit();
          agl.release();
          // Mark all jobs are being pending NS deletion (for being deleted them selves) 
          tatfrl.release();
          atfr.setAddress(arp->address);
          atfrl.lock(atfr);
          atfr.fetch();
          atfr.setAllJobsPendingNSdeletion();
          atfr.commit();
          // Unlink the jobs from the tape pools (it is safely referenced in the agent)
          auto atpl=atfr.dumpJobs();
          for (auto atpp=atpl.begin(); atpp!=atpl.end(); atpp++) {
            objectstore::TapePool atp(atpp->tapePoolAddress, m_objectStore);
            objectstore::ScopedExclusiveLock atpl(atp);
            atp.fetch();
            atp.removeJob(arp->address);
            atp.commit();
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



std::map<cta::TapePool, std::list<ArchiveToTapeCopyRequest> >
  OStoreDB::getArchiveRequests() const {
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  std::map<cta::TapePool, std::list<ArchiveToTapeCopyRequest> > ret;
  auto tpl = re.dumpTapePools();
  rel.release();
  for (auto tpp=tpl.begin(); tpp!=tpl.end(); tpp++) {
    objectstore::TapePool ostp(tpp->address, m_objectStore);
    ScopedSharedLock ostpl(ostp);
    ostp.fetch();
    cta::TapePool tp(tpp->tapePool, tpp->nbPartialTapes,
      ostp.getMountCriteriaByDirection(), tpp->log);    
    auto arl = ostp.dumpJobs();
    ostpl.release();
    for (auto ar=arl.begin(); ar!=arl.end(); ar++) {
      objectstore::ArchiveToFileRequest osar(ar->address, m_objectStore);
      ScopedSharedLock osarl(osar);
      osar.fetch();
      // Find which copy number is for this tape pool.
      // skip the request if not found
      auto jl = osar.dumpJobs();
      uint16_t copynb;
      bool copyndFound=false;
      for (auto j=jl.begin(); j!=jl.end(); j++) {
        if (j->tapePool == tpp->tapePool) {
          copynb = j->copyNb;
          copyndFound = true;
          break;
        }
      }
      if (!copyndFound) continue;
      ret[tp].push_back(cta::ArchiveToTapeCopyRequest(
        osar.getRemoteFile(),
        osar.getArchiveFile().path,
        copynb,
        tpp->tapePool,
        osar.getPriority(),
        osar.getCreationLog()));
    }
  }
  return ret;
}

std::list<ArchiveToTapeCopyRequest>
  OStoreDB::getArchiveRequests(const std::string& tapePoolName) const {
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto tpl = re.dumpTapePools();
  rel.release();
  for (auto tpp=tpl.begin(); tpp!=tpl.end(); tpp++) {
    if (tpp->tapePool != tapePoolName) continue;
    std::list<ArchiveToTapeCopyRequest> ret;
    objectstore::TapePool ostp(tpp->address, m_objectStore);
    ScopedSharedLock ostpl(ostp);
    ostp.fetch();
    cta::TapePool tp(tpp->tapePool, tpp->nbPartialTapes, 
      ostp.getMountCriteriaByDirection(), tpp->log);
    auto arl = ostp.dumpJobs();
    ostpl.release();
    for (auto ar=arl.begin(); ar!=arl.end(); ar++) {
      objectstore::ArchiveToFileRequest osar(ar->address, m_objectStore);
      ScopedSharedLock osarl(osar);
      osar.fetch();
      // Find which copy number is for this tape pool.
      // skip the request if not found
      auto jl = osar.dumpJobs();
      uint16_t copynb;
      bool copyndFound=false;
      for (auto j=jl.begin(); j!=jl.end(); j++) {
        if (j->tapePool == tpp->tapePool) {
          copynb = j->copyNb;
          copyndFound = true;
          break;
        }
      }
      if (!copyndFound) continue;
      ret.push_back(cta::ArchiveToTapeCopyRequest(
        osar.getRemoteFile(),
        osar.getArchiveFile().path,
        copynb,
        tpp->tapePool,
        osar.getPriority(),
        osar.getCreationLog()));
    }
    return ret;
  }
  throw NoSuchTapePool("In OStoreDB::getArchiveRequests: tape pool not found");
}

void OStoreDB::queue(const cta::RetrieveToFileRequest& rqst) {
  assertAgentSet();
  // Check at least one potential tape copy is provided.
  // In order to post the job, construct it first.
  objectstore::RetrieveToFileRequest rtfr(m_agent->nextId("RetrieveToFileRequest"), m_objectStore);
  rtfr.initialize();
  rtfr.setArchiveFile(rqst.getArchiveFile());
  rtfr.setRemoteFile(rqst.getRemoteFile());
  rtfr.setPriority(rqst.priority);
  rtfr.setCreationLog(rqst.creationLog);
  rtfr.setSize(rqst.getArchiveFile().size);
  // We will need to identity tapes is order to construct the request.
  // First load all the tapes information in a memory map
  std::map<std::string, std::string> vidToAddress;
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto tapePools = re.dumpTapePools();
  for(auto pool=tapePools.begin(); pool!=tapePools.end(); pool++) {
    objectstore::TapePool tp(pool->address, m_objectStore);
    objectstore::ScopedSharedLock tpl(tp);
    tp.fetch();
    auto tapes = tp.dumpTapesAndFetchStatus();
    for(auto tape=tapes.begin(); tape!=tapes.end(); tape++)
      vidToAddress[tape->vid] = tape->address;
  }
  // Now add all the candidate tape copies to the request. With validation
  for (auto tc=rqst.getTapeCopies().begin(); tc!=rqst.getTapeCopies().end(); tc++) {
    // Check the tape copy copynumber (range = [1 - copyCount] )
    if (tc->copyNb > rqst.getTapeCopies().size() || tc->copyNb < 1) {
      throw TapeCopyNumberOutOfRange("In OStoreDB::queue(RetrieveToFile): copy number out of range");
    }
  }
  // Add all the tape copies to the request
  try {
    for (auto tc=rqst.getTapeCopies().begin(); tc!=rqst.getTapeCopies().end(); tc++) {
      rtfr.addJob(*tc, vidToAddress.at(tc->vid));
    }
  } catch (std::out_of_range &) {
    throw NoSuchTape("In OStoreDB::queue(RetrieveToFile): tape not found");
  }
  // We now need to select the tape from which we will migrate next. This should
  // be the tape with the most jobs already queued.
  // TODO: this will have to look at tape statuses on the long run as well
  uint16_t selectedCopyNumber;
  uint64_t bestTapeQueuedBytes;
  std::string selectedVid;
  uint64_t selectedFseq;
  uint64_t selectedBlockid;
  {
    // First tape copy is always better than nothing. 
    auto tc=rqst.getTapeCopies().begin();
    selectedCopyNumber = tc->copyNb;
    selectedVid = tc->vid;
    selectedFseq = tc->fSeq;
    selectedBlockid = tc->blockId;
    // Get info for the tape.
    {
      objectstore::Tape t(vidToAddress.at(tc->vid), m_objectStore);
      objectstore::ScopedSharedLock tl(t);
      t.fetch();
      bestTapeQueuedBytes = t.getJobsSummary().bytes;
    }
    tc++;
    // Compare with the next ones
    for (;tc!=rqst.getTapeCopies().end(); tc++) {
      objectstore::Tape t(vidToAddress.at(tc->vid), m_objectStore);
      objectstore::ScopedSharedLock tl(t);
      t.fetch();
      if (t.getJobsSummary().bytes > bestTapeQueuedBytes) {
        bestTapeQueuedBytes = t.getJobsSummary().bytes;
        selectedCopyNumber = tc->copyNb;
        selectedVid = tc->vid;
        selectedFseq = tc->fSeq;
        selectedBlockid = tc->blockId;
      }
    }
  }
  // We now can enqueue the request on this most promising tape.
  {
    objectstore::Tape tp(vidToAddress.at(selectedVid), m_objectStore);
    ScopedExclusiveLock tpl(tp);
    tp.fetch();
    objectstore::RetrieveToFileRequest::JobDump jd;
    jd.copyNb = selectedCopyNumber;
    jd.tape = selectedVid;
    jd.tapeAddress = vidToAddress.at(selectedVid);
    jd.fseq = selectedFseq;
    jd.blockid = selectedBlockid;
    tp.addJob(jd, rtfr.getAddressIfSet(), rqst.getArchiveFile().size, rqst.priority, rqst.creationLog.time);
    tp.commit();
  }
  // The request is now fully set. It belongs to the tape.
  rtfr.setOwner(vidToAddress.at(selectedVid));
  rtfr.insert();
  // And remove reference from the agent
  {
    objectstore::ScopedExclusiveLock al(*m_agent);
    m_agent->fetch();
    m_agent->removeFromOwnership(rtfr.getAddressIfSet());
    m_agent->commit();
  }
}

std::list<RetrieveRequestDump> OStoreDB::getRetrieveRequests(const std::string& vid) const {
  throw exception::Exception("Not Implemented");
}

std::map<cta::Tape, std::list<RetrieveRequestDump> > OStoreDB::getRetrieveRequests() const {
  std::map<cta::Tape, std::list<RetrieveRequestDump> > ret;
  // Get list of tape pools and then tapes
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto tpl=re.dumpTapePools();
  rel.release();
  for (auto tpp = tpl.begin(); tpp != tpl.end(); tpp++) {
    // Get the list of tapes for the tape pool
    objectstore::TapePool tp(tpp->address, m_objectStore);
    objectstore::ScopedSharedLock tplock(tp);
    tp.fetch();
    auto tl = tp.dumpTapes();
    for (auto tptr = tl.begin(); tptr!= tl.end(); tptr++) {
      // Get the list of retrieve requests for the tape.
      objectstore::Tape t(tptr->address, m_objectStore);
      objectstore::ScopedSharedLock tlock(t);
      t.fetch();
      auto jobs = t.dumpAndFetchRetrieveRequests();
      // If the list is not empty, add to the map.
      if (jobs.size()) {
        cta::Tape tkey;
        // TODO tkey.capacityInBytes;
        tkey.creationLog = t.getCreationLog();
        // TODO tkey.dataOnTapeInBytes;
        tkey.logicalLibraryName = t.getLogicalLibrary();
        tkey.nbFiles = t.getLastFseq();
        // TODO tkey.status
        tkey.tapePoolName = tp.getName();
        tkey.vid = t.getVid();
        ret[tkey] = std::move(jobs);
      }
    }
  }
  return ret;
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
  // In order to create the mount, we have to:
  // Check we actually hold the scheduling lock
  // Check the tape exists, add it to ownership and set its activity status to 
  // busy, with the current agent pointing to it for unbusying
  // Set the drive status to up, but do not commit anything to the drive register
  // the drive register does not need garbage collection as it should reflect the
  // latest known state of the drive (and its absence of updating if needed)
  // Prepare the return value
  std::unique_ptr<OStoreDB::ArchiveMount> privateRet(
    new OStoreDB::ArchiveMount(m_objectStore, m_agent));
  auto &am = *privateRet;
  // Check we hold the scheduling lock
  if (!m_lockTaken)
    throw SchedulingLockNotHeld("In OStoreDB::TapeMountDecisionInfo::createArchiveMount: "
      "cannot create mount without holding scheduling lock");
  // Find the tape and update it
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto tplist = re.dumpTapePools();
  auto driveRegisterAddress = re.getDriveRegisterAddress();
  rel.release();
  {
    std::string tpAdress;
    for (auto tpp=tplist.begin(); tpp!=tplist.end(); tpp++)
      if (tpp->tapePool == tapePool)
        tpAdress = tpp->address;
    if (!tpAdress.size())
      throw NoSuchTapePool("In OStoreDB::TapeMountDecisionInfo::createArchiveMount:"
        " tape pool not found");
    objectstore::TapePool tp(tpAdress, m_objectStore);
    objectstore::ScopedSharedLock tpl(tp);
    tp.fetch();
    auto tlist = tp.dumpTapesAndFetchStatus();
    std::string tAddress;
    for (auto tptr = tlist.begin(); tptr!=tlist.end(); tptr++) {
      if (tptr->vid == vid)
        tAddress = tptr->address;
    }
    if (!tAddress.size())
      throw NoSuchTape("In OStoreDB::TapeMountDecisionInfo::createArchiveMount:"
        " tape not found");
    objectstore::Tape t(tAddress, m_objectStore);
    objectstore::ScopedExclusiveLock tlock(t);
    t.fetch();
    if (t.isFull())
      throw TapeNotWritable("In OStoreDB::TapeMountDecisionInfo::createArchiveMount:"
        " the tape is not writable (full)");
    if (t.isArchived())
      throw TapeNotWritable("In OStoreDB::TapeMountDecisionInfo::createArchiveMount:"
        " the tape is not writable (archived)");
    if (t.isReadOnly())
      throw TapeNotWritable("In OStoreDB::TapeMountDecisionInfo::createArchiveMount:"
        " the tape is not writable (readonly)");
    if (t.isDisabled())
      throw TapeNotWritable("In OStoreDB::TapeMountDecisionInfo::createArchiveMount:"
        " the tape is not writable (disabled)");
    if (t.isBusy())
      throw TapeIsBusy("In OStoreDB::TapeMountDecisionInfo::createArchiveMount:"
        " the tape is busy");
    // This tape seems fine for our purposes. We will set it as an owned object
    // so that garbage collection can unbusy the tape in case of a session crash
    {
      objectstore::ScopedExclusiveLock al(m_agent);
      m_agent.fetch();
      m_agent.addToOwnership(t.getAddressIfSet());
      m_agent.commit();
    }
    am.nbFilesCurrentlyOnTape = t.getLastFseq();
    am.mountInfo.vid = t.getVid();
    t.setBusy(driveName, objectstore::Tape::MountType::Archive, hostName, startTime, 
      m_agent.getAddressIfSet());
    t.commit();
  }
  // Fill up the mount info
  am.mountInfo.drive = driveName;
  am.mountInfo.mountId = m_schedulerGlobalLock->getIncreaseCommitMountId();
  m_schedulerGlobalLock->commit();
  am.mountInfo.tapePool = tapePool;
  am.mountInfo.tapePool = tapePool;
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
    dr.reportDriveStatus(driveName, logicalLibrary, 
      cta::common::DriveStatus::Starting, startTime, 
      cta::MountType::ARCHIVE, privateRet->mountInfo.mountId,
      0, 0, 0, vid, tapePool);
    dr.commit();
  }
  // We committed the scheduling decision. We can now release the scheduling lock.
  m_lockOnSchedulerGlobalLock.release();
  m_lockTaken = false;
  // We can now return the mount session object to the user.
  std::unique_ptr<SchedulerDatabase::ArchiveMount> ret(privateRet.release());
  return ret;
}

OStoreDB::TapeMountDecisionInfo::TapeMountDecisionInfo(
  objectstore::Backend& os, objectstore::Agent& a):
   m_lockTaken(false), m_objectStore(os), m_agent(a) {}


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
    new OStoreDB::RetrieveMount(m_objectStore, m_agent));
  auto &rm = *privateRet;
  // Check we hold the scheduling lock
  if (!m_lockTaken)
    throw SchedulingLockNotHeld("In OStoreDB::TapeMountDecisionInfo::createRetrieveMount: "
      "cannot create mount without holding scheduling lock");
  // Find the tape and update it
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto tplist = re.dumpTapePools();
  auto driveRegisterAddress = re.getDriveRegisterAddress();
  rel.release();
  {
    std::string tpAdress;
    for (auto tpp=tplist.begin(); tpp!=tplist.end(); tpp++)
      if (tpp->tapePool == tapePool)
        tpAdress = tpp->address;
    if (!tpAdress.size())
      throw NoSuchTapePool("In OStoreDB::TapeMountDecisionInfo::createRetrieveMount:"
        " tape pool not found");\
    objectstore::TapePool tp(tpAdress, m_objectStore);
    objectstore::ScopedSharedLock tpl(tp);
    tp.fetch();
    auto tlist = tp.dumpTapesAndFetchStatus();
    std::string tAddress;
    for (auto tptr = tlist.begin(); tptr!=tlist.end(); tptr++) {
      if (tptr->vid == vid)
        tAddress = tptr->address;
    }
    if (!tAddress.size())
      throw NoSuchTape("In OStoreDB::TapeMountDecisionInfo::createRetrieveMount:"
        " tape not found");
    objectstore::Tape t(tAddress, m_objectStore);
    objectstore::ScopedExclusiveLock tlock(t);
    t.fetch();
    if (t.isArchived())
      throw TapeNotWritable("In OStoreDB::TapeMountDecisionInfo::createRetrieveMount:"
        " the tape is not readable (archived)");
    if (t.isDisabled())
      throw TapeNotWritable("In OStoreDB::TapeMountDecisionInfo::createRetrieveMount:"
        " the tape is not readable (disabled)");
    if (t.isBusy())
      throw TapeIsBusy("In OStoreDB::TapeMountDecisionInfo::createRetrieveMount:"
        " the tape is busy");
    // This tape seems fine for our purposes. We will set it as an owned object
    // so that garbage collection can unbusy the tape in case of a session crash
    {
      objectstore::ScopedExclusiveLock al(m_agent);
      m_agent.fetch();
      m_agent.addToOwnership(t.getAddressIfSet());
      m_agent.commit();
    }
    t.setBusy(driveName, objectstore::Tape::MountType::Archive, hostName, startTime, 
      m_agent.getAddressIfSet());
    t.commit();
  }
  // Fill up the mount info
  rm.mountInfo.vid = vid;
  rm.mountInfo.drive = driveName;
  rm.mountInfo.logicalLibrary = logicalLibrary;
  rm.mountInfo.mountId = m_schedulerGlobalLock->getIncreaseCommitMountId();
  rm.mountInfo.tapePool = tapePool;
  // Update the status of the drive in the registry
  {
    // Get hold of the drive registry
    objectstore::DriveRegister dr(driveRegisterAddress, m_objectStore);
    objectstore::ScopedExclusiveLock drl(dr);
    dr.fetch();
    // The drive is already in-session, to prevent double scheduling before it 
    // goes to mount state. If the work to be done gets depleted in the mean time,
    // we will switch back to up.
    dr.reportDriveStatus(driveName, logicalLibrary, 
      cta::common::DriveStatus::Starting, startTime, 
      cta::MountType::RETRIEVE, privateRet->mountInfo.mountId,
      0, 0, 0, vid, tapePool);
    dr.commit();
  }
  // We committed the scheduling decision. We can now release the scheduling lock.
  m_lockOnSchedulerGlobalLock.release();
  m_lockTaken = false;
  // We can now return the mount session object to the user.
  std::unique_ptr<SchedulerDatabase::RetrieveMount> ret(privateRet.release());
  return ret;
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
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto tpl = re.dumpTapePools();
  rel.release();
  std::string tpAddress;
  for (auto tpp = tpl.begin(); tpp != tpl.end(); tpp++) {
    if (tpp->tapePool == mountInfo.tapePool)
      tpAddress = tpp->address;
  }
  if (!tpAddress.size())
    throw NoSuchTapePool("In OStoreDB::ArchiveMount::getNextJob(): tape pool not found");
  objectstore::TapePool tp(tpAddress, m_objectStore);
  objectstore::ScopedExclusiveLock tplock(tp);
  tp.fetch();
  // Loop until we find a job which is actually belonging to the tape pool
  while (tp.dumpJobs().size()) {
    // Get the tape pool's jobs list, and pop the first
    auto jl = tp.dumpJobs();
    // First take a lock on and download the job
    // If the request is not around anymore, we will just move the the next
    // Prepare the return value
    std::unique_ptr<OStoreDB::ArchiveJob> privateRet(new OStoreDB::ArchiveJob(
      jl.front().address, m_objectStore, m_agent, *this));
    privateRet->m_copyNb = jl.front().copyNb;
    objectstore::ScopedExclusiveLock atfrl;
    try {
      atfrl.lock(privateRet->m_atfr);
      privateRet->m_atfr.fetch();
    } catch (cta::exception::Exception &) {
      // we failed to access the object. It might be missing.
      // Just pop this job from the pool and move to the next.
      tp.removeJob(privateRet->m_atfr.getAddressIfSet());
      // Commit in case we do not pass by again.
      tp.commit();
      continue;
    }
    // Take ownership of the job
    // Add to ownership
    objectstore::ScopedExclusiveLock al(m_agent);
    m_agent.fetch();
    m_agent.addToOwnership(privateRet->m_atfr.getAddressIfSet());
    m_agent.commit();
    al.release();
    // Make the ownership official (for this job within the request)
    privateRet->m_atfr.setJobOwner(privateRet->m_copyNb, m_agent.getAddressIfSet());
    privateRet->m_atfr.commit();
    // Remove the job from the tape pool queue
    tp.removeJob(privateRet->m_atfr.getAddressIfSet());
    // We can commit and release the tape pool lock, we will only fill up
    // memory structure from here on.
    tp.commit();
    privateRet->archiveFile = privateRet->m_atfr.getArchiveFile();
    privateRet->remoteFile = privateRet->m_atfr.getRemoteFile();
    privateRet->nameServerTapeFile.tapeFileLocation.fSeq = ++nbFilesCurrentlyOnTape;
    privateRet->nameServerTapeFile.tapeFileLocation.copyNb = privateRet->m_copyNb;
    privateRet->nameServerTapeFile.tapeFileLocation.vid = mountInfo.vid;
    privateRet->nameServerTapeFile.tapeFileLocation.blockId =
      std::numeric_limits<decltype(privateRet->nameServerTapeFile.tapeFileLocation.blockId)>::max();
    privateRet->m_jobOwned = true;
    privateRet->m_mountId = mountInfo.mountId;
    privateRet->m_tapePool = mountInfo.tapePool;
    return std::unique_ptr<SchedulerDatabase::ArchiveJob> (privateRet.release());
  }
  return std::unique_ptr<SchedulerDatabase::ArchiveJob>();
}

void OStoreDB::ArchiveMount::complete(time_t completionTime) {
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
  dr.reportDriveStatus(mountInfo.drive, mountInfo.logicalLibrary, 
    cta::common::DriveStatus::Up, completionTime, 
    cta::MountType::NONE, 0,
    0, 0, 0, "", "");
  dr.commit();
  // Find the tape and unbusy it.
  objectstore::TapePool tp (re.getTapePoolAddress(mountInfo.tapePool), m_objectStore);
  rel.release();
  objectstore::ScopedSharedLock tpl(tp);
  tp.fetch();
  objectstore::Tape t(tp.getTapeAddress(mountInfo.vid), m_objectStore);
  objectstore::ScopedExclusiveLock tl(t);
  tpl.release();
  t.fetch();
  t.releaseBusy();
  t.commit();
  objectstore::ScopedExclusiveLock agl(m_agent);
  m_agent.fetch();
  m_agent.removeFromOwnership(t.getAddressIfSet());
  m_agent.commit();
}

OStoreDB::ArchiveJob::ArchiveJob(const std::string& jobAddress, 
  objectstore::Backend& os, objectstore::Agent& ag, ArchiveMount & am): m_jobOwned(false),
  m_objectStore(os), m_agent(ag), m_atfr(jobAddress, os), m_archiveMount(am) {}

OStoreDB::RetrieveMount::RetrieveMount(objectstore::Backend& os, objectstore::Agent& a):
  m_objectStore(os), m_agent(a) { }

const OStoreDB::RetrieveMount::MountInfo& OStoreDB::RetrieveMount::getMountInfo() {
  return mountInfo;
}

auto OStoreDB::RetrieveMount::getNextJob() -> std::unique_ptr<SchedulerDatabase::RetrieveJob> {
  // Find the next file to retrieve
  // Get the tape pool and then tape
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto tpl = re.dumpTapePools();
  rel.release();
  std::string tpAddress;
  for (auto tpp = tpl.begin(); tpp != tpl.end(); tpp++) {
    if (tpp->tapePool == mountInfo.tapePool)
      tpAddress = tpp->address;
  }
  if (!tpAddress.size())
    throw NoSuchTapePool("In OStoreDB::RetrieveMount::getNextJob(): tape pool not found");
  objectstore::TapePool tp(tpAddress, m_objectStore);
  objectstore::ScopedSharedLock tplock(tp);
  tp.fetch();
  auto tl = tp.dumpTapes();
  tplock.release();
  std::string tAddress;
  for (auto tptr = tl.begin(); tptr != tl.end(); tptr++) {
    if (tptr->vid == mountInfo.vid)
      tAddress = tptr->address;
  }
  if (!tAddress.size())
    throw NoSuchTape("In OStoreDB::RetrieveMount::getNextJob(): tape not found");
  objectstore::Tape t(tAddress, m_objectStore);
  objectstore::ScopedExclusiveLock tlock(t);
  t.fetch();
  while (t.dumpJobs().size()) {
    // Get the tape pool's jobs list, and pop the first
    auto jl=t.dumpJobs();
    // First take a lock on and download the job
    // If the request is not around anymore, we will just move the the next
    // Prepare the return value
    std::unique_ptr<OStoreDB::RetrieveJob> privateRet(new OStoreDB::RetrieveJob(
      jl.front().address, m_objectStore, m_agent));
    privateRet->m_copyNb = jl.front().copyNb;
    objectstore::ScopedExclusiveLock rtfrl;
    try {
      rtfrl.lock(privateRet->m_rtfr);
      privateRet->m_rtfr.fetch();
    } catch (cta::exception::Exception &) {
      // we failed to access the object. It might be missing.
      // Just pop this job from the pool and move to the next.
      t.removeJob(privateRet->m_rtfr.getAddressIfSet());
      // Commit in case we do not pass by again.
      t.commit();
      continue;
    }
    // Take ownership of the job
    // Add to ownership
    objectstore::ScopedExclusiveLock al(m_agent);
    m_agent.fetch();
    m_agent.addToOwnership(privateRet->m_rtfr.getAddressIfSet());
    m_agent.commit();
    al.release();
    // Make the ownership official (for the whole request in retrieves)
    privateRet->m_rtfr.setOwner(m_agent.getAddressIfSet());
    privateRet->m_rtfr.commit();
    // Remove the job from the tape pool queue
    t.removeJob(privateRet->m_rtfr.getAddressIfSet());
    // We can commit and release the tape pool lock, we will only fill up
    // memory structure from here on.
    t.commit();
    privateRet->archiveFile = privateRet->m_rtfr.getArchiveFile();
    privateRet->remoteFile = privateRet->m_rtfr.getRemoteFile();
    objectstore::RetrieveToFileRequest::JobDump jobDump = privateRet->m_rtfr.getJob(privateRet->m_copyNb);
    privateRet->nameServerTapeFile.tapeFileLocation.fSeq = jobDump.fseq;
    privateRet->nameServerTapeFile.tapeFileLocation.blockId = jobDump.blockid;
    privateRet->nameServerTapeFile.tapeFileLocation.copyNb = privateRet->m_copyNb;
    privateRet->nameServerTapeFile.tapeFileLocation.vid = mountInfo.vid;

      std::numeric_limits<decltype(privateRet->nameServerTapeFile.tapeFileLocation.blockId)>::max();
    privateRet->m_jobOwned = true;
    return std::unique_ptr<SchedulerDatabase::RetrieveJob> (privateRet.release());
  }
  return std::unique_ptr<SchedulerDatabase::RetrieveJob>();
}

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
  dr.reportDriveStatus(mountInfo.drive, mountInfo.logicalLibrary, 
    cta::common::DriveStatus::Up, completionTime, 
    cta::MountType::NONE, 0,
    0, 0, 0, "", "");
  dr.commit();
  // Find the tape and unbusy it.
  objectstore::TapePool tp (re.getTapePoolAddress(mountInfo.tapePool), m_objectStore);
  rel.release();
  objectstore::ScopedSharedLock tpl(tp);
  tp.fetch();
  objectstore::Tape t(tp.getTapeAddress(mountInfo.vid), m_objectStore);
  objectstore::ScopedExclusiveLock tl(t);
  tpl.release();
  t.fetch();
  t.releaseBusy();
  t.commit();
  objectstore::ScopedExclusiveLock agl(m_agent);
  m_agent.fetch();
  m_agent.removeFromOwnership(t.getAddressIfSet());
  m_agent.commit();
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

void OStoreDB::ArchiveMount::setTapeMaxFileCount(uint64_t maxFileId) {
  // Update the max file count for the tape
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  // Find the tape and update it.
  objectstore::TapePool tp (re.getTapePoolAddress(mountInfo.tapePool), m_objectStore);
  rel.release();
  objectstore::ScopedSharedLock tpl(tp);
  tp.fetch();
  objectstore::Tape t(tp.getTapeAddress(mountInfo.vid), m_objectStore);
  objectstore::ScopedExclusiveLock tl(t);
  tpl.release();
  t.fetch();
  // We should never set the max file to the same value or lower (it should)
  // always strictly go up.
  if (t.getLastFseq() >= maxFileId) {
    throw MaxFSeqNotGoingUp("In OStoreDB::ArchiveMount::setTapeMaxFileCount: trying to set maxFSeq to lower or equal value to before");
  }
  t.setLastFseq(maxFileId);
  t.releaseBusy();
  t.commit();
}


void OStoreDB::ArchiveJob::fail() {
  if (!m_jobOwned)
    throw JobNowOwned("In OStoreDB::ArchiveJob::fail: cannot fail a job not owned");
  // Lock the archive request. Fail the job.
  objectstore::ScopedExclusiveLock atfrl(m_atfr);
  m_atfr.fetch();
  // Add a job failure. If the job is failed, we will delete it.
  if (m_atfr.addJobFailure(m_copyNb, m_mountId)) {
    // The job will not be retried. Either another jobs for the same request is 
    // queued and keeps the request referenced or the request has been deleted.
    // In any case, we can forget it.
    objectstore::ScopedExclusiveLock al(m_agent);
    m_agent.fetch();
    m_agent.removeFromOwnership(m_atfr.getAddressIfSet());
    m_agent.commit();
    m_jobOwned = false;
    return;
  }
  // The job still has a chance, return it to its original tape pool's queue
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto tpl = re.dumpTapePools();
  rel.release();
  for (auto tpp=tpl.begin(); tpp!=tpl.end(); tpp++) {
    if (tpp->tapePool == m_tapePool) {
      objectstore::TapePool tp(tpp->address, m_objectStore);
      objectstore::ScopedExclusiveLock tplock(tp);
      tp.fetch();
      // Find the right job
      auto jl = m_atfr.dumpJobs();
      for (auto j=jl.begin(); j!=jl.end(); j++) {
        if (j->copyNb == m_copyNb) {
          tp.addJobIfNecessary(*j, m_atfr.getAddressIfSet(), m_atfr.getArchiveFile().path, m_atfr.getRemoteFile().status.size);
          tp.commit();
          tplock.release();
          // We have a pointer to the job, we can change the job ownership
          m_atfr.setJobOwner(m_copyNb, tpp->address);
          m_atfr.commit();
          atfrl.release();
          // We just have to remove the ownership from the agent and we're done.
          objectstore::ScopedExclusiveLock al(m_agent);
          m_agent.fetch();
          m_agent.removeFromOwnership(m_atfr.getAddressIfSet());
          m_agent.commit();
          m_jobOwned = false;
          return;
        }
      }
      throw NoSuchJob("In OStoreDB::ArchiveJob::fail(): could not find the job in the request object");
    }
  }
  throw NoSuchTapePool("In OStoreDB::ArchiveJob::fail(): could not find the tape pool");
  }

void OStoreDB::ArchiveJob::bumpUpTapeFileCount(uint64_t newFileCount) {
  m_archiveMount.setTapeMaxFileCount(newFileCount);
}


void OStoreDB::ArchiveJob::succeed() {
  // Lock the request and set the job as successful.
  objectstore::ScopedExclusiveLock atfrl(m_atfr);
  m_atfr.fetch();
  std::string atfrAddress = m_atfr.getAddressIfSet();
  if (m_atfr.setJobSuccessful(m_copyNb)) {
    m_atfr.remove();
  } else {
    m_atfr.commit();
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
    objectstore::ScopedExclusiveLock atfrl(m_atfr);
    m_atfr.fetch();
    m_atfr.garbageCollect(m_agent.getAddressIfSet());
    atfrl.release();
    // Remove ownership from agent
    objectstore::ScopedExclusiveLock al(m_agent);
    m_agent.fetch();
    m_agent.removeFromOwnership(m_atfr.getAddressIfSet());
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
  if (m_jobOwned) {
    // Re-queue the job entirely if we failed to handle it.
    try {
      // We now need to select the tape from which we will migrate next. This should
      // be the tape with the most jobs already queued.
      // TODO: this will have to look at tape statuses on the long run as well
      uint16_t selectedCopyNumber;
      uint64_t bestTapeQueuedBytes;
      std::string selectedVid;
      std::string selectedTapeAddress;
      objectstore::ScopedExclusiveLock rtfrl(m_rtfr);
      m_rtfr.fetch();
      auto jl=m_rtfr.dumpJobs();
      {
        // First tape copy is always better than nothing. 
        auto tc=jl.begin();
        selectedCopyNumber = tc->copyNb;
        selectedVid = tc->tape;
        selectedTapeAddress = tc->tapeAddress;
        // Get info for the tape.
        {
          objectstore::Tape t(tc->tapeAddress, m_objectStore);
          objectstore::ScopedSharedLock tl(t);
          t.fetch();
          bestTapeQueuedBytes = t.getJobsSummary().bytes;
        }
        tc++;
        // Compare with the next ones
        for (;tc!=jl.end(); tc++) {
          objectstore::Tape t(tc->tapeAddress, m_objectStore);
          objectstore::ScopedSharedLock tl(t);
          t.fetch();
          if (t.getJobsSummary().bytes > bestTapeQueuedBytes) {
            bestTapeQueuedBytes = t.getJobsSummary().bytes;
            selectedCopyNumber = tc->copyNb;
            selectedVid = tc->tape;
            selectedTapeAddress = tc->tapeAddress;
          }
        }
      }
      // We now can enqueue the request on this most promising tape.
      {
        objectstore::Tape tp(selectedTapeAddress, m_objectStore);
        ScopedExclusiveLock tpl(tp);
        tp.fetch();
        objectstore::RetrieveToFileRequest::JobDump jd;
        jd.copyNb = selectedCopyNumber;
        jd.tape = selectedVid;
        jd.tapeAddress = selectedTapeAddress;
        tp.addJob(jd, m_rtfr.getAddressIfSet(), m_rtfr.getSize(), m_rtfr.getPriority(), m_rtfr.getCreationLog().time);
        tp.commit();
      }
      // The request is now fully set. It belongs to the tape.
      std::string previousOwner = m_rtfr.getOwner();
      m_rtfr.setOwner(selectedTapeAddress);
      m_rtfr.commit();
      // And remove reference from the agent (if it was owned by an agent)
      try {
        if (!previousOwner.size())
          return;
        objectstore::Agent agent(previousOwner, m_objectStore);
        objectstore::ScopedExclusiveLock al(agent);
        agent.fetch();
        agent.removeFromOwnership(m_rtfr.getAddressIfSet());
        agent.commit();
      } catch (...) {}
    } catch (...) {}
  }
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
