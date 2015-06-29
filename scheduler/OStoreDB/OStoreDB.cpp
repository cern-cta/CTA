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
#include "scheduler/SecurityIdentity.hpp"
#include "objectstore/RootEntry.hpp"
#include "objectstore/TapePool.hpp"
#include "objectstore/Tape.hpp"
#include "objectstore/ArchiveToFileRequest.hpp"
#include "objectstore/RetrieveToFileRequest.hpp"
#include "common/exception/Exception.hpp"
#include "scheduler/AdminHost.hpp"
#include "scheduler/AdminUser.hpp"
#include "scheduler/ArchivalRoute.hpp"
#include "scheduler/ArchiveRequest.hpp"
#include "scheduler/ArchiveToFileRequest.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/RetrieveToFileRequest.hpp"
#include "scheduler/StorageClass.hpp"
#include "scheduler/TapePool.hpp"
#include "scheduler/Tape.hpp"
#include "ArchiveToDirRequest.hpp"
#include "RetrieveToFileRequest.hpp"
#include "TapeCopyLocation.hpp"
#include "RetrieveToDirRequest.hpp"
#include <algorithm>
#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */

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



void OStoreDB::createAdminHost(const SecurityIdentity& requester,
    const std::string& hostName, const std::string& comment) {
  RootEntry re(m_objectStore);
  objectstore::CreationLog cl(requester.getUser(), requester.getHost(), 
    time(NULL), comment);
  ScopedExclusiveLock rel(re);
  re.fetch();
  re.addAdminHost(hostName, cl);
  re.commit();
}

std::list<AdminHost> OStoreDB::getAdminHosts() const {
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  std::list<AdminHost> ret;
  auto hl=re.dumpAdminHosts();
  rel.release();
  for (auto h=hl.begin(); h!=hl.end(); h++) {
    ret.push_back(AdminHost(h->hostname, 
        cta::UserIdentity(h->log.user.uid, h->log.user.gid),
        h->log.comment, h->log.time));
  }
  return ret;
}


void OStoreDB::deleteAdminHost(const SecurityIdentity& requester, 
  const std::string& hostName) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  re.removeAdminHost(hostName);
  re.commit();
}

void OStoreDB::createAdminUser(const SecurityIdentity& requester, 
  const cta::UserIdentity& user, const std::string& comment) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  objectstore::CreationLog cl(requester.getUser(), requester.getHost(),
    time(NULL), comment);
  re.addAdminUser(objectstore::UserIdentity(user.uid, user.gid), cl);
  re.commit();
}

void OStoreDB::deleteAdminUser(const SecurityIdentity& requester, 
  const cta::UserIdentity& user) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  re.removeAdminUser(objectstore::UserIdentity(user.uid, user.gid));
  re.commit();
}

std::list<AdminUser> OStoreDB::getAdminUsers() const {
  std::list<AdminUser> ret;
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto aul = re.dumpAdminUsers();
  for (auto au=aul.begin(); au!=aul.end(); au++) {
    ret.push_back(
      AdminUser(
        cta::UserIdentity(au->user.uid, au->user.gid),
        cta::UserIdentity(au->log.user.uid, au->log.user.gid),
        au->log.comment,
        au->log.time
    ));
  }
  rel.release();
  return ret;
}

void OStoreDB::assertIsAdminOnAdminHost(const SecurityIdentity& id) const {
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  if (!re.isAdminUser(objectstore::UserIdentity(id.getUser().uid,
      id.getUser().gid))) {
    std::ostringstream msg;
    msg << "User uid=" << id.getUser().uid 
        << " gid=" << id.getUser().gid
        << " is not an administrator";
    throw exception::Exception(msg.str());
  }
  if (!re.isAdminHost(id.getHost())) {
    std::ostringstream msg;
    msg << "Host " << id.getHost() << " is not an administration host";
    throw exception::Exception(msg.str());
  }
}

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

void OStoreDB::createArchivalRoute(const std::string& storageClassName,
  const uint16_t copyNb, const std::string& tapePoolName,
  const cta::CreationLog& creationLog) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  re.addArchivalRoute(storageClassName, copyNb, tapePoolName, 
    objectstore::CreationLog(creationLog));
  re.commit();
}

std::list<ArchivalRoute> 
  OStoreDB::getArchivalRoutes(const std::string& storageClassName) const {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  auto scd = re.dumpStorageClass(storageClassName);
  rel.release();
  if (scd.routes.size() != scd.copyCount) {
    throw IncompleteRouting("In OStoreDB::getArchivalRoutes: routes are incomplete");
  }
  std::list<ArchivalRoute> ret;
  for (auto r=scd.routes.begin(); r!=scd.routes.end(); r++) {
    ret.push_back(ArchivalRoute(storageClassName, 
      r->copyNumber, r->tapePool, r->log));
  }
  return ret;
}

std::list<ArchivalRoute> OStoreDB::getArchivalRoutes() const {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  auto scd = re.dumpStorageClasses();
  rel.release();
  std::list<ArchivalRoute> ret;
  for (auto sc=scd.begin(); sc!=scd.end(); sc++) {
    for (auto r=sc->routes.begin(); r!=sc->routes.end(); r++) {
      ret.push_back(ArchivalRoute(sc->storageClass, 
        r->copyNumber, r->tapePool, r->log));
    }
  }
  return ret;
}

void OStoreDB::deleteArchivalRoute(const SecurityIdentity& requester, 
  const std::string& storageClassName, const uint16_t copyNb) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  re.removeArchivalRoute(storageClassName, copyNb);
  re.commit();
}

void OStoreDB::fileEntryCreatedInNS(const SecurityIdentity& requester,
  const std::string &archiveFile) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
} 

void OStoreDB::fileEntryDeletedFromNS(const SecurityIdentity& requester,
  const std::string &archiveFile) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

void OStoreDB::createTapePool(const std::string& name,
  const uint32_t nbPartialTapes, const cta::CreationLog &creationLog) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  assertAgentSet();
  re.addOrGetTapePoolAndCommit(name, nbPartialTapes, *m_agent, creationLog);
  re.commit();
}

std::list<cta::TapePool> OStoreDB::getTapePools() const {
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto tpd = re.dumpTapePools();
  rel.release();
  std::list<cta::TapePool> ret;
  for (auto tp=tpd.begin(); tp!=tpd.end(); tp++) {
    ret.push_back(cta::TapePool(tp->tapePool, tp->nbPartialTapes, tp->log));
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
  // Check that the tape exists and throw an exception if it does.
  try {
    tp.getTapeAddress(vid);
    throw TapeAlreadyExists("In OStoreDB::createTape: trying to create an existing tape.");
  } catch (cta::exception::Exception &) {}
  // Create the tape. The tape pool method takes care of the gory details for us.
  tp.addOrGetTapeAndCommit(vid, logicalLibraryName, capacityInBytes, 
      *m_agent, creationLog);
  tp.commit();
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
    auto tl=tp.dumpTapes();
    for (auto ti=tl.begin(); ti!=tl.end(); ti++) {
      objectstore::Tape t(ti->address, m_objectStore);
      ScopedSharedLock tl(t);
      t.fetch();
      ret.push_back(cta::Tape(ti->vid, ti->logicalLibraryName, tpi->tapePool,
        ti->capacityInBytes, t.getStoredData(), ti->log));
    }
  }
  return ret;
}

void OStoreDB::deleteTape(const SecurityIdentity& requester, 
  const std::string& vid) {
  throw exception::Exception("Not Implemented");
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
  // Check we are not deleting a non-empty library
  auto tpl = re.dumpTapePools();
  for (auto tpp=tpl.begin(); tpp!=tpl.end(); tpp++) {
    objectstore::TapePool tp(tpp->address, m_objectStore);
    ScopedSharedLock tplock(tp);
    tp.fetch();
    auto tl=tp.dumpTapes();
    for (auto t=tl.begin(); t!=tl.end(); t++) {
      if (t->logicalLibraryName == name)
        throw LibraryInUse("In OStoreDB::deleteLogicalLibrary: trying to delete a library used by a tape.");
    }
  }
  re.fetch();
  re.removeLibrary(name);
  re.commit();
}

void OStoreDB::queue(const cta::ArchiveToFileRequest& rqst) {
  assertAgentSet();
  // In order to post the job, construct it first.
  objectstore::ArchiveToFileRequest atfr(m_agent->nextId("ArchiveToFileRequest"), m_objectStore);
  atfr.initialize();
  atfr.setArchiveFile(rqst.getArchiveFile());
  atfr.setRemoteFile(rqst.getRemoteFile());
  atfr.setPriority(rqst.getPriority());
  atfr.setCreationLog(rqst.getCreationLog());
  // We will need to identity tapepools is order to construct the request
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto & cl = rqst.getCopyNbToPoolMap();
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
  // We successfully prepared the object. Time to create it and plug it to
  // the tree (it will first be referenced by the agent)
  {
    objectstore::ScopedExclusiveLock al(*m_agent);
    m_agent->fetch();
    m_agent->addToOwnership(atfr.getAddressIfSet());
    m_agent->commit();
  }
  atfr.setOwner(m_agent->getAddressIfSet());
  atfr.insert();
  ScopedExclusiveLock atfrl(atfr);
  // We can now plug the request onto its tape pools
  for (auto j=jl.begin(); j!=jl.end(); j++) {
    objectstore::TapePool tp(j->tapePoolAddress, m_objectStore);
    ScopedExclusiveLock tpl(tp);
    tp.fetch();
    tp.addJob(*j, atfr.getAddressIfSet(), atfr.getSize());
    tp.commit();
  }
  // The request is now fully set. As it's multi-owned, we do not set the owner
  atfr.setOwner("");
  atfr.commit();
  // And remove reference from the agent
  {
    objectstore::ScopedExclusiveLock al(*m_agent);
    m_agent->fetch();
    m_agent->removeFromOwnership(atfr.getAddressIfSet());
    m_agent->commit();
  }
}

void OStoreDB::queue(const ArchiveToDirRequest& rqst) {
  auto & archiveToFileRequests = rqst.getArchiveToFileRequests();
  for(auto req=archiveToFileRequests.begin(); req!=archiveToFileRequests.end(); req++) {
    queue(*req);
  }
}

void OStoreDB::deleteArchiveRequest(const SecurityIdentity& requester, 
  const std::string& archiveFile) {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::markArchiveRequestForDeletion(const SecurityIdentity& requester,
  const std::string& archiveFile) {
  throw exception::Exception("Not Implemented");
}

std::map<cta::TapePool, std::list<ArchiveToTapeCopyRequest> >
  OStoreDB::getArchiveRequests() const {
  throw exception::Exception("Not Implemented");
}

std::list<ArchiveToTapeCopyRequest>
  OStoreDB::getArchiveRequests(const std::string& tapePoolName) const {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::queue(const cta::RetrieveToFileRequest& rqst) {
  assertAgentSet();
  // In order to post the job, construct it first.
  objectstore::RetrieveToFileRequest rtfr(m_agent->nextId("RetrieveToFileRequest"), m_objectStore);
  rtfr.initialize();
  rtfr.setArchiveFile(rqst.getArchiveFile());
  rtfr.setRemoteFile(rqst.getRemoteFile());
  rtfr.setPriority(rqst.getPriority());
  rtfr.setCreationLog(rqst.getCreationLog());
  // We will need to identity tapes is order to construct the request
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto & tc = rqst.getTapeCopies();
  auto numberOfCopies = tc.size();
  srand (time(NULL));
  auto chosenCopyNumber = rand() % numberOfCopies;
  auto it = tc.begin();
  std::advance(it, chosenCopyNumber);
  auto tapePools = re.dumpTapePools();
  std::string tapeAddress = "";
  for(auto pool=tapePools.begin(); pool!=tapePools.end(); pool++) {
    objectstore::TapePool tp(pool->address, m_objectStore);
    auto tapes = tp.dumpTapes();
    for(auto tape=tapes.begin(); tape!=tapes.end(); tape++) {
      if(tape->vid==it->getVid()) {
        tapeAddress = tape->address;
        break;
      }
    }
  }
  rtfr.addJob(chosenCopyNumber, it->getVid(), tapeAddress);
  auto jl = rtfr.dumpJobs();
  if (!jl.size()) {
    throw RetrieveRequestHasNoCopies("In OStoreDB::queue: the retrieve to file request has no copies");
  }
  // We successfully prepared the object. Time to create it and plug it to
  // the tree.
  rtfr.setOwner(m_agent->getAddressIfSet());
  rtfr.insert();
  ScopedExclusiveLock rtfrl(rtfr);
  // We can now plug the request onto its tape
  for (auto j=jl.begin(); j!=jl.end(); j++) {
    objectstore::Tape tp(j->tapeAddress, m_objectStore);
    ScopedExclusiveLock tpl(tp);
    tp.fetch();
    tp.addJob(*j, rtfr.getAddressIfSet(), rtfr.getSize());
    tp.commit();
  }
  // The request is now fully set. As it's multi-owned, we do not set the owner
  rtfr.setOwner("");
  rtfr.commit();
  // And remove reference from the agent
  {
    objectstore::ScopedExclusiveLock al(*m_agent);
    m_agent->fetch();
    m_agent->removeFromOwnership(rtfr.getAddressIfSet());
    m_agent->commit();
  }
}

void OStoreDB::queue(const RetrieveToDirRequest& rqst) {
  auto & retrieveToFileRequests = rqst.getRetrieveToFileRequests();
  for(auto req=retrieveToFileRequests.begin(); req!=retrieveToFileRequests.end(); req++) {
    queue(*req);
  }
}

std::list<RetrieveFromTapeCopyRequest> OStoreDB::getRetrieveRequests(const std::string& vid) const {
  throw exception::Exception("Not Implemented");
}

std::map<cta::Tape, std::list<RetrieveFromTapeCopyRequest> > OStoreDB::getRetrieveRequests() const {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::deleteRetrieveRequest(const SecurityIdentity& requester, 
  const std::string& remoteFile) {
  throw exception::Exception("Not Implemented");
}


    
}
