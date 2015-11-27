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

#include "RootEntry.hpp"
#include "AgentRegister.hpp"
#include "Agent.hpp"
#include "TapePool.hpp"
#include "DriveRegister.hpp"
#include "SchedulerGlobalLock.hpp"
#include <cxxabi.h>
#include "ProtocolBuffersAlgorithms.hpp"

// construtor, when the backend store exists.
// Checks the existence and correctness of the root entry
cta::objectstore::RootEntry::RootEntry(Backend & os):
  ObjectOps<serializers::RootEntry>(os, "root") {}

// Initialiser. This uses the base object's initialiser and sets the defaults 
// of payload.
void cta::objectstore::RootEntry::initialize() {
  ObjectOps<serializers::RootEntry>::initialize();
  // There is nothing to do for the payload.
  m_payloadInterpreted = true;
}

bool cta::objectstore::RootEntry::isEmpty() {
  checkPayloadReadable();
  if (m_payload.storageclasses_size())
    return false;
  if (m_payload.tapepoolpointers_size())
    return false;
  if (m_payload.has_driveregisterpointer() &&
      m_payload.driveregisterpointer().address().size())
    return false;
  if (m_payload.agentregisterintent().size())
    return false;
  if (m_payload.has_agentregisterpointer() &&
      m_payload.agentregisterpointer().address().size())
    return false;
  if (m_payload.has_schedulerlockpointer() &&
      m_payload.schedulerlockpointer().address().size())
    return false;
  return true;
}

void cta::objectstore::RootEntry::removeIfEmpty() {
  checkPayloadWritable();
  if (!isEmpty()) {
    throw NotEmpty("In RootEntry::removeIfEmpty(): root entry not empty");
  }
  remove();
}



// =============================================================================
// ================ Admin Hosts manipulations ==================================
// =============================================================================

// This operator will be used in the following usage of the templated
// findElement and removeOccurences
namespace {
  bool operator==(const std::string & hostName, 
    const cta::objectstore::serializers::AdminHost & ah) {
    return ah.hostname() == hostName;
  }
}

void cta::objectstore::RootEntry::addAdminHost(const std::string& hostname,
  const CreationLog& log) {
  checkPayloadWritable();
  // Check that the host is not listed already
  try {
    serializers::findElement(m_payload.adminhosts(), hostname);
    throw DuplicateEntry("In RootEntry::addAdminHost: entry already exists");
  } catch (serializers::NotFound &) {}
  // Add the host
  auto * ah = m_payload.add_adminhosts();
  ah->set_hostname(hostname);
  log.serialize(*ah->mutable_log());
}

void cta::objectstore::RootEntry::removeAdminHost(const std::string & hostname) {
  checkPayloadWritable();
  if (!serializers::isElementPresent(m_payload.adminhosts(), hostname))
    throw NoSuchAdminHost(
      "In RootEntry::removeAdminHost: trying to delete non-existing admin host.");
  serializers::removeOccurences(m_payload.mutable_adminhosts(), hostname);
}

bool cta::objectstore::RootEntry::isAdminHost(const std::string& hostname) {
  checkPayloadReadable();
  return serializers::isElementPresent(m_payload.adminhosts(), hostname);
}

auto cta::objectstore::RootEntry::dumpAdminHosts() -> std::list<AdminHostDump> {
  checkPayloadReadable();
  std::list<AdminHostDump> ret;
  auto &list=m_payload.adminhosts();
  for (auto i=list.begin();i!=list.end(); i++) {
    ret.push_back(AdminHostDump());
    ret.back().hostname = i->hostname();
    ret.back().log.deserialize(i->log());
  }
  return ret;
}

// =============================================================================
// ================ Admin Users manipulations ==================================
// =============================================================================

// This operator will be used in the following usage of the templated
// findElement and removeOccurences
namespace {
  bool operator==(const cta::objectstore::UserIdentity & user, 
    const cta::objectstore::serializers::AdminUser & au) {
    return au.user().uid() == user.uid;
  }
}

void cta::objectstore::RootEntry::addAdminUser(const UserIdentity& user,
  const CreationLog& log) {
  checkPayloadWritable();
  // Check that the user does not exists already
  try {
    serializers::findElement(m_payload.adminusers(), user);
    throw DuplicateEntry("In RootEntry::addAdminUser: entry already exists");
  } catch (serializers::NotFound &) {}
  serializers::AdminUser * au = m_payload.add_adminusers();
  user.serialize(*au->mutable_user());
  log.serialize(*au->mutable_log());
}

void cta::objectstore::RootEntry::removeAdminUser(const UserIdentity& user) {
  checkPayloadWritable();
  if (!serializers::isElementPresent(m_payload.adminusers(), user))
    throw NoSuchAdminUser(
      "In RootEntry::removeAdminUser: trying to delete non-existing admin user.");
  serializers::removeOccurences(m_payload.mutable_adminusers(), user);
}

bool cta::objectstore::RootEntry::isAdminUser(const UserIdentity& user) {
  checkPayloadReadable();
  return serializers::isElementPresent(m_payload.adminusers(), user);
}

std::list<cta::objectstore::RootEntry::AdminUserDump> 
cta::objectstore::RootEntry::dumpAdminUsers() {
  checkPayloadReadable();
  std::list<cta::objectstore::RootEntry::AdminUserDump> ret;
  auto &list=m_payload.adminusers();
  for (auto i=list.begin(); i != list.end(); i++) {
    ret.push_back(AdminUserDump());
    ret.back().log.deserialize(i->log());
    ret.back().user.deserialize(i->user());
  }
  return ret;
}

// =============================================================================
// ========== Storage Class and archive routes manipulations ===================
// =============================================================================

// This operator will be used in the following usage of the templated
// findElement and removeOccurences
namespace {
  bool operator==(const std::string & scName, 
    const cta::objectstore::serializers::StorageClass & ssc) {
    return ssc.name() == scName;
  }
}

void cta::objectstore::RootEntry::checkStorageClassCopyCount(uint16_t copyCount) {
  // We cannot have a copy count of 0
  if (copyCount < 1) {
    throw InvalidCopyNumber(
      "In RootEntry::checkStorageClassCopyCount: copyCount cannot be less than 1");
  }
  if (copyCount > maxCopyCount) {
    std::stringstream err;
    err << "In RootEntry::checkStorageClassCopyCount: copyCount cannot be bigger than "
        << maxCopyCount;
    throw InvalidCopyNumber(err.str());
  }
}

void cta::objectstore::RootEntry::addStorageClass(const std::string storageClass, 
    uint16_t copyCount, const CreationLog & log) {
  checkPayloadWritable();
  // Check the storage class does not exist already
  try {
    serializers::findElement(m_payload.storageclasses(), storageClass);
    throw DuplicateEntry("In RootEntry::addStorageClass: trying to create duplicate entry");
  } catch (serializers::NotFound &) {}
  // Check the copy count is valid
  checkStorageClassCopyCount(copyCount);
  // Insert the storage class
  auto * sc = m_payload.mutable_storageclasses()->Add();
  sc->set_name(storageClass);
  sc->set_copycount(copyCount);
  log.serialize(*sc->mutable_log());
}

void cta::objectstore::RootEntry::removeStorageClass(const std::string storageClass) {
  checkPayloadWritable();
  if (!serializers::isElementPresent(m_payload.storageclasses(), storageClass))
    throw NoSuchStorageClass(
      "In RootEntry::removeStorageClass: trying to delete non-existing storage class.");
  if (serializers::findElement(m_payload.storageclasses(), storageClass).routes_size()) {
    throw StorageClassHasActiveRoutes(
      "In RootEntry::removeStorageClass: trying to delete storage class with active routes.");
  }
  serializers::removeOccurences(m_payload.mutable_storageclasses(), storageClass);
}

void cta::objectstore::RootEntry::setStorageClassCopyCount(
  const std::string& storageClass, uint16_t copyCount, const CreationLog & log ) {
  checkPayloadWritable();
  auto & sc = serializers::findElement(m_payload.mutable_storageclasses(), storageClass);
  // If we reduce the number of routes, we have to remove the extra ones.
  if (copyCount < sc.copycount()) {
    for (size_t i = copyCount+1; i<sc.copycount()+1; i++) {
      serializers::removeOccurences(sc.mutable_routes(), i);
    }
  }
  // update the creation log and set the count
  sc.set_copycount(copyCount);
}

uint16_t cta::objectstore::RootEntry::getStorageClassCopyCount (
  const std::string & storageClass) {
  checkPayloadReadable();
  auto & sc = serializers::findElement(m_payload.storageclasses(), storageClass);
  return sc.copycount();
}

// This operator will be used in the following usage of the findElement
// removeOccurences
namespace {
  bool operator==(uint16_t copyNb, 
    const cta::objectstore::serializers::ArchiveRoute & ar) {
    return ar.copynb() == copyNb;
  }
}

void cta::objectstore::RootEntry::addArchiveRoute(const std::string& storageClass,
  uint16_t copyNb, const std::string& tapePool, const CreationLog& cl) {
  checkPayloadWritable();
  // Find the storageClass entry
  if (copyNb > maxCopyCount || copyNb <= 0) {
    std::stringstream ss;
    ss << "In RootEntry::addArchiveRoute: invalid copy number: "  << 
        copyNb <<  " > " << maxCopyCount;
    throw InvalidCopyNumber(ss.str());
  }
  auto & sc = serializers::findElement(m_payload.mutable_storageclasses(), storageClass);
  if (copyNb > sc.copycount() || copyNb <= 0) {
    std::stringstream ss;
    ss << "In RootEntry::addArchiveRoute: copy number out of range: " <<
        copyNb << " >= " << sc.copycount();
    throw InvalidCopyNumber(ss.str());
  }
  // Find the archive route (if it exists)
  try {
    // It does: update is not allowed.
    auto &ar = serializers::findElement(sc.mutable_routes(), copyNb);
    throw ArchiveRouteAlreadyExists("In RootEntry::addArchiveRoute: route already exists");
    // Sanity check: is it the right route?
    if (ar.copynb() != copyNb) {
      throw exception::Exception(
        "In RootEntry::addArchiveRoute: internal error: extracted wrong route");
    }
    throw ArchiveRouteAlreadyExists("In RootEntry::addArchiveRoute: trying to add an existing route");
    cl.serialize(*ar.mutable_log());
    ar.set_tapepool(tapePool);
  } catch (serializers::NotFound &) {
    // The route is not present yet. Add it if we do not have the same tape pool
    // for 2 routes
    auto & routes = sc.routes();
    for (auto r=routes.begin(); r != routes.end(); r++) {
      if (r->tapepool() == tapePool) {
        throw TapePoolUsedInOtherRoute ("In RootEntry::addArchiveRoute: cannot add a second route to the same tape pool");
      }
    }
    auto *ar = sc.mutable_routes()->Add();
    cl.serialize(*ar->mutable_log());
    ar->set_copynb(copyNb);
    ar->set_tapepool(tapePool);
  }
}

void cta::objectstore::RootEntry::removeArchiveRoute(
  const std::string& storageClass, uint16_t copyNb) {
  checkPayloadWritable();
  // Find the storageClass entry
  auto & sc = serializers::findElement(m_payload.mutable_storageclasses(), storageClass);
  if (!serializers::isElementPresent(sc.routes(), copyNb))
    throw NoSuchArchiveRoute(
      "In RootEntry::removeArchiveRoute: trying to delete non-existing archive route.");
  serializers::removeOccurences(sc.mutable_routes(), copyNb);
}


std::vector<std::string> cta::objectstore::RootEntry::getArchiveRoutes(const std::string storageClass) {
  checkPayloadReadable();
  auto & sc = serializers::findElement(m_payload.storageclasses(), storageClass);
  std::vector<std::string> ret;
  int copycount = sc.copycount();
  copycount = copycount;
  ret.resize(sc.copycount());
  // If the number of routes is not right, fail.
  if (sc.copycount() != (uint32_t) sc.routes_size()) {
    std::stringstream err;
    err << "In RootEntry::getArchiveRoutes: not enough routes defined: "
        << sc.routes_size() << " found out of " << sc.copycount();
    throw IncompleteEntry(err.str());
  }
  auto & list = sc.routes();
  for (auto i = list.begin(); i!= list.end(); i++) {
    int copyNb = i->copynb();
    copyNb = copyNb;
    ret[i->copynb()-1] = i->tapepool();
  }
  return ret;
}

auto cta::objectstore::RootEntry::dumpStorageClasses() -> std::list<StorageClassDump> {
  checkPayloadReadable();
  std::list<StorageClassDump> ret;
  auto & scl = m_payload.storageclasses();
  for (auto i=scl.begin(); i != scl.end(); i++) {
    ret.push_back(StorageClassDump());
    ret.back().copyCount = i->copycount();
    ret.back().storageClass = i->name();
    ret.back().log.deserialize(i->log());
    auto & arl = i->routes();
    for (auto j=arl.begin(); j!= arl.end(); j++) {
      ret.back().routes.push_back(StorageClassDump::ArchiveRouteDump());
      auto &r = ret.back().routes.back();
      r.copyNumber = j->copynb();
      r.tapePool = j->tapepool();
      r.log.deserialize(j->log());
    }
  }
  return ret;
}

auto cta::objectstore::RootEntry::dumpStorageClass(const std::string& name)
  -> StorageClassDump {
  checkPayloadReadable();
  auto & scl = m_payload.storageclasses();
  for (auto i=scl.begin(); i != scl.end(); i++) {
    if (i->name() == name) {
      StorageClassDump ret;
      ret.copyCount = i->copycount();
      ret.storageClass = i->name();
      auto & arl = i->routes();
      for (auto j=arl.begin(); j!= arl.end(); j++) {
        ret.routes.push_back(StorageClassDump::ArchiveRouteDump());
        auto &r = ret.routes.back();
        r.copyNumber = j->copynb();
        r.tapePool = j->tapepool();
        r.log.deserialize(j->log());
      }
      return ret;
    }
  }
  std::stringstream err;
  err << "In RootEntry::dumpStorageClass: no such storage class: "
      << name;
  throw NoSuchStorageClass(err.str());
}

// =============================================================================
// ========== Libraries manipulations ==========================================
// =============================================================================

// This operator will be used in the following usage of the findElement
// removeOccurences
namespace {
  bool operator==(const std::string &l,
    const cta::objectstore::serializers::Library & sl) {
    return sl.name() == l;
  }
}

void cta::objectstore::RootEntry::addLibrary(const std::string& library,
    const CreationLog & log) {
  checkPayloadWritable();
  // Check the library does not exist aclready
  try {
    serializers::findElement(m_payload.libraries(), library);
    throw DuplicateEntry("In RootEntry::addLibrary: trying to create duplicate entry");
  } catch (serializers::NotFound &) {}
  // Insert the library
  auto * l = m_payload.mutable_libraries()->Add();
  l->set_name(library);
  log.serialize(*l->mutable_log());
}

void cta::objectstore::RootEntry::removeLibrary(const std::string& library) {
  checkPayloadWritable();
  if (!serializers::isElementPresent(m_payload.libraries(), library))
    throw NoSuchLibrary(
      "In RootEntry::removeLibrary: trying to delete non-existing library.");
  serializers::removeOccurences(m_payload.mutable_libraries(), library);
}

bool cta::objectstore::RootEntry::libraryExists(const std::string& library) {
  checkPayloadReadable();
  return serializers::isElementPresent(m_payload.libraries(), library);
}

auto cta::objectstore::RootEntry::dumpLibraries() -> std::list<LibraryDump> {
  checkPayloadReadable();
  std::list<LibraryDump> ret;
  auto & list=m_payload.libraries();
  for (auto i=list.begin(); i!=list.end(); i++) {
    ret.push_back(LibraryDump());
    ret.back().library = i->name();
    ret.back().log.deserialize(i->log());
  }
  return ret;
}

// =============================================================================
// ========== Tape pools manipulations =========================================
// =============================================================================


// This operator will be used in the following usage of the findElement
// removeOccurences
namespace {
  bool operator==(const std::string &tp,
    const cta::objectstore::serializers::TapePoolPointer & tpp) {
    return tpp.name() == tp;
  }
}

std::string cta::objectstore::RootEntry::addOrGetTapePoolAndCommit(const std::string& tapePool,
  uint32_t nbPartialTapes, uint16_t maxRetriesPerMount, uint16_t maxTotalRetries,
  Agent& agent, const CreationLog& log) {
  checkPayloadWritable();
  // Check the tape pool does not already exist
  try {
    return serializers::findElement(m_payload.tapepoolpointers(), tapePool).address();
  } catch (serializers::NotFound &) {}
  // Insert the tape pool, then its pointer, with agent intent log update
  // First generate the intent. We expect the agent to be passed locked.
  std::string tapePoolAddress = agent.nextId("tapePool");
  // TODO Do we expect the agent to be passed locked or not: to be clarified.
  ScopedExclusiveLock agl(agent);
  agent.fetch();
  agent.addToOwnership(tapePoolAddress);
  agent.commit();
  // Then create the tape pool object
  TapePool tp(tapePoolAddress, ObjectOps<serializers::RootEntry>::m_objectStore);
  tp.initialize(tapePool);
  tp.setOwner(agent.getAddressIfSet());
  tp.setMaxRetriesWithinMount(maxRetriesPerMount);
  tp.setMaxTotalRetries(maxTotalRetries);
  tp.setBackupOwner("root");
  tp.insert();
  ScopedExclusiveLock tpl(tp);
  // Now move the tape pool's ownership to the root entry
  auto * tpp = m_payload.mutable_tapepoolpointers()->Add();
  tpp->set_address(tapePoolAddress);
  tpp->set_name(tapePool);
  tpp->set_nbpartialtapes(nbPartialTapes);
  log.serialize(*tpp->mutable_log());
  // We must commit here to ensure the tape pool object is referenced.
  commit();
  // Now update the tape pool's ownership.
  tp.setOwner(getAddressIfSet());
  tp.setBackupOwner(getAddressIfSet());
  tp.commit();
  // ... and clean up the agent
  agent.removeFromOwnership(tapePoolAddress);
  agent.commit();
  return tapePoolAddress;
}

void cta::objectstore::RootEntry::removeTapePoolAndCommit(const std::string& tapePool) {
  checkPayloadWritable();
  // find the address of the tape pool object
  try {
    auto tpp = serializers::findElement(m_payload.tapepoolpointers(), tapePool);
    // Check that the tape pool is not referenced by any route
    auto & scl = m_payload.storageclasses();
    for (auto sc=scl.begin(); sc!=scl.end(); sc++) {
      auto & rl=sc->routes();
      for (auto r=rl.begin(); r!=rl.end(); r++) {
        if (r->tapepool() == tapePool)
          throw TapePoolUsedInRoute(
            "In RootEntry::removeTapePoolAndCommit: trying to remove a tape pool used in archive route");
      }
    }
    // Open the tape pool object
    TapePool tp (tpp.address(), ObjectOps<serializers::RootEntry>::m_objectStore);
    ScopedExclusiveLock tpl(tp);
    tp.fetch();
    // Verify this is the tapepool we're looking for.
    if (tp.getName() != tapePool) {
      std::stringstream err;
      err << "Unexpected tape pool name found in object pointed to for tape pool: "
          << tapePool << " found: " << tp.getName();
      throw WrongTapePool(err.str());
    }
    // Check the tape pool is empty
    if (!tp.isEmpty()) {
      throw TapePoolNotEmpty ("In RootEntry::removeTapePoolAndCommit: trying to "
          "remove a non-empty tape pool");
    }
    // We can delete the pool
    tp.remove();
    // ... and remove it from our entry
    serializers::removeOccurences(m_payload.mutable_tapepoolpointers(), tapePool);
    // We commit for safety and symmetry with the add operation
    commit();
  } catch (serializers::NotFound &) {
    // No such tape pool. Nothing to to.
    throw NoSuchTapePool("In RootEntry::removeTapePoolAndCommit: trying to remove non-existing tape pool");
  }
}

std::string cta::objectstore::RootEntry::getTapePoolAddress(const std::string& tapePool) {
  checkPayloadReadable();
  try {
    auto & tpp = serializers::findElement(m_payload.tapepoolpointers(), tapePool);
    return tpp.address();
  } catch (serializers::NotFound &) {
    throw NotAllocated("In RootEntry::getTapePoolAddress: tape pool not allocated");
  }
}

auto cta::objectstore::RootEntry::dumpTapePools() -> std::list<TapePoolDump> {
  checkPayloadReadable();
  std::list<TapePoolDump> ret;
  auto & tpl = m_payload.tapepoolpointers();
  for (auto i = tpl.begin(); i!=tpl.end(); i++) {
    ret.push_back(TapePoolDump());
    ret.back().address = i->address();
    ret.back().tapePool = i->name();
    ret.back().nbPartialTapes = i->nbpartialtapes();
    ret.back().log.deserialize(i->log());
  }
  return ret;
}

// =============================================================================
// ================ Drive register manipulation ================================
// =============================================================================

std::string cta::objectstore::RootEntry::addOrGetDriveRegisterPointerAndCommit(
  Agent & agent, const CreationLog & log) {
  checkPayloadWritable();
  // Check if the drive register exists
  try {
    return getDriveRegisterAddress();
  } catch (NotAllocated &) {
    // decide on the object's name and add to agent's intent. We expect the
    // agent to be passed locked.
    std::string drAddress (agent.nextId("driveRegister"));
    ScopedExclusiveLock agl(agent);
    agent.fetch();
    agent.addToOwnership(drAddress);
    agent.commit();
    // Then create the drive register object
    DriveRegister dr(drAddress, m_objectStore);
    dr.initialize();
    dr.setOwner(agent.getAddressIfSet());
    dr.setBackupOwner(getAddressIfSet());
    dr.insert();
    // Take a lock on drive registry
    ScopedExclusiveLock drLock(dr);
    // Move drive registry ownership to the root entry
    auto * mdrp = m_payload.mutable_driveregisterpointer();
    mdrp->set_address(drAddress);
    log.serialize(*mdrp->mutable_log());
    commit();
    // Record completion in drive registry
    dr.setOwner(getAddressIfSet());
    dr.setBackupOwner(getAddressIfSet());
    dr.commit();
    //... and clean up the agent
    agent.removeFromOwnership(drAddress);
    agent.commit();
    return drAddress;
  }
}

void cta::objectstore::RootEntry::removeDriveRegisterAndCommit() {
  checkPayloadWritable();
  // Get the address of the drive register (nothing to do if there is none)
  if (!m_payload.has_driveregisterpointer() || 
      !m_payload.driveregisterpointer().address().size())
    return;
  std::string drAddr = m_payload.driveregisterpointer().address();
  DriveRegister dr(drAddr, ObjectOps<serializers::RootEntry>::m_objectStore);
  ScopedExclusiveLock drl(dr);
  dr.fetch();
  // Check the drive register is empty
  if (!dr.isEmpty()) {
    throw DriveRegisterNotEmpty("In RootEntry::removeDriveRegisterAndCommit: "
      "trying to remove a non-empty drive register");
  }
  // we can delete the drive register
  dr.remove();
  // And update the root entry
  m_payload.mutable_driveregisterpointer()->set_address("");
  // We commit for safety and symmetry with the add operation
  commit();
}

std::string cta::objectstore::RootEntry::getDriveRegisterAddress() {
  checkPayloadReadable();
  if (m_payload.has_driveregisterpointer() && 
      m_payload.driveregisterpointer().address().size()) {
    return m_payload.driveregisterpointer().address();
  }
  throw NotAllocated("In RootEntry::getDriveRegisterAddress: drive register not allocated");
}


// =============================================================================
// ================ Agent register manipulation ================================
// =============================================================================
// Get the name of the agent register (or exception if not available)
std::string cta::objectstore::RootEntry::getAgentRegisterAddress() {
  checkPayloadReadable();
  // If the registry is defined, return it, job done.
  if (m_payload.has_agentregisterpointer() &&
      m_payload.agentregisterpointer().address().size())
    return m_payload.agentregisterpointer().address();
  throw NotAllocated("In RootEntry::getAgentRegister: agentRegister not yet allocated");
}

// Get the name of a (possibly freshly created) agent register
std::string cta::objectstore::RootEntry::addOrGetAgentRegisterPointerAndCommit(Agent & agent,
  const CreationLog & log) {
  // Check if the agent register exists
  try {
    return getAgentRegisterAddress();
  } catch (NotAllocated &) {
    // If we get here, the agent register is not created yet, so we have to do it:
    // lock the entry again, for writing. We take the lock ourselves if needed
    // This will make an autonomous transaction
    checkPayloadWritable();
    fetch();
    if (m_payload.has_agentregisterpointer() &&
        m_payload.agentregisterpointer().address().size()) {
      return m_payload.agentregisterpointer().address();
    }
    // decide on the object's name
    std::string arAddress (agent.nextId("agentRegister"));
    // Record the agent registry in our own intent
    addIntendedAgentRegistry(arAddress);
    commit();
    // Create the agent registry
    AgentRegister ar(arAddress, m_objectStore);
    ar.initialize();
    // There is no garbage collection for an agent registry: if it is not
    // plugged to the root entry, it does not exist.
    ar.setOwner("");
    ar.setBackupOwner("");
    ar.insert();
    // Take a lock on agent registry
    ScopedExclusiveLock arLock(ar);
    // Move agent registry from intent to official
    auto * marp = m_payload.mutable_agentregisterpointer();
    marp->set_address(arAddress);
    log.serialize(*marp->mutable_log());
    m_payload.set_agentregisterintent("");
    commit();
    // Record completion in agent registry
    ar.setOwner(getAddressIfSet());
    ar.setBackupOwner(getAddressIfSet());
    ar.commit();
    // And we are done. Release locks
    arLock.release();
    return arAddress;
  }
}

void cta::objectstore::RootEntry::removeAgentRegisterAndCommit() {
  checkPayloadWritable();
  // Check that we do have an agent register set. Cleanup a potential intent as
  // well
  if (m_payload.agentregisterintent().size()) {
    AgentRegister iar(m_payload.agentregisterintent(),
      ObjectOps<serializers::RootEntry>::m_objectStore);
    ScopedExclusiveLock iarl(iar);
    // An agent register only referenced in the intent should not be used
    // and hence empty. We'll see that.
    iar.fetch();
    if (!iar.isEmpty()) {
      throw AgentRegisterNotEmpty("In RootEntry::removeAgentRegister: found "
        "a non-empty intended agent register. Internal error.");
    }
    iar.remove();
    m_payload.set_agentregisterintent("");
    commit();
  }
  if (m_payload.has_agentregisterpointer() &&
      m_payload.agentregisterpointer().address().size()) {
    AgentRegister ar(m_payload.agentregisterpointer().address(),
      ObjectOps<serializers::RootEntry>::m_objectStore);
    ScopedExclusiveLock arl(ar);
    ar.fetch();
    if (!ar.isEmpty()) {
      throw AgentRegisterNotEmpty("In RootEntry::removeAgentRegister: the agent "
        "register is not empty. Cannot remove.");
    }
    ar.remove();
    m_payload.mutable_agentregisterpointer()->set_address("");
    commit();
  }
}

void cta::objectstore::RootEntry::addIntendedAgentRegistry(const std::string& address) {
  checkPayloadWritable();
  // We are supposed to have only one intended agent registry at a time.
  // If we got the lock and there is one entry, this means the previous
  // attempt to create one did not succeed.
  // When getting here, having a set pointer to the registry is an error.
  if (m_payload.has_agentregisterpointer() &&
      m_payload.agentregisterpointer().address().size()) {
    throw exception::Exception("In cta::objectstore::RootEntry::addIntendedAgentRegistry:"
        " pointer to registry already set");
  }
  if (m_payload.agentregisterintent().size()) {
    // The intended object might not have made it to the official pointer.
    // If it did, we just clean up the intent.
    // If it did not, we clean up the object if present, clean up the intent
    // and replace it with the new one.
    // We do not recycle the object, as the state is doubtful.
    if (ObjectOps<serializers::RootEntry>::m_objectStore.exists(
      m_payload.agentregisterintent())) {
      AgentRegister iar(m_payload.agentregisterintent(),
        ObjectOps<serializers::RootEntry>::m_objectStore);
      iar.fetch();
      if (!iar.isEmpty()) {
        throw AgentRegisterNotEmpty("In RootEntry::addIntendedAgentRegistry, "
          "found a non-empty intended agent register. Internal Error.");
      }
      iar.remove();
    }
  }
  m_payload.set_agentregisterintent(address);
}

// =============================================================================
// ================ Scheduler global lock manipulation =========================
// =============================================================================

std::string cta::objectstore::RootEntry::getSchedulerGlobalLock() {
  checkPayloadReadable();
  // If the scheduler lock is defined, return it, job done.
  if (m_payload.has_schedulerlockpointer() &&
      m_payload.schedulerlockpointer().address().size())
    return m_payload.schedulerlockpointer().address();
  throw NotAllocated("In RootEntry::getAgentRegister: scheduler global lock not yet allocated");
}

// Get the name of a (possibly freshly created) scheduler global lock
std::string cta::objectstore::RootEntry::addOrGetSchedulerGlobalLockAndCommit(Agent & agent,
  const CreationLog & log) {
  checkPayloadWritable();
  // Check if the drive register exists
  try {
    return getSchedulerGlobalLock();
  } catch (NotAllocated &) {
    // decide on the object's name and add to agent's intent. We expect the
    // agent to be passed locked.
    std::string sglAddress (agent.nextId("schedulerGlobalLock"));
    ScopedExclusiveLock agl(agent);
    agent.fetch();
    agent.addToOwnership(sglAddress);
    agent.commit();
    // Then create the drive register object
    SchedulerGlobalLock sgl(sglAddress, m_objectStore);
    sgl.initialize();
    sgl.setOwner(agent.getAddressIfSet());
    sgl.setBackupOwner(getAddressIfSet());
    sgl.insert();
    // Take a lock on scheduler global lock
    ScopedExclusiveLock sglLock(sgl);
    // Move drive registry ownership to the root entry
    auto * msgl = m_payload.mutable_schedulerlockpointer();
    msgl->set_address(sglAddress);
    log.serialize(*msgl->mutable_log());
    commit();
    // Record completion in scheduler global lock
    sgl.setOwner(getAddressIfSet());
    sgl.setBackupOwner(getAddressIfSet());
    sgl.commit();
    //... and clean up the agent
    agent.removeFromOwnership(sglAddress);
    agent.commit();
    return sglAddress;
  }
}

void cta::objectstore::RootEntry::removeSchedulerGlobalLockAndCommit() {
  checkPayloadWritable();
  // Get the address of the scheduler lock (nothing to do if there is none)
  if (!m_payload.has_schedulerlockpointer() ||
      !m_payload.schedulerlockpointer().address().size())
    return;
  std::string sglAddress = m_payload.schedulerlockpointer().address();
  SchedulerGlobalLock sgl(sglAddress, ObjectOps<serializers::RootEntry>::m_objectStore);
  ScopedExclusiveLock sgll(sgl);
  sgl.fetch();
  // Check the drive register is empty
  if (!sgl.isEmpty()) {
    throw DriveRegisterNotEmpty("In RootEntry::removeSchedulerGlobalLockAndCommit: "
      "trying to remove a non-empty scheduler global lock");
  }
  // we can delete the drive register
  sgl.remove();
  // And update the root entry
  m_payload.mutable_schedulerlockpointer()->set_address("");
  // We commit for safety and symmetry with the add operation
  commit();
}


// Dump the root entry
std::string cta::objectstore::RootEntry::dump () {
  checkPayloadReadable();
  std::stringstream ret;
  ret << "<<<< Root entry dump start" << std::endl;
  if (m_payload.has_agentregisterpointer() &&
      m_payload.agentregisterpointer().address().size())
    ret << "agentRegister=" << m_payload.agentregisterpointer().address() << std::endl;
  if (m_payload.agentregisterintent().size())
    ret << "agentRegister Intent=" << m_payload.agentregisterintent() << std::endl;
//  if (m_payload.has_jobpool()) ret << "jobPool=" << m_payload.jobpool() << std::endl;
/*  if (m_payload.has_driveregister()) ret << "driveRegister=" << m_payload.driveregister() << std::endl;
  if (m_payload.has_taperegister()) ret << "tapeRegister=" << m_payload.taperegister() << std::endl;*/
  ret << ">>>> Root entry dump end" << std::endl;
  return ret.str();
}
