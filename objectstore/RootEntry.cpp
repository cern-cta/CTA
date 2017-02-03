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
#include "AgentReference.hpp"
#include "ArchiveQueue.hpp"
#include "RetrieveQueue.hpp"
#include "DriveRegister.hpp"
#include "GenericObject.hpp"
#include "SchedulerGlobalLock.hpp"
#include <cxxabi.h>
#include "ProtocolBuffersAlgorithms.hpp"
#include <json-c/json.h>

namespace cta { namespace objectstore {

const std::string RootEntry::address("root");

// construtor, when the backend store exists.
// Checks the existence and correctness of the root entry
RootEntry::RootEntry(Backend & os):
  ObjectOps<serializers::RootEntry, serializers::RootEntry_t>(os, address) {}

RootEntry::RootEntry(GenericObject& go): 
  ObjectOps<serializers::RootEntry, serializers::RootEntry_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

// Initialiser. This uses the base object's initialiser and sets the defaults 
// of payload.
void RootEntry::initialize() {
  ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::initialize();
  // There is nothing to do for the payload.
  m_payloadInterpreted = true;
}

bool RootEntry::isEmpty() {
  checkPayloadReadable();
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
  if (m_payload.archivequeuepointers().size())
    return false;
  if (m_payload.retrievequeuepointers().size())
    return false;
  return true;
}

void RootEntry::removeIfEmpty() {
  checkPayloadWritable();
  if (!isEmpty()) {
    throw NotEmpty("In RootEntry::removeIfEmpty(): root entry not empty");
  }
  remove();
}

// =============================================================================
// ========== Archive queues manipulations =====================================
// =============================================================================


// This operator will be used in the following usage of the findElement
// removeOccurences
namespace {
  bool operator==(const std::string &tp,
    const serializers::ArchiveQueuePointer & tpp) {
    return tpp.name() == tp;
  }
}

std::string RootEntry::addOrGetArchiveQueueAndCommit(const std::string& tapePool, AgentReference& agentRef) {
  checkPayloadWritable();
  // Check the archive queue does not already exist
  try {
    return serializers::findElement(m_payload.archivequeuepointers(), tapePool).address();
  } catch (serializers::NotFound &) {}
  // Insert the archive queue, then its pointer, with agent intent log update
  // First generate the intent. We expect the agent to be passed locked.
  std::string archiveQueueAddress = agentRef.nextId("archiveQueue");
  // TODO Do we expect the agent to be passed locked or not: to be clarified.
  Agent agent(agentRef.getAgentAddress(), m_objectStore);
  ScopedExclusiveLock agl(agent);
  agent.fetch();
  agent.addToOwnership(archiveQueueAddress);
  agent.commit();
  // Then create the tape pool queue object
  ArchiveQueue aq(archiveQueueAddress, ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
  aq.initialize(tapePool);
  aq.setOwner(agent.getAddressIfSet());
  aq.setBackupOwner("root");
  aq.insert();
  ScopedExclusiveLock tpl(aq);
  // Now move the tape pool's ownership to the root entry
  auto * tpp = m_payload.mutable_archivequeuepointers()->Add();
  tpp->set_address(archiveQueueAddress);
  tpp->set_name(tapePool);
  // We must commit here to ensure the tape pool object is referenced.
  commit();
  // Now update the tape pool's ownership.
  aq.setOwner(getAddressIfSet());
  aq.setBackupOwner(getAddressIfSet());
  aq.commit();
  // ... and clean up the agent
  agent.removeFromOwnership(archiveQueueAddress);
  agent.commit();
  return archiveQueueAddress;
}

void RootEntry::removeArchiveQueueAndCommit(const std::string& tapePool) {
  checkPayloadWritable();
  // find the address of the archive queue object
  try {
    auto aqp = serializers::findElement(m_payload.archivequeuepointers(), tapePool);
    // Open the tape pool object
    ArchiveQueue aq (aqp.address(), m_objectStore);
    ScopedExclusiveLock aql;
    try {
      aql.lock(aq);
      aq.fetch();
    } catch (cta::exception::Exception & ex) {
      // The archive queue seems to not be there. Make sure this is the case:
      if (aq.exists()) {
        // We failed to access the queue, yet it is present. This is an error.
        // Let the exception pass through.
        throw;
      } else {
        // The queue object is already gone. We can skip to removing the 
        // reference from the RootEntry
        goto deleteFromRootEntry;
      }
    }
    // Verify this is the archive queue we're looking for.
    if (aq.getTapePool() != tapePool) {
      std::stringstream err;
      err << "In RootEntry::removeArchiveQueueAndCommit(): Unexpected tape pool name found in archive queue pointed to for tape pool: "
          << tapePool << " found: " << aq.getTapePool();
      throw WrongArchiveQueue(err.str());
    }
    // Check the archive queue is empty
    if (!aq.isEmpty()) {
      throw ArchivelQueueNotEmpty ("In RootEntry::removeArchiveQueueAndCommit(): trying to "
          "remove a non-empty tape pool");
    }
    // We can delete the queue
    aq.remove();
  deleteFromRootEntry:
    // ... and remove it from our entry
    serializers::removeOccurences(m_payload.mutable_archivequeuepointers(), tapePool);
    // We commit for safety and symmetry with the add operation
    commit();
  } catch (serializers::NotFound &) {
    // No such tape pool. Nothing to to.
    throw NoSuchArchiveQueue("In RootEntry::removeArchiveQueueAndCommit(): trying to remove non-existing archive queue");
  }
}

std::string RootEntry::getArchiveQueueAddress(const std::string& tapePool) {
  checkPayloadReadable();
  try {
    auto & tpp = serializers::findElement(m_payload.archivequeuepointers(), tapePool);
    return tpp.address();
  } catch (serializers::NotFound &) {
    throw NoSuchArchiveQueue("In RootEntry::getArchiveQueueAddress: archive queue not allocated");
  }
}

auto RootEntry::dumpArchiveQueues() -> std::list<ArchiveQueueDump> {
  checkPayloadReadable();
  std::list<ArchiveQueueDump> ret;
  auto & tpl = m_payload.archivequeuepointers();
  for (auto i = tpl.begin(); i!=tpl.end(); i++) {
    ret.push_back(ArchiveQueueDump());
    ret.back().address = i->address();
    ret.back().tapePool = i->name();
  }
  return ret;
}

// =============================================================================
// ========== Retrieve queues manipulations ====================================
// =============================================================================

// This operator will be used in the following usage of the findElement
// removeOccurences
namespace {
  bool operator==(const std::string &vid,
    const serializers::RetrieveQueuePointer & tpp) {
    return tpp.vid() == vid;
  }
}

std::string RootEntry::addOrGetRetrieveQueueAndCommit(const std::string& vid, AgentReference& agentRef) {
  checkPayloadWritable();
  // Check the retrieve queue does not already exist
  try {
    return serializers::findElement(m_payload.retrievequeuepointers(), vid).address();
  } catch (serializers::NotFound &) {}
  // Insert the retrieve queue, then its pointer, with agent intent log update
  // First generate the intent. We expect the agent to be passed locked.
  std::string retrieveQueueAddress = agentRef.nextId("retriveQueue");
  // TODO Do we expect the agent to be passed locked or not: to be clarified.
  Agent agent(agentRef.getAgentAddress(), m_objectStore);
  ScopedExclusiveLock agl(agent);
  agent.fetch();
  agent.addToOwnership(retrieveQueueAddress);
  agent.commit();
  // Then create the tape pool queue object
  RetrieveQueue rq(retrieveQueueAddress, ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
  rq.initialize(vid);
  rq.setOwner(agent.getAddressIfSet());
  rq.setBackupOwner("root");
  rq.insert();
  ScopedExclusiveLock tpl(rq);
  // Now move the tape pool's ownership to the root entry
  auto * rqp = m_payload.mutable_retrievequeuepointers()->Add();
  rqp->set_address(retrieveQueueAddress);
  rqp->set_vid(vid);
  // We must commit here to ensure the tape pool object is referenced.
  commit();
  // Now update the tape pool's ownership.
  rq.setOwner(getAddressIfSet());
  rq.setBackupOwner(getAddressIfSet());
  rq.commit();
  // ... and clean up the agent
  agent.removeFromOwnership(retrieveQueueAddress);
  agent.commit();
  return retrieveQueueAddress;
}

void RootEntry::removeRetrieveQueueAndCommit(const std::string& vid) {
  checkPayloadWritable();
  // find the address of the retrieve queue object
  try {
    auto rqp=serializers::findElement(m_payload.retrievequeuepointers(), vid);
    // Open the retrieve queue object
    RetrieveQueue rq(rqp.address(), m_objectStore);
    ScopedExclusiveLock rql;
    try {
      rql.lock(rq);
      rq.fetch();
    } catch (cta::exception::Exception & ex) {
      // The archive queue seems to not be there. Make sure this is the case:
      if (rq.exists()) {
        // We failed to access the queue, yet it is present. This is an error.
        // Let the exception pass through.
        throw;
      } else {
        // The queue object is already gone. We can skip to removing the 
        // reference from the RootEntry
        goto deleteFromRootEntry;
      }
    }
    // Verify this is the retrieve queue we're looking for.
    if (rq.getVid() != vid) {
      std::stringstream err;
      err << "Unexpected vid found in retrieve queue pointed to for vid: "
          << vid << " found: " << rq.getVid();
      throw WrongArchiveQueue(err.str());
    }
    // Check the retrieve queue is empty
    if (!rq.isEmpty()) {
      throw RetrieveQueueNotEmpty("In RootEntry::removeTapePoolQueueAndCommit: trying to "
          "remove a non-empty tape pool");
    }
    // We can now delete the queue
    rq.remove();
  deleteFromRootEntry:
    // ... and remove it from our entry
    serializers::removeOccurences(m_payload.mutable_retrievequeuepointers(), vid);
    // We commit for safety and symmetry with the add operation
    commit();
  } catch (serializers::NotFound &) {
    // No such tape pool. Nothing to to.
    throw NoSuchRetrieveQueue("In RootEntry::addOrGetRetrieveQueueAndCommit: trying to remove non-existing retrieve queue");
  }
}


std::string RootEntry::getRetrieveQueue(const std::string& vid) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

void RootEntry::removeArchiveQueueIfAddressMatchesAndCommit(const std::string& tapePool, const std::string& archiveQueueAddress) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

auto RootEntry::dumpRetrieveQueues() -> std::list<RetrieveQueueDump> {
  checkPayloadReadable();
  std::list<RetrieveQueueDump> ret;
  auto & tpl = m_payload.retrievequeuepointers();
  for (auto i = tpl.begin(); i!=tpl.end(); i++) {
    ret.push_back(RetrieveQueueDump());
    ret.back().address = i->address();
    ret.back().vid = i->vid();
  }
  return ret;
}

// =============================================================================
// ================ Drive register manipulation ================================
// =============================================================================

std::string RootEntry::addOrGetDriveRegisterPointerAndCommit(
  AgentReference& agentRef, const EntryLogSerDeser & log) {
  checkPayloadWritable();
  // Check if the drive register exists
  try {
    return getDriveRegisterAddress();
  } catch (NotAllocated &) {
    // decide on the object's name and add to agent's intent. We expect the
    // agent to be passed locked.
    std::string drAddress (agentRef.nextId("driveRegister"));
    Agent agent(agentRef.getAgentAddress(), m_objectStore);
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

void RootEntry::removeDriveRegisterAndCommit() {
  checkPayloadWritable();
  // Get the address of the drive register (nothing to do if there is none)
  if (!m_payload.has_driveregisterpointer() || 
      !m_payload.driveregisterpointer().address().size())
    return;
  std::string drAddr = m_payload.driveregisterpointer().address();
  DriveRegister dr(drAddr, ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
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

std::string RootEntry::getDriveRegisterAddress() {
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
std::string RootEntry::getAgentRegisterAddress() {
  checkPayloadReadable();
  // If the registry is defined, return it, job done.
  if (m_payload.has_agentregisterpointer() &&
      m_payload.agentregisterpointer().address().size())
    return m_payload.agentregisterpointer().address();
  throw NotAllocated("In RootEntry::getAgentRegister: agentRegister not yet allocated");
}

// Get the name of a (possibly freshly created) agent register
std::string RootEntry::addOrGetAgentRegisterPointerAndCommit(AgentReference& agentRef,
  const EntryLogSerDeser & log) {
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
    std::string arAddress (agentRef.nextId("agentRegister"));
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

void RootEntry::removeAgentRegisterAndCommit() {
  checkPayloadWritable();
  // Check that we do have an agent register set. Cleanup a potential intent as
  // well
  if (m_payload.agentregisterintent().size()) {
    AgentRegister iar(m_payload.agentregisterintent(),
      ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
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
      ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
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

void RootEntry::addIntendedAgentRegistry(const std::string& address) {
  checkPayloadWritable();
  // We are supposed to have only one intended agent registry at a time.
  // If we got the lock and there is one entry, this means the previous
  // attempt to create one did not succeed.
  // When getting here, having a set pointer to the registry is an error.
  if (m_payload.has_agentregisterpointer() &&
      m_payload.agentregisterpointer().address().size()) {
    throw exception::Exception("In RootEntry::addIntendedAgentRegistry:"
        " pointer to registry already set");
  }
  if (m_payload.agentregisterintent().size()) {
    // The intended object might not have made it to the official pointer.
    // If it did, we just clean up the intent.
    // If it did not, we clean up the object if present, clean up the intent
    // and replace it with the new one.
    // We do not recycle the object, as the state is doubtful.
    if (ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore.exists(
      m_payload.agentregisterintent())) {
      AgentRegister iar(m_payload.agentregisterintent(),
        ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
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

std::string RootEntry::getSchedulerGlobalLock() {
  checkPayloadReadable();
  // If the scheduler lock is defined, return it, job done.
  if (m_payload.has_schedulerlockpointer() &&
      m_payload.schedulerlockpointer().address().size())
    return m_payload.schedulerlockpointer().address();
  throw NotAllocated("In RootEntry::getAgentRegister: scheduler global lock not yet allocated");
}

// Get the name of a (possibly freshly created) scheduler global lock
std::string RootEntry::addOrGetSchedulerGlobalLockAndCommit(AgentReference& agentRef,
  const EntryLogSerDeser & log) {
  checkPayloadWritable();
  // Check if the drive register exists
  try {
    return getSchedulerGlobalLock();
  } catch (NotAllocated &) {
    // decide on the object's name and add to agent's intent. We expect the
    // agent to be passed locked.
    std::string sglAddress (agentRef.nextId("schedulerGlobalLock"));
    Agent agent(agentRef.getAgentAddress(), m_objectStore);
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

void RootEntry::removeSchedulerGlobalLockAndCommit() {
  checkPayloadWritable();
  // Get the address of the scheduler lock (nothing to do if there is none)
  if (!m_payload.has_schedulerlockpointer() ||
      !m_payload.schedulerlockpointer().address().size())
    return;
  std::string sglAddress = m_payload.schedulerlockpointer().address();
  SchedulerGlobalLock sgl(sglAddress, ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
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
std::string RootEntry::dump () {  
  checkPayloadReadable();
  std::stringstream ret;
  ret << "RootEntry" << std::endl;
  struct json_object * jo = json_object_new_object();
  
  json_object_object_add(jo, "agentregisterintent", json_object_new_string(m_payload.agentregisterintent().c_str()));
  
  {
    json_object * pointer = json_object_new_object();
    json_object_object_add(pointer, "address", json_object_new_string(m_payload.driveregisterpointer().address().c_str()));
    json_object * jlog = json_object_new_object();
    json_object_object_add(jlog, "host", json_object_new_string(m_payload.driveregisterpointer().log().host().c_str()));
    json_object_object_add(jlog, "time", json_object_new_int64(m_payload.driveregisterpointer().log().time()));
    json_object * id = json_object_new_object();
    json_object_object_add(id, "name", json_object_new_string(m_payload.driveregisterpointer().log().username().c_str()));
    json_object_object_add(jlog, "user", id);
    json_object_object_add(pointer, "log", jlog);
    json_object_object_add(jo, "driveregisterpointer", pointer);
  }
  {
    json_object * pointer = json_object_new_object();
    json_object_object_add(pointer, "address", json_object_new_string(m_payload.agentregisterpointer().address().c_str()));
    json_object * jlog = json_object_new_object();
    json_object_object_add(jlog, "host", json_object_new_string(m_payload.agentregisterpointer().log().host().c_str()));
    json_object_object_add(jlog, "time", json_object_new_int64(m_payload.agentregisterpointer().log().time()));
    json_object * id = json_object_new_object();
    json_object_object_add(id, "name", json_object_new_string(m_payload.driveregisterpointer().log().username().c_str()));
    json_object_object_add(jlog, "user", id);
    json_object_object_add(pointer, "log", jlog);
    json_object_object_add(jo, "agentregisterpointer", pointer);
  }
  {
    json_object * pointer = json_object_new_object();
    json_object_object_add(pointer, "address", json_object_new_string(m_payload.schedulerlockpointer().address().c_str()));
    json_object * jlog = json_object_new_object();
    json_object_object_add(jlog, "host", json_object_new_string(m_payload.schedulerlockpointer().log().host().c_str()));
    json_object_object_add(jlog, "time", json_object_new_int64(m_payload.schedulerlockpointer().log().time()));
    json_object * id = json_object_new_object();
    json_object_object_add(id, "name", json_object_new_string(m_payload.driveregisterpointer().log().username().c_str()));
    json_object_object_add(jlog, "user", id);
    json_object_object_add(pointer, "log", jlog);
    json_object_object_add(jo, "schedulerlockpointer", pointer);
  }
  {
    json_object * array = json_object_new_array();
    for (auto & i :m_payload.archivequeuepointers()) {
      json_object * jot = json_object_new_object();

      json_object_object_add(jot, "name", json_object_new_string(i.name().c_str())); 
      json_object_object_add(jot, "address", json_object_new_string(i.address().c_str()));  

      json_object_array_add(array, jot);
    }
    json_object_object_add(jo, "archivequeuepointers", array);
  }
  
  ret << json_object_to_json_string_ext(jo, JSON_C_TO_STRING_PRETTY) << std::endl;
  json_object_put(jo);
  return ret.str();
}

}} // namespace cta::objectstore
