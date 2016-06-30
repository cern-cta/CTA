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
#include "ArchiveQueue.hpp"
#include "RetrieveQueue.hpp"
#include "DriveRegister.hpp"
#include "GenericObject.hpp"
#include "SchedulerGlobalLock.hpp"
#include <cxxabi.h>
#include "ProtocolBuffersAlgorithms.hpp"
#include <json-c/json.h>

namespace cta { namespace objectstore {

// construtor, when the backend store exists.
// Checks the existence and correctness of the root entry
RootEntry::RootEntry(Backend & os):
  ObjectOps<serializers::RootEntry, serializers::RootEntry_t>(os, "root") {}

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
  if (m_payload.tapequeuepointers().size())
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

std::string RootEntry::addOrGetArchiveQueueAndCommit(const std::string& tapePool, Agent& agent) {
  checkPayloadWritable();
  // Check the tape pool does not already exist
  try {
    return serializers::findElement(m_payload.archivequeuepointers(), tapePool).address();
  } catch (serializers::NotFound &) {}
  // Insert the tape pool, then its pointer, with agent intent log update
  // First generate the intent. We expect the agent to be passed locked.
  std::string tapePoolQueueAddress = agent.nextId("tapePoolQueue");
  // TODO Do we expect the agent to be passed locked or not: to be clarified.
  ScopedExclusiveLock agl(agent);
  agent.fetch();
  agent.addToOwnership(tapePoolQueueAddress);
  agent.commit();
  // Then create the tape pool queue object
  ArchiveQueue tpq(tapePoolQueueAddress, ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
  tpq.initialize(tapePool);
  tpq.setOwner(agent.getAddressIfSet());
  tpq.setBackupOwner("root");
  tpq.insert();
  ScopedExclusiveLock tpl(tpq);
  // Now move the tape pool's ownership to the root entry
  auto * tpp = m_payload.mutable_archivequeuepointers()->Add();
  tpp->set_address(tapePoolQueueAddress);
  tpp->set_name(tapePool);
  // We must commit here to ensure the tape pool object is referenced.
  commit();
  // Now update the tape pool's ownership.
  tpq.setOwner(getAddressIfSet());
  tpq.setBackupOwner(getAddressIfSet());
  tpq.commit();
  // ... and clean up the agent
  agent.removeFromOwnership(tapePoolQueueAddress);
  agent.commit();
  return tapePoolQueueAddress;
}

void RootEntry::removeArchiveQueueAndCommit(const std::string& tapePool) {
  checkPayloadWritable();
  // find the address of the tape pool object
  try {
    auto tpp = serializers::findElement(m_payload.archivequeuepointers(), tapePool);
    // Open the tape pool object
    ArchiveQueue aq (tpp.address(), ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
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
      err << "Unexpected tape pool name found in archive queue pointed to for tape pool: "
          << tapePool << " found: " << aq.getTapePool();
      throw WrongArchiveQueue(err.str());
    }
    // Check the archive queue is empty
    if (!aq.isEmpty()) {
      throw ArchivelQueueNotEmpty ("In RootEntry::removeTapePoolQueueAndCommit: trying to "
          "remove a non-empty tape pool");
    }
    // We can delete the pool
    aq.remove();
  deleteFromRootEntry:
    // ... and remove it from our entry
    serializers::removeOccurences(m_payload.mutable_archivequeuepointers(), tapePool);
    // We commit for safety and symmetry with the add operation
    commit();
  } catch (serializers::NotFound &) {
    // No such tape pool. Nothing to to.
    throw NoSuchTapePoolQueue("In RootEntry::removeTapePoolQueueAndCommit: trying to remove non-existing tape pool");
  }
}

std::string RootEntry::getArchiveQueueAddress(const std::string& tapePool) {
  checkPayloadReadable();
  try {
    auto & tpp = serializers::findElement(m_payload.archivequeuepointers(), tapePool);
    return tpp.address();
  } catch (serializers::NotFound &) {
    throw NotAllocated("In RootEntry::getTapePoolQueueAddress: tape pool not allocated");
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
// ================ Drive register manipulation ================================
// =============================================================================

std::string RootEntry::addOrGetDriveRegisterPointerAndCommit(
  Agent & agent, const EntryLog & log) {
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
std::string RootEntry::addOrGetAgentRegisterPointerAndCommit(Agent & agent,
  const EntryLog & log) {
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
std::string RootEntry::addOrGetSchedulerGlobalLockAndCommit(Agent & agent,
  const EntryLog & log) {
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
    json_object_object_add(jlog, "comment", json_object_new_string(m_payload.driveregisterpointer().log().comment().c_str()));
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
    json_object_object_add(jlog, "comment", json_object_new_string(m_payload.agentregisterpointer().log().comment().c_str()));
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
    json_object_object_add(jlog, "comment", json_object_new_string(m_payload.schedulerlockpointer().log().comment().c_str()));
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
