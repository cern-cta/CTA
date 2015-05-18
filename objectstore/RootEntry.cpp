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
#include "JobPool.hpp"
#include <cxxabi.h>
#include "ProtcolBuffersAlgorithms.hpp"

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

// Get the name of the agent register (or exception if not available)
std::string cta::objectstore::RootEntry::getAgentRegister() {
  // Check that the fetch was done
  if (!m_payloadInterpreted)
    throw ObjectOpsBase::NotFetched("In RootEntry::getAgentRegister: object not yet fetched");
  // If the registry is defined, return it, job done.
  if (m_payload.agentregister().size())
    return m_payload.agentregister();
  throw NotAllocatedEx("In RootEntry::getAgentRegister: agentRegister not yet allocated");
}

// Get the name of a (possibly freshly created) agent register
std::string cta::objectstore::RootEntry::allocateOrGetAgentRegister(Agent & agent ) {
  // Check if the agent register exists
  try {
    return getAgentRegister();
  } catch (NotAllocatedEx &) {
    // If we get here, the agent register is not created yet, so we have to do it:
    // lock the entry again, for writing. We take the lock ourselves if needed
    // This will make an autonomous transaction
    std::auto_ptr<ScopedExclusiveLock> lockPtr;
    if (!m_locksForWriteCount)
      lockPtr.reset(new ScopedExclusiveLock(*this));
    // If the registry is already defined, somebody was faster. We're done.
    fetch();
    if (m_payload.agentregister().size()) {
      lockPtr.reset(NULL);
      return m_payload.agentregister();
    }
    // We will really create the register
    // decide on the object's name
    std::string arName (agent.nextId("agentRegister"));
    // Record the agent registry in our own intent
    addIntendedAgentRegistry(arName);
    commit();
    // Create the agent registry
    AgentRegister ar(arName, m_objectStore);
    ar.initialize();
    // There is no garbage collection for an agent registry: if it is not
    // plugged to the root entry, it does not exist.
    ar.setOwner("");
    ar.setBackupOwner("");
    ar.insert();
    // Take a lock on agent registry
    ScopedExclusiveLock arLock(ar);
    // Move agent registry from intent to official
    setAgentRegister(arName);
    deleteFromIntendedAgentRegistry(arName);
    commit();
    // Record completion in agent registry
    ar.setOwner(getNameIfSet());
    ar.setBackupOwner(getNameIfSet());
    ar.commit();
    // And we are done. Release locks
    lockPtr.reset(NULL);
    arLock.release();
    return arName;
  }
}

void cta::objectstore::RootEntry::addIntendedAgentRegistry(const std::string& name) {
  checkPayloadWritable();
  std::string * reg = m_payload.mutable_agentregisterintentlog()->Add();
  *reg = name;
}

void cta::objectstore::RootEntry::deleteFromIntendedAgentRegistry(const std::string& name) {
  checkPayloadWritable();
  serializers::removeString(m_payload.mutable_agentregisterintentlog(), name);
}

void cta::objectstore::RootEntry::setAgentRegister(const std::string& name) {
  checkPayloadWritable();
  m_payload.set_agentregister(name);
}

// Get the name of the JobPool (or exception if not available)
std::string cta::objectstore::RootEntry::getJobPool() {
  checkPayloadReadable();
  // If the registry is defined, return it, job done.
  if (m_payload.jobpool().size())
    return m_payload.jobpool();
  throw NotAllocatedEx("In RootEntry::getJobPool: jobPool not yet allocated");
}

// Get the name of a (possibly freshly created) job pool
std::string cta::objectstore::RootEntry::allocateOrGetJobPool(Agent & agent) {
  // Check if the job pool exists
  try {
    return getJobPool();
  } catch (NotAllocatedEx &) {
    // If we get here, the job pool is not created yet, so we have to do it:
    // lock the entry again, for writing
    ScopedExclusiveLock lock(*this);
    fetch();
    // If the registry is already defined, somebody was faster. We're done.
    if (m_payload.jobpool().size()) {
      lock.release();
      return m_payload.jobpool();
    }
    // We will really create the register
    // decide on the object's name
    std::string jpName (agent.nextId("jobPool"));
    // Record the agent in the intent log
    addIntendedJobPool(jpName);
    // Create and populate the object
    JobPool jp(jpName, m_objectStore);
    jp.initialize();
    jp.setOwner("");
    jp.setBackupOwner("");
    jp.insert();
    // Take a lock on the newly created job pool
    ScopedExclusiveLock jpLock(jp);
    // Move job pool from intent to official
    setJobPool(jpName);
    commit();
    // Record completion on the job pool
    jp.setOwner(getNameIfSet());
    jp.setBackupOwner(getNameIfSet());
    jp.commit();
    // and we are done
    return jpName;
  }
}

void cta::objectstore::RootEntry::addIntendedJobPool(const std::string& name) {
  checkPayloadWritable();
  std::string * jp = m_payload.mutable_jobpoolintentlog()->Add();
  *jp = name;
}

void cta::objectstore::RootEntry::deleteFromIntendedJobPool(const std::string& name) {
  checkPayloadWritable();
  serializers::removeString(m_payload.mutable_jobpoolintentlog(), name);
}

void cta::objectstore::RootEntry::setJobPool(const std::string& name) {
  checkPayloadWritable();
  m_payload.set_jobpool(name);
}

// Get the name of the admin user list (or exception if not available)
std::string cta::objectstore::RootEntry::getAdminUsersList() {
  // Check that the fetch was done
  if (!m_payloadInterpreted)
    throw ObjectOpsBase::NotFetched("In RootEntry::getAdminUsersList: object not yet fetched");
  // If the registry is defined, return it, job done.
  if (m_payload.adminuserslist().size())
    return m_payload.adminuserslist();
  throw NotAllocatedEx("In RootEntry::getAdminUsersList: adminUserList not yet allocated");
}

std::string cta::objectstore::RootEntry::allocateOrGetAdminUsersList(Agent& agent) {
  throw cta::exception::Exception("TODO");
}

void cta::objectstore::RootEntry::addIntendedAdminUsersList(const std::string& name) {
  throw cta::exception::Exception("TODO");
}

void cta::objectstore::RootEntry::deleteFromIntendedAdminUsersList(const std::string& name) {
  throw cta::exception::Exception("TODO");
}

void cta::objectstore::RootEntry::setAdminUsersList(const std::string& name) {
  throw cta::exception::Exception("TODO");
}


// Dump the root entry
std::string cta::objectstore::RootEntry::dump () {
  checkPayloadReadable();
  std::stringstream ret;
  ret << "<<<< Root entry dump start" << std::endl;
  if (m_payload.has_agentregister()) ret << "agentRegister=" << m_payload.agentregister() << std::endl;
  ret << "agentRegister Intent Log size=" << m_payload.agentregisterintentlog_size() << std::endl;
  for (int i=0; i<m_payload.agentregisterintentlog_size(); i++) {
    ret << "agentRegisterIntentLog=" << m_payload.agentregisterintentlog(i) << std::endl;
  }
  if (m_payload.has_jobpool()) ret << "jobPool=" << m_payload.jobpool() << std::endl;
/*  if (m_payload.has_driveregister()) ret << "driveRegister=" << m_payload.driveregister() << std::endl;
  if (m_payload.has_taperegister()) ret << "tapeRegister=" << m_payload.taperegister() << std::endl;*/
  ret << ">>>> Root entry dump start" << std::endl;
  return ret.str();
}
