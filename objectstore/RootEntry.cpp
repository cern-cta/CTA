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

// =============================================================================
// ================ Agent register manipulation ================================
// =============================================================================
// Get the name of the agent register (or exception if not available)
std::string cta::objectstore::RootEntry::getAgentRegisterPointer() {
  // Check that the fetch was done
  if (!m_payloadInterpreted)
    throw ObjectOpsBase::NotFetched("In RootEntry::getAgentRegisterPointer: object not yet fetched");
  // If the registry is defined, return it, job done.
  if (m_payload.agentregisterpointer().address().size())
    return m_payload.agentregisterpointer().address();
  throw NotAllocatedEx("In RootEntry::getAgentRegister: agentRegister not yet allocated");
}

// Get the name of a (possibly freshly created) agent register
std::string cta::objectstore::RootEntry::addOrGetAgentRegisterPointer(Agent & agent,
  const CreationLog & log) {
  // Check if the agent register exists
  try {
    return getAgentRegisterPointer();
  } catch (NotAllocatedEx &) {
    // If we get here, the agent register is not created yet, so we have to do it:
    // lock the entry again, for writing. We take the lock ourselves if needed
    // This will make an autonomous transaction
    std::auto_ptr<ScopedExclusiveLock> lockPtr;
    if (!m_locksForWriteCount)
      lockPtr.reset(new ScopedExclusiveLock(*this));
    // If the registry is already defined, somebody was faster. We're done.
    fetch();
    if (m_payload.agentregisterpointer().address().size()) {
      lockPtr.reset(NULL);
      return m_payload.agentregisterpointer().address();
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
    setAgentRegistry(arName, log);
    deleteIntendedAgentRegistry();
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

void cta::objectstore::RootEntry::addIntendedAgentRegistry(const std::string& address) {
  checkPayloadWritable();
  // We are supposed to have only one intended agent registry at a time.
  // If we got the lock and there is one entry, this means the previous
  // attempt to create one did not succeed.
  // When getting here, having a set pointer to the registry is an error.
  if (m_payload.agentregisterpointer().address().size()) {
    throw exception::Exception("In cta::objectstore::RootEntry::addIntendedAgentRegistry:"
        " pointer to registry already set");
  }
  if (m_payload.agentregisterintent().size()) {
    // The intended object might not have made it to the official pointer.
    // If it did, we just clean up the intent.
    // If it did not, we clean up the object if present, clean up the intent
    // and replace it with the new one.
    // We do not recycle the object, as the state is doubtful.
    if (ObjectOps<serializers::RootEntry>::m_objectStore.exists(m_payload.agentregisterintent())) {
      ObjectOps<serializers::RootEntry>::m_objectStore.read(m_payload.agentregisterintent());
    }
  }
  m_payload.set_agentregisterintent(address);
}

void cta::objectstore::RootEntry::deleteIntendedAgentRegistry() {
  checkPayloadWritable();
  m_payload.set_agentregisterintent("");
}

void cta::objectstore::RootEntry::setAgentRegistry(const std::string& address,
  const CreationLog & log) {
  checkPayloadWritable();
  m_payload.mutable_agentregisterpointer()->set_address(address);
  log.serialize(*m_payload.mutable_agentregisterpointer()->mutable_log());
}

// =============================================================================
// ================ Admin Users manipulations ==================================
// =============================================================================

void cta::objectstore::RootEntry::addAdminUser(const UserIdentity& user,
  const CreationLog& log) {
  checkPayloadWritable();
  // Check that the user does not exists already
  for (size_t i=0; i<(size_t)m_payload.adminusers_size(); i++) {
    if (user.uid == m_payload.adminusers(i).user().uid()) {
      throw DuplicateEntry("In RootEntry::addAdminUser: entry already exists");
    }
  }
  serializers::AdminUser * au = m_payload.add_adminusers();
  user.serialize(*au->mutable_user());
  log.serialize(*au->mutable_log());
}

void cta::objectstore::RootEntry::removeAdminUser(const UserIdentity& user) {
  checkPayloadWritable();
  bool found;
  auto *list = m_payload.mutable_adminusers();
  do {
    found = false;
    for (size_t i=0; i<(size_t)list->size(); i++) {
      if (list->Get(i).user().uid() == user.uid) {
        found = true;
        list->SwapElements(i, list->size()-1);
        list->ReleaseLast();
        break;
      }
    }
  } while (found);
}

bool cta::objectstore::RootEntry::isAdminUser(const UserIdentity& user) {
  checkPayloadReadable();
  auto &list=m_payload.adminusers();
  for (auto i=list.begin(); i != list.end(); i++) {
    if (i->user().uid() == user.uid) {
      return true;
    }
  }
  return false;
}

std::list<cta::objectstore::RootEntry::AdminUserDump> 
cta::objectstore::RootEntry::dumpAdminUsers() {
  std::list<cta::objectstore::RootEntry::AdminUserDump> ret;
  auto &list=m_payload.adminusers();
  for (auto i=list.begin(); i != list.end(); i++) {
    ret.push_back(AdminUserDump());
    ret.back().log.deserialize(i->log());
    ret.back().user.deserialize(i->user());
  }
  return ret;
}

//// Get the name of the JobPool (or exception if not available)
//std::string cta::objectstore::RootEntry::getJobPool() {
//  checkPayloadReadable();
//  // If the registry is defined, return it, job done.
//  if (m_payload.jobpool().size())
//    return m_payload.jobpool();
//  throw NotAllocatedEx("In RootEntry::getJobPool: jobPool not yet allocated");
//}
//
//// Get the name of a (possibly freshly created) job pool
//std::string cta::objectstore::RootEntry::allocateOrGetJobPool(Agent & agent) {
//  // Check if the job pool exists
//  try {
//    return getJobPool();
//  } catch (NotAllocatedEx &) {
//    // If we get here, the job pool is not created yet, so we have to do it:
//    // lock the entry again, for writing
//    ScopedExclusiveLock lock(*this);
//    fetch();
//    // If the registry is already defined, somebody was faster. We're done.
//    if (m_payload.jobpool().size()) {
//      lock.release();
//      return m_payload.jobpool();
//    }
//    // We will really create the register
//    // decide on the object's name
//    std::string jpName (agent.nextId("jobPool"));
//    // Record the agent in the intent log
//    addIntendedJobPool(jpName);
//    // Create and populate the object
//    JobPool jp(jpName, m_objectStore);
//    jp.initialize();
//    jp.setOwner("");
//    jp.setBackupOwner("");
//    jp.insert();
//    // Take a lock on the newly created job pool
//    ScopedExclusiveLock jpLock(jp);
//    // Move job pool from intent to official
//    setJobPool(jpName);
//    commit();
//    // Record completion on the job pool
//    jp.setOwner(getNameIfSet());
//    jp.setBackupOwner(getNameIfSet());
//    jp.commit();
//    // and we are done
//    return jpName;
//  }
//}
//
//void cta::objectstore::RootEntry::addIntendedJobPool(const std::string& name) {
//  checkPayloadWritable();
//  std::string * jp = m_payload.mutable_jobpoolintentlog()->Add();
//  *jp = name;
//}
//
//void cta::objectstore::RootEntry::deleteFromIntendedJobPool(const std::string& name) {
//  checkPayloadWritable();
//  serializers::removeString(m_payload.mutable_jobpoolintentlog(), name);
//}
//
//void cta::objectstore::RootEntry::setJobPool(const std::string& name) {
//  checkPayloadWritable();
//  m_payload.set_jobpool(name);
//}

// Get the name of the admin user list (or exception if not available)
//std::string cta::objectstore::RootEntry::getAdminUsersListPointer() {
//  // Check that the fetch was done
//  if (!m_payloadInterpreted)
//    throw ObjectOpsBase::NotFetched("In RootEntry::getAdminUsersList: object not yet fetched");
//  // If the registry is defined, return it, job done.
//  if (m_payload.adminuserslist().size())
//    return m_payload.adminuserslist();
//  throw NotAllocatedEx("In RootEntry::getAdminUsersList: adminUserList not yet allocated");
//}

//std::string cta::objectstore::RootEntry::allocateOrGetAdminUsersList(Agent& agent) {
//  throw cta::exception::Exception("TODO");
//}
//
//void cta::objectstore::RootEntry::addIntendedAdminUsersList(const std::string& name) {
//  throw cta::exception::Exception("TODO");
//}
//
//void cta::objectstore::RootEntry::deleteFromIntendedAdminUsersList(const std::string& name) {
//  throw cta::exception::Exception("TODO");
//}
//
//void cta::objectstore::RootEntry::setAdminUsersList(const std::string& name) {
//  throw cta::exception::Exception("TODO");
//}
//
//// Get the name of the admin user list (or exception if not available)
//std::string cta::objectstore::RootEntry::getStorageClassList() {
//  // Check that the fetch was done
//  if (!m_payloadInterpreted)
//    throw ObjectOpsBase::NotFetched("In RootEntry::getStorageClassList: object not yet fetched");
//  // If the registry is defined, return it, job done.
//  if (m_payload.storageclasslist().size())
//    return m_payload.storageclasslist();
//  throw NotAllocatedEx("In RootEntry::getStorageClassList: StorageClassList not yet allocated");
//}
//
//std::string cta::objectstore::RootEntry::allocateOrGetStorageClassList(Agent& agent) {
////  // Check if the job pool exists
////  try {
////    return getStorageClassList();
////  } catch (NotAllocatedEx &) {
////    // If we get here, the job pool is not created yet, so we have to do it:
////    // lock the entry again, for writing
////    ScopedExclusiveLock lock(*this);
////    fetch();
////    // If the registry is already defined, somebody was faster. We're done.
////    if (m_payload.storageclasslist().size()) {
////      lock.release();
////      return m_payload.storageclasslist();
////    }
////    // We will really create the register
////    // decide on the object's name
////    std::string sclName (agent.nextId("storageClassList"));
////    // Record the agent in the intent log
////    addIntendedStorageClassList(sclName);
////    // Create and populate the object
////    StorageClassList scl(sclName, m_objectStore);
////    scl.initialize();
////    scl.setOwner("");
////    scl.setBackupOwner("");
////    scl.insert();
////    // Take a lock on the newly created job pool
////    ScopedExclusiveLock sclLock(scl);
////    // Move job pool from intent to official
////    setStorageClassList(scl);
////    commit();
////    // Record completion on the job pool
////    scl.setOwner(getNameIfSet());
////    scl.setBackupOwner(getNameIfSet());
////    scl.commit();
////    // and we are done
////    return scl;
////  }
//  throw exception::Exception("TODO");
//}
//
//void cta::objectstore::RootEntry::addIntendedStorageClassList(const std::string& name) {
//  throw cta::exception::Exception("TODO");
//}
//
//void cta::objectstore::RootEntry::deleteFromIntendedStorageClassList(const std::string& name) {
//  throw cta::exception::Exception("TODO");
//}
//
//void cta::objectstore::RootEntry::setStorageClassList(const std::string& name) {
//  throw cta::exception::Exception("TODO");
//}

// Dump the root entry
std::string cta::objectstore::RootEntry::dump () {
  checkPayloadReadable();
  std::stringstream ret;
  ret << "<<<< Root entry dump start" << std::endl;
  if (m_payload.agentregisterpointer().address().size())
    ret << "agentRegister=" << m_payload.agentregisterpointer().address() << std::endl;
  if (m_payload.agentregisterintent().size())
    ret << "agentRegister Intent=" << m_payload.agentregisterintent() << std::endl;
//  if (m_payload.has_jobpool()) ret << "jobPool=" << m_payload.jobpool() << std::endl;
/*  if (m_payload.has_driveregister()) ret << "driveRegister=" << m_payload.driveregister() << std::endl;
  if (m_payload.has_taperegister()) ret << "tapeRegister=" << m_payload.taperegister() << std::endl;*/
  ret << ">>>> Root entry dump start" << std::endl;
  return ret.str();
}
