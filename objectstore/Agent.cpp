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

#include "Agent.hpp"
#include "AgentRegister.hpp"
#include "RootEntry.hpp"
#include "GenericObject.hpp"
#include "common/exception/Errnum.hpp"
#include "ProtocolBuffersAlgorithms.hpp"
#include <string>
#include <sstream>
#include <iomanip>
#include <sys/syscall.h>
#include <ctime>
#include <cxxabi.h>
#include <unistd.h>
#include <json-c/json.h>

cta::objectstore::Agent::Agent(Backend & os): 
  ObjectOps<serializers::Agent, serializers::Agent_t>(os), m_nextId(0) {}

cta::objectstore::Agent::Agent(GenericObject& go):
  ObjectOps<serializers::Agent, serializers::Agent_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

cta::objectstore::Agent::Agent(const std::string & name, Backend & os): 
  ObjectOps<serializers::Agent, serializers::Agent_t>(os, name), m_nextId(0) {}

void cta::objectstore::Agent::initialize() {
  ObjectOps<serializers::Agent, serializers::Agent_t>::initialize();
  m_payload.set_heartbeat(0);
  m_payload.set_timeout_us(60*1000*1000);
  m_payload.set_description("");
  m_payloadInterpreted = true;
}

void cta::objectstore::Agent::generateName(const std::string & typeName) {
  std::stringstream aid;
  // Get time
  time_t now = time(0);
  struct tm localNow;
  localtime_r(&now, &localNow);
  // Get hostname
  char host[200];
  cta::exception::Errnum::throwOnMinusOne(gethostname(host, sizeof(host)),
    "In AgentId::AgentId:  failed to gethostname");
  // gettid is a safe system call (never fails)
  aid << typeName << "-" << host << "-" << syscall(SYS_gettid) << "-"
    << 1900 + localNow.tm_year
    << std::setfill('0') << std::setw(2) 
    << 1 + localNow.tm_mon
    << std::setw(2) << localNow.tm_mday << "-"
    << std::setw(2) << localNow.tm_hour << ":"
    << std::setw(2) << localNow.tm_min << ":"
    << std::setw(2) << localNow.tm_sec;
  setAddress(aid.str());
}

void cta::objectstore::Agent::insertAndRegisterSelf() {
  // We suppose initialize was already called, and that the agent name
  // is set.
  // We need to get hold of the agent register, which we suppose is available
  RootEntry re(m_objectStore);
  ScopedSharedLock reLock(re);
  re.fetch();
  AgentRegister ar(re.getAgentRegisterAddress(), m_objectStore);
  reLock.release();
  // Then we should first create a pointer to our agent
  ScopedExclusiveLock arLock(ar);
  ar.fetch();
  ar.addAgent(getAddressIfSet());
  ar.commit();
  // Set the agent register as owner and backup owner
  setBackupOwner(ar.getAddressIfSet());
  setOwner(ar.getAddressIfSet());
  // Create the agent
  insert();
  // And release the agent register's lock
  arLock.release();
}

void cta::objectstore::Agent::removeAndUnregisterSelf() {
  // Check that we own the proper lock
  checkPayloadWritable();
  // Check that we are not empty
  if (!isEmpty()) {
    std::list<std::string> ownershipList = getOwnershipList();
    std::stringstream exSs;
    exSs << "In Agent::deleteAndUnregisterSelf: agent still owns objects. Here's the list:";
    for(auto i=ownershipList.begin(); i!=ownershipList.end(); i++) {
      exSs << " " << *i;
    }
    throw AgentStillOwnsObjects(exSs.str());
  }
  // First delete ourselves
  remove();
  // Then we remove the dangling pointer about ourselves in the agent register.
  // We need to get hold of the agent register, which we suppose is available
  RootEntry re(m_objectStore);
  ScopedSharedLock reLock(re);
  re.fetch();
  AgentRegister ar(re.getAgentRegisterAddress(), m_objectStore);
  reLock.release();
  // Then we should first create a pointer to our agent
  ScopedExclusiveLock arLock(ar);
  ar.fetch();
  ar.removeAgent(getAddressIfSet());
  ar.commit();
  arLock.release();
}

bool cta::objectstore::Agent::isEmpty() {
  checkPayloadReadable();
  if (m_payload.ownedobjects_size())
    return false;
  return true;
}


/*void cta::objectstore::Agent::create() {
  if (!m_setupDone)
    throw SetupNotDone("In Agent::create(): setup() not yet done");
  RootEntry re(m_objectStore);
  AgentRegister ar(re.allocateOrGetAgentRegister(*this), m_objectStore);
  ar.addIntendedElement(selfName(), *this);
  serializers::Agent as;
  as.set_heartbeat(0);
  writeChild(selfName(), as);
  ar.upgradeIntendedElementToActual(selfName(), *this);
  m_creationDone = true;
}*/

std::string cta::objectstore::Agent::nextId(const std::string & childType) {
  std::stringstream id;
  id << childType << "-" << getAddressIfSet() << "-" << m_nextId++;
  return id.str();
}

void cta::objectstore::Agent::addToOwnership(std::string name) {
  checkPayloadWritable();
  std::string * owned = m_payload.mutable_ownedobjects()->Add();
  *owned = name;
}

void cta::objectstore::Agent::removeFromOwnership(std::string name) {
  checkPayloadWritable();
  serializers::removeString(m_payload.mutable_ownedobjects(), name);
  /*
  bool found;
  do {
    found = false;
    for (int i=0; i<m_payload.mutable_ownedobjects()->size(); i++) {
      if (name == *m_payload.mutable_ownedobjects(i)) {
        found = true;
        m_payload.mutable_ownedobjects()->SwapElements(i, m_payload.mutable_ownedobjects()->size()-1);
        m_payload.mutable_ownedobjects()->RemoveLast();
        break;
      }
    }
  } while (found);*/
}

std::list<std::string> 
  cta::objectstore::Agent::getOwnershipList() {
  checkPayloadReadable();
  std::list<std::string> ret;
  for (int i=0; i<m_payload.ownedobjects_size(); i++) {
    ret.push_back(m_payload.ownedobjects(i));
  }
  return ret;
}

void cta::objectstore::Agent::bumpHeartbeat() {
  checkPayloadWritable();
  m_payload.set_heartbeat(m_payload.heartbeat()+1);
}

uint64_t cta::objectstore::Agent::getHeartbeatCount() {
  checkPayloadReadable();
  return m_payload.heartbeat();
}

double cta::objectstore::Agent::getTimeout() {
  checkPayloadReadable();
  return 0.000001 * m_payload.timeout_us();
}

void cta::objectstore::Agent::setTimeout_us(uint64_t timeout) {
  checkPayloadWritable();
  m_payload.set_timeout_us(timeout);
}

std::string cta::objectstore::Agent::dump() {
  checkPayloadReadable();
  std::stringstream ret;
  ret << "Agent" << std::endl;
  struct json_object * jo = json_object_new_object();
  // Array for owned objects
  json_object * joo = json_object_new_array();
  auto & ool = m_payload.ownedobjects();
  for (auto oo = ool.begin(); oo!=ool.end(); oo++) {
    json_object_array_add(joo, json_object_new_string(oo->c_str()));
  }
  json_object_object_add(jo, "ownedObjects", joo);
  json_object_object_add(jo, "description", json_object_new_string(m_payload.description().c_str()));
  json_object_object_add(jo, "heartbeat", json_object_new_int64(m_payload.heartbeat()));
  json_object_object_add(jo, "timeout_us", json_object_new_int64(m_payload.timeout_us()));
  ret << json_object_to_json_string_ext(jo, JSON_C_TO_STRING_PRETTY) << std::endl;
  json_object_put(jo);
  return ret.str();
}


