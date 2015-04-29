#include "Agent.hpp"
#include "AgentRegister.hpp"
#include "RootEntry.hpp"
#include "exception/Errnum.hpp"
#include "ProtcolBuffersAlgorithms.hpp"
#include <string>
#include <sstream>
#include <iomanip>
#include <sys/syscall.h>
#include <ctime>
#include <cxxabi.h>
#include <unistd.h>

cta::objectstore::Agent::Agent(Backend & os): 
  ObjectOps<serializers::Agent>(os), m_nextId(0) {}

cta::objectstore::Agent::Agent(const std::string & name, Backend & os): 
  ObjectOps<serializers::Agent>(os, name), m_nextId(0) {}

void cta::objectstore::Agent::initialize() {
  ObjectOps<serializers::Agent>::initialize();
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
  setName(aid.str());
}

void cta::objectstore::Agent::insertAndRegisterSelf() {
  // We suppose initialize was already called, and that the agent name
  // is set.
  // We need to get hold of the agent register, which we suppose is available
  RootEntry re(m_objectStore);
  ScopedSharedLock reLock(re);
  re.fetch();
  AgentRegister ar(re.getAgentRegister(), m_objectStore);
  reLock.release();
  // Then we should first create a pointer to our agent
  ScopedExclusiveLock arLock(ar);
  ar.fetch();
  ar.addAgent(getNameIfSet());
  ar.commit();
  // Set the agent register as owner and backup owner
  setBackupOwner(ar.getNameIfSet());
  setOwner(ar.getNameIfSet());
  // Create the agent
  insert();
  // And release the agent register's lock
  arLock.release();
}

void cta::objectstore::Agent::deleteAndUnregisterSelf() {
  // First delete ourselves
  remove();
  // Then we remove the dangling pointer about ourselves in the agent register.
  // We need to get hold of the agent register, which we suppose is available
  RootEntry re(m_objectStore);
  ScopedSharedLock reLock(re);
  re.fetch();
  AgentRegister ar(re.getAgentRegister(), m_objectStore);
  reLock.release();
  // Then we should first create a pointer to our agent
  ScopedExclusiveLock arLock(ar);
  ar.fetch();
  ar.removeAgent(getNameIfSet());
  ar.commit();
  arLock.release();
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
  id << childType << "-" << getNameIfSet() << "-" << m_nextId++;
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

