#include "Agent.hpp"
#include "AgentRegister.hpp"
#include "RootEntry.hpp"
#include "exception/Errnum.hpp"
#include <string>
#include <sstream>
#include <iomanip>
#include <sys/syscall.h>
#include <ctime>
#include <cxxabi.h>

cta::objectstore::Agent::Agent(Backend & os): 
  ObjectOps<serializers::Agent>(os), 
  m_setupDone(false), m_creationDone(false), m_nextId(0) {}

cta::objectstore::Agent::Agent(Backend & os, const std::string & typeName): 
  ObjectOps<serializers::Agent>(os), 
  m_setupDone(false), m_creationDone(false), m_nextId(0) {
  setup(typeName);
}

void cta::objectstore::Agent::setup(const std::string & typeName) {
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
  ObjectOps<serializers::Agent>::setName(aid.str());
  m_setupDone = true;
}

void cta::objectstore::Agent::create() {
  if (!m_setupDone)
    throw SetupNotDone("In Agent::create(): setup() not yet done");
  RootEntry re(*this);
  AgentRegister ar(re.allocateOrGetAgentRegister(*this), *this);
  ar.addIntendedElement(selfName(), *this);
  serializers::Agent as;
  as.set_heartbeat(0);
  writeChild(selfName(), as);
  ar.upgradeIntendedElementToActual(selfName(), *this);
  m_creationDone = true;
}

std::string cta::objectstore::Agent::type() {
  if (!m_setupDone)
    throw SetupNotDone("In Agent::type(): setup() not yet done");
  return m_typeName;
}

std::string cta::objectstore::Agent::name() {
  if (!m_setupDone)
    throw SetupNotDone("In Agent::name(): setup() not yet done");
  return selfName();
}

cta::objectstore::Agent::~Agent() {
  if (m_creationDone) {
    try {
      remove();
      RootEntry re(*this);
      AgentRegister ar(re.getAgentRegister(*this), *this);
      ar.removeElement(selfName(),*this);
    } catch (std::exception&) {
    }
  }
}

std::string cta::objectstore::Agent::nextId(const std::string & childType) {
  if (!m_setupDone)
    throw SetupNotDone("In Agent::nextId(): setup() not yet done");
  std::stringstream id;
  id << childType << "-" << name() << "-" << m_nextId++;
  return id.str();
}

void cta::objectstore::Agent::addToOwnership(std::string name) {
  if (!m_creationDone)
    throw CreationNotDone("In Agent::addToOwnership(): creation() not yet done");
  serializers::Agent as;
  lockExclusiveAndRead(as);
  std::string * owned = as.mutable_ownedobjects()->Add();
  *owned = name;
  write(as);
  unlock();
}

void cta::objectstore::Agent::removeFromOwnership(std::string name) {
  if (!m_creationDone)
    throw CreationNotDone("In Agent::removeFromOwnership(): creation() not yet done");
  serializers::Agent as;
  lockExclusiveAndRead(as);
  bool found;
  do {
    found = false;
    for (int i=0; i<as.mutable_ownedobjects()->size(); i++) {
      if (name == *as.mutable_ownedobjects(i)) {
        found = true;
        as.mutable_ownedobjects()->SwapElements(i, as.mutable_ownedobjects()->size()-1);
        as.mutable_ownedobjects()->RemoveLast();
        break;
      }
    }
  } while (found);
  write(as);
  unlock();
}

std::list<std::string> 
  cta::objectstore::Agent::getOwnershipLog() {
  if (!m_creationDone)
    throw CreationNotDone("In Agent::getIntentLog(): creation() not yet done");
  serializers::Agent as;
  updateFromObjectStore(as);
  std::list<std::string> ret;
  for (int i=0; i<as.ownedobjects_size(); i++) {
    ret.push_back(as.ownedobjects(i));
  }
  return ret;
}

cta::objectstore::Backend & cta::objectstore::Agent::objectStore() {
  return ObjectOps<serializers::Agent>::objectStore();
}

void cta::objectstore::Agent::heartbeat(Agent& agent) {
  serializers::Agent as;
  lockExclusiveAndRead(as);
  as.set_heartbeat(as.heartbeat()+1);
  write(as);
  unlock();
}

uint64_t cta::objectstore::Agent::getHeartbeatCount(Agent& agent) {
  serializers::Agent as;
  updateFromObjectStore(as);
  return as.heartbeat();
}
