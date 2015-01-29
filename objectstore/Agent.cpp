#include "Agent.hpp"
#include "AgentRegister.hpp"
#include "RootEntry.hpp"
#include <string>
#include <sstream>
#include <sys/syscall.h>
#include <ctime>

cta::objectstore::Agent::Agent(ObjectStore & os): 
  ObjectOps<serializers::Agent>(os), 
  m_nextId(0), m_setupDone(false), m_creationDone(false), m_observerVersion(false) {};

cta::objectstore::Agent::Agent(ObjectStore & os, const std::string & typeName): 
  ObjectOps<serializers::Agent>(os), 
  m_nextId(0), m_setupDone(false), m_creationDone(false), m_observerVersion(false) {
  setup(typeName);
}

// Passive constructor, used for looking at existing agent records
cta::objectstore::Agent::Agent(const std::string & name, Agent & agent):
  ObjectOps<serializers::Agent>(agent.objectStore(), name),
  m_nextId(0), m_setupDone(true), m_creationDone(true), m_observerVersion(true)
{
  // check the presence of the entry
  serializers::Agent as;
  updateFromObjectStore(as, agent.getFreeContext());
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
  as.set_name(selfName());
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
  for (size_t i=0; i < c_handleCount; i++) {
    m_contexts[i].release();
  }
  if (m_creationDone && !m_observerVersion) {
    try {
      remove();
      RootEntry re(*this);
      AgentRegister ar(re.getAgentRegister(*this), *this);
      ar.removeElement(selfName(),*this);
    } catch(...) {}
  }
}

std::string cta::objectstore::Agent::nextId(const std::string & childType) {
  if (!m_setupDone)
    throw SetupNotDone("In Agent::nextId(): setup() not yet done");
  if (m_observerVersion)
    throw ObserverOnly("In Agent::nextId(): this object is observer only");
  std::stringstream id;
  id << childType << "-" << name() << "-" << m_nextId++;
  return id.str();
}

cta::objectstore::ContextHandleImplementation<myOS> & 
  cta::objectstore::Agent::getFreeContext() {
  for (size_t i=0; i < c_handleCount; i++) {
    if (!m_contexts[i].isSet())
      return m_contexts[i];
  }
  throw cta::exception::Exception("Could not find free context slot");
}

void cta::objectstore::Agent::addToIntend (std::string container, std::string name, std::string typeName) {
  if (!m_creationDone)
    throw CreationNotDone("In Agent::addToIntend(): creation() not yet done");
  serializers::Agent as;
  ContextHandle & ctx = getFreeContext();
  lockExclusiveAndRead(as, ctx);
  serializers::ObjectCreationIntent * oca = 
    as.mutable_creationintent()->Add();
  oca->set_container(container);
  oca->set_name(name);
  oca->set_type(typeName);
  write(as);
  unlock(ctx);
}

void cta::objectstore::Agent::removeFromIntent (std::string container, std::string name, std::string typeName) {
  if (!m_creationDone)
    throw CreationNotDone("In Agent::removeFromIntent(): creation() not yet done");
  serializers::Agent as;
  ContextHandle & ctx = getFreeContext();
  lockExclusiveAndRead(as, ctx);
  bool found;
  do {
    found = false;
    for (int i=0; i<as.creationintent_size(); i++) {
      if (container == as.creationintent(i).container() &&
          name == as.creationintent(i).name() &&
          typeName == as.creationintent(i).type()) {
        found = true;
        as.mutable_creationintent()->SwapElements(i, as.creationintent_size()-1);
        as.mutable_creationintent()->RemoveLast();
        break;
      }
    }
  } while (found);
  write(as);
  unlock(ctx);
}

void cta::objectstore::Agent::addToOwnership(std::string name, std::string typeName) {
  if (!m_creationDone)
    throw CreationNotDone("In Agent::addToOwnership(): creation() not yet done");
  serializers::Agent as;
  ContextHandle & ctx = getFreeContext();
  lockExclusiveAndRead(as, ctx);
  serializers::ObjectOwnershipIntent * ooi = 
    as.mutable_ownershipintent()->Add();
  ooi->set_name(name);
  ooi->set_type(typeName);
  write(as);
  unlock(ctx);
}

void cta::objectstore::Agent::removeFromOwnership(std::string name, std::string typeName) {
  if (!m_creationDone)
    throw CreationNotDone("In Agent::removeFromOwnership(): creation() not yet done");
  serializers::Agent as;
  ContextHandle & ctx = getFreeContext();
  lockExclusiveAndRead(as, ctx);
  bool found;
  do {
    found = false;
    for (int i=0; i<as.ownershipintent_size(); i++) {
      if (name == as.ownershipintent(i).name() &&
          typeName == as.ownershipintent(i).type()) {
        found = true;
        as.mutable_creationintent()->SwapElements(i, as.ownershipintent_size()-1);
        as.mutable_creationintent()->RemoveLast();
        break;
      }
    }
  } while (found);
  write(as);
  unlock(ctx);
}

std::list<cta::objectstore::Agent::intentEntry> 
  cta::objectstore::Agent::getIntentLog() {
  if (!m_creationDone)
    throw CreationNotDone("In Agent::getIntentLog(): creation() not yet done");
  serializers::Agent as;
  updateFromObjectStore(as, getFreeContext());
  std::list<intentEntry> ret;
  for (int i=0; i<as.creationintent_size(); i++) {
    ret.push_back(intentEntry(as.creationintent(i).container(),
                              as.creationintent(i).name(),
                              as.creationintent(i).type()));
  }
  return ret;
}

std::list<cta::objectstore::Agent::ownershipEntry>
  cta::objectstore::Agent::getOwnershipLog() {
  if (!m_creationDone)
    throw CreationNotDone("In Agent::getOwnershipLog(): creation() not yet done");
  serializers::Agent as;
  updateFromObjectStore(as, getFreeContext());
  std::list<ownershipEntry> ret;
  for (int i=0; i<as.creationintent_size(); i++) {
    ret.push_back(ownershipEntry(as.creationintent(i).name(),
                                 as.creationintent(i).type()));
  }
  return ret;
}

cta::objectstore::ObjectStore & cta::objectstore::Agent::objectStore() {
  return ObjectOps<serializers::Agent>::objectStore();
}

std::string cta::objectstore::Agent::dump(Agent & agent) {
  serializers::Agent as;
  updateFromObjectStore(as, agent.getFreeContext());
  std::stringstream ret;
  ret<< "<<<< Agent " << selfName() << " dump start" << std::endl
    << "name=" << as.name() << std::endl
    << "Ownership intent size=" << as.ownershipintent_size() << std::endl;
  for (int i=0; i<as.ownershipintent_size(); i++) {
    ret << "ownershipIntent[" << i << "]: name=" << as.ownershipintent(i).name() 
        << " type=" << as.ownershipintent(i).type() << std::endl;
  }
  ret << "Creation intent size=" << as.creationintent_size() << std::endl;
  for (int i=0; i<as.creationintent_size(); i++) {
    ret << "creationIntent[" << i << "]: name=" << as.creationintent(i).name() 
        << " type=" << as.creationintent(i).type() 
        << " container=" << as.creationintent(i).container() << std::endl;
  }
  ret<< ">>>> Agent " << selfName() << " dump end" << std::endl;
  return ret.str();
}

void cta::objectstore::Agent::heartbeat(Agent& agent) {
  ContextHandle & context = agent.getFreeContext();
  serializers::Agent as;
  lockExclusiveAndRead(as, context);
}

uint64_t cta::objectstore::Agent::getHeartbeatCount(Agent& agent) {
  serializers::Agent as;
  updateFromObjectStore(as, agent.getFreeContext());
}


cta::objectstore::AgentWatchdog::AgentWatchdog(const std::string& agentName, Agent& agent):
m_agentVisitor(agentName, agent) {
  m_hearbeatCounter = m_agentVisitor.getHeartbeatCount(agent);
}

bool cta::objectstore::AgentWatchdog::checkAlive(Agent& agent) {
  uint64_t newHeartBeatCount = m_agentVisitor.getHeartbeatCount(agent);
  if (newHeartBeatCount == m_hearbeatCounter && m_timer.secs() > 0.1)
    return false;
  m_hearbeatCounter = newHeartBeatCount;
  return true;
}

