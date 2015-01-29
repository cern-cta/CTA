#include "Agent.hpp"
#include "AgentRegister.hpp"
#include "RootEntry.hpp"
#include <string>
#include <sstream>
#include <sys/syscall.h>
#include <ctime>

Agent::Agent(ObjectStore & os): 
  ObjectOps<cta::objectstore::Agent>(os), 
  m_nextId(0), m_setupDone(false), m_creationDone(false), m_observerVersion(false) {};

Agent::Agent(ObjectStore & os, const std::string & typeName): 
  ObjectOps<cta::objectstore::Agent>(os), 
  m_nextId(0), m_setupDone(false), m_creationDone(false), m_observerVersion(false) {
  setup(typeName);
}

// Passive constructor, used for looking at existing agent records
Agent::Agent(const std::string & name, Agent & agent):
  ObjectOps<cta::objectstore::Agent>(agent.objectStore(), name),
  m_nextId(0), m_setupDone(true), m_creationDone(true), m_observerVersion(true)
{
  // check the presence of the entry
  cta::objectstore::Agent as;
  updateFromObjectStore(as, agent.getFreeContext());
}

void Agent::setup(const std::string & typeName) {
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
  ObjectOps<cta::objectstore::Agent>::setName(aid.str());
  m_setupDone = true;
}

void Agent::create() {
  if (!m_setupDone)
    throw SetupNotDone("In Agent::create(): setup() not yet done");
  RootEntry re(*this);
  AgentRegister ar(re.allocateOrGetAgentRegister(*this), *this);
  ar.addIntendedElement(selfName(), *this);
  cta::objectstore::Agent as;
  as.set_name(selfName());
  writeChild(selfName(), as);
  ar.upgradeIntendedElementToActual(selfName(), *this);
  m_creationDone = true;
}

std::string Agent::type() {
  if (!m_setupDone)
    throw SetupNotDone("In Agent::type(): setup() not yet done");
  return m_typeName;
}

std::string Agent::name() {
  if (!m_setupDone)
    throw SetupNotDone("In Agent::name(): setup() not yet done");
  return selfName();
}

Agent::~Agent() {
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

std::string Agent::nextId(const std::string & childType) {
  if (!m_setupDone)
    throw SetupNotDone("In Agent::nextId(): setup() not yet done");
  if (m_observerVersion)
    throw ObserverOnly("In Agent::nextId(): this object is observer only");
  std::stringstream id;
  id << childType << "-" << name() << "-" << m_nextId++;
  return id.str();
}

ContextHandleImplementation<myOS> & Agent::getFreeContext() {
  for (size_t i=0; i < c_handleCount; i++) {
    if (!m_contexts[i].isSet())
      return m_contexts[i];
  }
  throw cta::exception::Exception("Could not find free context slot");
}

void Agent::addToIntend (std::string container, std::string name, std::string typeName) {
  if (!m_creationDone)
    throw CreationNotDone("In Agent::addToIntend(): creation() not yet done");
  cta::objectstore::Agent as;
  ContextHandle & ctx = getFreeContext();
  lockExclusiveAndRead(as, ctx);
  cta::objectstore::ObjectCreationIntent * oca = 
    as.mutable_creationintent()->Add();
  oca->set_container(container);
  oca->set_name(name);
  oca->set_type(typeName);
  write(as);
  unlock(ctx);
}

void Agent::removeFromIntent (std::string container, std::string name, std::string typeName) {
  if (!m_creationDone)
    throw CreationNotDone("In Agent::removeFromIntent(): creation() not yet done");
  cta::objectstore::Agent as;
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

void Agent::addToOwnership(std::string name, std::string typeName) {
  if (!m_creationDone)
    throw CreationNotDone("In Agent::addToOwnership(): creation() not yet done");
  cta::objectstore::Agent as;
  ContextHandle & ctx = getFreeContext();
  lockExclusiveAndRead(as, ctx);
  cta::objectstore::ObjectOwnershipIntent * ooi = 
    as.mutable_ownershipintent()->Add();
  ooi->set_name(name);
  ooi->set_type(typeName);
  write(as);
  unlock(ctx);
}

void Agent::removeFromOwnership(std::string name, std::string typeName) {
  if (!m_creationDone)
    throw CreationNotDone("In Agent::removeFromOwnership(): creation() not yet done");
  cta::objectstore::Agent as;
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

std::list<Agent::intentEntry> Agent::getIntentLog() {
  if (!m_creationDone)
    throw CreationNotDone("In Agent::getIntentLog(): creation() not yet done");
  cta::objectstore::Agent as;
  updateFromObjectStore(as, getFreeContext());
  std::list<intentEntry> ret;
  for (int i=0; i<as.creationintent_size(); i++) {
    ret.push_back(intentEntry(as.creationintent(i).container(),
                              as.creationintent(i).name(),
                              as.creationintent(i).type()));
  }
  return ret;
}

std::list<Agent::ownershipEntry> Agent::getOwnershipLog() {
  if (!m_creationDone)
    throw CreationNotDone("In Agent::getOwnershipLog(): creation() not yet done");
  cta::objectstore::Agent as;
  updateFromObjectStore(as, getFreeContext());
  std::list<ownershipEntry> ret;
  for (int i=0; i<as.creationintent_size(); i++) {
    ret.push_back(ownershipEntry(as.creationintent(i).name(),
                                 as.creationintent(i).type()));
  }
  return ret;
}

ObjectStore & Agent::objectStore() {
  return ObjectOps<cta::objectstore::Agent>::objectStore();
}

std::string Agent::dump(Agent & agent) {
  cta::objectstore::Agent as;
  updateFromObjectStore(as, agent.getFreeContext());
  std::stringstream ret;
  ret<< "<<<< Agent " << selfName() << " dump start" << std::endl
    << "name=" << as.name()
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
  ret<< ">>>> AgentRegister " << selfName() << " dump end" << std::endl;
  return ret.str();
}