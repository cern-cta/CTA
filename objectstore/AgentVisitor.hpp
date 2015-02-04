#pragma once

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include <string>

namespace cta { namespace objectstore {

  
class AgentVisitor: private ObjectOps<serializers::Agent> {
public:
  AgentVisitor(const std::string & name, Agent & agent):
  ObjectOps<serializers::Agent>(agent.objectStore(), name)
  {
    // check the presence of the entry
    serializers::Agent as;
    updateFromObjectStore(as, agent.getFreeContext());
  }
  
  std::string name(Agent & agent) {
    serializers::Agent as;
    updateFromObjectStore(as, agent.getFreeContext());
    return as.name();
  }
  
  void removeFromIntent (std::string container, std::string name, 
    serializers::ObjectType objectType, Agent & agent) {
    ContextHandle & context = agent.getFreeContext();
    serializers::Agent as;
    lockExclusiveAndRead(as, context, __func__);
    bool found;
    do {
      found = false;
      for (int i=0; i<as.mutable_creationintent()->size(); i++) {
        if (container == as.mutable_creationintent(i)->container() &&
            name == as.mutable_creationintent(i)->name() &&
            objectType == as.mutable_creationintent(i)->type()) {
          found = true;
          as.mutable_creationintent()->SwapElements(i, as.creationintent_size()-1);
          as.mutable_creationintent()->RemoveLast();
          break;
        }
      }
    } while (found);
    write(as);
    unlock(context);
  }

  void removeFromOwnership(std::string name, serializers::ObjectType objectType, Agent & agent) {
    serializers::Agent as;
    ContextHandle & context = agent.getFreeContext();
    lockExclusiveAndRead(as, context, __func__);
    bool found;
    do {
      found = false;
      for (int i=0; i<as.mutable_ownershipintent()->size(); i++) {
        if (name == as.mutable_ownershipintent(i)->name() &&
            objectType == as.mutable_ownershipintent(i)->type()) {
          found = true;
          as.mutable_creationintent()->SwapElements(i, as.ownershipintent_size()-1);
          as.mutable_creationintent()->RemoveLast();
          break;
        }
      }
    } while (found);
    write(as);
    unlock(context);
  }
  
  class intentEntry {
  public:
    intentEntry(const std::string & c,
                const std::string & n,
                serializers::ObjectType t):container(c), name(n), objectType(t) {}
    std::string container;
    std::string name;
    serializers::ObjectType objectType;
  };
    
  class ownershipEntry {
  public:
    ownershipEntry(const std::string & n,
                   serializers::ObjectType t):name(n), objectType(t) {}
    std::string name;
    serializers::ObjectType objectType;
  };
  
  std::list<intentEntry> getIntentLog(Agent & agent) {
    serializers::Agent as;
    updateFromObjectStore(as, agent.getFreeContext());
    std::list<intentEntry> ret;
    for (int i=0; i<as.creationintent_size(); i++) {
      ret.push_back(intentEntry(as.creationintent(i).container(),
                                as.creationintent(i).name(),
                                as.creationintent(i).type()));
    }
    return ret;
  }
  
  std::list<ownershipEntry> getOwnershipLog(Agent & agent) {
    serializers::Agent as;
    updateFromObjectStore(as, agent.getFreeContext());
    std::list<ownershipEntry> ret;
    for (int i=0; i<as.creationintent_size(); i++) {
      ret.push_back(ownershipEntry(as.creationintent(i).name(),
                                   as.creationintent(i).type()));
    }
    return ret;
  }
  
  void remove(Agent & agent) {
    removeOther(selfName());
  }
  
  uint64_t getHeartbeatCount(Agent& agent) {
    serializers::Agent as;
    updateFromObjectStore(as, agent.getFreeContext());
    return as.heartbeatcount();
  }
  
  std::string dump(Agent & agent) {
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
  
};

class AgentWatchdog {
public:
  AgentWatchdog(const std::string & agentName, Agent & agent):
  m_agentVisitor(agentName, agent) {
    m_hearbeatCounter = m_agentVisitor.getHeartbeatCount(agent);
  }
  
  bool checkAlive(Agent & agent) {
    uint64_t newHeartBeatCount = m_agentVisitor.getHeartbeatCount(agent);
    if (newHeartBeatCount == m_hearbeatCounter && m_timer.secs() > 0.1)
      return false;
    m_hearbeatCounter = newHeartBeatCount;
    return true;
  }
  
private:
  cta::utils::Timer m_timer;
  AgentVisitor m_agentVisitor;
  uint64_t m_hearbeatCounter;
};


}}