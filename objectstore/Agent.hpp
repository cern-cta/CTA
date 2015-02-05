#pragma once

#include "ObjectStoreChoice.hpp"
#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include "utils/Timer.hpp"
#include <cxxabi.h>

namespace cta { namespace objectstore {

/**
 * Class containing agent information and managing the update of the 
 * agent's persitent representation in the object store.
 * This object also manages the object id generator, and keeps track of
 * a ContextHandles
 * In all the agent is the case class for all actions.
 * It handles (in the base class):
 */

class Agent: protected ObjectOps<serializers::Agent> {
public:
  Agent(ObjectStore & os);
  
  Agent(ObjectStore & os, const std::string & typeName);
  
  void setup(const std::string & typeName);
  
  class SetupNotDone: public cta::exception::Exception {
  public:
    SetupNotDone(const std::string & w): cta::exception::Exception(w) {}
  };
  
  class CreationNotDone: public cta::exception::Exception {
  public:
    CreationNotDone(const std::string & w): cta::exception::Exception(w) {}
  };
  
  class ObserverOnly: public cta::exception::Exception {
  public:
    ObserverOnly(const std::string & w): cta::exception::Exception(w) {}
  };
  
  void create();
  
  std::string type();
  
  std::string name();
  
  void flushContexts();
  
  ~Agent();
  
  std::string nextId(const std::string & childType);
  
  ContextHandle & getFreeContext();
  
  class ScopedIntent {
  public:
    ScopedIntent(Agent & agent, std::string container, std::string name, serializers::ObjectType objectType):
    m_agent(agent), m_container(container), m_name(name), m_objectType(objectType), m_present(false) {
      m_agent.addToIntend(m_container, m_name, m_objectType);
      m_present = true;
    }
    void removeFromIntent() {
      if(!m_present) return;
      m_agent.removeFromIntent(m_container, m_name, m_objectType);
      m_present = false;
    }
    ~ScopedIntent() {
      try {
        removeFromIntent();
      } catch (abi::__forced_unwind &) {
        throw;
      } catch (...) {}
    }
  private:
    Agent & m_agent;
    std::string m_container;
    std::string m_name;
    serializers::ObjectType m_objectType;
    bool m_present;
  };
  
  class ScopedOwnership {
  public:
    ScopedOwnership(Agent & agent, std::string name, serializers::ObjectType objectType):
    m_agent(agent), m_name(name), m_objectType(objectType), m_present(false) {
      m_agent.addToOwnership(m_name, m_objectType);
      m_present = true;
    }
    void removeFromOwnership() {
      if(!m_present) return;
      m_agent.removeFromOwnership( m_name, m_objectType);
      m_present = false;
    }
    ~ScopedOwnership() {
      try {
        removeFromOwnership();
      } catch (abi::__forced_unwind &) {
        throw;
      } catch (...) {}
    }
  private:
    Agent & m_agent;
    std::string m_name;
    serializers::ObjectType m_objectType;
    bool m_present;
  };
  
  void addToIntend (std::string container, std::string name, serializers::ObjectType objectType);
  
  void removeFromIntent (std::string container, std::string name, serializers::ObjectType objectType);
  
  void addToOwnership(std::string name, serializers::ObjectType objectType);
  
  void removeFromOwnership(std::string name, serializers::ObjectType objectType);
  
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
  
  std::list<intentEntry> getIntentLog();
  
  std::list<ownershipEntry> getOwnershipLog();
  
  ObjectStore & objectStore();
  
  std::string dump(Agent & agent);
  
  void heartbeat(Agent & agent);
  
  uint64_t getHeartbeatCount(Agent & agent);
  
private:
  std::string m_typeName;
  bool m_setupDone;
  bool m_creationDone;
  bool m_observerVersion;
  uint64_t m_nextId;
  static const size_t c_handleCount = 100;
  ContextHandle m_contexts[c_handleCount];
};
  
}}