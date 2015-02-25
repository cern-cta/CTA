#pragma once

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include "utils/Timer.hpp"
#include <cxxabi.h>
#include <list>

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
  Agent(Backend & os);
  
  Agent(Backend & os, const std::string & typeName);
  
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
  
  class ScopedIntent {
  public:
    ScopedIntent(Agent & agent, std::string container, std::string name, serializers::ObjectType objectType):
    m_agent(agent), m_container(container), m_name(name), m_objectType(objectType), m_present(false) {
      m_agent.addToOwnership(m_name);
      m_present = true;
    }
    void removeFromIntent() {
      if(!m_present) return;
      m_agent.removeFromOwnership(m_name);
      m_present = false;
    }
    ~ScopedIntent() {
      try {
        removeFromIntent();
      } catch (std::exception &) {
      } catch (...) {throw;}
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
    ScopedOwnership(Agent & agent, std::string name):
    m_agent(agent), m_name(name), m_present(false) {
      m_agent.addToOwnership(m_name);
      m_present = true;
    }
    void removeFromOwnership() {
      if(!m_present) return;
      m_agent.removeFromOwnership( m_name);
      m_present = false;
    }
    ~ScopedOwnership() {
      try {
        removeFromOwnership();
      } catch (std::exception &) {
      } catch (...) {throw;}
    }
  private:
    Agent & m_agent;
    std::string m_name;
    bool m_present;
  };
  
  void addToOwnership(std::string name);
  
  void removeFromOwnership(std::string name);
  
  class ownedObject {
  public:
    ownedObject(const std::string & c,
                const std::string & n):container(c), name(n) {}
    std::string container;
    std::string name;
  };
  
  std::list<std::string> getOwnershipLog();
  
  Backend & objectStore();
  
  std::string dump(Agent & agent);
  
  void heartbeat(Agent & agent);
  
  uint64_t getHeartbeatCount(Agent & agent);
  
private:
  std::string m_typeName;
  bool m_setupDone;
  bool m_creationDone;
  uint64_t m_nextId;
};
  
}}