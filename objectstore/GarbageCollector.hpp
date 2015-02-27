#pragma once

#include "GenericObject.hpp"
#include "Agent.hpp"
#include "AgentWatchdog.hpp"
#include "AgentRegister.hpp"

/**
 * Plan => Garbage collector keeps track of the agents.
 * If an agent is declared dead => tape ownership of owned objects
 * Using the backup owner, re-post the objet to the container.
 * All containers will have a "repost" method, which is more thourough 
 * (and expensive) than the usual one. It can for example prevent double posting.
 */

namespace cta { namespace objectstore {

class GarbageCollector {
public:
  GarbageCollector(Backend & os, Agent & agent);
  
  void runOnePass();
  
  void aquireTargets();
  
  void trimGoneTargets();
  
  void checkHeartbeats();
  
  void cleanupDeadAgent(const std::string & name);
  
  void reinjectOwnedObject();
  
  void setTimeout(double timeout);
private:
  Backend & m_objectStore;
  Agent & m_ourAgent;
  AgentRegister m_agentRegister;
  std::map<std::string, AgentWatchdog * > m_watchedAgents;
  static const size_t c_maxWatchedAgentsPerGC;
  double m_timeout;
};
  
}}