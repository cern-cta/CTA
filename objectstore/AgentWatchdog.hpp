#pragma once

#include "Agent.hpp"
#include "utils/Timer.hpp"

namespace cta { namespace objectstore {
class AgentWatchdog {
public:
  AgentWatchdog(Agent & agent): m_agent(agent), 
    m_heartbeatCounter(readHeartbeat()) {}
  
  bool checkAlive() {
    uint64_t newHeartBeatCount = readHeartbeat();
  if (newHeartBeatCount == m_heartbeatCounter && m_timer.secs() > 5)
      return false;
    m_heartbeatCounter = newHeartBeatCount;
    m_timer.reset();
    return true; 
  }
private:
  cta::utils::Timer m_timer;
  Agent & m_agent;
  uint64_t m_heartbeatCounter;
  
  uint64_t readHeartbeat() {
    ScopedSharedLock lock(m_agent);
    m_agent.fetch();
    return m_agent.getHeartbeatCount();
  }
};
    
}}
