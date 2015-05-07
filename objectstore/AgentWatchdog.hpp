/**
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

#pragma once

#include "Agent.hpp"
#include "utils/Timer.hpp"

namespace cta { namespace objectstore {
class AgentWatchdog {
public:
  AgentWatchdog(const std::string & name, Backend & os): m_agent(name, os), 
    m_heartbeatCounter(readHeartbeat()), m_timeout(5.0) {}
  
  bool checkAlive() {
    uint64_t newHeartBeatCount = readHeartbeat();
    if (newHeartBeatCount == m_heartbeatCounter && m_timer.secs() > m_timeout)
      return false;
    m_heartbeatCounter = newHeartBeatCount;
    m_timer.reset();
    return true; 
  }
  
  void setTimeout(double timeout) {
    m_timeout = timeout;
  }
  
private:
  cta::utils::Timer m_timer;
  Agent m_agent;
  uint64_t m_heartbeatCounter;
  double m_timeout;
  
  uint64_t readHeartbeat() {
    ScopedSharedLock lock(m_agent);
    m_agent.fetch();
    return m_agent.getHeartbeatCount();
  }
};
    
}}
