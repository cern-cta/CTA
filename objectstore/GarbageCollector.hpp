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