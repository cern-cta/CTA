/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
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

#include "AgentWrapper.hpp"

namespace cta { namespace objectstore {

AgentWrapper::AgentWrapper(Agent& agent):m_agent(agent) {
}

AgentWrapper::~AgentWrapper() {
}

/**
* Adds an object address to the current agent. The additions and removals
* are queued in memory so that several threads can share the same access.
* The execution order is guaranteed.
* @param objectAddress
* @param backend reference to the backend to use.
*/
void AgentWrapper::addToOwnership(const std::string &objectAddress, objectstore::Backend& backend){
  ScopedExclusiveLock sel(m_agent);
  m_agent.fetch();
  m_agent.addToOwnership(objectAddress);
  m_agent.commit();
  sel.release();
}

/**
* Adds a list of object addresses to the current agent. The addition is immediate.
* @param objectAdresses
* @param backend reference to the backend to use.
*/
void AgentWrapper::addBatchToOwnership(const std::list<std::string> &objectAdresses, objectstore::Backend& backend){
  ScopedExclusiveLock sel(m_agent);
  m_agent.fetch();
  for(auto& address: objectAdresses){
    m_agent.addToOwnership(address);
  }
  m_agent.commit();
  sel.release();
}

/**
* Removes an object address from the current agent. The additions and removals
* are queued in memory so that several threads can share the same access.
* The execution order is guaranteed.
* @param objectAddress
*/
void AgentWrapper::removeFromOwnership(const std::string &objectAddress, objectstore::Backend& backend){
  ScopedExclusiveLock sel(m_agent);
  m_agent.fetch();
  m_agent.removeFromOwnership(objectAddress);
  m_agent.commit();
  sel.release();
}

/**
* Removes a list of object addresses to the current agent. The removal is immediate.
* @param objectAdresses
* @param backend reference to the backend to use.
*/
void AgentWrapper::removeBatchFromOwnership(const std::list<std::string> &objectAdresses, objectstore::Backend& backend){
  ScopedExclusiveLock sel(m_agent);
  m_agent.fetch();
  for(auto& address: objectAdresses){
    m_agent.removeFromOwnership(address);
  }
  m_agent.commit();
  sel.release();
}

std::string AgentWrapper::getAgentAddress(){
  return m_agent.getAddressIfSet();
}

}}