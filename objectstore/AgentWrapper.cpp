/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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