/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "AgentWrapper.hpp"

namespace cta::objectstore {

AgentWrapper::AgentWrapper(Agent& agent) : m_agent(agent) {}

/**
* Adds an object address to the current agent. The additions and removals
* are queued in memory so that several threads can share the same access.
* The execution order is guaranteed.
* @param objectAddress
* @param backend reference to the backend to use.
*/
void AgentWrapper::addToOwnership(const std::string& objectAddress, objectstore::Backend& backend) {
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
void AgentWrapper::addBatchToOwnership(const std::list<std::string>& objectAdresses, objectstore::Backend& backend) {
  ScopedExclusiveLock sel(m_agent);
  m_agent.fetch();
  for (auto& address : objectAdresses) {
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
void AgentWrapper::removeFromOwnership(const std::string& objectAddress, objectstore::Backend& backend) {
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
void AgentWrapper::removeBatchFromOwnership(const std::list<std::string>& objectAdresses,
                                            objectstore::Backend& backend) {
  ScopedExclusiveLock sel(m_agent);
  m_agent.fetch();
  for (auto& address : objectAdresses) {
    m_agent.removeFromOwnership(address);
  }
  m_agent.commit();
  sel.release();
}

std::string AgentWrapper::getAgentAddress() {
  return m_agent.getAddressIfSet();
}

}  // namespace cta::objectstore
