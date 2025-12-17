/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef AGENTWRAPPER_HPP
#define AGENTWRAPPER_HPP

#include "Agent.hpp"
#include "AgentReferenceInterface.hpp"

namespace cta::objectstore {

class AgentWrapper : public AgentReferenceInterface {
public:
  explicit AgentWrapper(Agent& agent);
  ~AgentWrapper() final = default;

  /**
   * Adds an object address to the referenced agent. The additions and removals
   * are queued in memory so that several threads can share the same access.
   * The execution order is guaranteed.
   * @param objectAddress
   * @param backend reference to the backend to use.
   */
  void addToOwnership(const std::string& objectAddress, objectstore::Backend& backend) override;

  /**
   * Adds a list of object addresses to the referenced agent. The addition is immediate.
   * @param objectAdresses
   * @param backend reference to the backend to use.
   */
  void addBatchToOwnership(const std::list<std::string>& objectAdresses, objectstore::Backend& backend) override;

  /**
   * Removes an object address from the referenced agent. The additions and removals
   * are queued in memory so that several threads can share the same access.
   * The execution order is guaranteed.
   * @param objectAddress
   */
  void removeFromOwnership(const std::string& objectAddress, objectstore::Backend& backend) override;

  /**
   * Removes a list of object addresses to the referenced agent. The removal is immediate.
   * @param objectAdresses
   * @param backend reference to the backend to use.
   */
  void removeBatchFromOwnership(const std::list<std::string>& objectAdresses, objectstore::Backend& backend) override;

  std::string getAgentAddress() override;

private:
  Agent& m_agent;
};

}  // namespace cta::objectstore

#endif /* AGENTWRAPPER_HPP */
