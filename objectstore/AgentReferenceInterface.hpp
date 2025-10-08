/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef AGENTREFERENCEDECORATOR_HPP
#define AGENTREFERENCEDECORATOR_HPP

#include "Backend.hpp"
#include <string>


namespace cta::objectstore {

class AgentReferenceInterface {
public:
  AgentReferenceInterface() = default;
  virtual ~AgentReferenceInterface() = default;
  /**
   * Adds an object address to the referenced agent. The additions and removals
   * are queued in memory so that several threads can share the same access.
   * The execution order is guaranteed.
   * @param objectAddress
   * @param backend reference to the backend to use.
   */
  virtual void addToOwnership(const std::string &objectAddress, objectstore::Backend& backend) = 0;

  /**
   * Adds a list of object addresses to the referenced agent. The addition is immediate.
   * @param objectAdresses
   * @param backend reference to the backend to use.
   */
  virtual void addBatchToOwnership(const std::list<std::string> &objectAdresses, objectstore::Backend& backend) = 0;

  /**
   * Removes an object address from the referenced agent. The additions and removals
   * are queued in memory so that several threads can share the same access.
   * The execution order is guaranteed.
   * @param objectAddress
   */
  virtual void removeFromOwnership(const std::string &objectAddress, objectstore::Backend& backend) = 0;

  /**
   * Removes a list of object addresses to the referenced agent. The removal is immediate.
   * @param objectAdresses
   * @param backend reference to the backend to use.
   */
  virtual void removeBatchFromOwnership(const std::list<std::string> &objectAdresses, objectstore::Backend& backend) = 0;

  /**
   * Returns the agent address
   * @return the agent address
   */
  virtual std::string getAgentAddress() = 0;
};

} // namespace cta::objectstore

#endif /* AGENTREFERENCEDECORATOR_HPP */

