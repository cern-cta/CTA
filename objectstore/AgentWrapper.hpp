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

#ifndef AGENTWRAPPER_HPP
#define AGENTWRAPPER_HPP

#include "AgentReferenceInterface.hpp"
#include "Agent.hpp"

namespace cta::objectstore {

class AgentWrapper: public AgentReferenceInterface {
public:
  explicit AgentWrapper(Agent& agent);
  virtual ~AgentWrapper() = default;
  
  /**
   * Adds an object address to the referenced agent. The additions and removals
   * are queued in memory so that several threads can share the same access.
   * The execution order is guaranteed.
   * @param objectAddress
   * @param backend reference to the backend to use.
   */
  void addToOwnership(const std::string &objectAddress, objectstore::Backend& backend) override;
  
  /**
   * Adds a list of object addresses to the referenced agent. The addition is immediate.
   * @param objectAdresses
   * @param backend reference to the backend to use.
   */
  void addBatchToOwnership(const std::list<std::string> &objectAdresses, objectstore::Backend& backend) override;
  
  /**
   * Removes an object address from the referenced agent. The additions and removals
   * are queued in memory so that several threads can share the same access.
   * The execution order is guaranteed.
   * @param objectAddress
   */
  void removeFromOwnership(const std::string &objectAddress, objectstore::Backend& backend) override;
  
  /**
   * Removes a list of object addresses to the referenced agent. The removal is immediate.
   * @param objectAdresses
   * @param backend reference to the backend to use.
   */
  void removeBatchFromOwnership(const std::list<std::string> &objectAdresses, objectstore::Backend& backend) override;
  
  std::string getAgentAddress() override;
  
private:
  Agent& m_agent;
};

} // namespace cta::objectstore

#endif /* AGENTWRAPPER_HPP */

