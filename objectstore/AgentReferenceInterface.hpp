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

#ifndef AGENTREFERENCEDECORATOR_HPP
#define AGENTREFERENCEDECORATOR_HPP

#include "Backend.hpp"
#include <string>

namespace cta {
namespace objectstore {

class AgentReferenceInterface {
public:
  AgentReferenceInterface();
  virtual ~AgentReferenceInterface();
  /**
   * Adds an object address to the referenced agent. The additions and removals
   * are queued in memory so that several threads can share the same access.
   * The execution order is guaranteed.
   * @param objectAddress
   * @param backend reference to the backend to use.
   */
  virtual void addToOwnership(const std::string& objectAddress, objectstore::Backend& backend) = 0;

  /**
   * Adds a list of object addresses to the referenced agent. The addition is immediate.
   * @param objectAdresses
   * @param backend reference to the backend to use.
   */
  virtual void addBatchToOwnership(const std::list<std::string>& objectAdresses, objectstore::Backend& backend) = 0;

  /**
   * Removes an object address from the referenced agent. The additions and removals
   * are queued in memory so that several threads can share the same access.
   * The execution order is guaranteed.
   * @param objectAddress
   */
  virtual void removeFromOwnership(const std::string& objectAddress, objectstore::Backend& backend) = 0;

  /**
   * Removes a list of object addresses to the referenced agent. The removal is immediate.
   * @param objectAdresses
   * @param backend reference to the backend to use.
   */
  virtual void removeBatchFromOwnership(const std::list<std::string>& objectAdresses,
                                        objectstore::Backend& backend) = 0;

  /**
   * Returns the agent address
   * @return the agent address
   */
  virtual std::string getAgentAddress() = 0;
};

}  // namespace objectstore
}  // namespace cta

#endif /* AGENTREFERENCEDECORATOR_HPP */
