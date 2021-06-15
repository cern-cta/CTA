/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef AGENTREFERENCEDECORATOR_HPP
#define AGENTREFERENCEDECORATOR_HPP

#include "Backend.hpp"
#include <string>


namespace cta { namespace objectstore {

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

}}

#endif /* AGENTREFERENCEDECORATOR_HPP */

