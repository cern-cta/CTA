/*
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

#include "common/threading/Mutex.hpp"
#include <atomic>
#include <string>

namespace cta { namespace objectstore {

/**
 * A class allowing the passing of the address of an Agent object, plus a thread safe
 * object name generator, that will allow unique name generation by several threads.
 * This object should be created once and for all per session (as the corresponding
 * Agent object in the object store).
 * A process 
 */
class AgentReference {
public:
  /**
   * Constructor will implicitly generate the address of the Agent object.
   * @param clientType is an indicative string used to generate the agent object's name.
   */
  AgentReference(const std::string &clientType);
  
  /**
   * Generates a unique address for a newly created child object. This function is thread
   * safe.
   * @param childType the name of the child object type.
   * @return a unique address for the child object, derived from the agent's address.
   */
  std::string nextId(const std::string & childType);
  
  /**
   * Gets the address of the Agent object generated on construction.
   * @return the agent object address.
   */
  std::string getAgentAddress();
private:
  std::atomic<uint64_t> m_nextId;
  std::string m_agentAddress;
};

}} 
