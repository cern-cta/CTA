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

#pragma once

#include "objectstore/Agent.hpp"
#include "objectstore/AgentReference.hpp"
#include "objectstore/Backend.hpp"

namespace cta::objectstore {

class BackendPopulator {
  
public:
  /**
   * Constructor
   * 
   * @param be The objectstore backend
   */
  BackendPopulator(cta::objectstore::Backend & be, const std::string &agentType, const cta::log::LogContext & lc);
  
  /**
   * Destructor
   */
  virtual ~BackendPopulator() throw();

  /**
   * Returns the agent
   * 
   * @return the agent
   */
  cta::objectstore::AgentReference & getAgentReference();
  
  /**
   * allow leaving bahind non-empty agents.
   */
  void leaveNonEmptyAgentsBehind();
  
private:
  /**
   * The objectstore backend
   */
  cta::objectstore::Backend & m_backend;
  
  /**
   * The agent
   */
  cta::objectstore::AgentReference m_agentReference;
  
  /**
   * A log context (copied) in order to be able to log in destructor.
   */
  cta::log::LogContext m_lc;
  
  /**
   * A switch allowing leaving behind a non-empty agent for garbage collection pickup.
   */
  bool m_leaveNonEmptyAgentBehind = false;
};

} // namespace cta::objectstore
