/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
  BackendPopulator(cta::objectstore::Backend& be, const std::string& agentType, const cta::log::LogContext& lc);

  /**
   * Destructor
   */
  virtual ~BackendPopulator() noexcept;

  /**
   * Returns the agent
   *
   * @return the agent
   */
  cta::objectstore::AgentReference& getAgentReference();

  /**
   * allow leaving bahind non-empty agents.
   */
  void leaveNonEmptyAgentsBehind();

private:
  /**
   * The objectstore backend
   */
  cta::objectstore::Backend& m_backend;

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

}  // namespace cta::objectstore
