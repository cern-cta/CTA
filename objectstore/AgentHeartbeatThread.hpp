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

#include "AgentReference.hpp"
#include "common/threading/Thread.hpp"
#include "common/log/Logger.hpp"

#include <future>
#include <chrono>

namespace cta { namespace objectstore {

/**
 * Thread handler managing a heartbeat updated for the agent object representing
 * the process. Just needs to be started and stopped.
 */
class AgentHeartbeatThread: private cta::threading::Thread {
public:
  /**
   * Constructor
   * @param agentReference reference to the agent object.
   */
  AgentHeartbeatThread(AgentReference & agentReference, Backend & backend, log::Logger & logger):
    m_backend(backend) , m_agentReference(agentReference), m_logger(logger) {}
  
  /**
   * Start the thread
   */
  void startThread() { start(); }
  
  /**
   * Stop and wait for the thread (graceful shutdown)
   */
  void stopAndWaitThread();
  
private:
  /// Reference to the object store backend.
  Backend & m_backend;
  
  /// Reference to the agent
  AgentReference & m_agentReference;
  
  /// The thread's run function
  void run() override;
  
  /// A promise used for graceful exit.
  std::promise<void> m_exit;
  
  /// The heartbeat update rate.
  std::chrono::seconds const m_heartRate = std::chrono::seconds(30);
  
  /// The logging context
  log::Logger & m_logger;
};

}} // namespace cta::objectstore