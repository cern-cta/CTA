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

#include "AgentReference.hpp"
#include "common/threading/Thread.hpp"
#include "common/log/Logger.hpp"

#include <future>
#include <chrono>

namespace cta {
namespace objectstore {

/**
 * Thread handler managing a heartbeat updated for the agent object representing
 * the process. Just needs to be started and stopped.
 */
class AgentHeartbeatThread : private cta::threading::Thread {
public:
  /**
   * Constructor
   * @param agentReference reference to the agent object.
   */
  AgentHeartbeatThread(AgentReference& agentReference, Backend& backend, log::Logger& logger) :
  m_backend(backend),
  m_agentReference(agentReference),
  m_logger(logger) {}

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
  Backend& m_backend;

  /// Reference to the agent
  AgentReference& m_agentReference;

  /// The thread's run function
  void run() override;

  /// A promise used for graceful exit.
  std::promise<void> m_exit;

  /// The heartbeat update rate.
  std::chrono::seconds const m_heartRate = std::chrono::seconds(30);

  /// The heartbeat update deadline.
  std::chrono::seconds const m_heartbeatDeadline = std::chrono::seconds(60);

  /// The logging context
  log::Logger& m_logger;
};

}  // namespace objectstore
}  // namespace cta