/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "AgentReference.hpp"
#include "common/threading/Thread.hpp"
#include "common/log/Logger.hpp"

#include <future>
#include <chrono>

namespace cta::objectstore {

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

  /// The heartbeat update deadline.
  std::chrono::seconds const m_heartbeatDeadline = std::chrono::seconds(120);

  /// The logging context
  log::Logger & m_logger;
};

} // namespace cta::objectstore
