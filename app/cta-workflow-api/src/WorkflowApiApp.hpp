/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "WorkflowApiConfig.hpp"
#include "common/log/Logger.hpp"

#include <condition_variable>
#include <mutex>

namespace cta::workflow_api {

/**
 * @brief The Workflow API application: serves the workflow gRPC services until stop() is called.
 */
class WorkflowApiApp final {
public:
  WorkflowApiApp() = default;

  ~WorkflowApiApp() = default;

  /**
   * @brief Requests the server to shut down. Safe to call from a signal handler thread.
   */
  void stop();

  /**
   * @brief Main application loop
   */
  int run(const WorkflowApiConfig& config, cta::log::Logger& log);

private:
  bool m_stopRequested = false;  //!< Guarded by m_mutex
  std::mutex m_mutex;
  std::condition_variable m_stopCv;
};

}  // namespace cta::workflow_api
