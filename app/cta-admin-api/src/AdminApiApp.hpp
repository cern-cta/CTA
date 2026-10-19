/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "AdminApiConfig.hpp"
#include "common/log/Logger.hpp"

#include <condition_variable>
#include <mutex>

namespace cta::admin_api {

/**
 * @brief The Admin API application: serves the admin gRPC services until stop() is called.
 */
class AdminApiApp final {
public:
  AdminApiApp() = default;

  ~AdminApiApp() = default;

  /**
   * @brief Requests the server to shut down. Safe to call from a signal handler thread.
   */
  void stop();

  /**
   * @brief Main application loop
   */
  int run(const AdminApiConfig& config, cta::log::Logger& log);

private:
  bool m_stopRequested = false;  //!< Guarded by m_mutex
  std::mutex m_mutex;
  std::condition_variable m_stopCv;
};

}  // namespace cta::admin_api
