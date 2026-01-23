/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/config/Config.hpp"
#include "common/log/LogContext.hpp"

#include <functional>
#include <httplib.h>
#include <thread>

namespace cta::common {

/**
 * Allows a service to expose health endpoints for readiness/liveness probes. It does this in the form of a lightweight HTTP server.
 */
class HealthServer {
public:
  HealthServer(cta::log::LogContext& lc,
               const std::string& host,
               int port,
               const std::function<bool()>& readinessFunc,
               const std::function<bool()>& livenessFunc);

  ~HealthServer();

  /**
   * Starts the Health Server on a separate thread
   */
  void start();

  /**
   * Starts a lightweight HTTP server that listens to /health/readiness and /health/liveness endpoints
   */
  void run(std::stop_token st);

  /**
   * Stop the HealthServer
   */
  void stop() noexcept;

private:
  cta::log::LogContext& m_lc;
  // The thread the HealthServer will run on when start() is called
  std::jthread m_thread;

  httplib::Server m_server;
  std::string m_host;
  int m_port;
  std::function<bool()> m_readinessFunc;
  std::function<bool()> m_livenessFunc;
};

}  // namespace cta::common
