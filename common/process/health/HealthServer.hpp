/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/config/Config.hpp"
#include "common/log/LogContext.hpp"

#include <functional>
#include <thread>

namespace httplib {
class Server;
}

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

  bool isRunning() const;

  bool usesUDS() const;

private:
  cta::log::LogContext& m_lc;
  // The thread the HealthServer will run on when start() is called
  std::jthread m_thread;

  // From a functional perspective, this doesn't need to be a pointer
  // However, it allows us to forward declare httplib::Server, preventing the expensive include in this header file
  std::unique_ptr<httplib::Server> m_server;
  const std::string m_host;
  int m_port;
  const std::function<bool()> m_readinessFunc;
  const std::function<bool()> m_livenessFunc;
};

}  // namespace cta::common
