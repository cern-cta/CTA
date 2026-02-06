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

namespace cta::runtime {

/**
 * Allows a service to expose health endpoints for readiness/liveness probes. It does this in the form of a lightweight HTTP server.
 */
class HealthServer final {
public:
  HealthServer(cta::log::Logger& log,
               const std::string& host,
               int port,
               const std::function<bool()>& readinessFunc,
               const std::function<bool()>& livenessFunc,
               int listenTimeoutMsec = 1000);

  ~HealthServer();

  /**
   * Starts the Health Server on a separate thread
   */
  void start();

  /**
   * Starts a lightweight HTTP server that listens to /health/readiness and /health/liveness endpoints.
   * As this function is intended to run in a separate thread, it is deliberately static,
   * so that all dependencies (which may or may not be thread safe) must be passed explicitly.
   */
  static void run(httplib::Server& server, std::string host, int port, cta::log::Logger& log);

  /**
   * Stop the HealthServer
   */
  void stop() noexcept;

  bool isRunning() const;

  static bool isUdsHost(std::string_view host);

private:
  cta::log::LogContext m_lc;
  // From a functional perspective, this doesn't need to be a pointer
  // However, it allows us to forward declare httplib::Server, preventing the expensive include in this header file
  std::unique_ptr<httplib::Server> m_server;
  const std::string m_host;
  int m_port;
  const std::function<bool()> m_readinessFunc;
  const std::function<bool()> m_livenessFunc;
  // Amount of time to wait for the server to start listening before we consider it a failure
  const int m_listenTimeoutMsec;

  // The thread the HealthServer will run on when start() is called
  std::jthread m_thread;
};

}  // namespace cta::runtime
