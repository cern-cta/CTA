/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "HealthServer.hpp"

#include "common/exception/Errnum.hpp"
#include "common/exception/TimeOut.hpp"
#include "common/utils/utils.hpp"

#include <httplib.h>

namespace cta::common {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
HealthServer::HealthServer(cta::log::Logger& log,
                           const std::string& host,
                           int port,
                           const std::function<bool()>& readinessFunc,
                           const std::function<bool()>& livenessFunc,
                           int listenTimeoutMsec)
    : m_host(host),
      m_port(port),
      m_readinessFunc(readinessFunc),
      m_livenessFunc(livenessFunc),
      m_listenTimeoutMsec(listenTimeoutMsec),
      m_lc(log::LogContext(log)) {
  if (isUdsHost(m_host)) {
    m_lc.log(log::INFO, "In HealthServer::HealthServer(): Unix Domain Socket detected. Ignoring port value.");
    m_port = 80;  // technically the port shouldn't be used but the httplib example uses port 80
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
HealthServer::~HealthServer() {
  // Gracefully shutdown the server
  stop();
}

//------------------------------------------------------------------------------
// HealthServer::isUdsHost
//------------------------------------------------------------------------------
bool HealthServer::isUdsHost(const std::string& host) {
  return host.ends_with(".sock");
}

//------------------------------------------------------------------------------
// HealthServer::start
//------------------------------------------------------------------------------
void HealthServer::start() {
  m_server = std::make_unique<httplib::Server>();
  m_server->Get("/health/ready", [readinessFunc = m_readinessFunc](const httplib::Request&, httplib::Response& res) {
    if (readinessFunc()) {
      res.status = 200;
      res.set_content("ok\n", "text/plain");
    } else {
      res.status = 503;
      res.set_content("not ready\n", "text/plain");
    }
  });

  m_server->Get("/health/live", [livenessFunc = m_livenessFunc](const httplib::Request&, httplib::Response& res) {
    if (livenessFunc()) {
      res.status = 200;
      res.set_content("ok\n", "text/plain");
    } else {
      res.status = 503;
      res.set_content("not live\n", "text/plain");
    }
  });

  // Prevent multiple processes being able to listen on the same port
  // So explicitly disable SO_REUSEPORT
  m_server->set_socket_options([](int sock) {
    int opt = 0;
    cta::exception::Errnum::throwOnNonZero(::setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)),
                                           "Failed to disable SO_REUSEPORT");
  });

  // Start listening on a separate thread, because the listen() call is blocking
  m_thread = std::jthread([this]() { run(*m_server, m_host, m_port, m_lc.logger()); });
  // Block until the server actually started listening
  try {
    utils::waitForCondition([&]() { return isRunning(); }, m_listenTimeoutMsec);
  } catch (cta::exception::TimeOut& ex) {
    m_lc.log(log::ERR, "In HealthServer::start(): failed to start healthServer");
    stop();
  }
}

//------------------------------------------------------------------------------
// HealthServer::stop
//------------------------------------------------------------------------------
void HealthServer::stop() noexcept {
  m_lc.log(log::INFO, "In HealthServer::stop(): stopping health server");
  if (m_thread.joinable()) {
    if (m_server) {
      m_server->stop();
    }
    try {
      m_thread.join();
    } catch (std::system_error& e) {
      log::ScopedParamContainer params(m_lc);
      params.add("exceptionMessage", e.what());
      m_lc.log(log::ERR, "In HealthServer::stop(): failed to join thread");
    }
  }
  m_lc.log(log::INFO, "In HealthServer::stop(): health server stopped");
}

//------------------------------------------------------------------------------
// HealthServer::isRunning
//------------------------------------------------------------------------------
bool HealthServer::isRunning() const {
  if (!m_server) {
    return false;
  }
  return m_server->is_running();
}

//------------------------------------------------------------------------------
// HealthServer::run
//------------------------------------------------------------------------------
void HealthServer::run(httplib::Server& server, std::string host, int port, cta::log::Logger& log) {
  // LogContext is not thread safe, which is why we pass in the logger and not the logcontext
  cta::log::LogContext lc(log);
  try {
    lc.log(log::INFO, "In HealthServer::run(): starting health server");
    bool listenSuccess;
    if (isUdsHost(host)) {
      // Unix domain socket
      listenSuccess = server.set_address_family(AF_UNIX).listen(host, port);
    } else {
      // Regular server
      listenSuccess = server.listen(host, port);
    }
    if (!listenSuccess) {
      log::ScopedParamContainer params(lc);
      params.add("host", host);
      params.add("port", port);
      lc.log(log::ERR, "In HealthServer::run(): listen() failed. Port potentially already in use");
      return;
    }
  } catch (std::exception& ex) {
    log::ScopedParamContainer exParams(lc);
    exParams.add("exceptionMessage", ex.what());
    lc.log(log::ERR, "In HealthServer::run(): received an exception");
  } catch (...) {
    lc.log(log::ERR, "In HealthServer::run(): received an unknown exception");
  }
  lc.log(log::INFO, "In HealthServer::run(): HealthServer stopped listening");
}

}  // namespace cta::common
