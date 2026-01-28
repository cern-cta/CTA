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
HealthServer::HealthServer(cta::log::LogContext& lc,
                           const std::string& host,
                           int port,
                           const std::function<bool()>& readinessFunc,
                           const std::function<bool()>& livenessFunc)
    : m_lc(lc),
      m_host(host),
      m_port(port),
      m_readinessFunc(readinessFunc),
      m_livenessFunc(livenessFunc) {
  if (usesUDS()) {
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
// HealthServer::usesUDS
//------------------------------------------------------------------------------
bool HealthServer::usesUDS() const {
  return m_host.ends_with(".sock");
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
  m_thread = std::jthread([this](std::stop_token st) { run(st); });
  // Block until the server actually started listening
  try {
    utils::waitForCondition([&]() { return isRunning(); }, 1000);
  } catch (std::exception::TimeOut ex) {
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
void HealthServer::run() {
  try {
    m_lc.log(log::INFO, "In HealthServer::run(): starting health server");
    bool listenSuccess;
    if (usesUDS()) {
      // Unix domain socket
      listenSuccess = m_server->set_address_family(AF_UNIX).listen(m_host, m_port);
    } else {
      // Regular server
      listenSuccess = m_server->listen(m_host, m_port);
    }
    if (!listenSuccess) {
      log::ScopedParamContainer params(m_lc);
      params.add("host", m_host);
      params.add("port", m_port);
      m_lc.log(log::ERR, "In HealthServer::run(): listen() failed. Port potentially already in use");
      return;
    }
  } catch (std::exception& ex) {
    log::ScopedParamContainer exParams(m_lc);
    exParams.add("exceptionMessage", ex.what());
    m_lc.log(log::ERR, "In HealthServer::run(): received an exception");
  } catch (...) {
    m_lc.log(log::ERR, "In HealthServer::run(): received an unknown exception");
  }
  m_lc.log(log::INFO, "In HealthServer::run(): HealthServer stopped listening");
}

}  // namespace cta::common
