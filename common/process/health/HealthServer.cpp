/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "HealthServer.hpp"

#include "common/exception/Errnum.hpp"

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
      m_livenessFunc(livenessFunc) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
HealthServer::~HealthServer() {
  // Gracefully shutdown the server
  stop();
}

//------------------------------------------------------------------------------
// HealthServer::start
//------------------------------------------------------------------------------
void HealthServer::start() {
  m_thread = std::jthread([this](std::stop_token st) { run(st); });
}

//------------------------------------------------------------------------------
// HealthServer::stop
//------------------------------------------------------------------------------
void HealthServer::stop() noexcept {
  m_lc.log(log::INFO, "In HealthServer::stop(): stopping health server");
  if (m_thread.joinable()) {
    m_thread.request_stop();
    m_server.stop();
  }
  m_lc.log(log::INFO, "In HealthServer::stop(): health server stopped");
}

//------------------------------------------------------------------------------
// HealthServer::run
//------------------------------------------------------------------------------
void HealthServer::run(std::stop_token st) {
  try {
    m_server.Get("/health/ready", [readinessFunc = m_readinessFunc](const httplib::Request&, httplib::Response& res) {
      if (readinessFunc()) {
        res.status = 200;
        res.set_content("ok\n", "text/plain");
      } else {
        res.status = 503;
        res.set_content("not ready\n", "text/plain");
      }
    });

    m_server.Get("/health/live", [livenessFunc = m_livenessFunc](const httplib::Request&, httplib::Response& res) {
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
    m_server.set_socket_options([](int sock) {
      int opt = 0;
      cta::exception::Errnum::throwOnNonZero(::setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)),
                                             "Failed to disable SO_REUSEPORT");
    });

    // Don't start listening if stop was requested before we could even start
    if (st.stop_requested()) {
      return;
    }
    m_lc.log(log::INFO, "In HealthServer::run(): starting health server");
    if (!m_server.listen(m_host, m_port)) {
      log::ScopedParamContainer params(m_lc);
      params.add("host", m_host);
      params.add("port", m_port);
      m_lc.log(log::ERR, "In HealthServer::run(): listen() failed. Port potentially already in use.");
      return;
    }
  } catch (std::exception& ex) {
    log::ScopedParamContainer exParams(m_lc);
    exParams.add("exceptionMessage", ex.what());
    m_lc.log(log::ERR, "In HealthServer::run(): received an exception.");
    throw ex;
  } catch (...) {
    m_lc.log(log::ERR, "In HealthServer::run(): received an unknown exception.");
    throw;
  }
}

}  // namespace cta::common
