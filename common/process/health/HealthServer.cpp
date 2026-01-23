/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "HealthServer.hpp"

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
    // Don't start listening if stop was requested before we could even start
    if (st.stop_requested()) {
      return;
    }
    m_lc.log(log::INFO, "In HealthServer::run(): starting health server");
    // 127.0.0.1 ensures we only accept connections coming from localhost
    m_server.listen(m_host, m_port);
  } catch (std::exception& ex) {
    log::ScopedParamContainer exParams(m_lc);
    exParams.add("exceptionMessage", ex.what());
    m_lc.log(log::ERR, "In HealthServer::run(): received a std::exception.");
    throw ex;
  } catch (...) {
    m_lc.log(log::ERR, "In HealthServer::run(): received an unknown exception.");
    throw;
  }
}

}  // namespace cta::common
