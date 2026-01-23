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
                           int port,
                           const std::function<bool()>& readinessFunc,
                           const std::function<bool()>& livenessFunc)
    : m_lc(lc),
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
  m_thread = std::jthread(&HealthServer::run, this);
}

//------------------------------------------------------------------------------
// HealthServer::stop
//------------------------------------------------------------------------------
void HealthServer::stop() noexcept {
  m_lc.log(log::INFO, "In HealthServer::stop(): stopping health server");
  if (m_thread.joinable()) {
    m_server.stop();
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
// HealthServer::run
//------------------------------------------------------------------------------
void HealthServer::run() {
  try {
    auto readinessFunc = m_readinessFunc;
    m_server.Get("/health/ready", [readinessFunc](const httplib::Request&, httplib::Response& res) {
      if (readinessFunc()) {
        res.status = 200;
        res.set_content("ok\n", "text/plain");
      } else {
        res.status = 503;
        res.set_content("not ready\n", "text/plain");
      }
    });

    auto livenessFunc = m_livenessFunc;
    m_server.Get("/health/live", [livenessFunc](const httplib::Request&, httplib::Response& res) {
      if (livenessFunc()) {
        res.status = 200;
        res.set_content("ok\n", "text/plain");
      } else {
        res.status = 503;
        res.set_content("not live\n", "text/plain");
      }
    });

    m_lc.log(log::INFO, "In HealthServer::run(): starting health server");
    m_server.listen("0.0.0.0", m_port);
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
