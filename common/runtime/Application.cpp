/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Application.hpp"

#include "common/log/FileLogger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/process/health/HealthServer.hpp"
#include "common/process/signals/SignalReactor.hpp"
#include "common/process/signals/SignalReactorBuilder.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/OtelInit.hpp"
#include "common/utils/utils.hpp"
#include "version.h"

#include <signal.h>

namespace cta::runtime {

Application& Application::addSignalFunction(int signal, const std::function<void()>& func) {
  m_signalReactorBuilder->addSignalFunction(signal, func);
  return *this;
}

void Application::initHealthServer() {
  if (m_appConfig.health_server.enabled) {
    m_healthServer = std::make_unique<HealthServer>(
      *m_logPtr,
      m_appConfig.health_server.host,
      m_appConfig.health_server.port,
      [this]() { return m_app.isReady(); },
      [this]() { return m_app.isLive(); });

    // We only start it if it's enabled. We explicitly construct the object outside the scope of this if statement
    // Otherwise, healthServer will go out of scope and be destructed immediately
    m_healthServer->start();
  }
}

void Application::initXRootD() {
  // Overwrite XRootD env variables
  ::setenv("XrdSecPROTOCOL", m_appConfig.xrootd.security_protocol.c_str(), 1);
  ::setenv("XrdSecSSSKT", m_appConfig.xrootd.sss_keytab_path.c_str(), 1);
  // TODO: check the keytab here
}

}  // namespace cta::runtime
