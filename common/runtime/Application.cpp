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

void Application::initLogging() {
  std::string shortHostName;
  try {
    shortHostName = utils::getShortHostname();
  } catch (const exception::Errnum& ex) {
    throw cta::exception::Exception("Failed to get short host name: " << ex.getMessage().str());
  }

  try {
    if (cmdLineArgs.logToFile) {
      m_logPtr = std::make_unique<log::FileLogger>(shortHostName, appName, cmdLineArgs.logFilePath, log::INFO);
    } else {
      // Default to stdout logger
      m_logPtr = std::make_unique<log::StdoutLogger>(shortHostName, appName);
    }
    m_logPtr->setLogFormat(m_appConfig.logging.format);
  } catch (exception::Exception& ex) {
    throw cta::exception::Exception("Failed to instantiate CTA logging system: " << ex.getMessage().str());
  }

  m_logPtr->setLogMask(m_appConfig.logging.level);
  std::map<std::string, std::string> logAttributes;
  // This should eventually be handled by auto-discovery
  if constexpr (HasSchedulerConfig<V>) {
    logAttributes["sched_backend"] = m_appConfig.scheduler.backend_name;
  }
  for (const auto& [key, value] : m_appConfig.logging.attributes) {
    logAttributes[key] = value;
  }
  m_logPtr->setStaticParams(logAttributes);
}

void Application::initHealthServer() {
  if (m_appConfig.health_server.enabled) {
    // TODO: handle lifetime of this object
    common::HealthServer healthServer = common::HealthServer(
      *m_logPtr,
      m_appConfig.health_server.host,
      m_appConfig.health_server.port,
      [this]() { return m_app.isReady(); },
      [this]() { return m_app.isLive(); });

    // We only start it if it's enabled. We explicitly construct the object outside the scope of this if statement
    // Otherwise, healthServer will go out of scope and be destructed immediately
    healthServer.start();
  }
}

// TODO: handle additional resource attributes here? Or let the SDK do this...
void Application::initTelemetry() {
  if (!config.experimental.telemetry_enabled || config.telemetry.config.empty()) {
    return;
  }
  log::LogContext lc(*m_logPtr);
  try {
    std::map<std::string, std::string> ctaResourceAttributes = {
      {cta::semconv::attr::kServiceName,       cta::semconv::attr::ServiceNameValues::kCtaMaintd},
      {cta::semconv::attr::kServiceVersion,    std::string(CTA_VERSION)                         },
      {cta::semconv::attr::kServiceInstanceId, cta::utils::generateUuid()                       },
      {cta::semconv::attr::kHostName,          cta::utils::getShortHostname()                   }
    };

    if constexpr (HasSchedulerConfig<V>) {
      ctaResourceAttributes[cta::semconv::attr::kSchedulerNamespace] = m_appConfig.scheduler.backend_name;
    }
    cta::telemetry::initOpenTelemetry(config.telemetry.config, ctaResourceAttributes, lc);
  } catch (exception::Exception& ex) {
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMessage", ex.getMessage().str());
    lc.log(log::ERR, "Failed to instantiate OpenTelemetry");
    cta::telemetry::cleanupOpenTelemetry(lc);
  }
}

void Application::initXRootD() {
  // Overwrite XRootD env variables
  ::setenv("XrdSecPROTOCOL", m_appConfig.xrootd.security_protocol.c_str(), 1);
  ::setenv("XrdSecSSSKT", m_appConfig.xrootd.sss_keytab_path.c_str(), 1);
  // TODO: check the keytab here
}

}  // namespace cta::runtime
