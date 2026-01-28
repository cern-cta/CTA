/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MaintdConfig.hpp"
#include "MaintenanceDaemon.hpp"
#include "common/CmdLineParams.hpp"
#include "common/config/Config.hpp"
#include "common/exception/Errnum.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/process/health/HealthServer.hpp"
#include "common/process/signals/SignalReactor.hpp"
#include "common/process/signals/SignalReactorBuilder.hpp"
#include "common/process/threading/System.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/OtelInit.hpp"
#include "common/utils/utils.hpp"
#include "version.h"

#include <getopt.h>
#include <iostream>
#include <memory>
#include <rfl.hpp>
#include <rfl/toml.hpp>
#include <signal.h>
#include <string>
#include <thread>

namespace cta::maintd {

void initTelemetry(const std::string& otelConfigPath, cta::log::LogContext& lc) {
  try {
    std::map<std::string, std::string> ctaResourceAttributes = {
      {cta::semconv::attr::kServiceName,       cta::semconv::attr::ServiceNameValues::kCtaMaintd},
      {cta::semconv::attr::kServiceVersion,    std::string(CTA_VERSION)                         },
      {cta::semconv::attr::kServiceInstanceId, cta::utils::generateUuid()                       },
      {cta::semconv::attr::kHostName,          cta::utils::getShortHostname()                   }
    };
    cta::telemetry::initOpenTelemetry(otelConfigPath, ctaResourceAttributes, lc);
  } catch (exception::Exception& ex) {
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMessage", ex.getMessage().str());
    lc.log(log::ERR, "Failed to instantiate OpenTelemetry");
    cta::telemetry::cleanupOpenTelemetry(lc);
  }
}

/**
 * exceptionThrowingMain
 *
 * The main function delegates the bulk of its implementation to this
 * exception throwing version.
 *
 * @param config The parsed config file
 * @param log The logging system
 * @return integer representing the exit code. Only used if we are gracefully shutting down.
 */
static int exceptionThrowingMain(MaintdConfig config, cta::log::Logger& log) {
  cta::log::LogContext lc(log);

  MaintenanceDaemon maintenanceDaemon(config, lc);

  // Set up the signal reactor
  process::SignalReactor signalReactor =
    process::SignalReactorBuilder(lc)
      .addSignalFunction(SIGTERM, [&maintenanceDaemon]() { maintenanceDaemon.stop(); })
      .addSignalFunction(SIGUSR1, [&log]() { log.refresh(); })
      .build();
  signalReactor.start();

  if (config.experimental.telemetry_enabled) {
    // Telemetry spawns some threads, so the SignalReactor must have started before this to correctly block signals
    initTelemetry(config.telemetry.config, lc);
  }

  common::HealthServer healthServer = common::HealthServer(
    lc,
    config.health_server.host,
    config.health_server.port,
    [&maintenanceDaemon]() { return maintenanceDaemon.isReady(); },
    [&maintenanceDaemon]() { return maintenanceDaemon.isLive(); });

  if (config.health_server.enabled) {
    // We only start it if it's enabled. We explicitly construct the object outside the scope of this if statement
    // Otherwise, healthServer will go out of scope and be destructed immediately
    healthServer.start();
  }

  // Run the maintenance daemon
  maintenanceDaemon.run();
  return 0;
}

}  // namespace cta::maintd

int main(const int argc, char** const argv) {
  using namespace cta;

  // Interpret the command line
  common::CmdLineParams cmdLineParams(argc, argv, "cta-maintd");

  std::string shortHostName;
  try {
    shortHostName = utils::getShortHostname();
  } catch (const exception::Errnum& ex) {
    std::cerr << "Failed to get short host name." << ex.getMessage().str();
    return EXIT_FAILURE;
  }

  // We need a logger before parsing the config file
  std::string defaultLogFormat = "json";
  std::unique_ptr<log::Logger> logPtr;
  try {
    if (cmdLineParams.logToFile) {
      logPtr = std::make_unique<log::FileLogger>(shortHostName, "cta-maintd", cmdLineParams.logFilePath, log::DEBUG);
    } else if (cmdLineParams.logToStdout) {
      logPtr = std::make_unique<log::StdoutLogger>(shortHostName, "cta-maintd");
    } else {
      // Default to stdout logger
      logPtr = std::make_unique<log::StdoutLogger>(shortHostName, "cta-maintd");
    }
    if (!cmdLineParams.logFormat.empty()) {
      logPtr->setLogFormat(cmdLineParams.logFormat);
    } else {
      logPtr->setLogFormat(defaultLogFormat);
    }
  } catch (exception::Exception& ex) {
    std::cerr << "Failed to instantiate object representing CTA logging system: " << ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  }
  log::Logger& log = *logPtr;

  // Read the config file
  // TODO: strictness settings
  cta::maintd::MaintdConfig config = rfl::toml::read<cta::maintd::MaintdConfig>(cmdLineParams.configFileLocation);

  log.setLogMask(config.logging.level);
  log.setStaticParams(config.logging.attributes);

  // Start
  int programRc = EXIT_FAILURE;
  try {
    log(log::INFO, "Launching cta-maintd", cmdLineParams.toLogParams());
    programRc = maintd::exceptionThrowingMain(config, log);
  } catch (exception::Exception& ex) {
    std::vector<cta::log::Param> params = {log::Param("exceptionMessage", ex.getMessage().str())};
    log(log::ERR, "Caught an unexpected CTA exception.", params);
    sleep(1);
  } catch (std::exception& se) {
    std::vector<cta::log::Param> params = {cta::log::Param("what", se.what())};
    log(log::ERR, "Caught an unexpected standard exception.", params);
    sleep(1);
  } catch (...) {
    log(log::ERR, "Caught an unexpected and unknown exception.");
    sleep(1);
  }
  log(log::INFO, "Exiting cta-maintd");
  return programRc;
}
