/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

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
#include <signal.h>
#include <string>
#include <thread>

namespace cta::maintd {

void initTelemetry(const common::Config& config, cta::log::LogContext& lc) {
  if (!config.getOptionValueBool("cta.experimental.telemetry.enabled").value_or(false)) {
    return;
  }
  try {
    std::map<std::string, std::string> ctaResourceAttributes = {
      {cta::semconv::attr::kServiceName,       cta::semconv::attr::ServiceNameValues::kCtaMaintd},
      {cta::semconv::attr::kServiceVersion,    std::string(CTA_VERSION)                         },
      {cta::semconv::attr::kServiceInstanceId, cta::utils::generateUuid()                       },
      {cta::semconv::attr::kHostName,          cta::utils::getShortHostname()                   }
    };
    auto otelConfigFile = config.getOptionValueStr("cta.telemetry.config").value_or("/etc/cta/cta-otel.yaml");
    cta::telemetry::initOpenTelemetry(otelConfigFile, ctaResourceAttributes, lc);
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
static int exceptionThrowingMain(common::Config config, cta::log::Logger& log) {
  cta::log::LogContext lc(log);

  MaintenanceDaemon maintenanceDaemon(config, lc);

  // Set up the signal reactor
  process::SignalReactor signalReactor =
    process::SignalReactorBuilder(lc)
      .addSignalFunction(SIGHUP, [&maintenanceDaemon]() { maintenanceDaemon.reload(); })
      .addSignalFunction(SIGTERM, [&maintenanceDaemon]() { maintenanceDaemon.stop(); })
      .addSignalFunction(SIGUSR1, [&log]() { log.refresh(); })
      .build();
  signalReactor.start();

  // Telemetry spawns some threads, so the SignalReactor must have started before this to correctly block signals
  initTelemetry(config, lc);

  common::HealthServer healthServer = common::HealthServer(
    lc,
    config.getOptionValueStr("cta.health_server.host").value_or("127.0.0.1"),
    config.getOptionValueInt("cta.health_server.port").value_or(8080),
    [&maintenanceDaemon]() { return maintenanceDaemon.isReady(); },
    [&maintenanceDaemon]() { return maintenanceDaemon.isLive(); });

  if (config.getOptionValueBool("cta.health_server.enabled").value_or(false)) {
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
  common::Config config(cmdLineParams.configFileLocation, &(*logPtr));

  // Some logger configuration is only available after we get the config file, so update the logger here
  if (!cmdLineParams.logFormat.empty()) {
    log.setLogFormat(config.getOptionValueStr("cta.log.format").value_or(defaultLogFormat));
  }
  log.setLogMask(config.getOptionValueStr("cta.log.level").value_or("INFO"));
  if (!config.getOptionValueStr("cta.instance_name").has_value()) {
    log(log::CRIT, "cta.instance_name is not set in configuration file " + cmdLineParams.configFileLocation);
    return EXIT_FAILURE;
  }
  if (!config.getOptionValueStr("cta.scheduler_backend_name").has_value()) {
    log(log::CRIT, "cta.scheduler_backend_name is not set in configuration file " + cmdLineParams.configFileLocation);
    return EXIT_FAILURE;
  }
  std::map<std::string, std::string> staticParamMap;
  staticParamMap["instance"] = config.getOptionValueStr("cta.instance_name").value();
  staticParamMap["sched_backend"] = config.getOptionValueStr("cta.scheduler_backend_name").value();
  log.setStaticParams(staticParamMap);

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
