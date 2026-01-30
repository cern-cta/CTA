/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MaintdConfig.hpp"
#include "RoutineRunner.hpp"
#include "RoutineRunnerFactory.hpp"
#include "common/CmdLineParams.hpp"
#include "common/config/Config.hpp"
#include "common/config/ConfigLoader.hpp"
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

void initTelemetry(const std::string& otelConfigPath, const std::string& schedulerNamespace, cta::log::LogContext& lc) {
  try {
    std::map<std::string, std::string> ctaResourceAttributes = {
      {cta::semconv::attr::kServiceName,        cta::semconv::attr::ServiceNameValues::kCtaMaintd},
      {cta::semconv::attr::kServiceVersion,     std::string(CTA_VERSION)                         },
      {cta::semconv::attr::kServiceInstanceId,  cta::utils::generateUuid()                       },
      {cta::semconv::attr::kHostName,           cta::utils::getShortHostname()                   },
      {cta::semconv::attr::kSchedulerNamespace, schedulerNamespace                               }
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
 */
static void runRoutineRunner(MaintdConfig config, cta::log::Logger& log) {
  cta::log::LogContext lc(log);

  RoutineRunnerFactory routineRunnerFactory(config, lc);
  // Create the routine runner
  auto routineRunner = routineRunnerFactory.create();

  // Set up the signal reactor
  process::SignalReactor signalReactor = process::SignalReactorBuilder(lc)
                                           .addSignalFunction(SIGTERM, [&routineRunner]() { routineRunner->stop(); })
                                           .addSignalFunction(SIGUSR1, [&log]() { log.refresh(); })
                                           .build();
  signalReactor.start();

  // Set up telemetry
  if (config.experimental.telemetry_enabled && !config.telemetry.config.empty()) {
    // Telemetry spawns some threads, so the SignalReactor must have started before this to correctly block signals
    initTelemetry(config.telemetry.config, config.scheduler.backend_name, lc);
  }

  // Set up the health server
  common::HealthServer healthServer = common::HealthServer(
    lc,
    config.health_server.host,
    config.health_server.port,
    [&routineRunner]() { return routineRunner->isReady(); },
    [&routineRunner]() { return routineRunner->isLive(); });

  if (config.health_server.enabled) {
    // We only start it if it's enabled. We explicitly construct the object outside the scope of this if statement
    // Otherwise, healthServer will go out of scope and be destructed immediately
    healthServer.start();
  }

  // Overwrite XRootD env variables
  // TODO: do we just always set the protocol to SSS?
  ::setenv("XrdSecPROTOCOL", config.xrootd.security_protocol.c_str(), 1);
  ::setenv("XrdSecSSSKT", config.xrootd.sss_keytab_path.c_str(), 1);

  // This run routine blocks until we explicitly tell the routine runner to stop
  routineRunner->run(lc);
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
  maintd::MaintdConfig config;
  try {
    // TODO: strict from commandline args
    config = config::loadFromToml<maintd::MaintdConfig>(cmdLineParams.configFileLocation, true);
  } catch (exception::UserError& ex) {
    log(log::CRIT, "FATAL: Failed to parse config file", {log::Param("exceptionMessage", ex.getMessage().str())});
    sleep(1);
    return EXIT_FAILURE;
  }

  log.setLogMask(config.logging.level);
  std::map<std::string, std::string> logAttributes;
  // This should eventually be handled by auto-discovery
  logAttributes["sched_backend"] = config.scheduler.backend_name;
  for (const auto& [key, value] : config.logging.attributes) {
    logAttributes[key] = value;
  }
  log.setStaticParams(logAttributes);

  // Start
  try {
    log(log::INFO, "Launching cta-maintd", cmdLineParams.toLogParams());
    maintd::runRoutineRunner(config, log);
  } catch (exception::Exception& ex) {
    log(log::CRIT,
        "FATAL: Caught an unexpected CTA exception",
        {log::Param("exceptionMessage", ex.getMessage().str())});
    sleep(1);
    return EXIT_FAILURE;
  } catch (std::exception& se) {
    log(log::CRIT, "FATAL: Caught an unexpected standard exception", {log::Param("error", se.what())});
    sleep(1);
    return EXIT_FAILURE;
  } catch (...) {
    log(log::CRIT, "FATAL: Caught an unexpected and unknown exception", {});
    sleep(1);
    return EXIT_FAILURE;
  }
  log(log::INFO, "Exiting cta-maintd");
  return EXIT_SUCCESS;
}
