/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <getopt.h>
#include <signal.h>
#include <string>
#include <iostream>
#include <thread>
#include <memory>

#include "common/exception/Errnum.hpp"
#include "common/CmdLineParams.hpp"
#include "common/config/Config.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/process/threading/System.hpp"
#include "common/process/SignalReactor.hpp"
#include "common/utils/utils.hpp"
#include "common/telemetry/TelemetryInit.hpp"
#include "common/telemetry/config/TelemetryConfig.hpp"
#include "common/semconv/Attributes.hpp"
#include "MaintenanceDaemon.hpp"
#include "version.h"

namespace cta::maintd {

static int setUserAndGroup(const std::string& userName, const std::string& groupName, cta::log::Logger& logger) {
  try {
    logger(log::INFO,
           "Setting user name and group name of current process",
           {
             {"userName",  userName },
             {"groupName", groupName}
    });
    cta::System::setUserAndGroup(userName, groupName);
  } catch (exception::Exception& ex) {
    std::list<log::Param> params = {log::Param("exceptionMessage", ex.getMessage().str())};
    logger(log::ERR, "Caught an unexpected CTA, exiting cta-maintd", params);
    return EXIT_FAILURE;
  }

  return 0;
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

  // Instantiate telemetry
  if (config.getOptionValueBool("cta.experimental.telemetry.enabled").value_or(false)) {
    try {
      std::string metricsBackend = config.getOptionValueStr("cta.telemetry.metrics.backend").value_or("NOOP");

      std::optional<std::string> otlpBasicAuthFile =
        config.getOptionValueStr("cta.telemetry.metrics.export.otlp.basic_auth_file");
      std::string otlpBasicAuthString =
        otlpBasicAuthFile.has_value() ? cta::telemetry::authStringFromFile(otlpBasicAuthFile.value()) : "";
      cta::telemetry::TelemetryConfig telemetryConfig =
        cta::telemetry::TelemetryConfigBuilder()
          .serviceName(cta::semconv::attr::ServiceNameValues::kCtaMaintd)
          .serviceNamespace(config.getOptionValueStr("cta.instance_name").value())
          .serviceVersion(CTA_VERSION)
          .retainInstanceIdOnRestart(
            config.getOptionValueBool("cta.telemetry.retain_instance_id_on_restart").value_or(false))
          .resourceAttribute(cta::semconv::attr::kSchedulerNamespace,
                             config.getOptionValueStr("cta.scheduler_backend_name").value())
          .metricsBackend(metricsBackend)
          .metricsExportInterval(std::chrono::milliseconds(
            config.getOptionValueInt("cta.telemetry.metrics.export.interval").value_or(15000)))
          .metricsExportTimeout(
            std::chrono::milliseconds(config.getOptionValueInt("cta.telemetry.metrics.export.timeout").value_or(3000)))
          .metricsOtlpEndpoint(config.getOptionValueStr("cta.telemetry.metrics.export.otlp.endpoint").value_or(""))
          .metricsOtlpBasicAuthString(otlpBasicAuthString)
          .metricsFileEndpoint(config.getOptionValueStr("cta.telemetry.metrics.export.file.endpoint")
                                 .value_or("/var/log/cta/cta-maintd-metrics.txt"))
          .build();
      cta::telemetry::initTelemetry(telemetryConfig, lc);
    } catch (exception::Exception& ex) {
      throw exception::Exception("Failed to instantiate OpenTelemetry. Exception message: " + ex.getMessage().str());
    }
  }

  MaintenanceDaemon maintenanceDaemon(config, lc);

  // Set up the signal reactor
  SignalReactor signalReactor(lc);
  signalReactor.registerSignalFunction(SIGHUP, [&maintenanceDaemon]() { maintenanceDaemon.reload(); });
  signalReactor.registerSignalFunction(SIGTERM, [&maintenanceDaemon]() { maintenanceDaemon.stop(); });

  // Run the signal reactor on a separate thread
  std::thread signalThread([&signalReactor]() { signalReactor.run(); });
  // Run the maintenance daemon
  try {
    maintenanceDaemon.run();
  } catch (...) {
    // In the case of an exception we still want to enforce graceful shutdown on the signal reactor
    signalReactor.stop();
    signalThread.join();
    throw;
  }

  // Graceful shutdown; stop the signal reactor
  signalReactor.stop();
  signalThread.join();
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

  // Change user and group
  int rc = cta::maintd::setUserAndGroup(config.getOptionValueStr("cta.daemon_user").value_or("cta"),
                                        config.getOptionValueStr("cta.daemon_group").value_or("tape"),
                                        log);

  if (rc) {
    return rc;
  }

  // Start
  int programRc = EXIT_FAILURE;
  try {
    log(log::INFO, "Launching cta-maintd", cmdLineParams.toLogParams());
    programRc = maintd::exceptionThrowingMain(config, log);
  } catch (exception::Exception& ex) {
    std::list<cta::log::Param> params = {log::Param("exceptionMessage", ex.getMessage().str())};
    log(log::ERR, "Caught an unexpected CTA exception.", params);
    sleep(1);
  } catch (std::exception& se) {
    std::list<cta::log::Param> params = {cta::log::Param("what", se.what())};
    log(log::ERR, "Caught an unexpected standard exception.", params);
    sleep(1);
  } catch (...) {
    log(log::ERR, "Caught an unexpected and unknown exception.");
    sleep(1);
  }

  return programRc;
}
