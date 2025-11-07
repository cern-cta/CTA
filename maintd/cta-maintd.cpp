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

#include "common/exception/Errnum.hpp"
#include "common/CmdLineParams.hpp"
#include "common/config/Config.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/process/threading/System.hpp"
#include "common/utils/utils.hpp"
#include "RoutineRunner.hpp"
#include "RoutineRunnerFactory.hpp"
#include "rdbms/Login.hpp"
#include "common/telemetry/TelemetryInit.hpp"
#include "common/telemetry/config/TelemetryConfig.hpp"
#include "common/semconv/Attributes.hpp"
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
  {
    std::list<cta::log::Param> params = {cta::log::Param("version", CTA_VERSION)};
    log(cta::log::INFO, "Starting cta-maintd", params);
  }

  // Set specific static headers for tape daemon
  std::map<std::string, std::string> staticParamMap;
  staticParamMap["instance"] = config.getOptionValueStr("cta.instance_name").value();
  staticParamMap["sched_backend"] = config.getOptionValueStr("cta.scheduler_backend_name").value();
  log.setStaticParams(staticParamMap);

  // Instantiate telemetry
  if (config.getOptionValueBool("cta.experimental.telemetry.enabled").value_or(false)) {
    try {
      std::string metricsBackend = config.getOptionValueStr("cta.telemetry.metrics.backend").value_or("NOOP");

      std::string otlpBasicAuthFile =
        config.getOptionValueStr("cta.telemetry.metrics.export.otlp.basic_auth_file").value();
      std::string otlpBasicAuthString =
        otlpBasicAuthFile.empty() ? "" : cta::telemetry::authStringFromFile(otlpBasicAuthFile);
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
      // taped is a special case where we only do initTelemetry after the process name has been set
      cta::telemetry::initTelemetry(telemetryConfig, lc);
    } catch (exception::Exception& ex) {
      throw InvalidConfiguration("Failed to instantiate OpenTelemetry. Exception message: " + ex.getMessage().str());
    }
  }

  // Start loop
  while (true) {
    // Create the routine runner
    auto routineRunner = RoutineRunnerFactory::create(lc, config);
    // Run it :o
    uint32_t rc = routineRunner->run(lc);
    switch (rc) {
      case SIGTERM:
        return 0;
      case SIGHUP:
        log(cta::log::INFO,
            "Reloading config for maintd process. Process user and group, telemetry, and log format will not be "
            "reloaded.");
        config.parse(log);
        break;
      default:
        log(cta::log::CRIT, "Received unexpected signal. Exiting");
        return EXIT_FAILURE;
    }
  }
}

}  // namespace cta::maintd

int main(const int argc, char** const argv) {
  using namespace cta;

  // Interpret the command line
  std::unique_ptr<common::CmdLineParams> cmdLineParams;
  cmdLineParams.reset(new common::CmdLineParams(argc, argv, "cta-maintd"));

  std::string shortHostName;
  try {
    shortHostName = utils::getShortHostname();
  } catch (const exception::Errnum& ex) {
    std::cerr << "Failed to get short host name." << ex.getMessage().str();
    return EXIT_FAILURE;
  }

  std::unique_ptr<log::Logger> logPtr;
  logPtr.reset(new log::StdoutLogger(shortHostName, "cta-maintd"));

  common::Config config(cmdLineParams->configFileLocation, &(*logPtr));

  // Change user and group
  int rc = cta::maintd::setUserAndGroup(config.getOptionValueStr("cta.daemon_user").value_or("cta"),
                                             config.getOptionValueStr("cta.daemon_group").value_or("tape"),
                                             *logPtr);

  if (rc) {
    return rc;
  }

  // Try to instantiate the logging system API
  try {
    if (cmdLineParams->logToFile) {
      logPtr.reset(new log::FileLogger(shortHostName, "cta-maintd", cmdLineParams->logFilePath, log::DEBUG));
    } else if (cmdLineParams->logToStdout) {
      logPtr.reset(new log::StdoutLogger(shortHostName, "cta-maintd"));
    }
    if (!cmdLineParams->logFormat.empty()) {
      logPtr->setLogFormat(cmdLineParams->logFormat);
    }
  } catch (exception::Exception& ex) {
    std::cerr << "Failed to instantiate object representing the CTA logging system: " << ex.getMessage().str()
              << std::endl;
    return EXIT_FAILURE;
  }

  log::Logger& log = *logPtr;

  log(log::INFO, "Launching cta-maintd", cmdLineParams->toLogParams());

  int programRc = EXIT_FAILURE;
  try {
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

