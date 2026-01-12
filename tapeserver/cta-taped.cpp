/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/CmdLineParams.hpp"
#include "common/exception/Errnum.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/process/ProcessCap.hpp"
#include "common/process/threading/System.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/TelemetryInit.hpp"
#include "common/telemetry/config/TelemetryConfig.hpp"
#include "tapeserver/daemon/TapeDaemon.hpp"
#include "tapeserver/daemon/common/TapedConfiguration.hpp"
#include "version.h"

#include <google/protobuf/stubs/common.h>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>

namespace cta::taped {

//------------------------------------------------------------------------------
// exceptionThrowingMain
//
// The main() function delegates the bulk of its implementation to this
// exception throwing version.
//
// @param argc The number of command-line arguments.
// @param argv The command-line arguments.
// @param log The logging system.
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const cta::common::CmdLineParams& commandLine, cta::log::Logger& log);

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const cta::common::CmdLineParams& commandLine, cta::log::Logger& log) {
  using namespace cta::tape::daemon::common;

  {
    std::vector<cta::log::Param> params = {cta::log::Param("version", CTA_VERSION)};
    auto cmdLineParams = commandLine.toLogParams();
    params.insert(params.end(),
                  std::make_move_iterator(cmdLineParams.begin()),
                  std::make_move_iterator(cmdLineParams.end()));
    log(log::INFO, "Starting cta-taped", params);
  }

  // Parse /etc/cta/cta-taped-unitName.conf parameters
  const TapedConfiguration globalConfig = TapedConfiguration::createFromConfigPath(commandLine.configFileLocation, log);

  // log a warning if the externalFreeDiskSpaceScript is not set
  if (globalConfig.externalFreeDiskSpaceScript.value().empty()) {
    log(log::WARNING, "externalFreeDiskSpaceScript not configured");
  }

  // Set log lines to JSON format if configured and not overridden on command line
  if (commandLine.logFormat.empty()) {
    log.setLogFormat(globalConfig.logFormat.value());
  }

  // Set specific static headers for tape daemon
  std::map<std::string, std::string> staticParamMap;
  staticParamMap["instance"] = globalConfig.instanceName.value();
  staticParamMap["sched_backend"] = globalConfig.schedulerBackendName.value();
  staticParamMap["drive_name"] = globalConfig.driveName.value();
  log.setStaticParams(staticParamMap);

  // Adjust log mask to the log level potentionally set in the configuration file
  log.setLogMask(globalConfig.logMask.value());
  {
    std::vector<cta::log::Param> params = {cta::log::Param("logMask", globalConfig.logMask.value())};
    log(log::INFO, "Set log mask", params);
  }

  // Create the main tapeserverd object
  cta::tape::daemon::TapeDaemon daemon(commandLine, log, globalConfig);

  // Run the tapeserverd daemon
  return daemon.mainImpl();
}

}  // namespace cta::taped

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char** const argv) {
  using namespace cta;

  // Interpret the command line
  std::unique_ptr<common::CmdLineParams> commandLine;
  commandLine.reset(new common::CmdLineParams(argc, argv, "cta-taped"));

  std::string shortHostName;
  try {
    shortHostName = utils::getShortHostname();
  } catch (const exception::Errnum& ex) {
    std::cerr << "Failed to get short host name." << ex.getMessage().str();
    return EXIT_FAILURE;
  }

  // Use a temporary stdoutlogger to parse the config file before
  // instantianting the logger system API.
  std::unique_ptr<log::Logger> logPtr;
  logPtr.reset(new log::StdoutLogger(shortHostName, "cta-taped"));

  // Initial parse of config file
  tape::daemon::common::TapedConfiguration globalConfig;
  try {
    globalConfig =
      tape::daemon::common::TapedConfiguration::createFromConfigPath(commandLine->configFileLocation, *logPtr);
  } catch (const exception::Exception& ex) {
    std::vector<cta::log::Param> params = {cta::log::Param("exceptionMessage", ex.getMessage().str())};
    (*logPtr)(log::ERR, "Caught an unexpected CTA exception, cta-taped cannot start", params);
    return EXIT_FAILURE;
  }

  // Change process capabilities.
  // process must be able to change user and group now.
  // rawio cap must be permitted now to be able to perform raw IO once we are no longer root.
  try {
    cta::server::ProcessCap::setProcText("cap_setgid,cap_setuid+ep cap_sys_rawio+p");
    (*logPtr)(log::INFO,
              "Set process capabilities",
              {
                {"capabilites", cta::server::ProcessCap::getProcText()}
    });
  } catch (const cta::exception::Exception& ex) {
    std::vector<cta::log::Param> params = {cta::log::Param("exceptionMessage", ex.getMessage().str())};
    (*logPtr)(log::ERR, "Caught an unexpected CTA exception, cta-taped cannot start", params);
    return EXIT_FAILURE;
  }

  // Change user and group
  const std::string userName = globalConfig.daemonUserName.value();
  const std::string groupName = globalConfig.daemonGroupName.value();

  try {
    (*logPtr)(log::INFO,
              "Setting user name and group name of current process",
              {
                {"userName",  userName },
                {"groupName", groupName}
    });
    cta::System::setUserAndGroup(userName, groupName);
    // There is no longer any need for the process to be able to change user,
    // however the process should still be permitted to make the raw IO
    // capability effective in the future when needed.
    cta::server::ProcessCap::setProcText("cap_sys_rawio+p");

  } catch (exception::Exception& ex) {
    std::vector<log::Param> params = {log::Param("exceptionMessage", ex.getMessage().str())};
    (*logPtr)(log::ERR, "Caught an unexpected CTA, cta-taped cannot start", params);
    return EXIT_FAILURE;
  }

  // Try to instantiate the logging system API
  try {
    if (commandLine->logToStdout) {
      logPtr.reset(new log::StdoutLogger(shortHostName, "cta-taped"));
    } else if (commandLine->logToFile) {
      logPtr.reset(new log::FileLogger(shortHostName, "cta-taped", commandLine->logFilePath, log::DEBUG));
    }
    if (!commandLine->logFormat.empty()) {
      logPtr->setLogFormat(commandLine->logFormat);
    }
  } catch (exception::Exception& ex) {
    std::cerr << "Failed to instantiate object representing CTA logging system: " << ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  }
  cta::log::Logger& log = *logPtr;

  // Instantiate telemetry
  if (globalConfig.telemetryEnabled.value()) {
    try {
      std::string metricsBackend = globalConfig.metricsBackend.value();
      if (cta::telemetry::stringToMetricsBackend(metricsBackend) == cta::telemetry::MetricsBackend::STDOUT) {
        log(log::WARNING, "OpenTelemetry backend STDOUT is not supported for cta-taped. Using NOOP backend instead...");
        metricsBackend = cta::telemetry::metricsBackendToString(cta::telemetry::MetricsBackend::NOOP);
      }

      std::string otlpBasicAuthPasswordFile = globalConfig.metricsOtlpAuthBasicPasswordFile.value();
      std::string otlpBasicAuthPassword =
        otlpBasicAuthPasswordFile.empty() ? "" : cta::telemetry::stringFromFile(otlpBasicAuthPasswordFile);
      std::string otlpBasicAuthUsername = globalConfig.metricsOtlpAuthBasicUsername.value();
      cta::telemetry::TelemetryConfig telemetryConfig =
        cta::telemetry::TelemetryConfigBuilder()
          .serviceName(cta::semconv::attr::ServiceNameValues::kCtaTaped)
          .serviceNamespace(globalConfig.instanceName.value())
          .serviceVersion(CTA_VERSION)
          .retainInstanceIdOnRestart(globalConfig.retainInstanceIdOnRestart.value())
          .resourceAttribute(cta::semconv::attr::kSchedulerNamespace, globalConfig.schedulerBackendName.value())
          .resourceAttribute(cta::semconv::attr::kTapeDriveName, globalConfig.driveName.value())
          .resourceAttribute(cta::semconv::attr::kTapeLibraryLogicalName, globalConfig.driveLogicalLibrary.value())
          .metricsBackend(metricsBackend)
          .metricsExportInterval(std::chrono::milliseconds(globalConfig.metricsExportInterval.value()))
          .metricsExportTimeout(std::chrono::milliseconds(globalConfig.metricsExportTimeout.value()))
          .metricsOtlpEndpoint(globalConfig.metricsOtlpEndpoint.value())
          .metricsOtlpBasicAuth(otlpBasicAuthUsername, otlpBasicAuthPassword)
          .metricsFileEndpoint(globalConfig.metricsFileEndpoint.value())
          .build();
      // taped is a special case where we only do initTelemetry after the process name has been set
      cta::telemetry::initTelemetryConfig(telemetryConfig);
    } catch (exception::Exception& ex) {
      std::vector<cta::log::Param> params = {cta::log::Param("exceptionMessage", ex.getMessage().str())};
      log(log::ERR, "Failed to instantiate OpenTelemetry", params);
      cta::log::LogContext lc(log);
      cta::telemetry::shutdownTelemetry(lc);
    }
  }

  int programRc = EXIT_FAILURE;  // Default return code when receiving an exception.
  try {
    programRc = cta::taped::exceptionThrowingMain(*commandLine, log);
  } catch (exception::Exception& ex) {
    std::vector<cta::log::Param> params = {cta::log::Param("exceptionMessage", ex.getMessage().str())};
    log(log::ERR, "Caught an unexpected CTA exception, cta-taped cannot start", params);
    sleep(1);
  } catch (std::exception& se) {
    std::vector<cta::log::Param> params = {cta::log::Param("what", se.what())};
    log(log::ERR, "Caught an unexpected standard exception, cta-taped cannot start", params);
    sleep(1);
  } catch (...) {
    log(log::ERR, "Caught an unexpected and unknown exception, cta-taped cannot start");
    sleep(1);
  }

  google::protobuf::ShutdownProtobufLibrary();
  return programRc;
}
