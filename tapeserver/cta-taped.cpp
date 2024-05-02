/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "common/Configuration.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/log/SyslogLogger.hpp"
#include "common/processCap/ProcessCap.hpp"
#include "tapeserver/daemon/CommandLineParams.hpp"
#include "tapeserver/daemon/common/TapedConfiguration.hpp"
#include "tapeserver/daemon/TapeDaemon.hpp"

#include "version.h"

#include <google/protobuf/stubs/common.h>
#include <memory>
#include <sstream>
#include <string>
#include <iostream>
#include <map>
#include <type_traits>

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
static int exceptionThrowingMain(const cta::daemon::CommandLineParams & commandLine,
  cta::log::Logger &log);

//------------------------------------------------------------------------------
// The help string
//------------------------------------------------------------------------------
std::string gHelpString =
    "Usage: cta-taped [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t--foreground             or -f         \tRemain in the Foreground\n"
    "\t--stdout                 or -s         \tPrint logs to standard output. Required --foreground\n"
    "\t--log-to-file <log-file> or -l         \tLogs to a given file (instead of default syslog)\n"
    "\t--log-format <format>    or -o         \tOutput format for log messages (default or json)\n"
    "\t--config <config-file>   or -c         \tConfiguration file\n"
    "\t--help                   or -h         \tPrint this help and exit\n";

//------------------------------------------------------------------------------
// Logs the start of the daemon.
//------------------------------------------------------------------------------
void logStartOfDaemon(cta::log::Logger &log,
  const daemon::CommandLineParams& commandLine);

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const cta::daemon::CommandLineParams& commandLine, cta::log::Logger& log) {
  using namespace cta::tape::daemon::common;

  logStartOfDaemon(log, commandLine);

  // Parse /etc/cta/cta-taped-unitName.conf parameters
  const TapedConfiguration globalConfig = TapedConfiguration::createFromCtaConf(commandLine.configFileLocation, log);

  // Set log lines to JSON format if configured and not overridden on command line
  if(commandLine.logFormat.empty()) {
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
    std::list<cta::log::Param> params = {cta::log::Param("logMask", globalConfig.logMask.value())};
    log(log::INFO, "Set log mask", params);
  }

  // Create the main tapeserverd object
  cta::tape::daemon::TapeDaemon daemon(
    commandLine,
    log,
    globalConfig);

  // Run the tapeserverd daemon
  return daemon.main();
}

//------------------------------------------------------------------------------
// logStartOfDaemon
//------------------------------------------------------------------------------
void logStartOfDaemon(cta::log::Logger &log,
  const cta::daemon::CommandLineParams & commandLine) {
  using namespace cta;

  std::list<cta::log::Param> params = {cta::log::Param("version", CTA_VERSION)};
  params.splice(params.end(), commandLine.toLogParams());
  log(log::INFO, "Starting cta-taped", params);
}

} // namespace cta::taped

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char **const argv) {
  using namespace cta;

  // Interpret the command line
  std::unique_ptr<cta::daemon::CommandLineParams> commandLine;
  try {
    commandLine.reset(new cta::daemon::CommandLineParams(argc, argv));
  } catch (exception::Exception &ex) {
    std::cerr <<
      "Failed to interpret the command line parameters: " <<
      ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  }

  if(commandLine->helpRequested) {
    std::cout << cta::taped::gHelpString << std::endl;
    return EXIT_SUCCESS;
  }

  std::string shortHostName;
  try {
    shortHostName = utils::getShortHostname();
  } catch (...) {
    std::cerr << "Failed to get short host name";
    return EXIT_FAILURE;
  }

  // Use a temporary stdoutlogger while setting some stuff
  std::unique_ptr<log::Logger> logPtr;
  logPtr.reset(new log::StdoutLogger(shortHostName, "cta-taped"));

  // Initial parse of config file
  tape::daemon::common::TapedConfiguration globalConfig;
  try {
  globalConfig =
      tape::daemon::common::TapedConfiguration::createFromCtaConf(commandLine->configFileLocation, *logPtr);
  } catch (...) {
    // Something went wrong with the config file. Report it now.
    return EXIT_FAILURE;
  }

  // Change process capabilities.
  // Process must be able to change user now and rawio should be set now to be able to perform
  // raw IO after change to non root user.
  try {
    cta::server::ProcessCap::setProcText("cap_setgid,cap_setuid+ep cap_sys_rawio+p");
    (*logPtr)(log::INFO, "Set process capabilities",
                  {{"capabilites", cta::server::ProcessCap::getProcText()}});
  } catch (cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to set process capabilities to '" << "cap_setgid,cap_setuid+ep cap_sys_rawio+p" <<
      "': " << ne.getMessage().str();
    return EXIT_FAILURE;
  }

  // Change user and group
  const std::string userName = globalConfig.daemonUserName.value();
  const std::string groupName = globalConfig.daemonGroupName.value();

  try {
    (*logPtr)(log::INFO, "Setting user name and group name of current process",
                  {{"userName", userName}, {"groupName", groupName}});
  } catch (exception::Exception& ex) {
    return EXIT_FAILURE;
  }

  // Try to instantiate the logging system API
  try {
    if(commandLine->logToStdout) {
      logPtr.reset(new log::StdoutLogger(shortHostName, "cta-taped"));
    } else if(commandLine->logToFile) {
      logPtr.reset(new log::FileLogger(shortHostName, "cta-taped", commandLine->logFilePath, log::DEBUG));
    } else {
      logPtr.reset(new log::SyslogLogger(shortHostName, "cta-taped", log::DEBUG));
    }
    if(!commandLine->logFormat.empty()) {
      logPtr->setLogFormat(commandLine->logFormat);
    }
  } catch(exception::Exception& ex) {
    std::cerr << "Failed to instantiate object representing CTA logging system: " << ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  }
  cta::log::Logger& log = *logPtr;

  int programRc = EXIT_FAILURE; // Default return code when receiving an exception.
  try {
    programRc = cta::taped::exceptionThrowingMain(*commandLine, log);
  } catch(exception::Exception &ex) {
    std::list<cta::log::Param> params = {
      cta::log::Param("exceptionMessage", ex.getMessage().str())};
    log(log::ERR, "Caught an unexpected CTA exception, cta-taped cannot start", params);
    sleep(1);
  } catch(std::exception &se) {
    std::list<cta::log::Param> params = {cta::log::Param("what", se.what())};
    log(log::ERR, "Caught an unexpected standard exception, cta-taped cannot start", params);
    sleep(1);
  } catch(...) {
    log(log::ERR, "Caught an unexpected and unknown exception, cta-taped cannot start");
    sleep(1);
  }

  google::protobuf::ShutdownProtobufLibrary();
  return programRc;
}
