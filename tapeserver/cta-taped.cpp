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
#include "tapeserver/daemon/TapedConfiguration.hpp"
#include "tapeserver/daemon/Tpconfig.hpp"
#include "tapeserver/daemon/TapeDaemon.hpp"

#include "version.h"

#include <google/protobuf/stubs/common.h>
#include <memory>
#include <sstream>
#include <string>
#include <iostream>

namespace cta { namespace taped {

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
    "\t--config <config-file>   or -c         \tConfiguration file\n"
    "\t--help                   or -h         \tPrint this help and exit\n";

//------------------------------------------------------------------------------
// Logs the start of the daemon.
//------------------------------------------------------------------------------
void logStartOfDaemon(cta::log::Logger &log,
  const daemon::CommandLineParams& commandLine);

//------------------------------------------------------------------------------
// Creates a string that contains the specified command-line arguments
// separated by single spaces.
//
// @param argc The number of command-line arguments.
// @param argv The array of command-line arguments.
//------------------------------------------------------------------------------
//static std::string argvToString(const int argc, const char *const *const argv);

////------------------------------------------------------------------------------
//// Writes the specified TPCONFIG lines to the specified logging system.
////
//// @param log The logging system.
//// @param lines The lines parsed from /etc/cta/TPCONFIG.
////------------------------------------------------------------------------------
//static void logTpconfigLines(cta::log::Logger &log,
//  const cta::tape::daemon::TpconfigLines &lines);
//
////------------------------------------------------------------------------------
//// Writes the specified TPCONFIG lines to the logging system.
////
//// @param log The logging system.
//// @param line The line parsed from /etc/cta/TPCONFIG.
////------------------------------------------------------------------------------
//static void logTpconfigLine(cta::log::Logger &log,
//  const cta::tape::daemon::TpconfigLine &line);

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
static int exceptionThrowingMain(
  const cta::daemon::CommandLineParams & commandLine,
  cta::log::Logger &log) {
  using namespace cta::tape::daemon;

  logStartOfDaemon(log, commandLine);

  // Parse /etc/cta/cta-taped.conf and /etc/cta/TPCONFIG for global parameters
  const TapedConfiguration globalConfig =
    TapedConfiguration::createFromCtaConf(commandLine.configFileLocation, log);

  // Adjust log mask to the log level potentionally set in the configuration file
  log.setLogMask(globalConfig.logMask.value());
  {
    std::list<cta::log::Param> params = {cta::log::Param("logMask", globalConfig.logMask.value())};
    log(log::INFO, "Set log mask", params);
  }

  // Create the object providing utilities for working with UNIX capabilities
  cta::server::ProcessCap capUtils;

  // Create the main tapeserverd object
  cta::tape::daemon::TapeDaemon daemon(
    commandLine,
    log,
    globalConfig,
    capUtils);

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

//------------------------------------------------------------------------------
// argvToString
//------------------------------------------------------------------------------
//static std::string argvToString(const int argc, const char *const *const argv) {
//  std::string str;
//
//  for(int i=0; i < argc; i++) {
//    if(i != 0) {
//      str += " ";
//    }
//
//    str += argv[i];
//  }
//  return str;
//}

////------------------------------------------------------------------------------
//// logTpconfigLines
////------------------------------------------------------------------------------
//static void logTpconfigLines(cta::log::Logger &log,
//  const cta::tape::daemon::TpconfigLines &lines) {
//  using namespace cta::tape::daemon;
//
//  for(TpconfigLines::const_iterator itor = lines.begin();
//    itor != lines.end(); itor++) {
//    logTpconfigLine(log, *itor);
//  }
//}
//
////------------------------------------------------------------------------------
//// logTpconfigLine
////------------------------------------------------------------------------------
//static void logTpconfigLine(cta::log::Logger &log,
//  const cta::tape::daemon::TpconfigLine &line) {
//  std::list<cta::log::Param> params = {
//    cta::log::Param("tapeDrive", line.unitName),
//    cta::log::Param("logicalLibrary", line.logicalLibrary),
//    cta::log::Param("devFilename", line.devFilename),
//    cta::log::Param("librarySlot", line.librarySlot)};
//  log(log::INFO, "TPCONFIG line", params);
//}

}} // namespace cta::taped

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

  // Try to instantiate the logging system API
  std::unique_ptr<log::Logger> logPtr;
  try {
    const std::string shortHostName = utils::getShortHostname();
    if (commandLine->logToStdout) {
      logPtr.reset(new log::StdoutLogger(shortHostName, "cta-taped"));
    } else if (commandLine->logToFile) {
      logPtr.reset(new log::FileLogger(shortHostName, "cta-taped", commandLine->logFilePath, log::DEBUG));
    } else {
      logPtr.reset(new log::SyslogLogger(shortHostName, "cta-taped", log::DEBUG));
    }
  } catch(exception::Exception &ex) {
    std::cerr <<
      "Failed to instantiate object representing CTA logging system: " <<
      ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  }
  cta::log::Logger &log = *logPtr;

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
