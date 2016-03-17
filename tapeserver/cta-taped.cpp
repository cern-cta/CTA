/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/Configuration.hpp"
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
    "\t--foreground            or -f         \tRemain in the Foreground\n"
    "\t--stdout                or -s         \tPrint logs to standard output. Required --foreground\n"
    "\t--config <config-file>  or -c         \tConfiguration file\n"
    "\t--help                  or -h         \tPrint this help and exit\n"
    "\n"
    "Comments to: Castor.Support@cern.ch\n";

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

  // Parse /etc/cta/cta.conf and /etc/cta/TPCONFIG for global parameters
  const TapedConfiguration globalConfig =
    TapedConfiguration::createFromCtaConf(commandLine.configFileLocation, log);
    
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

  std::list<log::Param> params = {log::Param("version", CTA_VERSION)};
  params.splice(params.end(), commandLine.toLogParams());
  log(log::INFO, "cta-taped started", params);
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
//    cta::log::Param("unitName", line.unitName),
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
    if (commandLine->logToStdout) {
      logPtr.reset(new log::StdoutLogger("cta-taped"));
    } else {
      logPtr.reset(new log::SyslogLogger(log::SOCKET_NAME, "cta-taped",
        log::DEBUG));
    }
  } catch(exception::Exception &ex) {
    std::cerr <<
      "Failed to instantiate object representing CTA logging system: " <<
      ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  }
  log::Logger &log = *logPtr;

  int programRc = EXIT_FAILURE; // Default return code when receiving an exception.
  try {
    programRc = cta::taped::exceptionThrowingMain(*commandLine, log);
  } catch(exception::Exception &ex) {
    std::list<log::Param> params = {
      log::Param("message", ex.getMessage().str())};
    log(log::ERR, "Caught an unexpected CTA exception", params);
  } catch(std::exception &se) {
    std::list<log::Param> params = {log::Param("what", se.what())};
    log(log::ERR, "Caught an unexpected standard exception", params);
  } catch(...) {
    log(log::ERR, "Caught an unexpected and unknown exception");
  }

  google::protobuf::ShutdownProtobufLibrary();
  return programRc;
}
