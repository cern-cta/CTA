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
#include "common/log/SyslogLogger.hpp"
#include "common/processCap/ProcessCap.hpp"
#include "tapeserver/daemon/GlobalConfiguration.hpp"
#include "tapeserver/daemon/TpconfigLines.hpp"
#include "tapeserver/daemon/TapeDaemon.hpp"

#include "version.h"

#include <google/protobuf/stubs/common.h>
#include <memory>
#include <sstream>
#include <string>
#include <iostream>

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
static int exceptionThrowingMain(const int argc, char **const argv,
  cta::log::Logger &log);

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char **const argv) {
  // Try to instantiate the logging system API
  std::unique_ptr<cta::log::SyslogLogger> logPtr;
  try {
    logPtr.reset(new cta::log::SyslogLogger("tapeserverd"));
  } catch(cta::exception::Exception &ex) {
    std::cerr <<
      "Failed to instantiate object representing CTA logging system: " <<
      ex.getMessage().str() << std::endl;
    return 1;
  }
  cta::log::Logger &log = *logPtr;

  int programRc = 1; // Be pessimistic
  try {
    programRc = exceptionThrowingMain(argc, argv, log);
  } catch(cta::exception::Exception &ex) {
    cta::log::Param params[] = {
      cta::log::Param("message", ex.getMessage().str())};
    log(LOG_ERR, "Caught an unexpected CASTOR exception", params);
  } catch(std::exception &se) {
    cta::log::Param params[] = {cta::log::Param("what", se.what())};
    log(LOG_ERR, "Caught an unexpected standard exception", params);
  } catch(...) {
    log(LOG_ERR, "Caught an unexpected and unknown exception");
  }

  google::protobuf::ShutdownProtobufLibrary();
  return programRc;
}

//------------------------------------------------------------------------------
// Logs the start of the daemon.
//------------------------------------------------------------------------------
static void logStartOfDaemon(cta::log::Logger &log, const int argc,
  const char *const *const argv);

//------------------------------------------------------------------------------
// Creates a string that contains the specified command-line arguments
// separated by single spaces.
//
// @param argc The number of command-line arguments.
// @param argv The array of command-line arguments.
//------------------------------------------------------------------------------
static std::string argvToString(const int argc, const char *const *const argv);

////------------------------------------------------------------------------------
//// Writes the specified TPCONFIG lines to the specified logging system.
////
//// @param log The logging system.
//// @param lines The lines parsed from /etc/castor/TPCONFIG.
////------------------------------------------------------------------------------
//static void logTpconfigLines(cta::log::Logger &log,
//  const cta::tape::daemon::TpconfigLines &lines);
//
////------------------------------------------------------------------------------
//// Writes the specified TPCONFIG lines to the logging system.
////
//// @param log The logging system.
//// @param line The line parsed from /etc/castor/TPCONFIG.
////------------------------------------------------------------------------------
//static void logTpconfigLine(cta::log::Logger &log,
//  const cta::tape::daemon::TpconfigLine &line);

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const int argc, char **const argv,
  cta::log::Logger &log) {
  using namespace cta::tape::daemon;

  logStartOfDaemon(log, argc, argv);

  // Parse /etc/cta/cta.conf and /etc/cta/TPCONFIG for global parameters
  const GlobalConfiguration globalConfig =
    GlobalConfiguration::createFromCtaConf(log);

  // Create the object providing utilities for working with UNIX capabilities
  cta::server::ProcessCap capUtils;

  // Create the main tapeserverd object
  cta::tape::daemon::TapeDaemon daemon(
    argc,
    argv,
    std::cout,
    std::cerr,
    log,
    globalConfig,
    capUtils);

  // Run the tapeserverd daemon
  return daemon.main();
}

//------------------------------------------------------------------------------
// logStartOfDaemon
//------------------------------------------------------------------------------
static void logStartOfDaemon(cta::log::Logger &log, const int argc,
  const char *const *const argv) {

  const std::string concatenatedArgs = argvToString(argc, argv);
  cta::log::Param params[] = {
    cta::log::Param("version", CTA_VERSION),
    cta::log::Param("argv", concatenatedArgs)};
  log(LOG_INFO, "tapeserverd started", params);
}

//------------------------------------------------------------------------------
// argvToString
//------------------------------------------------------------------------------
static std::string argvToString(const int argc, const char *const *const argv) {
  std::string str;

  for(int i=0; i < argc; i++) {
    if(i != 0) {
      str += " ";
    }

    str += argv[i];
  }
  return str;
}

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
//  cta::log::Param params[] = {
//    cta::log::Param("unitName", line.unitName),
//    cta::log::Param("logicalLibrary", line.logicalLibrary),
//    cta::log::Param("devFilename", line.devFilename),
//    cta::log::Param("librarySlot", line.librarySlot)};
//  log(LOG_INFO, "TPCONFIG line", params);
//}
