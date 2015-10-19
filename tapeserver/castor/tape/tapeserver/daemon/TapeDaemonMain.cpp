/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/common/CastorConfiguration.hpp"
#include "castor/log/SyslogLogger.hpp"
#include "castor/server/ProcessCap.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "castor/tape/tapeserver/daemon/TapeDaemon.hpp"
#include "castor/utils/utils.hpp"
#include "patchlevel.h"
#include "rmc_constants.h"
#include "vdqm_constants.h"
#include "vmgr_constants.h"

#include <google/protobuf/stubs/common.h>
#include <memory>
#include <sstream>
#include <string>

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
  castor::log::Logger &log);

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char **const argv) {
  // Try to instantiate the logging system API
  std::unique_ptr<castor::log::SyslogLogger> logPtr;
  try {
    logPtr.reset(new castor::log::SyslogLogger("tapeserverd"));
  } catch(castor::exception::Exception &ex) {
    std::cerr <<
      "Failed to instantiate object representing CASTOR logging system: " <<
      ex.getMessage().str() << std::endl;
    return 1;
  }
  castor::log::Logger &log = *logPtr.get();

  int programRc = 1; // Be pessimistic
  try {
    programRc = exceptionThrowingMain(argc, argv, log);
  } catch(castor::exception::Exception &ex) {
    castor::log::Param params[] = {
      castor::log::Param("message", ex.getMessage().str())};
    log(LOG_ERR, "Caught an unexpected CASTOR exception", params);
  } catch(std::exception &se) {
    castor::log::Param params[] = {castor::log::Param("what", se.what())};
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
static void logStartOfDaemon(castor::log::Logger &log, const int argc,
  const char *const *const argv);

//------------------------------------------------------------------------------
// Creates a string that contains the specified command-line arguments
// separated by single spaces.
//
// @param argc The number of command-line arguments.
// @param argv The array of command-line arguments.
//------------------------------------------------------------------------------
static std::string argvToString(const int argc, const char *const *const argv);

//------------------------------------------------------------------------------
// Writes the specified TPCONFIG lines to the specified logging system.
//
// @param log The logging system.
// @param lines The lines parsed from /etc/castor/TPCONFIG.
//------------------------------------------------------------------------------
static void logTpconfigLines(castor::log::Logger &log,
  const castor::tape::tapeserver::daemon::TpconfigLines &lines) throw();

//------------------------------------------------------------------------------
// Writes the specified TPCONFIG lines to the logging system.
//
// @param log The logging system.
// @param line The line parsed from /etc/castor/TPCONFIG.
//------------------------------------------------------------------------------
static void logTpconfigLine(castor::log::Logger &log,
  const castor::tape::tapeserver::daemon::TpconfigLine &line) throw();

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const int argc, char **const argv,
  castor::log::Logger &log) {
  using namespace castor;

  logStartOfDaemon(log, argc, argv);

  // Parse /etc/castor/castor.conf
  const tape::tapeserver::daemon::TapeDaemonConfig tapeDaemonConfig =
    tape::tapeserver::daemon::TapeDaemonConfig::createFromCastorConf(&log);

  // Parse /etc/castor/TPCONFIG
  const tape::tapeserver::daemon::TpconfigLines tpconfigLines =
    tape::tapeserver::daemon::TpconfigLines::parseFile("/etc/castor/TPCONFIG");
  logTpconfigLines(log, tpconfigLines);
  tape::tapeserver::daemon::DriveConfigMap driveConfigs;
  driveConfigs.enterTpconfigLines(tpconfigLines);

  const int netTimeout = 10; // Timeout in seconds

  tape::reactor::ZMQReactor reactor(log);

  // Create the object providing utilities for working with UNIX capabilities
  castor::server::ProcessCap capUtils;

  // Create the main tapeserverd object
  tape::tapeserver::daemon::TapeDaemon daemon(
    argc,
    argv,
    std::cout,
    std::cerr,
    log,
    netTimeout,
    driveConfigs,
    reactor,
    capUtils,
    tapeDaemonConfig);

  // Run the tapeserverd daemon
  return daemon.main();
}

//------------------------------------------------------------------------------
// logStartOfDaemon
//------------------------------------------------------------------------------
static void logStartOfDaemon(castor::log::Logger &log, const int argc,
  const char *const *const argv) {
  using namespace castor;

  const std::string concatenatedArgs = argvToString(argc, argv);
  std::ostringstream version;
  version << MAJORVERSION << "." << MINORVERSION << "." << MAJORRELEASE << "-"
    << MINORRELEASE;
  log::Param params[] = {
    log::Param("version", version.str()),
    log::Param("argv", concatenatedArgs)};
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

//------------------------------------------------------------------------------
// logTpconfigLines
//------------------------------------------------------------------------------
static void logTpconfigLines(castor::log::Logger &log,
  const castor::tape::tapeserver::daemon::TpconfigLines &lines) throw() {
  using namespace castor::tape::tapeserver::daemon;

  for(TpconfigLines::const_iterator itor = lines.begin();
    itor != lines.end(); itor++) {
    logTpconfigLine(log, *itor);
  }
}

//------------------------------------------------------------------------------
// logTpconfigLine
//------------------------------------------------------------------------------
static void logTpconfigLine(castor::log::Logger &log,
  const castor::tape::tapeserver::daemon::TpconfigLine &line) throw() {
  castor::log::Param params[] = {
    castor::log::Param("unitName", line.unitName),
    castor::log::Param("logicalLibrary", line.logicalLibrary),
    castor::log::Param("devFilename", line.devFilename),
    castor::log::Param("librarySlot", line.librarySlot)};
  log(LOG_INFO, "TPCONFIG line", params);
}
