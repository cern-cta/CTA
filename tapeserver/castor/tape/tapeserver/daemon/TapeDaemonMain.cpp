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
#include "castor/legacymsg/CupvProxyTcpIp.hpp"
#include "castor/legacymsg/VdqmProxyTcpIp.hpp"
#include "castor/legacymsg/VmgrProxyTcpIp.hpp"
#include "castor/log/SyslogLogger.hpp"
#include "castor/server/ProcessCap.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "castor/tape/tapeserver/daemon/TapeDaemon.hpp"
#include "castor/utils/utils.hpp"
#include "Cupv_constants.h"
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
// Writes the specified vdqm hosts to the logging system.  These hosts
// should have been parsed from the contents of the TapeServer:VdqmHosts
// entry (if there is one) in the CASTOR configuration file
// /etc/castor/castor.conf.
//------------------------------------------------------------------------------
static void logParsedTrustedVdqmHosts(castor::log::Logger &log,
  const std::vector<std::string> &vdqmHosts);

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
  logParsedTrustedVdqmHosts(log, tapeDaemonConfig.vdqmHosts);

  // Parse /etc/castor/TPCONFIG
  const tape::tapeserver::daemon::TpconfigLines tpconfigLines =
    tape::tapeserver::daemon::TpconfigLines::parseFile("/etc/castor/TPCONFIG");
  logTpconfigLines(log, tpconfigLines);
  tape::tapeserver::daemon::DriveConfigMap driveConfigs;
  driveConfigs.enterTpconfigLines(tpconfigLines);

  // Create proxy objects for the vdqm, vmgr and rmc daemons
  const int netTimeout = 10; // Timeout in seconds
  legacymsg::CupvProxyTcpIp cupv(log, tapeDaemonConfig.cupvHost, CUPV_PORT,
    netTimeout);
  legacymsg::VdqmProxyTcpIp vdqm(log, tapeDaemonConfig.vdqmHost, VDQM_PORT,
    netTimeout);
  legacymsg::VmgrProxyTcpIp vmgr(tapeDaemonConfig.vmgrHost, VMGR_PORT,
    netTimeout);

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
    cupv,
    vdqm,
    vmgr,
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
// logParsedTrustedVdqmHosts
//------------------------------------------------------------------------------
static void logParsedTrustedVdqmHosts(castor::log::Logger &log,
  const std::vector<std::string> &vdqmHosts) {
  std::ostringstream oss;
  for(std::vector<std::string>::const_iterator itor = vdqmHosts.begin();
    itor != vdqmHosts.end(); itor++) {
    if(itor != vdqmHosts.begin()) {
      oss << " ";
    }
    oss << *itor;
  }

  castor::log::Param params[] = {
    castor::log::Param("nbVdqmHosts", vdqmHosts.size()),
    castor::log::Param("vdqmHosts", oss.str())};
  log(LOG_INFO, "Parsed trusted vdqm hosts", params);
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
    castor::log::Param("dgn", line.dgn),
    castor::log::Param("devFilename", line.devFilename),
    castor::log::Param("librarySlot", line.librarySlot)};
  log(LOG_INFO, "TPCONFIG line", params);
}
