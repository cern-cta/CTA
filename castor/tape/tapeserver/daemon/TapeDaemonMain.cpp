/******************************************************************************
 *                 castor/tape/tapeserver/TapeDaemonMain.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/common/CastorConfiguration.hpp"
#include "castor/legacymsg/NsProxy_TapeAlwaysEmptyFactory.hpp"
#include "castor/legacymsg/RmcProxyTcpIpFactory.hpp"
#include "castor/legacymsg/VdqmProxyTcpIp.hpp"
#include "castor/legacymsg/VmgrProxyTcpIp.hpp"
#include "castor/log/SyslogLogger.hpp"
#include "castor/messages/TapeserverProxyZmqFactory.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"
#include "castor/tape/tapeserver/daemon/CapabilityUtilsImpl.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/tapeserver/daemon/MountSession.hpp"
#include "castor/tape/tapeserver/daemon/TapeDaemon.hpp"
#include "castor/utils/utils.hpp"
#include "h/rmc_constants.h"
#include "h/vdqm_constants.h"
#include "h/vmgr_constants.h"

#include <sstream>
#include <string>
namespace{
  class RAIIForContext{
    zmq::context_t& m_context;
    public:
    RAIIForContext(zmq::context_t& ctx):m_context(ctx){}
    ~RAIIForContext(){m_context.close();}
  };
}
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
  try {
    castor::log::SyslogLogger log("tapeserverd");

    try {
      return exceptionThrowingMain(argc, argv, log);
    } catch(castor::exception::Exception &ex) {
      castor::log::Param params[] = {castor::log::Param("message", ex.getMessage().str())};
      log(LOG_ERR, "Caught an unexpected CASTOR exception", params);
      return 1;
    } catch(std::exception &se) {
      castor::log::Param params[] = {castor::log::Param("what", se.what())};
      log(LOG_ERR, "Caught an unexpected standard exception", params);
      return 1;
    } catch(...) {
      log(LOG_ERR, "Caught an unexpected and unknown exception");
      return 1;
    }
  } catch(castor::exception::Exception &ex) {
    std::cerr << "Failed to instantiate object representing CASTOR logging system"
      ": " << ex.getMessage().str() << std::endl;
    return 1;
  } catch(std::exception &se) {
    std::cerr << "Failed to instantiate object representing CASTOR logging system"
      ": " << se.what() << std::endl;
    return 1;
  } catch(...) {
    std::cerr << "Failed to instantiate object representing CASTOR logging system"
      ": Caught an unknown exception" << std::endl;
    return 1;
  }
  return 0;
}

//------------------------------------------------------------------------------
// Writes the specified TPCONFIG lines to the specified logging system.
//
// @param log The logging system.
// @param lines The lines parsed from /etc/castor/TPCONFIG.
//------------------------------------------------------------------------------
static void logTpconfigLines(castor::log::Logger &log,
  const utils::TpconfigLines &lines) throw();

//------------------------------------------------------------------------------
// Writes the specified TPCONFIG lines to the logging system.
//
// @param log The logging system.
// @param line The line parsed from /etc/castor/TPCONFIG.
//------------------------------------------------------------------------------
static void logTpconfigLine(castor::log::Logger &log,
  const utils::TpconfigLine &line) throw();

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const int argc, char **const argv, castor::log::Logger &log) {
  using namespace castor::tape::tapeserver::daemon;
  
  const std::string vdqmHostName =
    castor::common::CastorConfiguration::getConfig().getConfEntString("VDQM", "HOST");
  const std::string vmgrHostName =
    castor::common::CastorConfiguration::getConfig().getConfEntString("VMGR", "HOST");
  zmq::context_t ctx;
  RAIIForContext raii(ctx);
  // Parse /etc/castor/TPCONFIG
  castor::tape::utils::TpconfigLines tpconfigLines;
  castor::tape::utils::parseTpconfigFile("/etc/castor/TPCONFIG", tpconfigLines);
  logTpconfigLines(log, tpconfigLines);
  castor::tape::utils::DriveConfigMap driveConfigs;
  driveConfigs.enterTpconfigLines(tpconfigLines);

  // Create proxy objects for the vdqm, vmgr and rmc daemons
  const int netTimeout = 10; // Timeout in seconds
  castor::legacymsg::VdqmProxyTcpIp vdqm(log, vdqmHostName, VDQM_PORT, netTimeout);
  castor::legacymsg::VmgrProxyTcpIp vmgr(log, vmgrHostName, VMGR_PORT, netTimeout);
  castor::legacymsg::RmcProxyTcpIpFactory rmcFactory(log, netTimeout);
  castor::messages::TapeserverProxyZmqFactory tapeserverFactory(log, TAPE_SERVER_INTERNAL_LISTENING_PORT,
    netTimeout);
  castor::legacymsg::NsProxy_TapeAlwaysEmptyFactory nsFactory;

  // Create the poll() reactor
  //castor::io::PollReactorImpl reactor(log);
  castor::tape::reactor::ZMQReactor reactor(log,ctx);

  // Create the object providing utilities for working with UNIX capabilities
  CapabilityUtilsImpl capUtils;

  // Create the main tapeserverd object
  TapeDaemon daemon(
    argc,
    argv,
    std::cout,
    std::cerr,
    log,
    driveConfigs,
    vdqm,
    vmgr,
    rmcFactory,
    tapeserverFactory,
    nsFactory,
    reactor,
    capUtils);

  // Run the tapeserverd daemon
  return daemon.main();
}

//------------------------------------------------------------------------------
// logTpconfigLines
//------------------------------------------------------------------------------
static void logTpconfigLines(castor::log::Logger &log,
  const utils::TpconfigLines &lines) throw() {
  for(utils::TpconfigLines::const_iterator itor = lines.begin();
    itor != lines.end(); itor++) {
    logTpconfigLine(log, *itor);
  }
}

//------------------------------------------------------------------------------
// logTpconfigLine
//------------------------------------------------------------------------------
static void logTpconfigLine(castor::log::Logger &log,
  const utils::TpconfigLine &line) throw() {
  castor::log::Param params[] = {
    castor::log::Param("unitName", line.unitName),
    castor::log::Param("dgn", line.dgn),
    castor::log::Param("devFilename", line.devFilename),
    castor::log::Param("density", line.density),
    castor::log::Param("librarySlot", line.librarySlot),
    castor::log::Param("devType", line.devType)};
  log(LOG_INFO, "TPCONFIG line", params);
}
