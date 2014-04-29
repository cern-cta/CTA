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
#include "castor/log/SyslogLogger.hpp"
#include "castor/io/PollReactorImpl.hpp"
#include "castor/legacymsg/RmcProxyTcpIpFactory.hpp"
#include "castor/legacymsg/VdqmProxyTcpIpFactory.hpp"
#include "castor/legacymsg/VmgrProxyTcpIpFactory.hpp"
#include "castor/tape/tapeserver/daemon/TapeDaemon.hpp"
#include "h/rmc_constants.h"
#include "h/vdqm_constants.h"
#include "h/vmgr_constants.h"

#include <sstream>
#include <string>

//------------------------------------------------------------------------------
// getVdqmHostName
//
// Tries to get the name of the host on which the vdqmd daemon is running by
// parsing /etc/castor/castor.conf.
//
// This function logs the host name if it is successful, else it logs an
// error message and returns an empty string.
//------------------------------------------------------------------------------
static std::string getVdqmHostName(castor::log::Logger &log) throw();

//------------------------------------------------------------------------------
// getVmgrHostName
//
// Tries to get the name of the host on which the vdqmd daemon is running by
// parsing /etc/castor/castor.conf.
//
// This function logs the host name if it is successful, else it logs an
// error message and returns an empty string.
//------------------------------------------------------------------------------
static std::string getVmgrHostName(castor::log::Logger &log) throw();

//------------------------------------------------------------------------------
// getConfigParam
//
// Tries to get the value of the specified parameter from parsing
// /etc/castor/castor.conf.
//------------------------------------------------------------------------------
static std::string getConfigParam(const std::string &category, const std::string &name) throw(castor::exception::Exception);

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char **const argv) {
  using namespace castor::tape::tapeserver::daemon;
  castor::log::SyslogLogger log("tapeserverd");

  // Get the names of the hosts on which the vdqm, vmgr and rmc daemons are running
  const std::string vdqmHostName = getVdqmHostName(log);
  if(vdqmHostName.empty()) {
    log(LOG_ERR, "Aborting: Cannot continue without vdqm host-name");
    return 1;
  }
  const std::string vmgrHostName = getVmgrHostName(log);
  if(vmgrHostName.empty()) {
    log(LOG_ERR, "Aborting: Cannot continue without vmgr host-name");
    return 1;
  }

  // Parse /etc/castor/TPCONFIG
  castor::tape::utils::TpconfigLines tpconfigLines;
  castor::tape::utils::parseTpconfigFile("/etc/castor/TPCONFIG", tpconfigLines);

  // Create proxy objects for the vdqm, vmgr and rmc daemons
  const int netTimeout = 10; // Timeout in seconds
  castor::legacymsg::VdqmProxyTcpIpFactory vdqmFactory(log, vdqmHostName, VDQM_PORT, netTimeout);
  castor::legacymsg::VmgrProxyTcpIpFactory vmgrFactory(log, vmgrHostName, VMGR_PORT, netTimeout);
  castor::legacymsg::RmcProxyTcpIpFactory rmcFactory(log, netTimeout);

  // Create the poll() reactor
  castor::io::PollReactorImpl reactor(log);

  // Create the main tapeserverd object
  TapeDaemon daemon(
    argc,
    argv,
    std::cout,
    std::cerr,
    log,
    tpconfigLines,
    vdqmFactory,
    vmgrFactory,
    rmcFactory,
    reactor);

  // Run the tapeserverd daemon
  return daemon.main();
}

//------------------------------------------------------------------------------
// getVdqmHostName
//------------------------------------------------------------------------------
static std::string getVdqmHostName(castor::log::Logger &log) throw() {
  using namespace castor;

  try {
    const std::string category = "VDQM";
    const std::string name = "HOST";
    const std::string value = getConfigParam(category, name);
    log::Param params[] = {log::Param("value", value)};
    log(LOG_INFO, "VDQM HOST", params);
    return value;
  } catch(castor::exception::Exception &ex) {
    log(LOG_ERR, ex.getMessage().str());
    return "";
  }
}

//------------------------------------------------------------------------------
// getVmgrHostName
//------------------------------------------------------------------------------
static std::string getVmgrHostName(castor::log::Logger &log) throw() {
  using namespace castor;

  try {
    const std::string category = "VMGR";
    const std::string name = "HOST";
    const std::string value = getConfigParam(category, name);
    log::Param params[] = {log::Param("value", value)};
    log(LOG_INFO, "VMGR HOST", params);
    return value;
  } catch(castor::exception::Exception &ex) {
    log(LOG_ERR, ex.getMessage().str());
    return "";
  }
}

//------------------------------------------------------------------------------
// getConfigParam
//------------------------------------------------------------------------------
static std::string getConfigParam(const std::string &category, const std::string &name) throw(castor::exception::Exception) {
  using namespace castor;

  std::ostringstream task;
  task << "get " << category << ":" << name << " from castor.conf";

  common::CastorConfiguration config;
  std::string value;

  try {
    config = common::CastorConfiguration::getConfig();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Failed to get castor configuration: " << ne.getMessage().str();
    throw ex;
  }

  try {
    value = config.getConfEnt(category.c_str(), name.c_str());
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Failed to get castor configuration entry: " << ne.getMessage().str();
    throw ex;
  }

  return value;
}
