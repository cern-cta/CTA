/******************************************************************************
 *                      RmNodeDaemon.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * This daemons collects data concerning its node and sends them to
 * the monitoring master daemon
 *
 * @author castor-dev team
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/System.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "castor/monitoring/rmnode/RmNodeDaemon.hpp"
#include "castor/monitoring/rmnode/StateThread.hpp"
#include "castor/monitoring/rmnode/MetricsThread.hpp"
#include "getconfent.h"
#include <map>

// Definitions
#define DEFAULT_RMMASTER_PORT    15003
#define DEFAULT_STATE_INTERVAL   60
#define DEFAULT_METRICS_INTERVAL 10


//-----------------------------------------------------------------------------
// Main
//-----------------------------------------------------------------------------
int main(int argc, char *argv[]) {

  try {
    castor::monitoring::rmnode::RmNodeDaemon daemon;

    // A container for a list of hosts and ports to send monitoring
    // information to
    std::map<std::string, u_signed64> hostList;
    std::ostringstream hosts;

    // Extract the list of hosts to send to. For reasons of redundancy we
    // support sending information to multiple hosts i.e. the master and slave
    // LSF masters.
    char **values;
    int  count;
    if (getconfent_multi("RM", "HOST", 1, &values, &count) < 0) {
      castor::exception::Exception e(EINVAL);
      e.getMessage() << "Missing configuration option RM/HOST" << std::endl;
      throw e;
    } else {
      for (int i = 0; i < count; i++) {
	hostList[values[i]] = 0;
	hosts << values[i] << " ";
	free(values[i]);
      }
      free(values);
    }

    // Extract the port for the hosts.
    char *value = getconfent("RM", "PORT", 0);
    int port = DEFAULT_RMMASTER_PORT;
    if (value) {
      try {
	port = castor::System::porttoi(value);
      } catch (castor::exception::Exception& ex) {
	castor::exception::Exception e(EINVAL);
	e.getMessage() << "Invalid RM/PORT value: "
		       << ex.getMessage() << std::endl;
	throw e;
      }
    }

    // If any hosts have a 0 port entry set it to the default
    for (std::map<std::string, u_signed64>::iterator it =
	   hostList.begin();
	 it != hostList.end(); it++) {
      if ((*it).second == 0) {
	(*it).second = port;
      }
    }

    // Extract the frequency at which the state thread runs
    value = getconfent("RmNode", "StateUpdateInterval", 0);
    u_signed64 stateInterval = DEFAULT_STATE_INTERVAL;
    if (value) {
      stateInterval = strtol(value, 0, 10);
      if (stateInterval == 0) {
	stateInterval = DEFAULT_STATE_INTERVAL;
	// "Invalid RmNode/StateUpdateInterval option, using default"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("Default", stateInterval)};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 1, 1, params);
      }
    }

    // Extract the frequency at which the metrics thread runs
    value = getconfent("RmNode", "MetricsUpdateInterval", 0);
    u_signed64 metricsInterval = DEFAULT_METRICS_INTERVAL;
    if (value) {
      metricsInterval = strtol(value, 0, 10);
      if (metricsInterval == 0) {
	metricsInterval = DEFAULT_METRICS_INTERVAL;
	// "Invalid RmNode/MetricsUpdateInterval option, using default"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("Default", metricsInterval)};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 1, 1, params);
      }
    }

    // "RmNode Daemon started"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Servers", hosts.str()),
       castor::dlf::Param("Port", port),
       castor::dlf::Param("StateInterval", stateInterval),
       castor::dlf::Param("MetricsInterval", metricsInterval)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 4, 4, params);

    // Metrics Thread
    daemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("Metrics",
        new castor::monitoring::rmnode::MetricsThread(hostList, port),
        metricsInterval));
    daemon.getThreadPool('M')->setNbThreads(1);

    // State Thread
    daemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("State",
        new castor::monitoring::rmnode::StateThread(hostList, port),
        stateInterval));
    daemon.getThreadPool('S')->setNbThreads(1);

    // Start daemon
    daemon.parseCommandLine(argc, argv);
    daemon.start();
    return 0;

  } catch (castor::exception::Exception& e) {
    std::cerr << "Caught exception: "
	      << sstrerror(e.code()) << std::endl
	      << e.getMessage().str() << std::endl;

    // "Exception caught when starting RmNodeDaemon"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 3, 2, params);
  } catch (...) {
    std::cerr << "Caught exception!" << std::endl;
  }

  return 1;
}


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::monitoring::rmnode::RmNodeDaemon::RmNodeDaemon() :
  castor::server::BaseDaemon("rmnoded") {

  // Now with predefined messages
  castor::dlf::Message messages[] = {

    // General
    { 1,  "Invalid RmNode/StateUpdateInterval option, using default" },
    { 2,  "Invalid RmNode/MetricsUpdateInterval option, using default" },
    { 3,  "Exception caught when starting RmNode" },
    { 4,  "RmNode Daemon started" },
    { 5,  "Failed to update the RmNode/StatusFile, filesystem acknowledgement mismatch" },

    // State Thread
    { 6,  "State thread created" },
    { 8,  "State thread running" },
    { 12, "State thread destroyed" },
    { 13, "Received unknown object for acknowledgement from socket" },
    { 14, "Error caught when trying to send state information" },
    { 16, "State information has been sent" },
    { 18, "Invalid RmNode/MinFreeSpace option, using default" },
    { 19, "Invalid RmNode/MaxFreeSpace option, using default" },
    { 20, "Invalid RmNode/MinAllowedFreeSpace option, using default" },
    { 21, "Failed to send state information" },
    { 22, "Failed to update the RmNode/StatusFile" },
    { 23, "Failed to collect diskserver status" },

    // Metrics Thread
    { 7,  "Metrics thread created" },
    { 9,  "Metrics thread running" },
    { 13, "Metrics thread destroyed" },
    { 15, "Error caught when trying to send metric information" },
    { 17, "Metrics information has been sent" },
    { 24, "Failed to collect diskserver metrics" },
    { 25, "Failed to send metrics information" },
    { 26, "Received no acknowledgement from server" },
    { 27, "Failed to collect filesystem metrics" },
 
    { -1, "" }};
  dlfInit(messages);
}


//-----------------------------------------------------------------------------
// getMountPoints
//-----------------------------------------------------------------------------
std::vector<std::string>
castor::monitoring::rmnode::RmNodeDaemon::getMountPoints()
  throw(castor::exception::Exception) {

  // Convert the value of the RmNode/MountPoints configuration option into a
  // vector of strings
  std::vector<std::string> fileSystems;
  char **values;
  int  count;
  if (getconfent_multi("RmNode", "MountPoints", 1, &values, &count) == 0) {
    for (int i = 0; i < count; i++) {
      fileSystems.push_back(values[i]);
      free(values[i]);
    }
    free(values);
  }
  return fileSystems;
}
