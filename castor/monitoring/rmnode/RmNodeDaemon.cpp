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
 * @(#)$RCSfile $ $Author $
 *
 * This daemons collects data concerning its node and sends them to
 * the monitoring master daemon
 *
 * @author castor-dev team
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "castor/monitoring/rmnode/RmNodeDaemon.hpp"
#include "castor/monitoring/rmnode/StateThread.hpp"
#include "castor/monitoring/rmnode/MetricsThread.hpp"

#define RMMASTER_PORT 15003
#define STATE_UPDATE_INTERVAL 60 // in seconds
#define METRICS_UPDATE_INTERVAL 10 // in seconds

// -----------------------------------------------------------------------
// External C function used for getting configuration from shift.conf file
// -----------------------------------------------------------------------
extern "C" {
  char* getconfent (const char *, const char *, int);
}

//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------
int main(int argc, char* argv[]) {

  try {

    // new BaseDaemon as Server
    castor::monitoring::rmnode::RmNodeDaemon daemon;

    // intervals default values
    int stateInterval = STATE_UPDATE_INTERVAL;
    int metricsInterval = METRICS_UPDATE_INTERVAL;

    // Try to retrieve interval values from the config file
    // otherwise use the default one
    char* intervalStr = getconfent("RmNode","StateUpdateInterval", 0);
    if (intervalStr){
      stateInterval = std::strtol(intervalStr,0,10);
      if (0 == stateInterval) {
        // Go back to default
        stateInterval = STATE_UPDATE_INTERVAL;
        // "Bad state interval value in configuration file"
        castor::dlf::Param initParams[] =
          {castor::dlf::Param("Given value", intervalStr)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, initParams);
      }
    }
    intervalStr = getconfent("RmNode","MetricsUpdateInterval", 0);
    if (intervalStr) {
      metricsInterval = std::strtol(intervalStr,0,10);
      if (0 == metricsInterval) {
        // Go back to default
        metricsInterval = METRICS_UPDATE_INTERVAL;
        // "Bad metrics interval value in configuration file"
        castor::dlf::Param initParams[] =
          {castor::dlf::Param("Given value", intervalStr)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 2, 1, initParams);
      }
    }

    // Try to retrieve the rmmaster machine and port from the
    // config file. No default provided for the machine.
    char* rmMasterHost = getconfent("RM","HOST", 0);
    if (0 == rmMasterHost) {
      // Raise an exception
      castor::exception::NoEntry e;
      e.getMessage() << "Found not entry RM/HOST in config file";
      throw e;
    } else {
      rmMasterHost = strdup(rmMasterHost);
    }
    int rmMasterPort = RMMASTER_PORT;
    char* rmMasterPortStr = getconfent("RM","PORT", 0);
    if (rmMasterPortStr){
      rmMasterPort = std::strtol(rmMasterPortStr,0,10);
      if (0 == rmMasterPort) {
        // Go back to default
        rmMasterPort = STATE_UPDATE_INTERVAL;
        // "Bad rmmaster port value in configuration file"
        castor::dlf::Param initParams[] =
          {castor::dlf::Param("Given value", rmMasterPortStr)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 5, 1, initParams);
      }
    }    

    // Inform user : "Starting RmNode Daemon"
    castor::dlf::Param initParams[] =
      {castor::dlf::Param("Update state interval value", stateInterval),
       castor::dlf::Param("Update metrics interval value", metricsInterval),
       castor::dlf::Param("Rmmaster host", rmMasterHost),
       castor::dlf::Param("Rmmaster port", rmMasterPort)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 4, 4, initParams);

    // Create a thread for DiskServer State monitoring
    daemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("State",
        new castor::monitoring::rmnode::StateThread
        (rmMasterHost, rmMasterPort),
        0,
        stateInterval));
    daemon.getThreadPool('S')->setNbThreads(1);

    // Create a thread for DiskServer State monitoring
    daemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("Metrics",
        new castor::monitoring::rmnode::MetricsThread
        (rmMasterHost, rmMasterPort),
        0,
        metricsInterval));
    daemon.getThreadPool('M')->setNbThreads(1);

    daemon.parseCommandLine(argc, argv);
    daemon.start();

  } catch (castor::exception::Exception e) {

    // "Exception caught problem to start the daemon"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 3, 2, params);
    return -1;
  }
  catch (...) {

    std::cerr << "Caught general exception!" << std::endl;
    castor::dlf::Param params2[] =
      {castor::dlf::Param("Standard Message", "Caught general exception in cleaning daemon.")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 3, 1, params2);
    return -1;

  }
  return 0;
}

//------------------------------------------------------------------------------
// RmNodeDaemon Constructor
// also initialises the logging facility
//------------------------------------------------------------------------------

castor::monitoring::rmnode::RmNodeDaemon::RmNodeDaemon() :
  castor::server::BaseDaemon("RmNodeDaemon") {

  castor::BaseObject::initLog("RmNode", castor::SVC_DLFMSG);
  // Initializes the DLF logging. This includes
  // registration of the predefined messages

  castor::dlf::Message messages[] =
    {{ 0, ""},
     { 1, "Bad state interval value in configuration file"},
     { 2, "Bad metrics interval value in configuration file"},
     { 3, "Starting of RmNode Daemon failed"},
     { 4, "Starting RmNode Daemon"},
     { 5, "Bad rmmaster port value in configuration file"},
     { 6, "State thread created"},
     { 7, "Metrics thread created"},
     { 8, "State thread started"},
     { 9, "Metrics thread started"},
     {12, "State thread destructed"},
     {13, "Metrics thread destructed"},
     {14, "Error caught in StateThread::run"},
     {15, "Error caught in MetricsThread::run"},
     {16, "State sent to rmMaster"},
     {17, "Metrics sent to rmMaster"},
     {18, "Bad minFreeSpace value in configuration file"},
     {19, "Bad maxFreeSpace interval value in configuration file"},
     {20, "Bad minAllowedFreeSpace interval value in configuration file"},
     {-1, ""}};
  castor::dlf::dlf_init("RmNode", messages);

}



