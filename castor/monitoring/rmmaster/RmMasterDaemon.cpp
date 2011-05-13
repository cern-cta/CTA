/******************************************************************************
 *                      RmMasterDaemon.cpp
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
 * The monitoring resource master daemon is resonsible for collecting all
 * input from the diskservers and updating both the database and the LSF
 * scheduler shared memory. It also receives UDP messages migration and recall
 * processes whenever a stream is open/closed
 *
 * @author castor-dev team
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "castor/server/UDPListenerThreadPool.hpp"
#include "castor/server/TCPListenerThreadPool.hpp"
#include "castor/monitoring/ClusterStatus.hpp"
#include "castor/monitoring/rmmaster/RmMasterDaemon.hpp"
#include "castor/monitoring/rmmaster/CollectorThread.hpp"
#include "castor/monitoring/rmmaster/UpdateThread.hpp"
#include "castor/monitoring/rmmaster/DatabaseActuatorThread.hpp"
#include "castor/monitoring/rmmaster/HeartbeatThread.hpp"
#include "castor/monitoring/rmmaster/IRmMasterSvc.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/Services.hpp"
#include "castor/System.hpp"
#include "getconfent.h"

// Definitions
#define DEFAULT_LISTENING_PORT  15003
#define DEFAULT_UPDATE_INTERVAL 10


//-----------------------------------------------------------------------------
// Main
//-----------------------------------------------------------------------------
int main(int argc, char *argv[]) {

  try {
    castor::monitoring::rmmaster::RmMasterDaemon daemon;

    // Extract the frequency at which the synchronisation from the shared
    // memory to the database is performed
    char *value = getconfent("RmMaster", "UpdateInterval", 0);
    u_signed64 updateInterval = DEFAULT_UPDATE_INTERVAL;
    if (value) {
      updateInterval = strtol(value, 0, 10);
      if (updateInterval == 0) {
	updateInterval = DEFAULT_UPDATE_INTERVAL;
	// "Invalid RmMaster/UpdateInterval option, using default"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("Default", updateInterval)};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 1, 1, params);
      }
    }

    // Determine the listening port for incoming data from diskservers and
    // requests to change status with rmAdminNode
    int listenPort = DEFAULT_LISTENING_PORT;
    if ((value = getconfent("RM", "PORT", 0))) {
      try {
	listenPort = castor::System::porttoi(value);
      } catch (castor::exception::Exception& ex) {
	castor::exception::Exception e(EINVAL);
	e.getMessage() << "Invalid RM/PORT value: "
		       << ex.getMessage() << std::endl;
	throw e;
      }
    }

    // Check whether we should run in noLSF mode
    char *noLSFValue = getconfent("RmMaster", "NoLSFMode", 0);
    bool noLSF = (noLSFValue != NULL && strcasecmp(noLSFValue, "yes") == 0);

    if (!noLSF) {
        
      // Attempt to find the LSF cluster and master name for logging purposes.
      // This isn't really required but could be useful for future debugging
      // efforts
      std::string clusterName("Unknown");
      std::string masterName("Unknown");
      char **results = NULL;

      // Errors are ignored here!
      lsb_init((char*)"rmmasterd");
      clusterInfo *cInfo = ls_clusterinfo(NULL, NULL, results, 0, 0);
      if (cInfo != NULL) {
        clusterName = cInfo[0].clusterName;
        masterName  = cInfo[0].masterName;
      }

      // "RmMaster Daemon started"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Port", listenPort),
         castor::dlf::Param("UpdateInterval", updateInterval),
         castor::dlf::Param("Cluster", clusterName),
         castor::dlf::Param("Master", masterName),
         castor::dlf::Param("LSFEnabled", "Yes")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 2, 5, params);

    } else {
        
      // "RmMaster Daemon started"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Port", listenPort),
         castor::dlf::Param("UpdateInterval", updateInterval),
         castor::dlf::Param("LSFEnabled", "No")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 2, 3, params);
    }
    
    // DB threadpool
    daemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("DatabaseActuator",
        new castor::monitoring::rmmaster::DatabaseActuatorThread
        (daemon.clusterStatus()), updateInterval));
    daemon.getThreadPool('D')->setNbThreads(1);

    // Update threadpool
    daemon.addThreadPool
      (new castor::server::UDPListenerThreadPool
       ("Update",
        new castor::monitoring::rmmaster::UpdateThread
        (daemon.clusterStatus()), listenPort));
    daemon.getThreadPool('U')->setNbThreads(1);

    // Monitor threadpool
    daemon.addThreadPool
      (new castor::server::TCPListenerThreadPool
       ("Collector",
        new castor::monitoring::rmmaster::CollectorThread
        (daemon.clusterStatus()), listenPort));
    daemon.getThreadPool('C')->setNbThreads(6);

    // Heartbeat threadpool
    daemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("Heartbeat",
        new castor::monitoring::rmmaster::HeartbeatThread
	(daemon.clusterStatus()), updateInterval));
    daemon.getThreadPool('H')->setNbThreads(1);

    // Start daemon
    daemon.parseCommandLine(argc, argv);
    daemon.start();
    return 0;

  } catch (castor::exception::Exception& e) {
    std::cerr << "Caught exception: "
	      << sstrerror(e.code()) << std::endl
	      << e.getMessage().str() << std::endl;

    // "Exception caught when starting RmMasterDaemon"
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
castor::monitoring::rmmaster::RmMasterDaemon::RmMasterDaemon() :
  castor::server::BaseDaemon("rmmasterd") {

  // Now with predefined messages
  castor::dlf::Message messages[] = {

    // General
    { 1,  "Invalid RmMaster/UpdateInterval option, using default" },
    { 2,  "RmMaster Daemon started" },
    { 3,  "Exception caught when starting RmMaster" },
    { 13, "Received unknown object from socket" },
    { 17, "General exception caught" },
    { 35, "Updating cluster status information from database to shared memory" },
    { 46, "Failed to determine the status of LSF, skipping database synchronization" },

    // Heartbeat thread
    { 18, "Invalid RmMaster/HeartbeatInterval option, using default" },
    { 36, "Heartbeat thread created" },
    { 37, "Heartbeat thread running" },
    { 38, "Heartbeat check failed for diskserver, status changed to DISABLED" },

    // Update thread
    { 19, "Update thread running" },
    { 20, "Caught exception in UpdateThread" },
    { 34, "Update thread created" },

    // Collector thread
    { 9,  "Thread Collector running" },
    { 10, "Collector thread created" },
    { 12, "Unable to read object from socket" },
    { 15, "Sending acknowledgement to client" },
    { 16, "Unable to send ack to client" },
    { 45, "Caught exception in CollectorThread" },

    // Status Update Helper
    { 21, "Ignored state report for machine with empty name" },
    { 22, "Ignored metrics report for machine with empty name" },
    { 23, "Ignored admin diskserver report for machine with empty name" },
    { 24, "Ignored admin filesystem report for machine with empty name" },
    { 25, "Ignored admin filesystem report for filesystem with empty name" },
    { 26, "Ignored metrics report for filesystem with empty name" },
    { 27, "Ignored state report for filesystem with empty name" },
    { 28, "Unable to allocate SharedMemoryString" },
    { 29, "Ignored admin diskserver report for unknown machine" },
    { 30, "Ignored admin filesystem report for unknown machine" },
    { 31, "Ignored admin filesystem report for unknown mountpoint" },
    { 39, "Heartbeat resumed for diskserver, status changed to PRODUCTION" },
    { 40, "Admin change request detected, filesystem DELETED" },
    { 41, "Admin change request detected for filesystem, setting new status" },
    { 42, "Admin change request detected, diskserver DELETED" },
    { 43, "Admin change request detected for diskserver, setting new status" },

    // Database actuator thread
    { 32, "DatabaseActuator thread running" },
    { 33, "DatabaseActuator thread created" },
    { 44, "Failed to synchronise shared memory with stager database" },
    { 47, "Assuming role as production RmMaster server, failover detected" },
    { 48, "Assuming role as slave RmMaster server, failover detected" },
    { 49, "Initialization information" },

    { -1, "" }};
  dlfInit(messages);

  // Update the shared memory with the initial status and metrics for the
  // diskservers and filesystems as currently recorded in the stager database
  bool created = true;
  m_clusterStatus =
    castor::monitoring::ClusterStatus::getClusterStatus(created);

  // Initialize the oracle rmmaster service.
  castor::IService *orasvc =
    castor::BaseObject::services()->service("OraRmMasterSvc",
					    castor::SVC_ORARMMASTERSVC);
  if (orasvc == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to get OraRmMasterSVC";
    throw e;
  }
  castor::monitoring::rmmaster::IRmMasterSvc *rmMasterService =
    dynamic_cast<castor::monitoring::rmmaster::IRmMasterSvc *>(orasvc);
  if (rmMasterService == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Could not convert newly retrieved service into "
		   << "IRmMasterSvc" << std::endl;
    throw e;
  }

  // "Updating cluster status information from database to shared memory"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 35, 0, 0);
  rmMasterService->retrieveClusterStatus(m_clusterStatus);
}
