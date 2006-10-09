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
 * @(#)$RCSfile $ $Author $
 *
 * The monitoring Daemon master, collecting all the inputs from
 * the different nodes and updating both the database and the
 * LSF scheduler shared memory
 *
 * @author castor-dev team
 *****************************************************************************/

// Include Files

#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "castor/rmmaster/RmMasterDaemon.hpp"
#include "castor/rmmaster/ClusterStatus.hpp"
#include "castor/rmmaster/Collector.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/Services.hpp"

#define UPDATE_INTERVAL 10 // in seconds

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

    castor::stager::IStagerSvc* myService;
    try {
      castor::IService* orasvc = castor::BaseObject::services()-> service("OraStagerSvc", castor::SVC_ORASTAGERSVC);
      if (0 == orasvc) {
	std::cout << "Could Not get OraStagerSvc" << std::endl;
	return -1;
      }
      
      myService = dynamic_cast<castor::stager::IStagerSvc*>(orasvc);
      if (0 == myService) {
	std::cout << "Could Not cast OraStagerSvc into IStagerSvc" << std::endl;
	return -1;
      }
      
    } catch (castor::exception::Exception e) {
      // up to now, we are not a daemon, so we log to std::cerr
      std::cerr << "Exception caught problem to start the daemon :\n"
		<< sstrerror(e.code()) << "\n"
		<< e.getMessage().str() << std::endl;
      return -1;
    }

    // new BaseDaemon as Server
    castor::rmmaster::RmMasterDaemon daemon;
    int interval;

    //default values
    interval = UPDATE_INTERVAL;

    // Try to retrieve values from the config file otherwise use the default one
    char* intervalStr = getconfent("RmMaster","UPDATE_INTERVAL", 0);
    if (intervalStr){
      interval = std::strtol(intervalStr,0,10);
      if (0 == interval) {
        // Go back to default
        interval = UPDATE_INTERVAL;
        // Log this
        castor::dlf::Param initParams[] =
          {castor::dlf::Param("Given value", intervalStr)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, initParams);
      }
    }

    // Inform user
    castor::dlf::Param initParams[] =
      {castor::dlf::Param("Update interval value", interval)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 2, 1, initParams);

    // Create the thread pool (actually a single thread here)
    daemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("Collector",
        new castor::rmmaster::Collector
        (myService,
         daemon.clusterStatus()), 0, interval));
    daemon.getThreadPool('C')->setNbThreads(1);

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
// RmMasterDaemon Constructor
// also initialises the logging facility
//------------------------------------------------------------------------------

castor::rmmaster::RmMasterDaemon::RmMasterDaemon() :
  castor::server::BaseDaemon("RmMasterDaemon") {

  castor::BaseObject::initLog("newrmmaster", castor::SVC_DLFMSG);
  // Initializes the DLF logging. This includes
  // registration of the predefined messages

  castor::dlf::Message messages[] =
    {{ 0, "Could Not get OraStagerSvc"},
     { 1, "Bad interval value in configuration file"},
     { 2, "Starting RmMaster Daemon"},
     { 3, "Starting of RmMaster Daemon failed"},
     { 4, "Unable to get shared memory id. Giving up"},
     { 5, "Unable to create shared memory. Giving up"},
     { 6, "Created the shared memory."},
     { 7, "Unable to get pointer to shared memory. Giving up"},
     { 8, "Exception caught in Collector::run"},
     { 9, "Thread Collector started. Updating shared memory data."},
     {-1, ""}};
  castor::dlf::dlf_init("newrmmaster", messages);

  // get the cluster status singleton
  m_clusterStatus = ClusterStatus::getClusterStatus();
  m_clusterStatus->clear();

}



