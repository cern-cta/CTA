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

#define UPDATE_INTERVAL 10 // in seconds
#define RMMASTER_DEFAULT_PORT 15003

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
    castor::monitoring::rmmaster::RmMasterDaemon daemon;
    int interval;
    int port = RMMASTER_DEFAULT_PORT;

    //default values
    interval = UPDATE_INTERVAL;

    // Try to retrieve values from the config file otherwise use the default one
    char* intervalStr = getconfent("RmMaster","UpdateInterval", 0);
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
    char* portStr = getconfent("RM","PORT", 0);
    if (portStr){
      port = std::strtol(portStr,0,10);
      if (0 == port) {
        // Go back to default
        port = RMMASTER_DEFAULT_PORT;
        // Log this
        castor::dlf::Param initParams[] =
          {castor::dlf::Param("Given value", portStr)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 18, 1, initParams);
      }
    }

    // Inform user
    castor::dlf::Param initParams[] =
      {castor::dlf::Param("Update interval value", interval)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 2, 1, initParams);

    // DB threadPool
    daemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("DatabaseActuator",
        new castor::monitoring::rmmaster::DatabaseActuatorThread
        (daemon.clusterStatus()), 0, interval));
    // this threadpool is supposed to have a single thread because
    // it uses shared member fields, see its implementation  
    daemon.getThreadPool('D')->setNbThreads(1);
    
    // Update threadpool
    daemon.addThreadPool
      (new castor::server::UDPListenerThreadPool
       ("Update",
        new castor::monitoring::rmmaster::UpdateThread
        (daemon.clusterStatus()),
        port));
    daemon.getThreadPool('U')->setNbThreads(3);

    // Monitor threadpool
    daemon.addThreadPool
      (new castor::server::TCPListenerThreadPool
       ("Collector",
        new castor::monitoring::rmmaster::CollectorThread
        (daemon.clusterStatus()),
        port));
    daemon.getThreadPool('C')->setNbThreads(1);

    // Heartbeat threadpool
    daemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("Heartbeat",
        new castor::monitoring::rmmaster::HeartbeatThread
	(daemon.clusterStatus()), 0, interval));
    daemon.getThreadPool('H')->setNbThreads(1);

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
// Constructor
//------------------------------------------------------------------------------
castor::monitoring::rmmaster::RmMasterDaemon::RmMasterDaemon()
throw (castor::exception::Exception) :
  castor::server::BaseDaemon("RmMasterDaemon") {

  castor::BaseObject::initLog("RmMaster", castor::SVC_DLFMSG);
  // Initializes the DLF logging. This includes
  // registration of the predefined messages
  castor::dlf::Message messages[] =
    {{ 0, ""},
     { 1, "Bad interval value in configuration file"},
     { 2, "Starting RmMaster Daemon"},
     { 3, "Starting of RmMaster Daemon failed"},
     { 4, "Unable to get shared memory id. Giving up"},
     { 5, "Unable to create shared memory. Giving up"},
     { 6, "Created the shared memory."},
     { 7, "Unable to get pointer to shared memory. Giving up"},
     { 8, "Exception caught in Collector::run"},
     { 9, "Thread Collector started."},
     {10, "Thread Collector created."},
     {11, "Error caught in CollectorThread::run"},
     {12, "Unable to read Object from socket"},
     {13, "Received unknown object from socket"},
     {14, "Caught exception in CollectorThread"},
     {15, "Sending acknowledgement to client"},
     {16, "Unable to send Ack to client"},
     {17, "General exception caught"},
     {18, "Bad port value in configuration file"},
     {19, "Thread Update started."},
     {20, "Caught exception in UpdateThread"},
     {21, "Ignored state report for machine with empty name"},
     {22, "Ignored metrics report for machine with empty name"},
     {23, "Ignored admin diskserver report for machine with empty name"},
     {24, "Ignored admin filesystem report for machine with empty name"},
     {25, "Ignored admin filesystem report for filesystem with empty name"},
     {26, "Ignored metrics report for filesystem with empty name"},
     {27, "Ignored state report for filesystem with empty name"},
     {28, "Unable to allocate SharedMemoryString"},
     {29, "Ignored admin diskServer report for unknown machine"},
     {30, "Ignored admin fileSystem report for unknown machine"},
     {31, "Ignored admin fileSystem report for unknown mountPoint"},
     {32, "Thread DatabaseActuator started. Synchronizing shared memory data"},
     {33, "Thread DatabaseActuator created"},
     {34, "Thread Update created"},
     {35, "Retrieving cluster status from database to shared memory"},
     {36, "Heartbeat Thread created."},
     {37, "Heartbeat Thread started."},
     {38, "Heartbeat check failed for disk server, status changed to DISABLED."},
     {-1, ""}};
  castor::dlf::dlf_init("RmMaster", messages);

  // get the cluster status singleton
  bool created = true;
  m_clusterStatus = castor::monitoring::ClusterStatus::getClusterStatus(created);

  if(created) {
    // the cluster status has just been created and it's empty, probably RmMaster got restarted
    // get db service
    castor::IService* orasvc = castor::BaseObject::services()->service("OraRmMasterSvc", castor::SVC_ORARMMASTERSVC);
    if (0 == orasvc) {
      castor::exception::Internal e;
      e.getMessage() << "Unable to get OraRmMasterSvc";
      throw e;
    }
    castor::monitoring::rmmaster::IRmMasterSvc* rmMasterService = dynamic_cast<castor::monitoring::rmmaster::IRmMasterSvc*>(orasvc);
    if (0 == rmMasterService) {
      castor::exception::Internal e;
      e.getMessage() << "Could not cast newly retrieved service into IRmMasterSvc" << std::endl;
      throw e;
    }

    // now populate cluster status with last saved status in the database if any
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 35);
    rmMasterService->retrieveClusterStatus(m_clusterStatus);    
  }
}
