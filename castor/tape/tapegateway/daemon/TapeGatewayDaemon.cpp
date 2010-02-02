/******************************************************************************
*                      TapeGatewayDaemon.cpp
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
* @(#)$RCSfile: TapeGatewayDaemon.cpp,v $ $Author: waldron $
*
*
*
* @author Giulia Taurelli
*****************************************************************************/


// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include "castor/tape/python/python.hpp"


// Include Files

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"

#include "castor/PortNumbers.hpp"

#include "castor/server/SignalThreadPool.hpp"
#include "castor/server/TCPListenerThreadPool.hpp"

#include "castor/Services.hpp"

#include "castor/tape/python/ScopedPythonLock.hpp"

#include "castor/tape/tapegateway/daemon/Constants.hpp"
#include "castor/tape/tapegateway/daemon/ITapeGatewaySvc.hpp"
#include "castor/tape/tapegateway/daemon/MigratorErrorHandlerThread.hpp"
#include "castor/tape/tapegateway/daemon/RecallerErrorHandlerThread.hpp"
#include "castor/tape/tapegateway/daemon/TapeGatewayDaemon.hpp"
#include "castor/tape/tapegateway/daemon/TapeStreamLinkerThread.hpp"
#include "castor/tape/tapegateway/daemon/VdqmRequestsCheckerThread.hpp"
#include "castor/tape/tapegateway/daemon/VdqmRequestsProducerThread.hpp"
#include "castor/tape/tapegateway/daemon/WorkerThread.hpp"
#include "castor/tape/tapegateway/TapeGatewayDlfMessageConstants.hpp"

#include <Cgetopt.h>
#include <iostream>
#include <string>
#include <memory>
#include <u64subr.h>


extern "C" {
  char* getconfent(const char *, const char *, int);
}


//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------

int castor::tape::tapegateway::TapeGatewayDaemon::main(int argc, char* argv[]){


// Try to initialize the DLF logging system, quitting with an error message
  // to stderr if the initialization fails
  try {

    castor::server::BaseServer::dlfInit(s_dlfMessages);

  } catch(castor::exception::Exception &ex) {
    std::cerr << std::endl <<
      "Failed to start daemon"
      ": Failed to initialize DLF"
      ": " << ex.getMessage().str() << std::endl << std::endl;

    return 1;
  }

  // Try to start the daemon, quitting with an error message to stderr and DLF
  // if the start fails
  
try {

    exceptionThrowingMain(argc, argv);

  } catch (castor::exception::Exception &ex) {
    std::cerr << std::endl << "Failed to start daemon: "
      << ex.getMessage().str() << std::endl << std::endl;

    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code()            )};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
      TAPE_GATEWAY_FAILED_TO_START, params);

    return 1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int castor::tape::tapegateway::TapeGatewayDaemon::exceptionThrowingMain(int argc,char **argv) throw(castor::exception::Exception) {

  // Log the start of the daemon
  logStart(argc, argv);

  // Check the service to access the database can be obtained

  // load the TapeGateway service to check that everything is fine with it

  castor::IService* dbSvc = castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc = dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);
  
  // Throw an exception if the Oracle database service could not
  // be obtained

  if (0 == oraSvc) {

    castor::exception::Internal ex;
    ex.getMessage() <<
      "Failed to get  TapeGateway Oracle database service";
    throw(ex);

  }

  // check if the db configuration is such that the tapegateway can be used 
  // to be dropped as soon as rtcpclientd is not supported any longer 

  try {
    oraSvc->checkConfiguration();
  } catch (castor::exception::Exception e){

    castor::exception::Internal ex;
    ex.getMessage() << "Caught castor exception : "
     << sstrerror(e.code()) << e.getMessage().str() << std::endl;
    throw(ex);
    
  }

 
  //Retrive the retry policies

  char* migrationPolicyName=NULL;
  migrationPolicyName = getconfent("Policy","MigrationRetry",0);
  if (migrationPolicyName==NULL){
    castor::dlf::Param params[] =
      {castor::dlf::Param("message","No policy for migration retry in castor.conf")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, castor::tape::tapegateway::NO_RETRY_POLICY_FOUND, params);
  }

  char* recallPolicyName=NULL;
  recallPolicyName = getconfent("Policy","RecallRetry",0);
  
  if (recallPolicyName == NULL){
    castor::dlf::Param params[] =
      {castor::dlf::Param("message","No policy for recall retry in castor.conf")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, castor::tape::tapegateway::NO_RETRY_POLICY_FOUND, params);
    
  }

  // Initialize Python

  castor::tape::python::initializePython();

  castor::tape::python::ScopedPythonLock scopedLock;

  // Load the policies

  PyObject* migrationDict = NULL;
  
  if (migrationPolicyName){
  
    migrationDict = castor::tape::python::importPolicyPythonModule(migrationPolicyName);

  }

  PyObject* recallDict = NULL;

  if (recallPolicyName){

    recallDict = castor::tape::python::importPolicyPythonModule(recallPolicyName);

  }

  // Get the functions from the dictionaries to be used by the threads
  // name of the function is the same as the module
    
  PyObject * migrationFunction=NULL;

  if (migrationDict && migrationPolicyName)
    migrationFunction = castor::tape::python::getPythonFunction(migrationDict,migrationPolicyName);

  PyObject * recallFunction=NULL;

  if (recallDict && recallPolicyName)
    recallFunction = castor::tape::python::getPythonFunction(recallDict,recallPolicyName);


  // Get the min and max number of thread used by the Worker

  int minThreadsNumber = MIN_WORKER_THREADS;
  int maxThreadsNumber = MAX_WORKER_THREADS;

  char* tmp=NULL;
  if ( (tmp= getconfent("TAPEGATEWAY","MINWORKERTHREADS",0)) != NULL ){
    char* dp = tmp;
    minThreadsNumber= strtoul(tmp, &dp, 0);
    if (*dp != 0 || minThreadsNumber <=0 ) {
      minThreadsNumber = MIN_WORKER_THREADS;
    }
  }

  if ( (tmp= getconfent("TAPEGATEWAY","MAXWORKERTHREADS",0)) != NULL ){
    char* dp = tmp;
    maxThreadsNumber= strtoul(tmp, &dp, 0);
    if (*dp != 0 || maxThreadsNumber <=0 ) {
      maxThreadsNumber = MAX_WORKER_THREADS;
    }
  }

  parseCommandLine(argc, argv);

  // run as stage st

  runAsStagerSuperuser();

  // send request to vdmq

  std::auto_ptr<castor::tape::tapegateway::VdqmRequestsProducerThread> vpThread(new castor::tape::tapegateway::VdqmRequestsProducerThread(listenPort()));// port used just to be sent to vdqm
  std::auto_ptr<castor::server::SignalThreadPool> vpPool(new castor::server::SignalThreadPool("ProducerOfVdqmRequestsThread", vpThread.release(), DEFAULT_SLEEP_INTERVAL));
  addThreadPool(vpPool.release()); 
  getThreadPool('P')->setNbThreads(1);

  // check requests for vdqm
  std::auto_ptr<castor::tape::tapegateway::VdqmRequestsCheckerThread> vcThread(new castor::tape::tapegateway::VdqmRequestsCheckerThread(VDQM_TIME_OUT_INTERVAL));
  std::auto_ptr<castor::server::SignalThreadPool> vcPool(new castor::server::SignalThreadPool("CheckerOfVdqmRequestsThread", vcThread.release(), DEFAULT_SLEEP_INTERVAL));
  addThreadPool(vcPool.release());
  getThreadPool('C')->setNbThreads(1);
  
  // query vmgr for tape and tapepools
  
  std::auto_ptr<castor::tape::tapegateway::TapeStreamLinkerThread> tsThread(new castor::tape::tapegateway::TapeStreamLinkerThread());
  std::auto_ptr<castor::server::SignalThreadPool> tsPool(new castor::server::SignalThreadPool("TapeStreamLinkerThread", tsThread.release(), DEFAULT_SLEEP_INTERVAL));
  addThreadPool(tsPool.release());
  getThreadPool('T')->setNbThreads(1);

    // migration error handler
        
    std::auto_ptr<castor::tape::tapegateway::MigratorErrorHandlerThread> mThread(new castor::tape::tapegateway::MigratorErrorHandlerThread(migrationFunction));
    std::auto_ptr<castor::server::SignalThreadPool> mPool(new castor::server::SignalThreadPool("MigrationErrorHandlerThread", mThread.release(),  DEFAULT_SLEEP_INTERVAL));
    addThreadPool(mPool.release());
    getThreadPool('M')->setNbThreads(1);
    
    // recaller error handler

    std::auto_ptr<castor::tape::tapegateway::RecallerErrorHandlerThread> rThread(new castor::tape::tapegateway::RecallerErrorHandlerThread(recallFunction));
    std::auto_ptr<castor::server::SignalThreadPool> rPool(new castor::server::SignalThreadPool("RecallerErrorHandlerThread", rThread.release(), DEFAULT_SLEEP_INTERVAL));
    addThreadPool(rPool.release());
    getThreadPool('R')->setNbThreads(1);

    // recaller/migration dynamic thread pool

    std::auto_ptr<castor::tape::tapegateway::WorkerThread> wThread(new castor::tape::tapegateway::WorkerThread());
    std::auto_ptr<castor::server::TCPListenerThreadPool> wPool(new castor::server::TCPListenerThreadPool("WorkerThread", wThread.release(),listenPort(),true, minThreadsNumber, maxThreadsNumber,TG_THRESHOLD, TG_MAXTASKS ));
    addThreadPool(wPool.release());

    // start the daemon

    start();

    // Finalize Python

    Py_Finalize();

    return 0;
    
}

//------------------------------------------------------------------------------
// TapeGatewayDaemon Constructor
//------------------------------------------------------------------------------

castor::tape::tapegateway::TapeGatewayDaemon::TapeGatewayDaemon() : castor::server::BaseDaemon("tapegatewayd")
{

  // get the port
  char* tmp=NULL;

  // This let's try to  the tapegateway port in castor.conf

  if ( (tmp= getconfent("TAPEGATEWAY","PORT",0)) != NULL ){
    char* dp = tmp;
    m_listenPort = strtoul(tmp, &dp, 0);
    if (*dp != 0) {
        castor::exception::Internal ex;
        ex.getMessage() << "Bad port value in enviroment variable "
                        << tmp << std::endl;
        throw ex;
    }
    if ( m_listenPort > 65535 ){
        castor::exception::Internal ex;
        ex.getMessage() << "Given port no. in enviroment variable "
                        << "exceeds 65535 !"<< std::endl;
        throw ex;
    }
  } else {
     m_listenPort= castor::TAPEGATEWAY_DEFAULT_NOTIFYPORT;
  }

}


//------------------------------------------------------------------------------
// logStart
//------------------------------------------------------------------------------

void castor::tape::tapegateway::TapeGatewayDaemon::logStart(const int argc,
  const char *const *const argv) throw() {
  std::string concatenatedArgs;

  // Concatenate all of the command-line arguments into one string
  for(int i=0; i < argc; i++) {
    if(i != 0) {
      concatenatedArgs += " ";
    }

    concatenatedArgs += argv[i];
  }

  castor::dlf::Param params[] = {
    castor::dlf::Param("argv", concatenatedArgs)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, DAEMON_START,params);
}


//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------



void castor::tape::tapegateway::TapeGatewayDaemon::parseCommandLine(int argc, char* argv[]){
  if (argc < 1 ) {
    usage();
    return;
    }
  Coptind = 1;
  Copterr = 1;
  int c;
  while ( (c = Cgetopt(argc,argv,"fh")) != -1 ) {
    switch (c) {
    case 'f':
      m_foreground = true;
      break;
    case 'h':
      usage();
      exit(0);
    default:
      usage();
      exit(0);
    }
  }

}



//------------------------------------------------------------------------------
// Usage
//------------------------------------------------------------------------------


void castor::tape::tapegateway::TapeGatewayDaemon::usage(){
  std::cout << "\nUsage: " << "tapegateway [-f][-h]\n"
	    << "-f     : to run in foreground\n"
	    <<std::endl;
}
