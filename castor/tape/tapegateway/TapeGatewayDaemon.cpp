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
* @(#)$RCSfile: TapeGatewayDaemon.cpp,v $ $Author: gtaur $
*
*
*
* @author Giulia Taurelli
*****************************************************************************/

// Include Files


#include <iostream>
#include <string>
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include <Cgetopt.h>
#include <u64subr.h>
#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"


#include "castor/infoPolicy/TapeRetryPySvc.hpp"
#include "castor/tape/tapegateway/TapeGatewayDaemon.hpp"
#include "castor/tape/tapegateway/ITapeGatewaySvc.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "castor/server/TCPListenerThreadPool.hpp"

#include "castor/tape/tapegateway/VdqmRequestsProducerThread.hpp"
#include "castor/tape/tapegateway/VdqmRequestsCheckerThread.hpp"
#include "castor/tape/tapegateway/TapeStreamLinkerThread.hpp"
#include "castor/tape/tapegateway/MigratorErrorHandlerThread.hpp"
#include "castor/tape/tapegateway/RecallerErrorHandlerThread.hpp"
#include "castor/tape/tapegateway/WorkerThread.hpp"
#include "castor/tape/tapegateway/VmgrTapeGatewayHelper.hpp"
#include "castor/PortNumbers.hpp"

#define  DEFAULT_SLEEP_INTERVAL   10
#define  VDQM_TIME_OUT_INTERVAL 600 // Timeout between two polls on a VDQM request 
#define  MIN_WORKER_THREADS 5
#define  MAX_WORKER_THREADS 240

extern "C" {
  char* getconfent(const char *, const char *, int);
}
 

//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------

int main(int argc, char* argv[]){

 // service to access the database
  castor::IService* dbSvc = castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc = dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);
  

  if (0 == oraSvc) {
    // we don't have DLF yet, and this is a major fault, so log to stderr and exit
    std::cerr << "Couldn't load the oracle tapegateway service, check the castor.conf for DynamicLib entries" << std::endl;
    return -1;
  }
 
  castor::tape::tapegateway::TapeGatewayDaemon tgDaemon;

  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 0, NULL);
  
  // create the retry policies
  try{
    char* prm=NULL;
    char* prr=NULL;
    std::string retryMigrationPolicyName;
    std::string retryRecallPolicyName;
    std::string migrationFunctionName;
    std::string recallFunctionName;
    
    // get migration retry policy 

    if ( (prm = getconfent("Policy","RetryMigration",0)) != NULL ){ 
      retryMigrationPolicyName=prm;
    } else {
       castor::dlf::Param params[] =
	  {castor::dlf::Param("message","No policy for migration retry in castor.conf")};
       castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, 4, 1, params);

    }
    prm=NULL;
    if ( (prm = getconfent("Policy","RetryMigrationFunction",0)) != NULL ){ 
      migrationFunctionName=prm;
    } else {
        castor::dlf::Param params[] =
	  {castor::dlf::Param("message","No global function name for migration retry policy  in castor.conf")};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, 4, 1, params);
    }


    // get recall retry  policy 

    if ( (prr = getconfent("Policy","RetryRecall",0)) != NULL ){ 
      retryRecallPolicyName=prr;
    } else {
      castor::dlf::Param params[] =
	  {castor::dlf::Param("message","No policy for recall retry  in castor.conf")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, 4, 1, params);

    }
    prr=NULL;
    if ( (prr = getconfent("Policy","RetryRecallFunction",0)) != NULL ){ 
      recallFunctionName=prr;
    } else {
        castor::dlf::Param params[] =
	  {castor::dlf::Param("message","No global function name for recall retry policy  in castor.conf")};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, 4, 1, params);
    }


    castor::infoPolicy::TapeRetryPySvc* retryMigrationSvc=NULL;
    castor::infoPolicy::TapeRetryPySvc* retryRecallSvc=NULL;
    
    // migration
    if (!retryMigrationPolicyName.empty() && !migrationFunctionName.empty())
      retryMigrationSvc = new castor::infoPolicy::TapeRetryPySvc(retryMigrationPolicyName,migrationFunctionName);

    //recaller
    if (!retryRecallPolicyName.empty() && !recallFunctionName.empty())
      retryRecallSvc = new castor::infoPolicy::TapeRetryPySvc(retryRecallPolicyName,recallFunctionName);

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

    tgDaemon.parseCommandLine(argc, argv);

    // run as stage st

    tgDaemon.runAsStagerSuperuser();

     // send request to vdmq
    
    tgDaemon.addThreadPool(
			   new castor::server::SignalThreadPool("ProducerOfVdqmRequestsThread", new castor::tape::tapegateway::VdqmRequestsProducerThread(oraSvc,tgDaemon.listenPort()), DEFAULT_SLEEP_INTERVAL)); // port used just to be sent to vdqm
			   tgDaemon.getThreadPool('P')->setNbThreads(1);

     // check requests for vdmq
    
     tgDaemon.addThreadPool(
			    new castor::server::SignalThreadPool("CheckerOfVdqmRequestsThread", new castor::tape::tapegateway::VdqmRequestsCheckerThread(oraSvc, VDQM_TIME_OUT_INTERVAL), DEFAULT_SLEEP_INTERVAL ));
    tgDaemon.getThreadPool('C')->setNbThreads(1);

    
    // query vmgr for tape and tapepools
   
    tgDaemon.addThreadPool(
      new castor::server::SignalThreadPool("TapeStreamLinkerThread", new castor::tape::tapegateway::TapeStreamLinkerThread(oraSvc), DEFAULT_SLEEP_INTERVAL));
      tgDaemon.getThreadPool('T')->setNbThreads(1);
    
    // migration error handler

    tgDaemon.addThreadPool(
      new castor::server::SignalThreadPool("MigrationErrorHandlerThread", new castor::tape::tapegateway::MigratorErrorHandlerThread(oraSvc,retryMigrationSvc),  DEFAULT_SLEEP_INTERVAL));
    tgDaemon.getThreadPool('M')->setNbThreads(1);
    
    // recaller error handler

    tgDaemon.addThreadPool(
      new castor::server::SignalThreadPool("RecallerErrorHandlerThread", new castor::tape::tapegateway::RecallerErrorHandlerThread(oraSvc,retryRecallSvc), DEFAULT_SLEEP_INTERVAL ));
    tgDaemon.getThreadPool('R')->setNbThreads(1);
    
    // recaller/migration dynamic thread pool

    tgDaemon.addThreadPool(
			   new castor::server::TCPListenerThreadPool("WorkerThread", new castor::tape::tapegateway::WorkerThread(oraSvc),tgDaemon.listenPort(),true, minThreadsNumber, maxThreadsNumber )); 
    
    // start the daemon

    tgDaemon.start();
  
  }// end try block
  catch (castor::exception::Exception e) {
    std::cerr << "Caught castor exception : "
     << sstrerror(e.code()) << std::endl
     << e.getMessage().str() << std::endl;
    return -1;
  }
  catch (...) {
    
    std::cerr << "Caught general exception!" << std::endl;    
    return -1;
    
  } 

  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 2, 0, NULL);
  return 0;
}

//------------------------------------------------------------------------------
// TapeGatewayDaemon Constructor
// also initialises the logging facility
//------------------------------------------------------------------------------

castor::tape::tapegateway::TapeGatewayDaemon::TapeGatewayDaemon() : castor::server::BaseDaemon("TapeGateway") 
{
 
  // Initializes the DLF logging. This includes
  // registration of the predefined messages
  // Initializes the DLF logging. This includes
  // defining the predefined messages

  castor::dlf::Message messages[] =
  {{1,  "Service startup"},
   {2,  "Service shutdown"},
   {3,  "Cleanup could not be performed"},
   {4,  "Incomplete parameters for retry policies"},
   {5,  "VdqmRequestsProducer: getting tapes to submit"},
   {6,  "VdqmRequestsProducer: impossible to get tapes to submit"},
   {7,  "VdqmRequestsProducer: impossible to update the db with submitted tapes"},
   {8,  "VdqmRequestsProducer: queried vmgr"},
   {9,  "VdqmRequestsProducer: request sent to vdqm"},
   {10, "VdqmRequestsChecker: getting tapes to check"},
   {11, "VdqmRequestsChecker: impossible to get tapes to check"},
   {12, "VdqmRequestsChecker: checked vdqm request state"},
   {13, "VdqmRequestsChecker: vdqm request lost for this tape"},
   {14, "VdqmRequestsChecker: impossible to update the db with checked tapes"},
   {15, "TapeStreamLinker: getting streams to resolve"},
   {16, "TapeStreamLinker: impossible to get streams to resolve"},
   {17, "TapeStreamLinker: querying vmgr for the stream"},
   {18, "TapeStreamLinker: tape and stream linked"},
   {19, "TapeStreamLinker: impossible to update the db with resolved streams"},
   {20, "TapeStreamLinker: released tape in vmgr because of db error"},
   {21, "MigratorErrorHandlerThread: getting failed migration candidates"},
   {22, "MigratorErrorHandlerThread: impossible to get failed migration candidates"},
   {23, "MigratorErrorHandlerThread: policy allows retry"},
   {24, "MigratorErrorHandlerThread: policy does not allows retry"},
   {25, "MigratorErrorHandlerThread: exception in calling the policy, retry allowed as side effect"},
   {26, "MigratorErrorHandlerThread: impossible to update the db with the result of the retry policy"},
   {27, "RecallerErrorHandlerThread: getting failed recall candidates"},
   {28, "RecallerErrorHandlerThread: impossible to get failed recall candidates"},
   {29, "RecallerErrorHandlerThread: policy allows retry"},
   {30, "RecallerErrorHandlerThread: policy does not allows retry"},
   {31, "RecallerErrorHandlerThread: exception in calling the policy, retry allowed as side effect"},
   {32, "RecallerErrorHandlerThread: impossible to update the db with the result of the retry policy"},
   {33, "Worker: impossible to connect to the database"},
   {34, "Worker: impossible to get client information"},
   {35, "Worker: received a StartWorkerRequest"},
   {36, "Worker: received an invalid request"},
   {37, "Worker: impossible to read the request"},
   {38, "Worker: impossible to update the db"},
   {39, "Worker: impossible to answer the client"},
   {40, "Recaller: impossible to read the request"},
   {41, "Recaller: invalid request"},
   {42, "Recaller: EndWorkerRequest received"},
   {43, "Recaller: FileRecalled"},
   {44, "Recaller: File to recall requested"},
   {45, "Recaller: Nothing to recall from this tape"},
   {46, "Recaller: sent file to recall from this tape"},
   {47, "Recaller: Impossible to send response to the aggregator"},
   {48, "Recaller: error in updating the db after the recall"},
   {49, "Recaller: impossible to get a file to recall from the db"},
   {50, "Migrator: impossible to read the request"},
   {51, "Migrator: invalid request"},
   {52, "Migrator: EndWorkerRequest received"},
   {53, "Migrator: FileMigrated"},
   {54, "Migrator: File to migrate  requested"},
   {55, "Migrator: Nothing to migrate from this stream"},
   {56, "Migrator: sent file to migrate from this stream"},
   {57, "Migrator: Impossible to send response to the aggregator"},
   {58, "Migrator: error in updating the db after the migration"},
   {59, "Migrator: impossible to get a file to migrate from the db"},
   {60, "Migrator: updated vmgr"},
   {61, "Migrator: failure in vmgr update"},
   {62, "Migrator: failure in checking if the migration was issued by Repack"},
   {63, "Migrator: updated nameserver"},
   {64, "Migrator: failure in nameserver update"},
   {65, "Migrator: updated nameserver for repack"},
   {66, "Worker: released BUSY tape"},
   {67, "Worker: impossible to release BUSY tape"},
   {68, "Worker: unexpected exception"},
   {69, "TapeGateway: impossible to release BUSY tape"},
   {70, "TapeGateway: released BUSY tape"},
   {71, "Recaller: impossible to recall the file"},
   {72, "Migrator: impossible to migrate the file"},
   {73, "Worker: impossible to send stream report to RmMasterDaemon"},
   {-1, ""}
  };
  dlfInit(messages);


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

void castor::tape::tapegateway::TapeGatewayDaemon::usage(){
  std::cout << "\nUsage: " << "tapegateway [-f][-h]\n" 
	    << "-f     : to run in foreground\n"
	    <<std::endl; 
}
