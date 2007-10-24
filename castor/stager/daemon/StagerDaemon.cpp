/******************************************************************************
 *                castor/stager/daemon/StagerMainDaemon.cpp
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
 * @(#)$RCSfile: StagerDaemon.cpp,v $ $Revision: 1.18 $ $Release$ $Date: 2007/10/24 09:42:23 $ $Author: itglp $
 *
 * Main stager daemon
 *
 * @author castor dev team
 *****************************************************************************/

#include <iostream>
#include <string>
#include <errno.h>
#include <serrno.h>
#include <getconfent.h>

#include "castor/Constants.hpp"
#include "castor/BaseObject.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/BaseDaemon.hpp"
#include "castor/server/SignalThreadPool.hpp"

#include "castor/stager/dbService/StagerMainDaemon.hpp"
#include "castor/stager/dbService/JobRequestSvc.hpp"
#include "castor/stager/dbService/PreRequestSvc.hpp"
#include "castor/stager/dbService/StgRequestSvc.hpp"
#include "castor/stager/dbService/QueryRequestSvcThread.hpp"
#include "castor/stager/dbService/ErrorSvcThread.hpp"
#include "castor/stager/dbService/JobSvcThread.hpp"
#include "castor/stager/dbService/GcSvcThread.hpp"

#include "castor/stager/dbService/StagerDlfMessages.hpp"

// default values
#define STAGER_JOBREQNOTIFYPORT    10001
#define STAGER_PREPREQNOTIFYPORT   10002
#define STAGER_STAGEREQNOTIFYPORT  10003
#define STAGER_QUERYREQNOTIFYPORT  10004
#define STAGER_ERRORNOTIFYPORT     10010
#define STAGER_JOBNOTIFYPORT       10011
#define STAGER_GCNOTIFYPORT        10012


int getConfigPort(const char* configLabel, int defaultValue) {
  const char* value;
  int notifyPort = 0;

  if ((value = getconfent((char*)"STAGER", (char*)configLabel, 0))) {
    notifyPort = std::strtol(value, 0, 10);
    if (notifyPort == 0) {
      notifyPort = defaultValue;
    } else if (notifyPort > 65535) {
      std::cerr << "Invalid value configured for " << configLabel <<": " 
         << notifyPort << " - must be < 65535" << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  return notifyPort;
}



int main(int argc, char* argv[]){
  try{

    castor::stager::dbService::StagerMainDaemon stagerDaemon;
    
    // read the jobManager notification port
    int jobManagerPort = 0;
    char *value = getconfent((char*)"JOBMANAGER", (char*)"NOTIFYPORT", 0);
    if(value) {
      jobManagerPort = std::strtol(value, 0, 10);
    }
    if ((jobManagerPort <= 0) || (jobManagerPort > 65535)) {
      castor::exception::Exception e(EINVAL);
      e.getMessage() << "Invalid JOBMANAGER NOTIFYPORT value configured: " << jobManagerPort<< "- must be < 65535" << std::endl;
      throw e;
    }
    std::string jobManagerHost = getconfent((char*)"JOBMANAGER", (char*)"HOST", 0);
    if(jobManagerHost == "") {
      castor::exception::Exception e(EINVAL);
      e.getMessage() << "No JOBMANAGER HOST value configured" << std::endl;
      throw e;
    }
    
    /*******************************/
    /* thread pools for the stager */
    /*******************************/
    stagerDaemon.addThreadPool(
      new castor::server::SignalThreadPool("JobRequestSvcThread", 
        new castor::stager::dbService::JobRequestSvc(jobManagerHost, jobManagerPort), 
          getConfigPort("JOBREQNOTIFYPORT", STAGER_JOBREQNOTIFYPORT)));
    
    stagerDaemon.addThreadPool(
      new castor::server::SignalThreadPool("PrepRequestSvcThread", 
        new castor::stager::dbService::PreRequestSvc(jobManagerHost, jobManagerPort),
          getConfigPort("PREPREQNOTIFYPORT", STAGER_PREPREQNOTIFYPORT)));


    stagerDaemon.addThreadPool(
      new castor::server::SignalThreadPool("StageRequestSvcThread", 
        new castor::stager::dbService::StgRequestSvc(),
          getConfigPort("STAGEREQNOTIFYPORT", STAGER_STAGEREQNOTIFYPORT)));
     
    stagerDaemon.addThreadPool(
      new castor::server::SignalThreadPool("QueryRequestSvcThread", 
        new castor::stager::dbService::QueryRequestSvcThread(),
          getConfigPort("QUERYREQNOTIFYPORT", STAGER_QUERYREQNOTIFYPORT)));
     
    stagerDaemon.addThreadPool(
      new castor::server::SignalThreadPool("ErrorSvcThread", 
        new castor::stager::dbService::ErrorSvcThread(),
          getConfigPort("ERRORNOTIFYPORT", STAGER_ERRORNOTIFYPORT)));

    stagerDaemon.addThreadPool(
      new castor::server::SignalThreadPool("jobSvcThread", 
        new castor::stager::dbService::JobSvcThread(),
          getConfigPort("JOBNOTIFYPORT", STAGER_JOBNOTIFYPORT)));

    stagerDaemon.addThreadPool(
      new castor::server::SignalThreadPool("GcsSvcThread", 
        new castor::stager::dbService::GcSvcThread(),
          getConfigPort("GCNOTIFYPORT", STAGER_GCNOTIFYPORT)));

    stagerDaemon.getThreadPool('J')->setNbThreads(10);
    stagerDaemon.getThreadPool('P')->setNbThreads(5);
    stagerDaemon.getThreadPool('S')->setNbThreads(5);
    stagerDaemon.getThreadPool('Q')->setNbThreads(10);
    stagerDaemon.getThreadPool('E')->setNbThreads(8);
    stagerDaemon.getThreadPool('j')->setNbThreads(20);
    stagerDaemon.getThreadPool('G')->setNbThreads(5);
    
    /* we need to call this function before setting the number of threads */
    stagerDaemon.parseCommandLine(argc, argv);

    stagerDaemon.start();  

  } catch (castor::exception::Exception e) {
    std::cerr << "Caught exception: "
	      << sstrerror(e.code()) << std::endl
	      << e.getMessage().str() << std::endl;

    // "Exception caught when starting Stager"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Code", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 1, 2, params);
  } catch (...) {
    std::cerr << "Caught general exception!" << std::endl;
  }
  
  return 0;
}// end main


/******************************************************************************************/
/* constructor: initiallizes the DLF logging and set the default value to its attributes */
/****************************************************************************************/
castor::stager::dbService::StagerMainDaemon::StagerMainDaemon() throw(castor::exception::Exception)
  : castor::server::BaseDaemon("Stager") {
	
	castor::dlf::Message stagerDlfMessages[]={

	  { STAGER_DAEMON_START, "Stager Daemon started"},
	  { STAGER_DAEMON_EXECUTION, "Stager Daemon execution"},
	  { STAGER_DAEMON_ERROR_CONFIG, "Stager Daemon configuration error"},
	  { STAGER_DAEMON_EXCEPTION, "Stager Daemon Exception"},
	  { STAGER_DAEMON_EXCEPTION_GENERAL, "Stager Daemon General Exception"},
	  { STAGER_DAEMON_FINISHED, "Stager Daemon successfully finished"},
	  
	  
	  { STAGER_DAEMON_POOLCREATION, "Stager Daemon Pool creation"},
	  
	  
	  
	  /*******************************************************************************************************/
	  /* Constants related with the StagerDBService SvcThreads: JobRequestSvc, PreRequestSvc, StgRequestSvc */
	  /*****************************************************************************************************/
	  
	  /************************/
	  /* JobRequestSvcThread */
	  
	  /* JobRequestSvc flow */
	  { STAGER_JOBREQSVC_CREATION, "Created new JobRequestSvc Thread"},
	  { STAGER_JOBREQSVC_SELECT , "JobRequestSvc thread Select method"},
	  { STAGER_JOBREQSVC_PROCESS, "JobRequestSvc thread Process method"},
	  { STAGER_JOBREQSVC_EXCEPTION, "JobRequestSvc thread Exception"},
	  { STAGER_JOBREQSVC_EXCEPTION_GENERAL, "JobRequestSvc thread General Exception"},
	  
	  
	  
	  /* StagerGetHandler:  */
	  { STAGER_GET, "Get Handler execution"},
	  { STAGER_GET_EXCEPTION, "Get Handler Exception"},
	  { STAGER_GET_EXCEPTION_GENERAL,"Get Handler General Exception"},
	  
	  /* StagerUpdateHandler:  */
	  { STAGER_UPDATE, "Update Handler execution"},
	  { STAGER_UPDATE_EXCEPTION,"Update Handler Exception"},
	  { STAGER_UPDATE_EXCEPTION_GENERAL,"Update Handler General Exception"},
	  
	  /* StagerPutHandler:  */
	  { STAGER_PUT,"Put Handler execution"},
	  { STAGER_PUT_EXCEPTION,"Put Handler Exception"},
	  { STAGER_PUT_EXCEPTION_GENERAL,"Put Handler General Exception"},
	  
	  
	  /************************/
	  /* PreRequestSvcThread */
	  
	  /* PreRequestSvc flow */
	  { STAGER_PREREQSVC_CREATION,"Created new PreRequestSvc Thread"},
	  { STAGER_PREREQSVC_SELECT,"PreRequestSvc thread Select method"},
	  { STAGER_PREREQSVC_PROCESS,"PreRequestSvc thread Process method"},
	  { STAGER_PREREQSVC_EXCEPTION,"PreRequestSvc thread Exception"},
	  { STAGER_PREREQSVC_EXCEPTION_GENERAL,"PreRequestSvc thread General Exception"},
	  
	  
	  /* StagerRepackHandler */
	  { STAGER_REPACK,"Repack Handler execution"},
	  { STAGER_REPACK_EXCEPTION,"Repack Handler Exception"},
	  { STAGER_REPACK_EXCEPTION_GENERAL,"Repack Handler General Exception"},
	  
	  /* StagerPrepareToGetHandler */
	  { STAGER_PREPARETOGET,"PrepareToGet Handler execution"},
	  { STAGER_PREPARETOGET_EXCEPTION, "PrepareToGet Handler Exception"},
	  { STAGER_PREPARETOGET_EXCEPTION_GENERAL,"PrepareToGet Handler General Exception"},
	  
	  /* StagerPrepareToUpdateHandler */
	  { STAGER_PREPARETOUPDATE,"PrepareToUpdate Handler execution"},
	  { STAGER_PREPARETOUPDATE_EXCEPTION, "PrepareToUpdate Handler Exception"},
	  { STAGER_PREPARETOUPDATE_EXCEPTION_GENERAL,"PrepareToUpdate Handler General Exception"},
	  
	  /* StagerPrepareToPutHandler */
	  { STAGER_PREPARETOPUT,"PrepareToPut Handler execution"},
	  { STAGER_PREPARETOPUT_EXCEPTION,"PrepareToPut Handler Exception"},
	  { STAGER_PREPARETOPUT_EXCEPTION_GENERAL,"PrepareToPut Handler General Exception"},
	  
	  
	  /*************************/
	  /* StgRequestSvcThread  */
	  
	  /* StgRequestSvc flow */
	  { STAGER_STGREQSVC_CREATION,"Created new StgRequestSvc Thread"},
	  { STAGER_STGREQSVC_SELECT,"StgRequestSvc thread Select method"},
	  { STAGER_STGREQSVC_PROCESS,"StgRequestSvc thread Process method"},
	  { STAGER_STGREQSVC_EXCEPTION,"StgRequestSvc thread Exception"},
	  { STAGER_STGREQSVC_EXCEPTION_GENERAL,"StgRequestSvc thread General Exception"},
	  
	  
	  /* StagerSetGCHandler: */
	  { STAGER_SETGC,"SetGC Handler execution"},
	  { STAGER_SETGC_EXCEPTION,"SetGC Handler Exception"},
	  { STAGER_SETGC_EXCEPTION_GENERAL,"SetGC Handler General Exception"},
	  
	  
	  /* StagerRmHandler:  */
	  { STAGER_RM, "Rm Handler execution"},
	  { STAGER_RM_EXCEPTION, "Rm Handler Exception "},
	  { STAGER_RM_EXCEPTION_GENERAL,"Rm Handler General Exception"},
	  
	  
	  
	  
	  /* StagerPutDoneHandler:  */
	  { STAGER_PUTDONE,"PutDone Handler execution"},
	  { STAGER_PUTDONE_EXCEPTION,"PutDone Handler Exception"},
	  { STAGER_PUTDONE_EXCEPTION_GENERAL,"PutDone Handler General Exception"},
	  
	  
	  /*************************/
	  /* StagerRequestHandler */
	  { STAGER_REQHANDLER_METHOD,"RequestHandler method"},
	  { STAGER_REQHANDLER_EXCEPTION,"RequestHandler Exception"},
	  { STAGER_REQHANDLER_EXCEPTION_GENERAL,"RequestHandler General Exception"},
	  
	  /****************************/
	  /* StagerJobRequestHandler */
	  { STAGER_JOBREQHANDLER_METHOD,"JobRequestHandler method"},
	  { STAGER_JOBREQHANDLER_EXCEPTION,"JobRequestHandler Exception"},
	  { STAGER_JOBREQHANDLER_EXCEPTION_GENERAL,"JobRequestHandler General Exception"},
	  
	  
	  /************************/
	  /* StagerRequestHelper */
	  { STAGER_REQHELPER_CONSTRUCTOR,"Request Helper constructor"},
	  { STAGER_REQHELPER_METHOD,"Request Helper method"},
	  { STAGER_REQHELPER_EXCEPTION,"Request Helper Exception"},
	  { STAGER_REQHELPER_EXCEPTION_GENERAL,"Request Helper General Exception"},
	  
	  
	  /********************/
	  /* StagerCnsHelper */
	  { STAGER_CNSHELPER_CONSTRUCTOR,"Cns Helper constructor"},
	  { STAGER_CNSHELPER_METHOD,"Cns Helper method"},
	  { STAGER_CNSHELPER_EXCEPTION,"Cns Helper Exception"},
	  { STAGER_CNSHELPER_EXCEPTION_GENERAL,"Cns Helper General Exception"},
	  
     
	  
	  /*********************/
	  /* StagerReplyHelper*/
	  { STAGER_REPLYHELPER_CONSTRUCTOR,"Reply Helper constructor"},
	  { STAGER_REPLYHELPER_METHOD,"Reply Helper method"},
	  { STAGER_REPLYHELPER_EXCEPTION,"Reply Helper Exception"},
	  { STAGER_REPLYHELPER_EXCEPTION_GENERAL,"Reply Helper General Exception"},


	  /*******************/
	  /* QueryRequestSvc */
	  { STAGER_QRYSVC_GETSVC, "Could not get QuerySvc"},
	  { STAGER_QRYSVC_EXCEPT, "Unexpected exception caught"},
          { STAGER_QRYSVC_NOCLI,  "No client associated with request ! Cannot answer !"},
          { STAGER_QRYSVC_INVSC,  "Invalid ServiceClass name"},
          { STAGER_QRYSVC_UNKREQ, "Unknown Request type"},
          { STAGER_QRYSVC_FQNOPAR,"StageFileQueryRequest has no parameters"},
          { STAGER_QRYSVC_FQUERY ,"Processing File Query by fileName"},
          { STAGER_QRYSVC_IQUERY ,"Processing File Query by fileId"},
          { STAGER_QRYSVC_RQUERY ,"Processing File Query by Request"},

	  /*********/
	  /* GcSvc */
	  { STAGER_GCSVC_GETSVC,  "Could not get GCSvc"},
	  { STAGER_GCSVC_EXCEPT,  "Unexpected exception caught"},
          { STAGER_GCSVC_NOCLI,   "No client associated with request ! Cannot answer !"},
          { STAGER_GCSVC_UNKREQ,  "Unknown Request type"},
          { STAGER_GCSVC_FDELOK,  "Invoking filesDeleted"},
          { STAGER_GCSVC_FDELFAIL,"Invoking filesDeletionFailed"},

	  /************/
	  /* ErrorSvc */
	  { STAGER_ERRSVC_GETSVC,  "Could not get StagerSvc"},
	  { STAGER_ERRSVC_EXCEPT,  "Unexpected exception caught"},
          { STAGER_ERRSVC_NOREQ,   "No request associated with subrequest ! Cannot answer !"},
          { STAGER_ERRSVC_NOCLI,   "No client associated with request ! Cannot answer !"},

	  /**********/
	  /* JobSvc */
	  { STAGER_JOBSVC_GETSVC,  "Could not get JobSvc"},
	  { STAGER_JOBSVC_EXCEPT,  "Unexpected exception caught"},
          { STAGER_JOBSVC_NOSREQ,  "Could not find subrequest associated to Request"},
          { STAGER_JOBSVC_BADSRT,  "Expected SubRequest in Request but found another type"},
          { STAGER_JOBSVC_NOFSOK,  "Could not find suitable filesystem"},
          { STAGER_JOBSVC_GETUPDS, "Invoking getUpdateStart"},
          { STAGER_JOBSVC_PUTS,    "Invoking PutStart"},
          { STAGER_JOBSVC_D2DCBAD, "Invoking disk2DiskCopyFailed"},
          { STAGER_JOBSVC_D2DCOK,  "Invoking disk2DiskCopyDone"},
          { STAGER_JOBSVC_PFMIG,   "Invoking PrepareForMigration"},
          { STAGER_JOBSVC_GETUPDO, "Invoking getUpdateDone"},
          { STAGER_JOBSVC_GETUPFA, "Invoking getUpdateFailed"},
          { STAGER_JOBSVC_PUTFAIL, "Invoking putFailed"},
          { STAGER_JOBSVC_NOCLI,   "No client associated with request ! Cannot answer !"},
          { STAGER_JOBSVC_UNKREQ,  "Unknown Request type"}

	};
	
  dlfInit(stagerDlfMessages);
}

/*************************************************************/
/* help method for the configuration (from the command line) */
/*************************************************************/
void castor::stager::dbService::StagerMainDaemon::help(std::string programName)
{
	  std::cout << "Usage: " << programName << " [options]\n"
	    "\n"
	    "where options can be:\n"
	    "\n"
	    "\t--Pthreads    or -P {integer >= 0}  \tNumber of threads for the Prepare requests\n"
	    "\n"
	    "Comments to: Castor.Support@cern.ch\n";
}



