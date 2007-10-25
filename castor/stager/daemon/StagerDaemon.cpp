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
 * @(#)$RCSfile: StagerDaemon.cpp,v $ $Revision: 1.22 $ $Release$ $Date: 2007/10/25 13:55:01 $ $Author: itglp $
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

	  /***************************************/
	  /* StagerMainDaemon: To DLF_LVL_DEBUG */
	  /*************************************/

	  { STAGER_DAEMON_START, "Stager Daemon started"},
	  { STAGER_DAEMON_EXECUTION, "Stager Daemon execution"},
	  { STAGER_DAEMON_ERROR_CONFIG, "Stager Daemon configuration error"},
	  { STAGER_DAEMON_EXCEPTION, "Stager Daemon Exception"},
	  { STAGER_DAEMON_EXCEPTION_GENERAL, "Stager Daemon General Exception"},
	  { STAGER_DAEMON_FINISHED, "Stager Daemon successfully finished"},  	  
	  { STAGER_DAEMON_POOLCREATION, "Stager Daemon Pool creation"},	  	  
	  { STAGER_CONFIGURATION, "Got wrong configuration, using default"}, /* DLF_LVL_USAGE */
	  { STAGER_CONFIGURATION_ERROR, "Impossible to get (right) configuration"}, /* DLF_LVL_ERROR */

	  /*******************************************************************************************************/
	  /* Constants related with the StagerDBService SvcThreads: JobRequestSvc, PreRequestSvc, StgRequestSvc */
	  /*****************************************************************************************************/
	  /* JobRequestSvcThread */
	  { STAGER_JOBREQSVC_CREATION, "Created new JobRequestSvc Thread"},
	  { STAGER_GET, "Get Request"},
	  { STAGER_UPDATE, "Update Request"},
	  { STAGER_PUT,"Put Request"},
	  
	  
	  /************************/
	  /* PreRequestSvcThread */
	  { STAGER_PREREQSVC_CREATION,"Created new PreRequestSvc Thread"},
	  { STAGER_REPACK,"Repack Request"},
	  { STAGER_PREPARETOGET,"PrepareToGet Request"},
	  { STAGER_PREPARETOUPDATE,"PrepareToUpdate Request"},
	  { STAGER_PREPARETOPUT,"PrepareToPut Request"},
	  
	  
	  /*************************/
	  /* StgRequestSvcThread  */
	  { STAGER_STGREQSVC_CREATION,"Created new StgRequestSvc Thread"},
	  { STAGER_SETGC,"SetGC Request"},
	  { STAGER_SETGC_DETAILS, "SetGC details"},/* SYSTEM LEVEL ALSO */
	  { STAGER_RM, "Rm Request"},
	  { STAGER_RM_DETAILS, "Rm details"},/* SYSTEM LEVEL ALSO */
	  { STAGER_PUTDONE,"PutDone Request"},
	  
	  /*  SYSTEM LEVEL */
	  /* after calling the corresponding stagerService function, to show the decision taken */
	  {STAGER_ARCHIVE_SUBREQ, "Archiving subrequest"},
	  {STAGER_NOTHING_TOBEDONE, "Diskcopy available, nothing to be done"},
	  {STAGER_WAIT_SUBREQ, "Request moved to Wait"},
	  {STAGER_REPACK_MIGRATION, "Starting Repack Migration"},
	  {STAGER_DISKTODISK_COPY, "Triggering Disk2Disk Copy"},
	  {STAGER_TAPE_RECALL, "Triggering Tape Recall"},
	  {STAGER_CASTORFILE_RECREATION, "Recreating CastorFile"},
    {STAGER_SCHEDULINGJOB, "Diskcopy available, scheduling job"},
	 
	  /* DLF_LVL_ERROR */
	  {STAGER_SERVICES_EXCEPTION, "Impossible to get the Service"},
	  {STAGER_SVCCLASS_EXCEPTION, "Impossible to get the SvcClass"},
	  {STAGER_USER_INVALID, "Invalid user"},
	  {STAGER_USER_PERMISSION, "User doenst have the right permission"},
	  {STAGER_USER_NONFILE, "User asking for a Non Existing File"},
	  {STAGER_INVALID_FILESYSTEM, "Invalid fileSystem"},
	  {STAGER_UNABLETOPERFORM, "Unable to perform request, notifying user"},
	  {STAGER_EXPERT_EXCEPTION, "Error while asking the Expert System"},

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
          { STAGER_GCSVC_SELF2DEL,"Invoking selectFiles2Delete"},
          { STAGER_GCSVC_FSEL4DEL,"File selected for deletion"},

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



