/******************************************************************************
 *                castor/stager/daemon/StagerDaemon.cpp
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
#include "castor/exception/InvalidArgument.hpp"
#include "castor/PortNumbers.hpp"
#include "castor/System.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "castor/replier/RequestReplier.hpp"
#include "castor/db/DbCnvSvc.hpp"

#include "castor/stager/daemon/StagerDaemon.hpp"
#include "castor/stager/daemon/JobRequestSvcThread.hpp"
#include "castor/stager/daemon/PrepRequestSvcThread.hpp"
#include "castor/stager/daemon/StageRequestSvcThread.hpp"
#include "castor/stager/daemon/BulkStageReqSvcThread.hpp"
#include "castor/stager/daemon/QueryRequestSvcThread.hpp"
#include "castor/stager/daemon/ErrorSvcThread.hpp"
#include "castor/stager/daemon/JobSvcThread.hpp"
#include "castor/stager/daemon/GcSvcThread.hpp"
#include "castor/stager/daemon/LoggingThread.hpp"
#include "castor/stager/daemon/NsOverride.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"


int main(int argc, char* argv[]){
  try{
    castor::stager::daemon::StagerDaemon stagerDaemon;

    castor::stager::IStagerSvc* stgService =
      dynamic_cast<castor::stager::IStagerSvc*>
      (castor::BaseObject::services()->service
       ("DbStagerSvc", castor::SVC_DBSTAGERSVC));
    if(stgService == 0) {
      castor::exception::Exception e(EINVAL);
      e.getMessage() << "Failed to load DbStagerSvc, check for shared libraries configuration" << std::endl;
      throw e;
    }

    /*******************************/
    /* thread pools for the stager */
    /*******************************/
    stagerDaemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("JobRequestSvcThread",
	new castor::stager::daemon::JobRequestSvcThread()));

    stagerDaemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("PrepRequestSvcThread",
	new castor::stager::daemon::PrepRequestSvcThread()));

    stagerDaemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("StageRequestSvcThread",
	new castor::stager::daemon::StageRequestSvcThread()));

    stagerDaemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("BulkStageReqSvcThread",
	new castor::stager::daemon::BulkStageReqSvcThread()));

    stagerDaemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("QueryRequestSvcThread",
	new castor::stager::daemon::QueryRequestSvcThread()));

    // These threads poll the database every 2 seconds.
    stagerDaemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("ErrorSvcThread",
	new castor::stager::daemon::ErrorSvcThread(), 2));

    stagerDaemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("jobSvcThread",
	new castor::stager::daemon::JobSvcThread()));

    stagerDaemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("GcSvcThread",
	new castor::stager::daemon::GcSvcThread()));

    // This thread dumps the logs from the DB
    stagerDaemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("LoggingThread",
	new castor::stager::daemon::LoggingThread(), 5));

    stagerDaemon.getThreadPool('J')->setNbThreads(10);
    stagerDaemon.getThreadPool('P')->setNbThreads(6);
    stagerDaemon.getThreadPool('S')->setNbThreads(4);
    stagerDaemon.getThreadPool('B')->setNbThreads(3);
    stagerDaemon.getThreadPool('Q')->setNbThreads(6);
    stagerDaemon.getThreadPool('E')->setNbThreads(2);
    stagerDaemon.getThreadPool('j')->setNbThreads(6);
    stagerDaemon.getThreadPool('G')->setNbThreads(4);
    stagerDaemon.getThreadPool('L')->setNbThreads(1);

    // Determine the notification port for the notifier thread pool. The port
    // will be used to recieve UDP notifications from the request handler to
    // wake up the various stager services.
    int notifyPort = castor::STAGER_DEFAULT_NOTIFYPORT;
    const char *value = getconfent("STAGER", "NOTIFYPORT", 0);
    if (value) {
      try {
	notifyPort = castor::System::porttoi((char *)value);
      } catch (castor::exception::Exception& ex) {
	castor::exception::InvalidArgument e;
	e.getMessage() << "Invalid STAGER/NOTIFYPORT value: "
		       << ex.getMessage().str() << std::endl;
	throw e;
      }
    }
    stagerDaemon.addNotifierThreadPool(notifyPort);

    stagerDaemon.parseCommandLine(argc, argv);

    stagerDaemon.start();

  } catch (castor::exception::Exception& e) {
    std::cerr << "Caught exception: "
	      << sstrerror(e.code()) << std::endl
	      << e.getMessage().str() << std::endl;

    // "Exception caught when starting "
    castor::dlf::Param params[] =
      {castor::dlf::Param("Code", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, castor::stager::daemon::STAGER_DAEMON_EXCEPTION, 2, params);
  } catch (...) {
    std::cerr << "Caught general exception!" << std::endl;
  }

  return 0;
}// end main


/******************************************************************************************/
/* constructor: initiallizes the DLF logging and set the default value to its attributes */
/****************************************************************************************/
castor::stager::daemon::StagerDaemon::StagerDaemon()
  throw (castor::exception::Exception)
  : castor::server::BaseDaemon("stagerd") {

  castor::dlf::Message stagerDlfMessages[]={

    /***************************************/
    /* StagerDaemon: To DLF_LVL_DEBUG */
    /*************************************/

    { STAGER_DAEMON_START, "StagerDaemon started"},
    { STAGER_DAEMON_EXECUTION, "StagerDaemon execution"},
    { STAGER_DAEMON_ERROR_CONFIG, "StagerDaemon configuration error"},
    { STAGER_DAEMON_EXCEPTION, "Exception caught when starting "},
    { STAGER_CONFIGURATION, "Got wrong configuration, using default"},
    { STAGER_CONFIGURATION_ERROR, "Impossible to get (right) configuration"},

    /*******************************************************************************************************/
    /* Constants related with the DBService SvcThreads: JobRequestSvc, PreRequestSvc, StgRequestSvc */
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
    { STAGER_SETGC,"SetGCWeight Request"},
    { STAGER_RM, "Rm Request"},
    { STAGER_RMPERFORMED, "Rm performed"},
    { STAGER_PUTDONE,"PutDone Request"},
    { STAGER_CHGPRIV,"ChangePrivilege Request"},

    /***********************/
    /* BulkStageReqSvcThread  */
    { STAGER_BLKSTGSVC_ABORT, "Abort processed"},
    { STAGER_BLKSTGSVC_REPACK, "Repack initiated"},
    { STAGER_BLKSTGSVC_UNKREQ, "Unknown request processed"},

    /*  SYSTEM LEVEL */
    /* after calling the corresponding stagerService function, to show the decision taken */
    { STAGER_SUBREQ_SELECTED, "Request selected by subRequestToDo"},
    { STAGER_REQ_PROCESSED, "Request processed"},
    { STAGER_ARCHIVE_SUBREQ, "Archiving subrequest"},
    { STAGER_WAITSUBREQ, "Request moved to Wait"},
    { STAGER_REPACK_MIGRATION, "Starting Repack Migration"},
    { STAGER_GET_REPLICATION, "Triggering internal DiskCopy replication"},
    { STAGER_PREPARETOGET_DISK2DISKCOPY, "Triggering DiskCopy replication"},
    { STAGER_TAPE_RECALL, "Triggering Tape Recall"},
    { STAGER_CASTORFILE_RECREATION, "Recreating CastorFile"},
    { STAGER_RECREATION_IMPOSSIBLE, "Impossible to recreate CastorFile"},
    { STAGER_SCHEDULINGJOB, "Diskcopy available, scheduling job"},

    /* DLF_LVL_ERROR */
    { STAGER_SERVICES_EXCEPTION, "Impossible to get the Service"},
    { STAGER_SVCCLASS_EXCEPTION, "Impossible to get the SvcClass"},
    { STAGER_USER_INVALID, "Invalid user"},
    { STAGER_USER_PERMISSION, "User doesn't have the right permission"},
    { STAGER_USER_NONFILE, "User asking for a non existing file"},
    { STAGER_INVALID_FILESYSTEM, "Invalid fileSystem"},
    { STAGER_UNABLETOPERFORM, "Unable to perform request, notifying user"},
    { STAGER_CNS_EXCEPTION, "Error on the Name Server"},
    { STAGER_SUBREQUESTUUID_EXCEPTION, "Impossible to get the subrequest Uuid"},
    { STAGER_REQUESTUUID_EXCEPTION, "Impossible to get the request Uuid"},
    { STAGER_CASTORFILE_EXCEPTION, "Impossible to get the CastorFile"},
    { STAGER_INVALID_TYPE, "Request type not valid for this thread pool"},
    { STAGER_QRYSVC_DFAILED, "Failed to process DiskPoolQuery"},

    /*******************/
    /* QueryRequestSvc */
    { STAGER_QRYSVC_GETSVC, "Could not get QuerySvc"},
    { STAGER_QRYSVC_EXCEPT, "Unexpected exception caught"},
    { STAGER_QRYSVC_NOCLI,  "No client associated with request ! Cannot answer !"},
    { STAGER_QRYSVC_INVSC,  "Invalid ServiceClass name"},
    { STAGER_QRYSVC_UNKREQ, "Unknown request type"},
    { STAGER_QRYSVC_INVARG, "Invalid argument"},
    { STAGER_QRYSVC_FQNOPAR,"StageFileQueryRequest has no parameters"},
    { STAGER_QRYSVC_FQUERY, "Processing File Query by filename"},
    { STAGER_QRYSVC_IQUERY, "Processing File Query by fileid"},
    { STAGER_QRYSVC_RQUERY, "Processing File Query by Request"},
    { STAGER_QRYSVC_DSQUERY, "Processing DiskPoolQuery by SvcClass"},
    { STAGER_QRYSVC_DDQUERY, "Processing DiskPoolQuery by DiskPool"},
    { STAGER_QRYSVC_VQUERY, "Processing VersionQuery"},
    { STAGER_QRYSVC_CPRIV, "Processing ChangePrivilege"},
    { STAGER_QRYSVC_LPRIV, "Processing ListPrivilege"},

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
    { STAGER_GCSVC_NSFILDEL,"Invoking nsFilesDeleted"},
    { STAGER_GCSVC_STGFILDEL, "Invoking stgFilesDeleted"},
    { STAGER_GCSVC_FSTGDEL, "File to be unlinked since it disappeared from the stager"},
    { STAGER_GCSVC_FNSDEL, "File deleted since it disappeared from nameserver"},

    /************/
    /* ErrorSvc */
    { STAGER_ERRSVC_GETSVC,  "Could not get Svc"},
    { STAGER_ERRSVC_EXCEPT,  "Unexpected exception caught"},
    { STAGER_ERRSVC_NOREQ,   "No request associated with subrequest ! Cannot answer !"},
    { STAGER_ERRSVC_NOCLI,   "No client associated with request ! Cannot answer !"},

    /**********/
    /* JobSvc */
    { STAGER_JOBSVC_GETSVC,  "Could not get JobSvc"},
    { STAGER_JOBSVC_EXCEPT,  "Unexpected exception caught"},
    { STAGER_JOBSVC_NOSREQ,  "Could not find subrequest associated to Request"},
    { STAGER_JOBSVC_BADSRT,  "Expected SubRequest in Request but found another type"},
    { STAGER_JOBSVC_GETUPDS, "Invoking getUpdateStart"},
    { STAGER_JOBSVC_PUTS,    "Invoking putStart"},
    { STAGER_JOBSVC_D2DCBAD, "Invoking disk2DiskCopyFailed"},
    { STAGER_JOBSVC_D2DCOK,  "Invoking disk2DiskCopyDone"},
    { STAGER_JOBSVC_PFMIG,   "Invoking prepareForMigration"},
    { STAGER_JOBSVC_GETUPDO, "Invoking getUpdateDone"},
    { STAGER_JOBSVC_GETUPFA, "Invoking getUpdateFailed"},
    { STAGER_JOBSVC_PUTFAIL, "Invoking putFailed"},
    { STAGER_JOBSVC_NOCLI,   "No client associated with request ! Cannot answer !"},
    { STAGER_JOBSVC_UNKREQ,  "Unknown Request type"},
    { STAGER_JOBSVC_D2DCS,   "Invoking disk2DiskCopyStart"},
    { STAGER_JOBSVC_1STBWR,  "Invoking firstByteWritten"},
    { STAGER_JOBSVC_DELWWR,  "File was removed by another user while being modified"},
    { STAGER_JOBSVC_CHKMISMATCH, "Preset checksum mismatch detected, invoking putFailed"},

    /**************/
    /* LoggingSvc */
    { STAGER_LOGGING_DONE,   "Dump of the DB logs completed"},
    { STAGER_LOGGING_EXCEPT, "Unexpected exception caught"},

    { -1, "" }

  };
  dlfInit(stagerDlfMessages);

  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, STAGER_DAEMON_START, 0, NULL);
}

/*************************************************************/
/* help method for the configuration (from the command line) */
/*************************************************************/
void castor::stager::daemon::StagerDaemon::help(std::string programName)
{
  std::cout << "Usage: " << programName << " [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t--Jthreads    or -J {integer >= 0}  \tNumber of threads for the Job requests service\n"
    "\t--Pthreads    or -P {integer >= 0}  \tNumber of threads for the Prepare requests service\n"
    "\t--Sthreads    or -S {integer >= 0}  \tNumber of threads for the Stage requests service\n"
    "\t--Qthreads    or -Q {integer >= 0}  \tNumber of threads for the Query requests service\n"
    "\t--Ethreads    or -E {integer >= 0}  \tNumber of threads for the Error service\n"
    "\t--jthreads    or -j {integer >= 0}  \tNumber of threads for the Job service\n"
    "\t--Gthreads    or -G {integer >= 0}  \tNumber of threads for the GC service\n"
    "\n"
    "Comments to: Castor.Support@cern.ch\n";
}

void castor::stager::daemon::StagerDaemon::waitAllThreads() throw()
{
  castor::server::BaseDaemon::waitAllThreads();
  castor::replier::RequestReplier::getInstance()->terminate();
}
