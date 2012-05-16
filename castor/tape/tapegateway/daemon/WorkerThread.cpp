/******************************************************************************
 *                      WorkerThread.cpp
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
 * @(#)$RCSfile: WorkerThread.cpp,v $ $Author: waldron $
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <errno.h>
#include <getconfent.h>
#include <sys/time.h>
#include <sys/types.h>
#include <inttypes.h>
#include <stdlib.h>
#include <unistd.h>
#include <u64subr.h>
#include <memory>
#include <typeinfo>
#include <algorithm>
#include <queue>
#include <cstring>
#include <typeinfo>

#include "Ctape_constants.h" // for log only
#include "Cns_api.h" // for log only

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"

#include "castor/tape/net/net.hpp"

#include "castor/tape/tapegateway/TapeGatewayDlfMessageConstants.hpp"
#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/EndNotificationFileErrorReport.hpp"
#include "castor/tape/tapegateway/FileErrorReport.hpp"
#include "castor/tape/tapegateway/FileMigratedNotification.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"
#include "castor/tape/tapegateway/FileToMigrateRequest.hpp"
#include "castor/tape/tapegateway/FileRecalledNotification.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/FileToRecallRequest.hpp"
#include "castor/tape/tapegateway/GatewayMessage.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/BaseFileInfoStruct.hpp"
#include "castor/tape/tapegateway/FileMigratedNotificationStruct.hpp"
#include "castor/tape/tapegateway/FileErrorReportStruct.hpp"
#include "castor/tape/tapegateway/FileRecalledNotificationStruct.hpp"
#include "castor/tape/tapegateway/FilesListRequest.hpp"
#include "castor/tape/tapegateway/FileMigrationReportList.hpp"
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"
#include "castor/tape/tapegateway/FilesToRecallList.hpp"
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include "castor/tape/tapegateway/FileRecallReportList.hpp"
#include "castor/tape/tapegateway/FilesToMigrateListRequest.hpp"
#include "castor/tape/tapegateway/FilesToRecallListRequest.hpp"
#include "castor/stager/TapeTpModeCodes.hpp"

#include "castor/tape/tapegateway/daemon/NsTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/daemon/VmgrTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/daemon/WorkerThread.hpp"

#include "castor/tape/utils/utils.hpp"
#include "castor/tape/utils/Timer.hpp"

#include "castor/tape/tapegateway/ScopedTransaction.hpp"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::WorkerThread::WorkerThread():BaseObject(){}

/* TODO: Cleanup the classes:
 * EndNotificationFileErrorReport
 *
 */

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::WorkerThread::run(void* arg)
{
  // to comunicate with the tape Tapebridge
  // service to access the database
  castor::IService* dbSvc = castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc = dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);
  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR,0, NULL);
    return;
  }
  try{
    // Gather information about the request to allow detailed and compact logging
    // in request handlers.
    requesterInfo requester; // This will be initialised by constructor.
    
    // create a socket
    std::auto_ptr<castor::io::ServerSocket> sock((castor::io::ServerSocket*)arg); 
    try {
      sock->getPeerIp(requester.port, requester.ip);
      net::getPeerHostName(sock->socket(), requester.hostName);
    } catch(castor::exception::Exception& e) {
      // "Exception caught : ignored" message
      castor::dlf::Param params[] =
	{castor::dlf::Param("Standard Message", sstrerror(e.code())),
	 castor::dlf::Param("Precise Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_UNKNOWN_CLIENT, params);
      return;
    }
    // get the incoming request
    try {
      std::auto_ptr<castor::IObject>  obj(sock->readObject());
      if (obj.get() == NULL){
        castor::dlf::Param params[] = {
            castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
            castor::dlf::Param("Port",requester.port),
            castor::dlf::Param("HostName",requester.hostName)
        };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_INVALID_REQUEST, params);
	return;
      }
      // first let's just extract for logging the GatewayMessage info
      GatewayMessage& message = dynamic_cast<GatewayMessage&>(*obj);
      castor::dlf::Param params[] = {
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId",message.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId",message.aggregatorTransactionId()),
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, WORKER_MESSAGE_RECEIVED, params);

      //let's call the handler
      castor::IObject* handler_response = NULL;
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, WORKER_DISPATCHING, params);
      switch (obj->type()) {
        case OBJ_VolumeRequest:
          handler_response = handleStartWorker(*obj,*oraSvc,requester);
          break;
        case OBJ_FileRecalledNotification:
          handler_response = handleRecallUpdate(*obj,*oraSvc,requester);
          break;
        case OBJ_FileMigratedNotification:
          handler_response = handleMigrationUpdate(*obj,*oraSvc,requester);
          break;
        case OBJ_FileToRecallRequest:
          handler_response = handleRecallMoreWork(*obj,*oraSvc,requester);
          break;
        case OBJ_FileToMigrateRequest:
          handler_response = handleMigrationMoreWork(*obj,*oraSvc,requester);
          break;
        case OBJ_EndNotification:
          handler_response = handleEndWorker(*obj,*oraSvc,requester);
          break;
        case OBJ_EndNotificationErrorReport:
          handler_response = handleFailWorker(*obj,*oraSvc,requester);
          break;
        case OBJ_FileMigrationReportList:
          handler_response = handleFileMigrationReportList(*obj,*oraSvc,requester);
          break;
        case OBJ_FileRecallReportList:
          handler_response = handleFileRecallReportList(*obj,*oraSvc,requester);
          break;
        case OBJ_FilesToMigrateListRequest:
          handler_response = handleFilesToMigrateListRequest(*obj,*oraSvc,requester);
          break;
        case OBJ_FilesToRecallListRequest:
          handler_response = handleFilesToRecallListRequest(*obj,*oraSvc,requester);
          break;
        default:
          //object not valid
          delete handler_response;
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_INVALID_REQUEST, params);
          return;
      }
      // send back the response
      std::auto_ptr<castor::IObject> response(handler_response);
      try {

	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,WORKER_RESPONDING, params);

	sock->sendObject(*response);

      }catch (castor::exception::Exception& e){
        castor::dlf::Param params[] = {
            castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
            castor::dlf::Param("Port",requester.port),
            castor::dlf::Param("HostName",requester.hostName),
            castor::dlf::Param("errorCode",sstrerror(e.code())),
            castor::dlf::Param("errorMessage",e.getMessage().str())
        };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,WORKER_CANNOT_RESPOND, params);
      }

    } catch (castor::exception::Exception& e) {
      
      // "Unable to read Request from socket" message
      
      castor::dlf::Param params[] = {
	  castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
	  castor::dlf::Param("Port",requester.port),
	  castor::dlf::Param("HostName",requester.hostName),
	  castor::dlf::Param("errorCode",sstrerror(e.code())),
	  castor::dlf::Param("errorMessage",e.getMessage().str())
	};
	
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,WORKER_CANNOT_RECEIVE, params);
      return;
    }
    catch( std::bad_cast &ex){
      // "Invalid Request" message
      castor::dlf::Param params[] = {
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName)
      };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_INVALID_REQUEST, params);
	return;
    }
  } catch(...) {
    //castor one are trapped before 
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_UNKNOWN_EXCEPTION, 0, NULL);
    
  }

}

castor::IObject* castor::tape::tapegateway::WorkerThread::handleStartWorker(
    castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc,
    requesterInfo& requester  ) throw(castor::exception::Exception){

  // I received a start worker request
  Volume* response=new Volume();

  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, WORKER_VOLUME_REQUESTED,0, NULL);

  try{
    VolumeRequest &startRequest = dynamic_cast<VolumeRequest&>(obj);
    castor::dlf::Param params[] = {
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", startRequest.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", startRequest.aggregatorTransactionId()),
    };
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, WORKER_GETTING_VOLUME, params);

    castor::tape::utils::Timer timer;
    try {
    
      // GET FROM DB
      // The wrapper function has no hidden effect
      // The SQL commits in all cases.
      oraSvc.startTapeSession(startRequest, *response); 
    
    } catch (castor::exception::Exception& e){
      if (response) delete response;

      EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
      errorReport->setErrorCode(e.code());
      errorReport->setErrorMessage(e.getMessage().str());
      errorReport->setMountTransactionId(startRequest.mountTransactionId());
      errorReport->setAggregatorTransactionId(startRequest.aggregatorTransactionId());
    
      castor::dlf::Param params[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId", startRequest.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId", startRequest.aggregatorTransactionId()),
          castor::dlf::Param("errorCode",sstrerror(e.code())),
          castor::dlf::Param("errorMessage",e.getMessage().str())
	};

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_NO_VOLUME, params);
      return errorReport;
    }
     
    if (response->mountTransactionId()==0 ) {
      // I don't have anything to recall I send a NoMoreFiles
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_NO_FILE, params);
      
      delete response;

      NoMoreFiles* noMore= new NoMoreFiles();
      noMore->setMountTransactionId(startRequest.mountTransactionId());
      noMore->setAggregatorTransactionId(startRequest.aggregatorTransactionId());
      return noMore;
    }
    
    response->setAggregatorTransactionId(startRequest.aggregatorTransactionId());
   
    castor::dlf::Param params0[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", startRequest.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", startRequest.aggregatorTransactionId()),
        castor::dlf::Param("TPVID",response->vid()),
        castor::dlf::Param("mode",response->mode()),
        castor::dlf::Param("label",response->label()),
        castor::dlf::Param("density",response->density()),
        castor::dlf::Param("ProcessingTime", timer.secs())
      };
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_VOLUME_FOUND, params0);

  } catch( std::bad_cast &ex) {

    if (response) delete response;
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, WORKER_INVALID_CAST, 0, NULL);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }

  return response;

}

/* TODO: adapt to bulk (remove?) */
castor::IObject* castor::tape::tapegateway::WorkerThread::handleRecallUpdate(
    castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc,
    requesterInfo& requester) throw(castor::exception::Exception){

  // Initialize auto-rollback in case of error
  ScopedTransaction scpTrans(&oraSvc);

  try {
    // I received a FileRecalledNotification
    FileRecalledNotification &fileRecalled = dynamic_cast<FileRecalledNotification&>(obj);

    // log "Worker: received recall notification"
    struct Cns_fileid castorFileId;
    memset(&castorFileId,'\0',sizeof(castorFileId));
    strncpy(castorFileId.server, fileRecalled.nshost().c_str(), sizeof(castorFileId.server)-1);
    castorFileId.fileid = fileRecalled.fileid();
    std::stringstream checksumHex;
    checksumHex << "0x" << std::hex << fileRecalled.checksum();
    castor::dlf::Param paramsComplete[] = {
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId",fileRecalled.aggregatorTransactionId()),
        castor::dlf::Param("fseq",fileRecalled.fseq()),
        castor::dlf::Param("checksum name",fileRecalled.checksumName().c_str()),
        castor::dlf::Param("checksum", checksumHex.str()),
        castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_NOTIFIED, paramsComplete, &castorFileId);

    // update stager DB (autocommited)
    // note that the logging is done in the DB code as well as the error handling
    oraSvc.setFileRecalled(fileRecalled);

    // create response
    NotificationAcknowledge* response = new NotificationAcknowledge();
    response->setMountTransactionId(fileRecalled.mountTransactionId());
    response->setAggregatorTransactionId(fileRecalled.aggregatorTransactionId());
    return response;
  } catch  (std::bad_cast &ex) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_INVALID_CAST, 0, NULL);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }
  // never reached
  return 0;
}

/* TODO: adapt to bulk (remove?) */

// This function is the one handling the writing in the tape gateway.
// It receives a writing report from the tape server, and first
// updates the vmgr. Only after this is accomplished, the written file
// is safe on tape. After, we can reference this new, safe tape file
// into the name server, and finish with administrative updates in the DB.
// The vmgr's fseq will be bumped up even if there is an error in updating the
// file. Failure to do so would result in a count off by one for the subsequent
// files and hence create a danger of data loss.
//
// In order to update the vmgr as efficiently as possible, we get the smallest
// possible information from the DB (the VID). The file's details will be
// retreived in a second access.
// Failure to update the vmgr, whichever the reason, will be handled by
// making the tape read only.
castor::IObject*  castor::tape::tapegateway::WorkerThread::handleMigrationUpdate(
    castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc,
    requesterInfo& requester ) throw(castor::exception::Exception){
  // Setup the scoped rollback mechanism for proper exceptions handling
  ScopedTransaction scpTrans(&oraSvc);

  // Helper objects
  NsTapeGatewayHelper nsHelper;
  castor::tape::utils::Timer timer;

  // Check we received a FileMigratedResponse
  // Pointer is used temporarily to avoid code blocks that are one into another.
  FileMigratedNotification * fileMigratedPtr  = dynamic_cast<FileMigratedNotification*>(&obj);
  if (!fileMigratedPtr) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_INVALID_CAST, 0, NULL);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }
  FileMigratedNotification & fileMigrated = *fileMigratedPtr;
  fileMigratedPtr = NULL;

  // Extract CASTOR FILE ID (mostly used in logging)
  struct Cns_fileid castorFileId;
  memset(&castorFileId,'\0',sizeof(castorFileId));
  strncpy(
      castorFileId.server,
      fileMigrated.nshost().c_str(),
      sizeof(castorFileId.server)-1
  );
  castorFileId.fileid = fileMigrated.fileid();

  // Get the VID from the DB (and the tape pool)
  std::string vid;
  std::string tapePool;
  try{
    oraSvc.getMigrationMountVid(fileMigrated.mountTransactionId(), vid, tapePool);
  } catch (castor::exception::Exception e) {
    // No Vid, not much we can do... Just tell the tape server to abort.
    logMigrationCannotFindVid (nullCuuid, &castorFileId, requester,
        fileMigrated,e);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(ENOENT);
    errorReport->setErrorMessage("Session not found in DB.");
    errorReport->setMountTransactionId(fileMigrated.mountTransactionId());
    errorReport->setAggregatorTransactionId(fileMigrated.aggregatorTransactionId());
    return errorReport;
  }

  // Update the VMGR. Stop here in case of failure.
  try {
    timer.reset();
    VmgrTapeGatewayHelper::updateTapeInVmgr(fileMigrated, vid, m_shuttingDown);
    logMigrationVmgrUpdate(nullCuuid, &castorFileId, requester, fileMigrated,
      tapePool, vid, timer.usecs());
  } catch (castor::exception::Exception& e){
    logMigrationVmgrFailure(nullCuuid, &castorFileId, requester, fileMigrated,
      tapePool, e);
    // Attempt to move the tape to read only as a stuck fseq is the worst we can get in
    // terms of data protection: what got written will potentially be overwritten.
    try {
      VmgrTapeGatewayHelper::setTapeAsReadonlyAndUnbusy(vid, m_shuttingDown);
      logTapeReadOnly(nullCuuid, &castorFileId, requester, fileMigrated,
          tapePool);
    } catch (castor::exception::Exception e) {
      logCannotReadOnly(nullCuuid, &castorFileId, requester, fileMigrated,
          tapePool, e);
    }
    // We could not update the VMGR for this tape: better leave it alone and stop the session
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EIO);
    errorReport->setErrorMessage("Failed to update VMGR");
    errorReport->setMountTransactionId(fileMigrated.mountTransactionId());
    errorReport->setAggregatorTransactionId(fileMigrated.aggregatorTransactionId());
    return errorReport;
  }

  // Once we    passed this point, the data on tape is protected from overwrites
  // by the vmgr.
  const std::string blockid = utils::tapeBlockIdToString(fileMigrated.blockId0(),
      fileMigrated.blockId1() ,fileMigrated.blockId2() ,fileMigrated.blockId3());
  char checksumHex[19];
  checksumHex[0] = '0';
  checksumHex[1] = 'x';
  try {
    utils::toHex((uint64_t)fileMigrated.checksum(), &checksumHex[2], 17);
  } catch (castor::exception::Exception& e) {
    logFatalError (nullCuuid, &castorFileId, requester, fileMigrated, e);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    errorReport->setMountTransactionId(fileMigrated.mountTransactionId());
    errorReport->setAggregatorTransactionId(fileMigrated.aggregatorTransactionId());
    return errorReport;
  }

  logMigrationNotified(nullCuuid, &castorFileId,
      requester, fileMigrated, checksumHex, blockid);
  // CHECK DB
  std::string originalVid;
  int originalCopyNumber;
  int copyNumber;
  std::string serviceClass;
  std::string fileClass;
  u_signed64 lastModificationTime;
  try {
    // This SQL procedure does not do any lock/updates.
    oraSvc.getMigratedFileInfo(fileMigrated,vid,copyNumber,lastModificationTime,
                               originalVid,originalCopyNumber,fileClass);
  } catch (castor::exception::Exception& e){
    logMigrationFileNotfound (nullCuuid, &castorFileId, requester, fileMigrated, e);
    // The migrated file can not be found in the system: this is non-fatal: it could have been deleted by the user
    NotificationAcknowledge* response= new NotificationAcknowledge;
    response->setMountTransactionId(fileMigrated.mountTransactionId());
    response->setAggregatorTransactionId(fileMigrated.aggregatorTransactionId());
    return response;
  }
  logMigrationGetDbInfo (nullCuuid, &castorFileId, requester, fileMigrated, serviceClass, fileClass,
      tapePool, blockid, vid, copyNumber, lastModificationTime, timer.usecs());

  // UPDATE NAMESERVER
  // We'll flag this if the migration turned out to be of an old version of
  // an overwritten file.
  bool fileStale = false;
  bool segmentSuperfluous = false;
  try {
    timer.reset();
    if (originalVid.empty()) {
      // update the name server (standard migration)
      nsHelper.updateMigratedFile( fileMigrated, copyNumber, vid, lastModificationTime);
      logMigrationNsUpdate(nullCuuid, &castorFileId, requester, fileMigrated, serviceClass, fileClass,
      tapePool, blockid, vid, copyNumber, lastModificationTime, checksumHex, timer.usecs());
    } else {
      // update the name server (repacked file)
      nsHelper.updateRepackedFile( fileMigrated, originalCopyNumber, originalVid,
                                   copyNumber, vid, lastModificationTime);
      logRepackNsUpdate(nullCuuid, &castorFileId, requester, fileMigrated, serviceClass, fileClass,
          tapePool, blockid, vid, copyNumber, lastModificationTime, checksumHex, originalVid, timer.usecs());
    }
  } catch (castor::tape::tapegateway::NsTapeGatewayHelper::NoSuchFileException) {
    fileStale = true;
    logRepackFileRemoved(nullCuuid, &castorFileId, requester, fileMigrated, serviceClass, fileClass,
          tapePool, blockid, vid, copyNumber, lastModificationTime, checksumHex, originalVid, timer.usecs());
  } catch (castor::tape::tapegateway::NsTapeGatewayHelper::FileMutatedException) {
    fileStale = true;
    logRepackStaleFile (nullCuuid, &castorFileId, requester, fileMigrated, serviceClass, fileClass,
        tapePool, blockid, vid, copyNumber, lastModificationTime, checksumHex, originalVid, timer.usecs());
  } catch (castor::tape::tapegateway::NsTapeGatewayHelper::FileMutationUnconfirmedException) {
    fileStale = true;
    logRepackUncomfirmedStaleFile(nullCuuid, &castorFileId, requester, fileMigrated, serviceClass, fileClass,
        tapePool, blockid, vid, copyNumber, lastModificationTime, checksumHex, originalVid, timer.usecs());
  } catch (castor::tape::tapegateway::NsTapeGatewayHelper::SuperfluousSegmentException ssex) {
    segmentSuperfluous = true;
    logSuperfluousSegment(nullCuuid, &castorFileId, requester, fileMigrated, serviceClass, fileClass,
        tapePool, blockid, vid, copyNumber, lastModificationTime, checksumHex, originalVid, timer.usecs());
  } catch (castor::exception::Exception& e) {
    logMigrationNsFailure(nullCuuid, &castorFileId, requester, fileMigrated, serviceClass, fileClass,
        tapePool, blockid, vid, copyNumber, lastModificationTime, checksumHex, originalVid, timer.usecs(),e);
    try {
      // The underlying SQL does NOT commit
      oraSvc.failFileTransfer(fileMigrated.mountTransactionId(), fileMigrated.fileid(),
                              fileMigrated.nshost(), e.code());
    } catch(castor::exception::Exception& e) {
      logMigrationCannotUpdateDb(nullCuuid, &castorFileId, requester, fileMigrated, serviceClass, fileClass,
          tapePool, e);
    }
    // As the previous call did modify things, better committing before returning.
    scpTrans.commit();
    // Any problem in the NS does not involve the tape server: it did its job and left some useless data on the tape
    // we carry on.
    NotificationAcknowledge* response= new NotificationAcknowledge;
    response->setMountTransactionId(fileMigrated.mountTransactionId());
    response->setAggregatorTransactionId(fileMigrated.aggregatorTransactionId());
    return response;
  }

  // UPDATE DB
  timer.reset();
  try {
    if (fileStale) {
      // The underlying SQL DOES commit.
      oraSvc.setFileStaleInMigration(fileMigrated);
    } else if (segmentSuperfluous){
      // The underlying SQL DOES commit.
      oraSvc.dropSuperfluousSegment(fileMigrated);
    } else {
      // The underlying SQL DOES commit.
      oraSvc.setFileMigrated(fileMigrated);
    }
  } catch (castor::exception::Exception& e) {
    logMigrationCannotUpdateDb(nullCuuid, &castorFileId,
        requester, fileMigrated, serviceClass, fileClass, tapePool, e);
    try {
      // The underlying SQL does NOT commit.
      oraSvc.failFileTransfer(fileMigrated.mountTransactionId(), fileMigrated.fileid(),
                              fileMigrated.nshost(), e.code());
      // There will be no subsequent call to the DB so better comminting here
      scpTrans.commit();
    } catch(castor::exception::Exception& e) {
      logMigrationCannotUpdateDb(nullCuuid, &castorFileId,
              requester, fileMigrated, serviceClass, fileClass, tapePool, e);
    }
  }
  logMigrationDbUpdate(nullCuuid, &castorFileId, requester, fileMigrated, serviceClass, fileClass,
      tapePool, blockid, vid, copyNumber, timer.usecs());

  // Commit for added safety as the SQL is not to be trusted. (maybe a dupe, though)
  scpTrans.commit();
  // Any problem with the DB does not involve the tape server: it did its job. We failed to keep track of it and will
  // re-migrate later. (Or most likely everything went fine when reaching this point).
  NotificationAcknowledge* response= new NotificationAcknowledge;
  response->setMountTransactionId(fileMigrated.mountTransactionId());
  response->setAggregatorTransactionId(fileMigrated.aggregatorTransactionId());
  return response;
}

/* TODO:adapt to bulk (remove?) */
castor::IObject*  castor::tape::tapegateway::WorkerThread::handleRecallMoreWork(
    castor::IObject& obj,castor::tape::tapegateway::ITapeGatewaySvc& oraSvc,
    requesterInfo& requester ) throw(castor::exception::Exception){
  // Create the scoped transaction object that will rollback in case of problem.
  ScopedTransaction scpTrans(&oraSvc);

  // I received FileToRecallRequest
  FileToRecall* response= new FileToRecall();
  try {
    FileToRecallRequest& fileToRecall = dynamic_cast<FileToRecallRequest&>(obj);
    castor::dlf::Param params_outloop[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",fileToRecall.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId",fileToRecall.aggregatorTransactionId())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,WORKER_RECALL_REQUESTED,params_outloop);
    
    castor::tape::utils::Timer timer;
    try {
      // CALL THE DB
      // The undelying SQL takes locks, updates but does not commit in SQL.
      // Wrapper function does (not as usual).
      oraSvc.getFileToRecall(fileToRecall,*response);
    } catch (castor::exception::Exception& e) {
      castor::dlf::Param params[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId",fileToRecall.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId",fileToRecall.aggregatorTransactionId()),
          castor::dlf::Param("errorCode",sstrerror(e.code())),
          castor::dlf::Param("errorMessage",e.getMessage().str())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_RECALL_RETRIEVING_DB_ERROR, params);
      EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
      errorReport->setErrorCode(e.code());
      errorReport->setErrorMessage(e.getMessage().str());
      errorReport->setMountTransactionId(fileToRecall.mountTransactionId());
      errorReport->setAggregatorTransactionId(fileToRecall.aggregatorTransactionId());
      if (response) delete response;
      return errorReport;
    }
    
    if (response->mountTransactionId()  == 0 ) {
      // I don't have anything to recall I send a NoMoreFiles
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_NO_FILE_TO_RECALL,params_outloop);
      delete response;
      
      NoMoreFiles* noMore= new NoMoreFiles();
      noMore->setMountTransactionId(fileToRecall.mountTransactionId());
      noMore->setAggregatorTransactionId(fileToRecall.aggregatorTransactionId());
      return noMore;
    }
    response->setAggregatorTransactionId(fileToRecall.aggregatorTransactionId());
    const std::string blockid = utils::tapeBlockIdToString(response->blockId0() , response->blockId1() , response->blockId2() , response->blockId3());
    
    struct Cns_fileid castorFileId;
    memset(&castorFileId,'\0',sizeof(castorFileId));
    strncpy(
	    castorFileId.server,
	    response->nshost().c_str(),
	    sizeof(castorFileId.server)-1
	    );
    castorFileId.fileid = response->fileid();

    castor::dlf::Param completeParams[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",response->mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId",response->aggregatorTransactionId()),
        castor::dlf::Param("NSHOSTNAME",response->nshost()),
        castor::dlf::Param("NSFILEID",response->fileid()),
        castor::dlf::Param("fseq",response->fseq()),
        castor::dlf::Param("path",response->path()),
        castor::dlf::Param("blockid", blockid ),
        castor::dlf::Param("fileTransactionId", response->fileTransactionId()),
        castor::dlf::Param("ProcessingTime", timer.secs())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_RETRIEVED,completeParams, &castorFileId);
  } catch (std::bad_cast &ex) {
    // "Invalid Request" message
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,WORKER_INVALID_CAST, 0,NULL);
    
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    if (response) delete response;
    return errorReport;
  }

  // Commit should be handled, just release.
  scpTrans.release();
  return response;
}

/* TODO: adapt to bulk (remove?)*/
castor::IObject* castor::tape::tapegateway::WorkerThread::handleMigrationMoreWork(
    castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc,
    requesterInfo& requester ) throw(castor::exception::Exception){
  // Setup the auto rollback in case of exception or error.
  ScopedTransaction scpTrans (&oraSvc);

  // I received FileToMigrateRequest
  // I get a file from the db
  // send a new file
  FileToMigrate* response=new FileToMigrate();
  
  try {
    
    FileToMigrateRequest& fileToMigrate = dynamic_cast<FileToMigrateRequest&>(obj);
    
    castor::dlf::Param params_outloop[] = {
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",fileToMigrate.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId",fileToMigrate.aggregatorTransactionId()),
    };
     
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, WORKER_MIGRATION_REQUESTED,params_outloop);

    castor::tape::utils::Timer timer;
    try {
   
      // CALL THE DB. This SQL is self contained (commit included)
      // We keep the auto out of scoped rollback as a safety cleanup mechanism
      oraSvc.getFileToMigrate(fileToMigrate,*response);

    } catch (castor::exception::Exception& e) {
      castor::dlf::Param params[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId",fileToMigrate.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId",fileToMigrate.aggregatorTransactionId()),
          castor::dlf::Param("errorCode",sstrerror(e.code())),
          castor::dlf::Param("errorMessage",e.getMessage().str())
      };

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_MIGRATION_RETRIEVING_DB_ERROR,params);
    
      EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
      errorReport->setErrorCode(e.code());
      errorReport->setErrorMessage(e.getMessage().str());
      errorReport->setMountTransactionId(fileToMigrate.mountTransactionId());
      errorReport->setAggregatorTransactionId(fileToMigrate.aggregatorTransactionId());
      if (response) delete response;
      return errorReport;
    }
      
    if ( response->mountTransactionId() == 0 ) {
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_NO_FILE_TO_MIGRATE, params_outloop);
      // I don't have anything to migrate I send an NoMoreFiles

      NoMoreFiles* noMore= new NoMoreFiles();
      noMore->setMountTransactionId(fileToMigrate.mountTransactionId());
      noMore->setAggregatorTransactionId(fileToMigrate.aggregatorTransactionId());
      delete response;
      return noMore;
    }

    response->setAggregatorTransactionId(fileToMigrate.aggregatorTransactionId());

    struct Cns_fileid castorFileId;
    memset(&castorFileId,'\0',sizeof(castorFileId));
    strncpy(
	    castorFileId.server,
	    response->nshost().c_str(),
	    sizeof(castorFileId.server)-1
	    );
    castorFileId.fileid = response->fileid();

    castor::dlf::Param paramsComplete[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",response->mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId",response->aggregatorTransactionId()),
        castor::dlf::Param("fseq",response->fseq()),
        castor::dlf::Param("path",response->path()),
        castor::dlf::Param("fileSize", response->fileSize()),
        castor::dlf::Param("lastKnownFileName", response->lastKnownFilename()),
        castor::dlf::Param("lastModificationTime", response->lastModificationTime()),
        castor::dlf::Param("fileTransactionId", response->fileTransactionId()),
        castor::dlf::Param("ProcessingTime", timer.secs())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_RETRIEVED, paramsComplete, &castorFileId);

  } catch (std::bad_cast &ex) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_INVALID_CAST, 0, NULL);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    if (response) delete response;
    return errorReport;
  }
  return response; // check went fine

}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleEndWorker(
    castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    requesterInfo& requester) throw(castor::exception::Exception){
  // Rollback mechanism.
  ScopedTransaction scpTrans(&oraSvc);
  // I received an EndTransferRequest, I send back an EndTransferResponse
  std::auto_ptr <NotificationAcknowledge> response(new NotificationAcknowledge());
  EndNotification * pEndRep =  dynamic_cast<EndNotification *>(&obj);
  if (!pEndRep) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_INVALID_CAST, 0, NULL);
    std::auto_ptr<EndNotificationErrorReport> errorReport(new EndNotificationErrorReport());
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport.release();
  }
  EndNotification& endRequest = *pEndRep;
  pEndRep = NULL;

  // Log the end notification.
  {
    castor::dlf::Param params[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_END_NOTIFICATION, params);
  }
  response->setMountTransactionId(endRequest.mountTransactionId());
  response->setAggregatorTransactionId(endRequest.aggregatorTransactionId());
  castor::tape::utils::Timer timer;
  try {
    // ACCESS DB to get tape to release
    castor::tape::tapegateway::ITapeGatewaySvc::TapeToReleaseInfo tape;
    // Straightforward wrapper, read-only sql.
    oraSvc.getTapeToRelease(endRequest.mountTransactionId(),tape);
    castor::dlf::Param paramsComplete[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
        castor::dlf::Param("TPVID",tape.vid),
        castor::dlf::Param("mode",tape.mode),
        castor::dlf::Param("ProcessingTime", timer.secs())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, WORKER_END_GET_TAPE_TO_RELEASE, paramsComplete);
    try {
      // UPDATE VMGR
      if (tape.mode == WRITE_ENABLE) { // just for write case
        if (tape.full) {
          // The tape has been determined as full during a file operation (in error report)
          // Set it as FULL in the VMGR.
          try {
            timer.reset();
            VmgrTapeGatewayHelper::setTapeAsFull(tape.vid, m_shuttingDown);
            castor::dlf::Param paramsVmgr[] ={
                castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
                castor::dlf::Param("Port",requester.port),
                castor::dlf::Param("HostName",requester.hostName),
                castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
                castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
                castor::dlf::Param("TPVID",tape.vid),
                castor::dlf::Param("mode",tape.mode),
                castor::dlf::Param("ProcessingTime", timer.secs())
            };
            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_TAPE_MAKED_FULL, paramsVmgr);
          } catch (castor::exception::Exception& e) {
            castor::dlf::Param params[] ={
                castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
                castor::dlf::Param("Port",requester.port),
                castor::dlf::Param("HostName",requester.hostName),
                castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
                castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
                castor::dlf::Param("TPVID", tape.vid),
                castor::dlf::Param("errorCode",sstrerror(e.code())),
                castor::dlf::Param("errorMessage",e.getMessage().str())
            };
            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_CANNOT_MARK_TAPE_FULL, params);
          }
        }
        timer.reset();
        VmgrTapeGatewayHelper::resetBusyTape(tape.vid, m_shuttingDown);
        castor::dlf::Param paramsVmgr[] ={
            castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
            castor::dlf::Param("Port",requester.port),
            castor::dlf::Param("HostName",requester.hostName),
            castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
            castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
            castor::dlf::Param("TPVID",tape.vid),
            castor::dlf::Param("mode",tape.mode),
            castor::dlf::Param("ProcessingTime", timer.secs())
        };
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_END_RELEASE_TAPE, paramsVmgr);
      }
    } catch (castor::exception::Exception& e) {
      castor::dlf::Param params[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
          castor::dlf::Param("TPVID", tape.vid),
          castor::dlf::Param("errorCode",sstrerror(e.code())),
          castor::dlf::Param("errorMessage",e.getMessage().str())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_CANNOT_RELEASE_TAPE, params);
    }
    // ACCESS DB now we mark it as done
    timer.reset();
    // Wrapper function is straightforward. No trap.
    // SQL commits in the end but could fail in many selects.
    // Rollback required for this one.
    oraSvc.endTapeSession(endRequest.mountTransactionId());
    castor::dlf::Param paramsDb[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
        castor::dlf::Param("TPVID",tape.vid),
        castor::dlf::Param("mode",tape.mode),
        castor::dlf::Param("ProcessingTime", timer.secs())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_END_DB_UPDATE, paramsDb);
  } catch (castor::exception::Exception& e){
    castor::dlf::Param params[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
        castor::dlf::Param("errorCode",sstrerror(e.code())),
        castor::dlf::Param("errorMessage",e.getMessage().str())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_END_DB_ERROR, params);
    return response.release();
  }
  // No release of the scpTrans as all is supposed to be self-contained.
  // Leving the rollback as a safety mechanism (SQL does not catch all exceptions)
  // TODO review SQL.
  return  response.release();
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleFailWorker(
    castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    requesterInfo& requester ) throw(castor::exception::Exception){
  // We received an EndNotificationErrorReport
  EndNotificationErrorReport *pEndErrRep =  dynamic_cast<EndNotificationErrorReport *>(&obj);
  if (!pEndErrRep) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_INVALID_CAST, 0, NULL);
    std::auto_ptr<EndNotificationErrorReport> errorReport(new EndNotificationErrorReport());
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport.release();
  }
  EndNotificationErrorReport& endRequest = *pEndErrRep;
  pEndErrRep = NULL;
  std::auto_ptr<NotificationAcknowledge> response(new NotificationAcknowledge());
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
      castor::dlf::Param("errorcode", endRequest.errorCode()),
      castor::dlf::Param("errorMessage", endRequest.errorMessage())
  };
  // Depending on the error we get, this can be an innocent end of tape or something else.
  // The ENOSPC case is business as usual. Rest remains an error.
  castor::dlf::dlf_writep(nullCuuid,
                          (endRequest.errorCode() == ENOSPC)?DLF_LVL_SYSTEM:DLF_LVL_ERROR,
                          WORKER_FAIL_NOTIFICATION, params);
  response->setMountTransactionId(endRequest.mountTransactionId());
  response->setAggregatorTransactionId(endRequest.aggregatorTransactionId());
  castor::tape::utils::Timer timer;
  try {
    // ACCESS DB to get tape to release
    castor::tape::tapegateway::ITapeGatewaySvc::TapeToReleaseInfo tape;
    // Safe, read only SQL
    oraSvc.getTapeToRelease(endRequest.mountTransactionId(), tape);
    castor::dlf::Param paramsComplete[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
        castor::dlf::Param("TPVID",tape.vid),
        castor::dlf::Param("mode",tape.mode),
        castor::dlf::Param("ProcessingTime", timer.secs())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, WORKER_FAIL_GET_TAPE_TO_RELEASE, paramsComplete);
    // Update VMGR just for write case, we release/mark the tape.
    if (tape.mode == WRITE_ENABLE) {
      // CHECK IF THE ERROR WAS DUE TO A FULL TAPE, or if the tape was previously marked as full
      if (endRequest.errorCode() == ENOSPC || tape.full) {
        try {
          timer.reset();
          VmgrTapeGatewayHelper::setTapeAsFull(tape.vid, m_shuttingDown);
          castor::dlf::Param paramsVmgr[] ={
              castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
              castor::dlf::Param("Port",requester.port),
              castor::dlf::Param("HostName",requester.hostName),
              castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
              castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
              castor::dlf::Param("TPVID",tape.vid),
              castor::dlf::Param("mode",tape.mode),
              castor::dlf::Param("ProcessingTime", timer.secs())
          };
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_TAPE_MAKED_FULL, paramsVmgr);
        } catch (castor::exception::Exception& e) {
          castor::dlf::Param params[] ={
              castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
              castor::dlf::Param("Port",requester.port),
              castor::dlf::Param("HostName",requester.hostName),
              castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
              castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
              castor::dlf::Param("TPVID", tape.vid),
              castor::dlf::Param("errorCode",sstrerror(e.code())),
              castor::dlf::Param("errorMessage",e.getMessage().str())
          };
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_CANNOT_MARK_TAPE_FULL, params);
        }
      } else {
        try {
          // We just release the tape
          timer.reset();
          VmgrTapeGatewayHelper::resetBusyTape(tape.vid, m_shuttingDown);
          castor::dlf::Param paramsVmgr[] ={
              castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
              castor::dlf::Param("Port",requester.port),
              castor::dlf::Param("HostName",requester.hostName),
              castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
              castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
              castor::dlf::Param("TPVID",tape.vid),
              castor::dlf::Param("mode",tape.mode),
              castor::dlf::Param("ProcessingTime", timer.secs())
          };
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_FAIL_RELEASE_TAPE, paramsVmgr);
        } catch (castor::exception::Exception& e) {
          castor::dlf::Param params[] ={
              castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
              castor::dlf::Param("Port",requester.port),
              castor::dlf::Param("HostName",requester.hostName),
              castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
              castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
              castor::dlf::Param("TPVID", tape.vid),
              castor::dlf::Param("errorCode",sstrerror(e.code())),
              castor::dlf::Param("errorMessage",e.getMessage().str())
          };
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_CANNOT_RELEASE_TAPE, params);
        }
      }
    }
    // ACCESS db now we fail it
    timer.reset();
    // Direct wrapper, committing SQL
    oraSvc.endTapeSession(endRequest.mountTransactionId(), endRequest.errorCode());
    castor::dlf::Param paramsDb[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
        castor::dlf::Param("TPVID",tape.vid),
        castor::dlf::Param("mode",tape.mode),
        castor::dlf::Param("ProcessingTime", timer.secs())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_FAIL_DB_UPDATE, paramsDb);
  } catch (castor::exception::Exception& e){
    castor::dlf::Param params[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
        castor::dlf::Param("errorCode",sstrerror(e.code())),
        castor::dlf::Param("errorMessage",e.getMessage().str())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_FAIL_DB_ERROR, params);
    return response.release();
  }
  return  response.release();
}

/* TODO: adapt to bulk (remove?) */
castor::IObject*  castor::tape::tapegateway::WorkerThread::handleFileFailWorker(
    castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    requesterInfo& requester ) throw(castor::exception::Exception){
  // We received an EndNotificationFileErrorReport
  try {
    EndNotificationFileErrorReport& endRequest = dynamic_cast<EndNotificationFileErrorReport&>(obj);
    castor::dlf::Param params[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
        castor::dlf::Param("errorcode", endRequest.errorCode()),
        castor::dlf::Param("errorMessage", endRequest.errorMessage()),
        castor::dlf::Param("NSHOSTNAME", endRequest.nsHost()),
        castor::dlf::Param("NSFILEID", endRequest.fileId()),
        castor::dlf::Param("fseq", endRequest.fSeq())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, WORKER_FAIL_NOTIFICATION_FOR_FILE,params);
    // We received a a failed file information. We will fail the file transfer first in the DB,
    // and then fail the session as usual.
    try {
      // This procedure autocommits
      oraSvc.failFileTransfer(endRequest.mountTransactionId(), endRequest.fileId(),
                              endRequest.nsHost(), endRequest.errorCode());
    } catch (castor::exception::Exception& e){
      castor::dlf::Param params[] = {
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
          castor::dlf::Param("errorCode",sstrerror(e.code())),
          castor::dlf::Param("errorMessage",e.getMessage().str())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_FAIL_DB_ERROR, params);
    }

    // Convert the error report into a EndNotificationErrorReport
    // and fail the session
    EndNotificationErrorReport sessionError;
    sessionError.setAggregatorTransactionId(endRequest.aggregatorTransactionId());
    sessionError.setErrorCode(endRequest.errorCode());
    sessionError.setErrorMessage(endRequest.errorMessage());
    sessionError.setMountTransactionId(endRequest.mountTransactionId());
    return handleFailWorker(sessionError, oraSvc, requester);
  } catch (std::bad_cast){
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_INVALID_CAST, 0, NULL);

    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }
  return NULL; // We should not get here.
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleFileMigrationReportList(
    castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    requesterInfo& requester ) throw(castor::exception::Exception){
  // first check we are called with the proper class
  FileMigrationReportList* rep = dynamic_cast<FileMigrationReportList *>(&obj);
  if (!rep) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_INVALID_CAST, 0, NULL);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }
  FileMigrationReportList & fileMigrationReportList = *rep;
  rep = NULL;

  // Log a summary for the report
  {
    castor::dlf::Param params[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", fileMigrationReportList.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", fileMigrationReportList.aggregatorTransactionId()),
        castor::dlf::Param("successes", fileMigrationReportList.successfulMigrations().size()),
        castor::dlf::Param("failures", fileMigrationReportList.failedMigrations().size()),
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIG_REPORT_LIST_RECEIVED, params);
  }

  // We have 2 lists of both successes and errors.
  // The database side of the application will take care of updating both the
  // name server and the stager database.
  // The VMGR is still the responsibility of the gateway.

  // Check we are not confronted too early with a future feature of the tape bridge (update of fseq in
  // VMGR from tape server side instead of from gateway side).
  if (fileMigrationReportList.fseqSet()) {
    // fseq should not have been set by tape server. Error and end tape session.
    // We got an unexpected message type. Internal error. Better stop here.
    std::string report = "fseqSet flag set by tape bridge.";
    report += "This is not supported yet in this version of the tape gateway. ";
    report += "Please check deployment release notes. ";
    report += "In handleFileMigrationReportList: aborting the session.";
    castor::dlf::Param params[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", fileMigrationReportList.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", fileMigrationReportList.aggregatorTransactionId()),
        castor::dlf::Param("errorCode",SEINTERNAL),
        castor::dlf::Param("errorMessage",report)
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, INTERNAL_ERROR, params);
    // Abort all the rest.
    std::auto_ptr<EndNotificationErrorReport> errorReport (new EndNotificationErrorReport());
    errorReport->setErrorCode(SEINTERNAL);
    errorReport->setErrorMessage(report);
    return errorReport.release();
  }

  // Step 1: get the VID from the DB (once for all files)
  // Get the VID from the DB (and the tape pool)
  std::string vid;
  std::string tapePool;
  try{
    oraSvc.getMigrationMountVid(fileMigrationReportList.mountTransactionId(), vid, tapePool);
  } catch (castor::exception::Exception e) {
    // No Vid, not much we can do... The DB is failing on us. We can try to
    // mark the session as closing, be better warn the tape server of a problem
    // so we have a chance to not loop.
    logMigrationCannotFindVid (nullCuuid, requester, fileMigrationReportList, e);
    // This helper procedure will log its own failures.
    setSessionToClosing(oraSvc, requester, fileMigrationReportList.mountTransactionId());
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(ENOENT);
    errorReport->setErrorMessage("Session not found in DB.");
    errorReport->setMountTransactionId(fileMigrationReportList.mountTransactionId());
    errorReport->setAggregatorTransactionId(fileMigrationReportList.aggregatorTransactionId());
    return errorReport;
  }

  // Step 2: update the VMGR, so that the written tape copies are safe from overwrite.
  // Failing here is fatal.
  // We can update the vmgr in one go here, reporting for several files at a time.
  // XXX Limitation: today, any migration error is fatal and terminates the migration
  // session. This means only record the successes in the VMGR.
  // When the tape server will handle failed migrations gracefully (leave a "hole"
  // on the tape and carry on), we will have
  // the record the empty hole left by the failure in the middle of the successes.
  // Today we just bump up the tape's statistics for the successes.
  {
    // 2.1 Build the statistics of the successes. Also log the reception of them.
    u_signed64 totalBytes = 0, totalCompressedBytes = 0, highestFseq = -1;

    for (std::vector<FileMigratedNotificationStruct *>::iterator sm =
           fileMigrationReportList.successfulMigrations().begin();
        sm <  fileMigrationReportList.successfulMigrations().end(); sm++) {
      totalBytes += (*sm)->fileSize();
      totalCompressedBytes += (*sm)->compressedFileSize();
      highestFseq = u_signed64((*sm)->fseq()) > highestFseq? (*sm)->fseq(): highestFseq;
      // Log the file notification while we're at it.
      Cns_fileid fileId;
      fileId.fileid = (*sm)->fileid();
      strncpy(fileId.server, (*sm)->nshost().c_str(),sizeof(fileId.server)-1);
      logMigrationNotified(nullCuuid, fileMigrationReportList.mountTransactionId(),
          fileMigrationReportList.aggregatorTransactionId(), &fileId, requester, **sm);
    }

    // 2.2 Update the VMGR for fseq. Stop here in case of failure.
    castor::tape::utils::Timer timer;
    try {
      VmgrTapeGatewayHelper::bulkUpdateTapeInVmgr(
          fileMigrationReportList.successfulMigrations().size(),
          highestFseq, totalBytes, totalCompressedBytes,
          vid, m_shuttingDown);
      logMigrationBulkVmgrUpdate(nullCuuid, requester, fileMigrationReportList,
        fileMigrationReportList.successfulMigrations().size(),
        highestFseq, totalBytes, totalCompressedBytes,
        tapePool, vid, timer.usecs());
    } catch (castor::exception::Exception& e){
      logMigrationVmgrFailure(nullCuuid, requester, fileMigrationReportList, vid,
        tapePool, e);
      // Attempt to move the tape to read only as a stuck fseq is the worst we can get in
      // terms of data protection: what got written will potentially be overwritten.
      try {
        VmgrTapeGatewayHelper::setTapeAsReadonlyAndUnbusy(vid, m_shuttingDown);
        logTapeReadOnly(nullCuuid, requester, fileMigrationReportList,
            tapePool, vid);
      } catch (castor::exception::Exception e) {
        logCannotReadOnly(nullCuuid, requester, fileMigrationReportList,
            tapePool, vid, e);
      }
      // We could not update the VMGR for this tape: better leave it alone and stop the session
      EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
      errorReport->setErrorCode(EIO);
      errorReport->setErrorMessage("Failed to update VMGR");
      errorReport->setMountTransactionId(fileMigrationReportList.mountTransactionId());
      errorReport->setAggregatorTransactionId(fileMigrationReportList.aggregatorTransactionId());
      return errorReport;
    }
  }

  {
    // 2.3 Scan all errors to find tape full condition for more update of the
    // VMGR (at end session). Right now, the full status is just recorded in the
    // stager DB, to be used at end of tape session.
    // This is tape full conditions currently.
    bool tapeFull = false;
    for (std::vector<FileErrorReportStruct *>::iterator fm =
        fileMigrationReportList.failedMigrations().begin();
        fm < fileMigrationReportList.failedMigrations().end(); fm++){
      fileErrorClassification cl = classifyBridgeMigrationFileError((*fm)->errorCode());
      tapeFull |= cl.tapeIsFull;
    }
    if (tapeFull) {
      try {
        oraSvc.flagTapeFullForMigrationSession(fileMigrationReportList.mountTransactionId());
        oraSvc.commit();
      } catch (castor::exception::Exception e){
        logInternalError (e, requester, fileMigrationReportList);
      }
    }
  }

  // Step 3: update NS and stager DB.
  // Once we passed that point, whatever is on tape is safe. We are free to update
  // the name server, and the record "job done" in stager DB. Both steps are
  // handled by the stager DB as is has a DB link to the name server's.
  try {
    ITapeGatewaySvc::ptr2ref_vector <ITapeGatewaySvc::BulkDbRecordingResult> dbResults;
    oraSvc.setBulkFileMigrationResult(
        fileMigrationReportList.mountTransactionId(),
        fileMigrationReportList.successfulMigrations(),
        fileMigrationReportList.failedMigrations(),
        dbResults);
    // If anything went wrong, we'll mark this session as closing
    bool need_closing = false;
    for (ITapeGatewaySvc::ptr2ref_vector<ITapeGatewaySvc::BulkDbRecordingResult>::iterator r
        = dbResults.begin(); r < dbResults.end(); r++) {
      // Differentiate successes and failures for proper logging.
      if (typeid(*r) == typeid(ITapeGatewaySvc::BulkMigrationDbRecordingResult)) {
        ITapeGatewaySvc::BulkMigrationDbRecordingResult & mr =
            dynamic_cast<ITapeGatewaySvc::BulkMigrationDbRecordingResult &> (*r);
        // The name server update logging is done in the DB. We just log the final step:
        // update of the stager Db for job done.
        // In case of failure, we assume the worst, and end the session.
        Cns_fileid cfi;
        cfi.fileid = mr.migratedFile.fileid();
        strncpy(cfi.server, mr.migratedFile.nshost().c_str(), CA_MAXNAMELEN);
        if (!mr.errorCode) {
          logMigrationDbUpdate(nullCuuid, &cfi, fileMigrationReportList.mountTransactionId(),
              requester,  mr.migratedFile, mr.svcClass, mr.fileClass, tapePool,
              vid, mr.copyNumber);
        } else {
          need_closing = true;
          logMigrationCannotUpdateDb(nullCuuid, &cfi, fileMigrationReportList.mountTransactionId(),
              requester, mr.migratedFile, mr.svcClass, mr.fileClass,tapePool,
              vid, mr.copyNumber, mr.errorCode);
        }
      } else {
        ITapeGatewaySvc::BulkErrorDbRecordingResult & fr =
            dynamic_cast<ITapeGatewaySvc::BulkErrorDbRecordingResult &> (*r);
        Cns_fileid cfi;
        cfi.fileid = fr.failedFile.fileid();
        strncpy(cfi.server, fr.failedFile.nshost().c_str(), CA_MAXNAMELEN);
        fileErrorClassification cl(classifyBridgeMigrationFileError(fr.errorCode));
        if (!fr.errorCode) {
          logFileError(nullCuuid, requester, DLF_LVL_ERROR,
              fileMigrationReportList.mountTransactionId(),
              fr.failedFile, "Migration", cl);
        } else {
          need_closing = true;
          logMigrationErrorCannotUpdateDb(nullCuuid, &cfi, fileMigrationReportList.mountTransactionId(),
              requester, fr.failedFile, fr.svcClass, fr.fileClass, tapePool,
              vid, fr.copyNumber, fr.errorCode);
        }
      }
    }
    if (need_closing) {
      oraSvc.setTapeSessionClosing(fileMigrationReportList.mountTransactionId());
      logSetSessionClosing(nullCuuid, fileMigrationReportList.mountTransactionId(),
              requester, tapePool, vid, "migration");
    }
  } catch (castor::exception::Exception e) {
    logInternalError (e, requester, fileMigrationReportList);
  } catch (std::bad_cast bc) {
    castor::exception::Internal e;
    e.getMessage() << "Got a bad cast exception in handleFileMigrationReportList: "
        << bc.what();
    logInternalError (e, requester, fileMigrationReportList);
  }

  // Log completion of the processing.
  {
    castor::dlf::Param params[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", fileMigrationReportList.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", fileMigrationReportList.aggregatorTransactionId())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIG_REPORT_LIST_PROCESSED, params);
  }
  std::auto_ptr <NotificationAcknowledge> ack (new NotificationAcknowledge());
  ack->setAggregatorTransactionId(fileMigrationReportList.aggregatorTransactionId());
  ack->setMountTransactionId(fileMigrationReportList.mountTransactionId());
  return ack.release();
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleFileRecallReportList(
    castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    requesterInfo& requester ) throw(castor::exception::Exception){
  // Unlike the migrations, which are linked by the sequential writing, the recalls
  // Can be considered as independent entities.
  // We can then push the data to the DB blindly in both success and failure cases.

  // first check we are called with the proper class
  FileRecallReportList * recRep = dynamic_cast <FileRecallReportList *>(&obj);
  if (!recRep) {
    // We did not get the expected class
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_INVALID_CAST, 0, NULL);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }
  FileRecallReportList &fileRecallReportList = *recRep;
  recRep = NULL;

  // Log a summary for the report
  {
    castor::dlf::Param params[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", fileRecallReportList.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", fileRecallReportList.aggregatorTransactionId()),
        castor::dlf::Param("successes", fileRecallReportList.successfulRecalls().size()),
        castor::dlf::Param("failures", fileRecallReportList.failedRecalls().size()),
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_REC_REPORT_LIST_RECEIVED, params);
  }

  // We have 2 lists of both successes and errors.
  // The database side of the application will take care of updating the stager database.
  // As opposed to migrations, the VMGR needs no update here.
  // Just extract a bit of info from the DB (VI

  // Directly update the stager DB
  // results are also logged by the DB procedures.
  try {
    ITapeGatewaySvc::ptr2ref_vector<ITapeGatewaySvc::BulkDbRecordingResult> dbResults;
    oraSvc.setBulkFileRecallResult(
        fileRecallReportList.mountTransactionId(),
        fileRecallReportList.successfulRecalls(),
        fileRecallReportList.failedRecalls(),
        dbResults);
  } catch (castor::exception::Exception e) {
    logInternalError (e, requester, fileRecallReportList);
  } catch (std::bad_cast bc) {
    castor::exception::Internal e;
    e.getMessage() << "Got a bad cast exception in handleFileRecallReportList: "
        << bc.what();
    logInternalError (e, requester, fileRecallReportList);
  }

  // Log completion of the processing.
  {
    castor::dlf::Param params[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", fileMigrationReportList.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", fileMigrationReportList.aggregatorTransactionId())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_REC_REPORT_LIST_PROCESSED, params);
  }
  std::auto_ptr <NotificationAcknowledge> ack (new NotificationAcknowledge());
  ack->setAggregatorTransactionId(fileRecallReportList.aggregatorTransactionId());
  ack->setMountTransactionId(fileRecallReportList.mountTransactionId());
  return ack.release();
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleFilesToMigrateListRequest(
    castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    requesterInfo& requester ) throw(castor::exception::Exception){
  // first check we are called with the proper class
  FilesToMigrateListRequest* req_p = dynamic_cast <FilesToMigrateListRequest *>(&obj);
  if (!req_p) {
    // We did not get the expected class
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_INVALID_CAST, 0, NULL);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }
  FilesToMigrateListRequest &ftmlr = *req_p;
  req_p = NULL;

  // Log the request
  castor::dlf::Param params_outloop[] = {
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",ftmlr.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",ftmlr.aggregatorTransactionId()),
      castor::dlf::Param("maxFiles",ftmlr.maxFiles()),
      castor::dlf::Param("maxBytes",ftmlr.maxBytes())
  };
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, WORKER_MIGRATION_REQUESTED,params_outloop);

  // Setup the auto roll back in case of exception or error.
  ScopedTransaction scpTrans (&oraSvc);

  // Prepare the file list container
  std::queue <FileToMigrateStruct> files_list;
  bool dbFailure = false;
  double dbServingTime;
  castor::tape::utils::Timer timer;

  // Transmit the request to the DB directly
  try {
    oraSvc.getBulkFilesToMigrate(ftmlr.mountTransactionId(),
        ftmlr.maxFiles(), ftmlr.maxBytes(),
        files_list);
  } catch (castor::exception::Exception& e) {
    castor::dlf::Param params[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",ftmlr.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId",ftmlr.aggregatorTransactionId()),
        castor::dlf::Param("maxBytes",ftmlr.maxBytes()),
        castor::dlf::Param("maxFiles",ftmlr.maxFiles()),
        castor::dlf::Param("errorCode",sstrerror(e.code())),
        castor::dlf::Param("errorMessage",e.getMessage().str())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_MIGRATION_RETRIEVING_DB_ERROR,params);
    // The tape server does not need to know about our messing up.
    // We just report no more files to it.
    dbFailure = true;
  }
  dbServingTime = timer.secs();
  // If it was a simple no more work, log it as such
  if (files_list.empty())
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_NO_FILE_TO_MIGRATE, params_outloop);

  // If the list turned up empty, or the DB blew up, reply with a no more files.
  if (dbFailure || files_list.empty()) {
    std::auto_ptr<NoMoreFiles> noMore(new NoMoreFiles);
    noMore->setMountTransactionId(ftmlr.mountTransactionId());
    noMore->setAggregatorTransactionId(ftmlr.aggregatorTransactionId());
    return noMore.release();
  }

  //The list is non-empty, we build the response and gather a few stats
  u_signed64 filesCount = 0, bytesCount = 0;
  std::auto_ptr<FilesToMigrateList> files_response(new FilesToMigrateList);
  files_response->setAggregatorTransactionId(ftmlr.aggregatorTransactionId());
  files_response->setMountTransactionId(ftmlr.mountTransactionId());
  while (!files_list.empty()) {
    std::auto_ptr<FileToMigrateStruct> ftm (new FileToMigrateStruct);
    *ftm = files_list.front();
    filesCount++;
    bytesCount+= ftm->fileSize();
    // Log the per-file information
    castor::dlf::Param paramsComplete[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",ftmlr.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId",ftmlr.aggregatorTransactionId()),
        castor::dlf::Param("fseq",ftm->fseq()),
        castor::dlf::Param("path",ftm->path()),
        castor::dlf::Param("fileSize", ftm->fileSize()),
        castor::dlf::Param("lastKnownFileName", ftm->lastKnownFilename()),
        castor::dlf::Param("lastModificationTime", ftm->lastModificationTime()),
        castor::dlf::Param("fileTransactionId", ftm->fileTransactionId())
      };
    struct Cns_fileid fileId;
    memset(&fileId,'\0',sizeof(fileId));
    strncpy(fileId.server, ftm->nshost().c_str(), sizeof(fileId.server)-1);
    fileId.fileid = ftm->fileid();
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_RETRIEVED, paramsComplete, &fileId);
    // ... and store in the response.
    files_response->filesToMigrate().push_back(ftm.release());
    files_list.pop();
  }

  // Log the summary of the recall request
  {
    castor::dlf::Param params[] = {
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",ftmlr.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId",ftmlr.aggregatorTransactionId()),
        castor::dlf::Param("maxFiles",ftmlr.maxFiles()),
        castor::dlf::Param("maxBytes",ftmlr.maxBytes()),
        castor::dlf::Param("filesCount",filesCount),
        castor::dlf::Param("totalSize",bytesCount),
        castor::dlf::Param("DbProcessingTime", dbServingTime),
        castor::dlf::Param("DbProcessingTimePerFile", dbServingTime/filesCount),
        castor::dlf::Param("ProcessingTime", timer.secs()),
        castor::dlf::Param("ProcessingTimePerFile", timer.secs()/filesCount)
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, WORKER_MIGRATION_LIST_RETRIEVED,params);
  }
  // and reply to requester.
  return files_response.release();
}

/* TODO: adapt to bulk */
castor::IObject*  castor::tape::tapegateway::WorkerThread::handleFilesToRecallListRequest(
    castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    requesterInfo& requester ) throw(castor::exception::Exception){
  // first check we are called with the proper class
  FilesToRecallListRequest * req = dynamic_cast <FilesToRecallListRequest *>(&obj);
  if (!req) {
    // We did not get the expected class
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_INVALID_CAST, 0, NULL);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }
  FilesToRecallListRequest &ftrlr = *req;
  req = NULL;

  // Log the request
  castor::dlf::Param params_outloop[] = {
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",ftrlr.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",ftrlr.aggregatorTransactionId()),
      castor::dlf::Param("maxFiles",ftrlr.maxFiles()),
      castor::dlf::Param("maxBytes",ftrlr.maxBytes())
  };
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, WORKER_RECALL_REQUESTED, params_outloop);

  // Setup the auto roll back in case of exception or error.
  ScopedTransaction scpTrans (&oraSvc);

  // Prepare the file list container
  std::queue <FileToRecallStruct> files_list;
  bool dbFailure = false;
  double dbServingTime;
  castor::tape::utils::Timer timer;

  // Transmit the request to the DB directly
  try {
    oraSvc.getBulkFilesToRecall(ftmlr.mountTransactionId(),
        ftmlr.maxFiles(), ftmlr.maxBytes(),
        files_list);
  } catch (castor::exception::Exception& e) {
    castor::dlf::Param params[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",ftrlr.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId",ftrlr.aggregatorTransactionId()),
        castor::dlf::Param("maxBytes",ftrlr.maxBytes()),
        castor::dlf::Param("maxFiles",ftrlr.maxFiles()),
        castor::dlf::Param("errorCode",sstrerror(e.code())),
        castor::dlf::Param("errorMessage",e.getMessage().str())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_RECALL_RETRIEVING_DB_ERROR, params);
    // The tape server does not need to know about our messing up.
    // We just report no more files to it.
    dbFailure = true;
  }

  dbServingTime = timer.secs();
  // If it was a simple no more work, log it as such
  if (files_list.empty())
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_NO_FILE_TO_RECALL, params_outloop);

  // If the list turned up empty, or the DB blew up, reply with a no more files.
  if (dbFailure || files_list.empty()) {
    std::auto_ptr<NoMoreFiles> noMore(new NoMoreFiles);
    noMore->setMountTransactionId(ftrlr.mountTransactionId());
    noMore->setAggregatorTransactionId(ftrlr.aggregatorTransactionId());
    return noMore.release();
  }

  //The list is non-empty, we build the response and gather a few stats
  u_signed64 filesCount = 0;
  std::auto_ptr<FilesToRecallList> files_response(new FilesToRecallList);
  files_response->setAggregatorTransactionId(ftmlr.aggregatorTransactionId());
  files_response->setMountTransactionId(ftmlr.mountTransactionId());
  while (!files_list.empty()) {
    std::auto_ptr<FileToRecallStruct> ftr (new FileToMigrateStruct);
    *ftr = files_list.front();
    filesCount++;
    // Log the per-file information
    std::stringstream blockid;
     blockid << std::hex << std::uppercase << std::noshowbase
         << std::setw(2) << ftr->blockId0()
         << std::setw(2) << ftr->blockId1()
         << std::setw(2) << ftr->blockId2()
         << std::setw(2) << ftr->blockId3();
         castor::dlf::Param paramsComplete[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",ftrlr.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId",ftrlr.aggregatorTransactionId()),
        castor::dlf::Param("fseq",ftr->fseq()),
        castor::dlf::Param("path",ftr->path()),
        castor::dlf::Param("blockId",blockid.str()),
        castor::dlf::Param("fileTransactionId", ftr->fileTransactionId())
      };
    struct Cns_fileid fileId;
    memset(&fileId,'\0',sizeof(fileId));
    strncpy(fileId.server, ftr->nshost().c_str(), sizeof(fileId.server)-1);
    fileId.fileid = ftr->fileid();
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_RETRIEVED, paramsComplete, &fileId);
    // ... and store in the response.
    files_response->filesToMigrate().push_back(ftr.release());
    files_list.pop();
  }

  // Log the summary of the recall request
  {
    castor::dlf::Param params[] = {
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",ftrlr.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId",ftrlr.aggregatorTransactionId()),
        castor::dlf::Param("maxFiles",ftrlr.maxFiles()),
        castor::dlf::Param("maxBytes",ftrlr.maxBytes()),
        castor::dlf::Param("filesCount",filesCount),
        castor::dlf::Param("DbProcessingTime", dbServingTime),
        castor::dlf::Param("DbProcessingTimePerFile", dbServingTime/filesCount),
        castor::dlf::Param("ProcessingTime", timer.secs()),
        castor::dlf::Param("ProcessingTimePerFile", timer.secs()/filesCount)
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, WORKER_MIGRATION_LIST_RETRIEVED,params);
  }
  // and reply to requester.
  return files_response.release();
}

/* TODO: adapt to bulk (remove?) */
void castor::tape::tapegateway::WorkerThread::failFileMigrationsList(castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    castor::tape::tapegateway::WorkerThread::requesterInfo& requester,
    std::list<castor::tape::tapegateway::FileErrorReport>& filesToFailForRetry)
throw (castor::exception::Exception){
  if (!filesToFailForRetry.empty()) {
    for (std::list<FileErrorReport>::iterator i = filesToFailForRetry.begin();
        i != filesToFailForRetry.end(); i++) {
      try {
        // This SQL does not commit. Commit outside of the loop for efficiency
        oraSvc.failFileTransfer(i->mountTransactionId(), i->fileid(),
                                i->nshost(), i->errorCode());
      } catch (castor::exception::Exception& e){
        castor::dlf::Param params[] ={
            castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
            castor::dlf::Param("Port",requester.port),
            castor::dlf::Param("HostName",requester.hostName),
            castor::dlf::Param("mountTransactionId", i->mountTransactionId()),
            castor::dlf::Param("tapebridgeTransId", i->aggregatorTransactionId()),
            castor::dlf::Param("errorCode",sstrerror(e.code())),
            castor::dlf::Param("errorMessage",e.getMessage().str())
        };
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_FAIL_DB_ERROR, params);
      }
    }
    oraSvc.commit();
  }
}

/* TODO: adapt to bulk (remove?) */
void castor::tape::tapegateway::WorkerThread::failFileRecallsList   (castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    castor::tape::tapegateway::WorkerThread::requesterInfo& requester,
    std::list<castor::tape::tapegateway::FileErrorReport>& filesToFailForRetry)
throw (castor::exception::Exception) {
  failFileMigrationsList (oraSvc, requester, filesToFailForRetry);
}

/* TODO: diagnose */
void castor::tape::tapegateway::WorkerThread::permanentlyFailFileMigrationsList(castor::tape::tapegateway::ITapeGatewaySvc&  /*oraSvc*/,
    castor::tape::tapegateway::WorkerThread::requesterInfo& /*requester*/,
    std::list<castor::tape::tapegateway::FileErrorReport>& /*filesToFailPermanently*/)
throw (castor::exception::Exception) {
  //TODO XXX
  castor::exception::Internal ie;
  ie.getMessage() << "permanentlyFailFileMigrationsList not implemented in 2.1.11 version. Will be in 2.1.12";
}

/* TODO: diagnose */
void castor::tape::tapegateway::WorkerThread::permanentlyFailFileRecallsList   (castor::tape::tapegateway::ITapeGatewaySvc&  /*oraSvc*/,
    castor::tape::tapegateway::WorkerThread::requesterInfo& /*requester*/,
    std::list<castor::tape::tapegateway::FileErrorReport>& /*filesToFailPermanently*/)
throw (castor::exception::Exception) {
  //TODO XXX
  castor::exception::Internal ie;
  ie.getMessage() << "permanentlyFailFileRecallsList not implemented in 2.1.11 version. Will be in 2.1.12";
}

// Helper function for setting a session to closing in the DB.
// This is usually in reaction to a problem, which is logged in the caller.
// Comes with logging of its own problems as internal error (context in a
// string).
// As it's a last ditch action, there is no need to send further exceptions.
void castor::tape::tapegateway::WorkerThread::setSessionToClosing (
    castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    castor::tape::tapegateway::WorkerThread::requesterInfo& requester,
    u_signed64 mountTransactionId)
throw ()
{
  try {
    oraSvc.setTapeSessionClosing(mountTransactionId);
  } catch (castor::exception::Exception e) {
    // If things still go wrong, log an internal error.
    std::stringstream report;
    report <<  "Failed to set the tape session to closing in the DB "
      "in: castor::tape::tapegateway::WorkerThread::setSessionToClosing: "
      "error=" << e.code() << " errorMessage=" << e.getMessage();
    castor::dlf::Param params[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", mountTransactionId),
        castor::dlf::Param("errorCode",SEINTERNAL),
        castor::dlf::Param("errorMessage",report.str())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, INTERNAL_ERROR, params);
  }
}

castor::tape::tapegateway::WorkerThread::fileErrorClassification
castor::tape::tapegateway::WorkerThread::classifyBridgeMigrationFileError(int errorCode) throw () {
  // We rely on the default for the return values:
  //fileInvolved(true),
  //fileRetryable(true),
  //tapeIsFull(false)
  fileErrorClassification ret;
  if (errorCode == ENOSPC) {
    ret.tapeIsFull = true;
    ret.fileInvolved = false;
  }
  return ret;
}

castor::tape::tapegateway::WorkerThread::fileErrorClassification
castor::tape::tapegateway::WorkerThread::classifyBridgeRecallFileError(int /*errorCode*/) throw () {
  // We rely on the default for the return values:
  //fileInvolved(true),
  //fileRetryable(true),
  //tapeIsFull(false)
  fileErrorClassification ret;
  return ret;
}

/* TODO: diagnose all error utilities */
void castor::tape::tapegateway::WorkerThread::logDbError (
    castor::exception::Exception e, requesterInfo& requester,
    FileMigrationReportList & fileMigrationReportList) throw () {
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId", fileMigrationReportList.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId", fileMigrationReportList.aggregatorTransactionId()),
      castor::dlf::Param("errorCode",sstrerror(e.code())),
      castor::dlf::Param("errorMessage",e.getMessage().str())
  };
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_FAIL_DB_ERROR, params);
}

void castor::tape::tapegateway::WorkerThread::logDbError (
    castor::exception::Exception e, requesterInfo& requester,
    FileRecallReportList &fileRecallReportList) throw () {
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId", fileRecallReportList.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId", fileRecallReportList.aggregatorTransactionId()),
      castor::dlf::Param("errorCode",sstrerror(e.code())),
      castor::dlf::Param("errorMessage",e.getMessage().str())
  };
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_FAIL_DB_ERROR, params);
}

void castor::tape::tapegateway::WorkerThread::logMigrationErrorCannotUpdateDb (
    Cuuid_t uuid, struct Cns_fileid* castorFileId,
        u_signed64 mountTransactionId,
        const requesterInfo& requester, const FileErrorReportStruct & fileMigError,
        const std::string & serviceClass, const std::string & fileClass,
        const std::string & tapePool, const std::string & vid,
        int copyNumber, int error) throw () {
  castor::dlf::Param paramsDbUpdate[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",mountTransactionId),
      castor::dlf::Param("serviceClass",serviceClass),
      castor::dlf::Param("fileClass",fileClass),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fseq",fileMigError.fseq()),
      castor::dlf::Param("fileTransactionId",fileMigError.fileTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("copyNb",copyNumber),
      castor::dlf::Param("errorCode",error)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, WORKER_FAIL_DB_ERROR, paramsDbUpdate, castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logInternalError (
    castor::exception::Exception e, requesterInfo& requester,
    FileMigrationReportList & fileMigrationReportList) throw () {
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId", fileMigrationReportList.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId", fileMigrationReportList.aggregatorTransactionId()),
      castor::dlf::Param("errorCode",SEINTERNAL),
      castor::dlf::Param("errorMessage",e.getMessage().str())
  };
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, INTERNAL_ERROR, params);
}

void castor::tape::tapegateway::WorkerThread::logInternalError (
    castor::exception::Exception e, requesterInfo& requester,
    FileRecallReportList &fileRecallReportList) throw () {
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId", fileRecallReportList.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId", fileRecallReportList.aggregatorTransactionId()),
      castor::dlf::Param("errorCode",SEINTERNAL),
      castor::dlf::Param("errorMessage",e.getMessage().str())
  };
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, INTERNAL_ERROR, params);
}

void castor::tape::tapegateway::WorkerThread::logFatalError (Cuuid_t uuid,
    struct Cns_fileid* castorFileId,
    const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
    castor::exception::Exception & e)
{
  castor::dlf::Param paramsError[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("fseq",fileMigrated.fseq()),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("errorCode",sstrerror(e.code())),
      castor::dlf::Param("errorMessage",e.getMessage().str())
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, FATAL_ERROR, paramsError, castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logMigrationNotified (Cuuid_t uuid,
    struct Cns_fileid* castorFileId,
    const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
    const char (&checksumHex)[19], const std::string &blockid)
{
  castor::dlf::Param paramsComplete[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("fseq",fileMigrated.fseq()),
      castor::dlf::Param("checksum name",fileMigrated.checksumName()),
      castor::dlf::Param("checksum",checksumHex),
      castor::dlf::Param("fileSize",fileMigrated.fileSize()),
      castor::dlf::Param("compressedFileSize",fileMigrated.compressedFileSize()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId())
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_NOTIFIED, paramsComplete, castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logMigrationNotified (Cuuid_t uuid,
    u_signed64 mountTransactionId, u_signed64 aggregatorTransactionId,
    struct Cns_fileid* castorFileId,
    const requesterInfo& requester, const FileMigratedNotificationStruct & fileMigrated)
{
  std::stringstream checkSum;
  checkSum << std::hex << std::showbase << std::uppercase << fileMigrated.checksum();
  castor::dlf::Param paramsComplete[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",mountTransactionId),
      castor::dlf::Param("tapebridgeTransId",aggregatorTransactionId),
      castor::dlf::Param("fseq",fileMigrated.fseq()),
      castor::dlf::Param("checksum name",fileMigrated.checksumName()),
      castor::dlf::Param("checksum",checkSum.str()),
      castor::dlf::Param("fileSize",fileMigrated.fileSize()),
      castor::dlf::Param("compressedFileSize",fileMigrated.compressedFileSize()),
      castor::dlf::Param("blockid", utils::tapeBlockIdToString(fileMigrated.blockId0(),
          fileMigrated.blockId1(), fileMigrated.blockId2(),
          fileMigrated.blockId3())),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId())
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_NOTIFIED, paramsComplete, castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logMigrationFileNotfound (Cuuid_t uuid,
    struct Cns_fileid* castorFileId,
    const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
    castor::exception::Exception & e)
{
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("errorCode",sstrerror(e.code())),
      castor::dlf::Param("errorMessage",e.getMessage().str())
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_FILE_NOT_FOUND, params, castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logMigrationGetDbInfo (Cuuid_t uuid,
    struct Cns_fileid* castorFileId,
    const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
    const std::string & serviceClass, const std::string & fileClass,
    const std::string & tapePool, const std::string & blockid,
    const std::string & vid, int copyNumber, u_signed64 lastModificationTime,
    signed64 procTime)
{
  castor::dlf::Param paramsDb[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("serviceClass",serviceClass),
      castor::dlf::Param("fileClass",fileClass),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fseq",fileMigrated.fseq()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("copyNb",copyNumber),
      castor::dlf::Param("lastModificationTime",lastModificationTime),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_DEBUG,WORKER_MIGRATION_GET_DB_INFO, paramsDb, castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logMigrationVmgrUpdate (Cuuid_t uuid, struct Cns_fileid* castorFileId,
    const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
    const std::string & tapePool, const std::string & vid, signed64 procTime)
{
  castor::dlf::Param paramsVmgr[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fseq",fileMigrated.fseq()),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("fileSize",fileMigrated.fileSize()),
      castor::dlf::Param("compressedFileSize",fileMigrated.compressedFileSize()),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_VMGR_UPDATE, paramsVmgr, castorFileId);
}

/* TODO: new */
void castor::tape::tapegateway::WorkerThread::logMigrationBulkVmgrUpdate (Cuuid_t uuid,
    const requesterInfo& requester,  const FileMigrationReportList & fileMigrationReportList,
    int filesCount, u_signed64 highestFseq, u_signed64 totalBytes,
    u_signed64 totalCompressedBytes, const std::string & tapePool,
    const std::string & vid, signed64 procTime)
{
  castor::dlf::Param paramsVmgr[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrationReportList.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrationReportList.aggregatorTransactionId()),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("filesCount",filesCount),
      castor::dlf::Param("highestFseq",highestFseq),
      castor::dlf::Param("filesCount",filesCount),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("totalSize",totalBytes),
      castor::dlf::Param("compressedTotalSize",totalCompressedBytes),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_VMGR_UPDATE, paramsVmgr);
}

void castor::tape::tapegateway::WorkerThread::logMigrationVmgrFailure (Cuuid_t uuid,
    struct Cns_fileid* castorFileId,
    const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
    const std::string & tapePool, castor::exception::Exception & e)
{
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("errorCode",sstrerror(e.code())),
      castor::dlf::Param("errorMessage",e.getMessage().str())
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR,WORKER_MIGRATION_VMGR_FAILURE, params, castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logMigrationVmgrFailure (
    Cuuid_t uuid, const requesterInfo& requester,
    const FileMigrationReportList &fileMigrationReportList,
    const std::string & vid, const std::string & tapePool,
    castor::exception::Exception & e)
{
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrationReportList.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrationReportList.aggregatorTransactionId()),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("errorCode",sstrerror(e.code())),
      castor::dlf::Param("errorMessage",e.getMessage().str())
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR,WORKER_MIGRATION_VMGR_FAILURE, params);
}

void castor::tape::tapegateway::WorkerThread::logMigrationCannotUpdateDb (Cuuid_t uuid, struct Cns_fileid* castorFileId,
    const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
    const std::string & serviceClass, const std::string & fileClass,
    const std::string & tapePool, castor::exception::Exception & e)
{
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("serviceClass",serviceClass),
      castor::dlf::Param("fileClass",fileClass),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("errorCode",sstrerror(e.code())),
      castor::dlf::Param("errorMessage",e.getMessage().str())
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, WORKER_MIGRATION_CANNOT_UPDATE_DB, params,castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logMigrationCannotUpdateDb (
    Cuuid_t uuid, struct Cns_fileid* castorFileId,
    u_signed64 mountTransactionId,
    const requesterInfo& requester, const FileMigratedNotificationStruct & fileMigrated,
    const std::string & serviceClass, const std::string & fileClass,
    const std::string & tapePool, const std::string & vid,
    int copyNumber, int error)
{
  std::stringstream blockid;
  blockid << std::noshowbase << std::hex << std::uppercase
      << std::setw(2) << fileMigrated.blockId0()
      << std::setw(2) << fileMigrated.blockId1()
      << std::setw(2) << fileMigrated.blockId2()
      << std::setw(2) << fileMigrated.blockId3();
  castor::dlf::Param paramsDbUpdate[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",mountTransactionId),
      castor::dlf::Param("serviceClass",serviceClass),
      castor::dlf::Param("fileClass",fileClass),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fseq",fileMigrated.fseq()),
      castor::dlf::Param("blockid", blockid.str()),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("copyNb",copyNumber),
      castor::dlf::Param("errorCode",error)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, WORKER_MIGRATION_CANNOT_UPDATE_DB, paramsDbUpdate, castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logMigrationNsUpdate(Cuuid_t uuid, struct Cns_fileid* castorFileId,
        const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
        const std::string & serviceClass, const std::string & fileClass,
        const std::string & tapePool, const std::string & blockid,
        const std::string & vid, int copyNumber, u_signed64 lastModificationTime,
        const char (&checksumHex)[19], signed64 procTime)
{
  castor::dlf::Param paramsNs[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("serviceClass",serviceClass),
      castor::dlf::Param("fileClass",fileClass),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fseq",fileMigrated.fseq()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("copyNb",copyNumber),
      castor::dlf::Param("lastModificationTime",lastModificationTime),
      castor::dlf::Param("fileSize",fileMigrated.fileSize()),
      castor::dlf::Param("compressedFileSize",fileMigrated.compressedFileSize()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("checksum name",fileMigrated.checksumName()),
      castor::dlf::Param("checksum",checksumHex),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_NS_UPDATE, paramsNs,castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logMigrationNsUpdate(
    Cuuid_t uuid, struct Cns_fileid* castorFileId,
    const requesterInfo& requester,
    const ITapeGatewaySvc::BulkMigrationDbRecordingResult & mr,
    const std::string & tapePool, const std::string & vid)
{
  std::stringstream blockid;
  blockid << std::noshowbase << std::hex << std::uppercase
      << std::setw(2) << mr.migratedFile.blockId0()
      << std::setw(2) << mr.migratedFile.blockId1()
      << std::setw(2) << mr.migratedFile.blockId2()
      << std::setw(2) << mr.migratedFile.blockId3();
  std::stringstream checksumHex;
  checksumHex << "0x" << std::hex << mr.migratedFile.checksum();
  castor::dlf::Param paramsNs[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",mr.mountTransactionId),
      castor::dlf::Param("serviceClass",mr.svcClass),
      castor::dlf::Param("fileClass",mr.fileClass),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fseq",mr.migratedFile.fseq()),
      castor::dlf::Param("blockid", blockid.str()),
      castor::dlf::Param("fileTransactionId",mr.migratedFile.fileTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("copyNb",mr.copyNumber),
      castor::dlf::Param("lastModificationTime",mr.lastModificationTime),
      castor::dlf::Param("fileSize",mr.migratedFile.fileSize()),
      castor::dlf::Param("compressedFileSize",mr.migratedFile.compressedFileSize()),
      castor::dlf::Param("checksum name",mr.migratedFile.checksumName()),
      castor::dlf::Param("checksum",checksumHex.str())
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_NS_UPDATE, paramsNs,castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logRepackNsUpdate (Cuuid_t uuid, struct Cns_fileid* castorFileId,
        const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
        const std::string & serviceClass, const std::string & fileClass,
        const std::string & tapePool, const std::string & blockid,
        const std::string & vid, int copyNumber, u_signed64 lastModificationTime,
        const char (&checksumHex)[19],const std::string & originalVid, signed64 procTime)
{
  castor::dlf::Param paramsRepack[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("serviceClass",serviceClass),
      castor::dlf::Param("fileClass",fileClass),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fseq",fileMigrated.fseq()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("copyNb",copyNumber),
      castor::dlf::Param("lastModificationTime",lastModificationTime),
      castor::dlf::Param("fileSize",fileMigrated.fileSize()),
      castor::dlf::Param("compressedFileSize",fileMigrated.compressedFileSize()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("checksum name",fileMigrated.checksumName()),
      castor::dlf::Param("checksum",checksumHex),
      castor::dlf::Param("original vid",originalVid),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, WORKER_REPACK_NS_UPDATE, paramsRepack,castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logRepackFileRemoved (Cuuid_t uuid, struct Cns_fileid* castorFileId,
        const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
        const std::string & serviceClass, const std::string & fileClass,
        const std::string & tapePool, const std::string & blockid,
        const std::string & vid, int copyNumber, u_signed64 lastModificationTime,
        const char (&checksumHex)[19], const std::string & originalVid, signed64 procTime)
{
  castor::dlf::Param paramsStaleFile[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("serviceClass",serviceClass),
      castor::dlf::Param("fileClass",fileClass),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fseq",fileMigrated.fseq()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("copyNb",copyNumber),
      castor::dlf::Param("lastModificationTime",lastModificationTime),
      castor::dlf::Param("fileSize",fileMigrated.fileSize()),
      castor::dlf::Param("compressedFileSize",fileMigrated.compressedFileSize()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("checksum name",fileMigrated.checksumName()),
      castor::dlf::Param("checksum",checksumHex),
      castor::dlf::Param("original vid",originalVid),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, WORKER_REPACK_FILE_REMOVED, paramsStaleFile,castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logRepackStaleFile (Cuuid_t uuid, struct Cns_fileid* castorFileId,
        const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
        const std::string & serviceClass, const std::string & fileClass,
        const std::string & tapePool, const std::string & blockid,
        const std::string & vid, int copyNumber, u_signed64 lastModificationTime,
        const char (&checksumHex)[19],const std::string & originalVid, signed64 procTime)
{
  castor::dlf::Param paramsStaleFile[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("serviceClass",serviceClass),
      castor::dlf::Param("fileClass",fileClass),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fseq",fileMigrated.fseq()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("copyNb",copyNumber),
      castor::dlf::Param("lastModificationTime",lastModificationTime),
      castor::dlf::Param("fileSize",fileMigrated.fileSize()),
      castor::dlf::Param("compressedFileSize",fileMigrated.compressedFileSize()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("checksum name",fileMigrated.checksumName()),
      castor::dlf::Param("checksum",checksumHex),
      castor::dlf::Param("original vid",originalVid),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, WORKER_REPACK_STALE_FILE, paramsStaleFile,castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logSuperfluousSegment (Cuuid_t uuid, struct Cns_fileid* castorFileId,
        const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
        const std::string & serviceClass, const std::string & fileClass,
        const std::string & tapePool, const std::string & blockid,
        const std::string & vid, int copyNumber, u_signed64 lastModificationTime,
        const char (&checksumHex)[19],const std::string & originalVid, signed64 procTime)
{
  castor::dlf::Param paramsStaleFile[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("serviceClass",serviceClass),
      castor::dlf::Param("fileClass",fileClass),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fseq",fileMigrated.fseq()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("copyNb",copyNumber),
      castor::dlf::Param("lastModificationTime",lastModificationTime),
      castor::dlf::Param("fileSize",fileMigrated.fileSize()),
      castor::dlf::Param("compressedFileSize",fileMigrated.compressedFileSize()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("checksum name",fileMigrated.checksumName()),
      castor::dlf::Param("checksum",checksumHex),
      castor::dlf::Param("original vid",originalVid),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, WORKER_SUPERFLUOUS_SEGMENT, paramsStaleFile,castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logRepackUncomfirmedStaleFile (Cuuid_t uuid, struct Cns_fileid* castorFileId,
    const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
    const std::string & serviceClass, const std::string & fileClass,
    const std::string & tapePool, const std::string & blockid,
    const std::string & vid, int copyNumber, u_signed64 lastModificationTime,
    const char (&checksumHex)[19],const std::string & originalVid, signed64 procTime)
{
  castor::dlf::Param paramsStaleFile[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("serviceClass",serviceClass),
      castor::dlf::Param("fileClass",fileClass),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fseq",fileMigrated.fseq()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("copyNb",copyNumber),
      castor::dlf::Param("lastModificationTime",lastModificationTime),
      castor::dlf::Param("fileSize",fileMigrated.fileSize()),
      castor::dlf::Param("compressedFileSize",fileMigrated.compressedFileSize()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("checksum name",fileMigrated.checksumName()),
      castor::dlf::Param("checksum",checksumHex),
      castor::dlf::Param("original vid",originalVid),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_WARNING, WORKER_REPACK_UNCONFIRMED_STALE_FILE, paramsStaleFile,castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logMigrationNsFailure (Cuuid_t uuid, struct Cns_fileid* castorFileId,
        const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
        const std::string & serviceClass, const std::string & fileClass,
        const std::string & tapePool, const std::string & blockid,
        const std::string & vid, int copyNumber, u_signed64 lastModificationTime,
        const char (&checksumHex)[19],const std::string & originalVid, signed64 procTime,
        castor::exception::Exception & e)
{
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("serviceClass",serviceClass),
      castor::dlf::Param("fileClass",fileClass),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fseq",fileMigrated.fseq()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("copyNb",copyNumber),
      castor::dlf::Param("lastModificationTime",lastModificationTime),
      castor::dlf::Param("fileSize",fileMigrated.fileSize()),
      castor::dlf::Param("compressedFileSize",fileMigrated.compressedFileSize()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("checksum name",fileMigrated.checksumName()),
      castor::dlf::Param("checksum",checksumHex),
      castor::dlf::Param("repack vid",originalVid),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001),
      castor::dlf::Param("errorCode",sstrerror(e.code())),
      castor::dlf::Param("errorMessage",e.getMessage().str())
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_WARNING, WORKER_MIGRATION_NS_FAILURE, params,castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logMigrationDbUpdate (Cuuid_t uuid, struct Cns_fileid* castorFileId,
        const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
        const std::string & serviceClass, const std::string & fileClass,
        const std::string & tapePool, const std::string & blockid,
        const std::string & vid, int copyNumber, signed64 procTime)
{
  castor::dlf::Param paramsDbUpdate[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("serviceClass",serviceClass),
      castor::dlf::Param("fileClass",fileClass),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fseq",fileMigrated.fseq()),
      castor::dlf::Param("blockid", blockid),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("copyNb",copyNumber),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_DB_UPDATE, paramsDbUpdate,castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logMigrationDbUpdate (
    Cuuid_t uuid, struct Cns_fileid* castorFileId,
    u_signed64 mountTransactionId,
    const requesterInfo& requester, const FileMigratedNotificationStruct & fileMigrated,
    const std::string & serviceClass, const std::string & fileClass,
    const std::string & tapePool, const std::string & vid,
    int copyNumber)
{
  std::stringstream blockid;
  blockid << std::noshowbase << std::hex << std::uppercase
      << std::setw(2) << fileMigrated.blockId0()
      << std::setw(2) << fileMigrated.blockId1()
      << std::setw(2) << fileMigrated.blockId2()
      << std::setw(2) << fileMigrated.blockId3();
  castor::dlf::Param paramsDbUpdate[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",mountTransactionId),
      castor::dlf::Param("serviceClass",serviceClass),
      castor::dlf::Param("fileClass",fileClass),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("fseq",fileMigrated.fseq()),
      castor::dlf::Param("blockid", blockid.str()),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("copyNb",copyNumber)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_DB_UPDATE, paramsDbUpdate,castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logMigrationCannotFindVid (Cuuid_t uuid, struct Cns_fileid* castorFileId,
    const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
    castor::exception::Exception & e)
{
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("errorCode",sstrerror(e.code())),
      castor::dlf::Param("errorMessage",e.getMessage().str())
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, WORKER_MIG_CANNOT_FIND_VID, params,castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logMigrationCannotFindVid (Cuuid_t uuid,
    const requesterInfo& requester, const FileMigrationReportList & migrationReport,
    castor::exception::Exception & e)
{
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",migrationReport.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",migrationReport.aggregatorTransactionId()),
      castor::dlf::Param("errorCode",sstrerror(e.code())),
      castor::dlf::Param("errorMessage",e.getMessage().str())
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, WORKER_MIG_CANNOT_FIND_VID, params);
}

void castor::tape::tapegateway::WorkerThread::logTapeReadOnly (Cuuid_t uuid, struct Cns_fileid* castorFileId,
      const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
      const std::string& vid)
{
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("TPVID",vid)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, WORKER_MIG_TAPE_RDONLY, params,castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logTapeReadOnly (
    Cuuid_t uuid, const requesterInfo& requester,
    const FileMigrationReportList & fileMigrationReportList,
    const std::string & tapePool, const std::string & vid)
{
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrationReportList.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrationReportList.aggregatorTransactionId()),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("TPVID",vid)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, WORKER_MIG_TAPE_RDONLY, params);
}


void castor::tape::tapegateway::WorkerThread::logCannotReadOnly (Cuuid_t uuid, struct Cns_fileid* castorFileId,
    const requesterInfo& requester, const FileMigratedNotification & fileMigrated,
    const std::string & vid, castor::exception::Exception & e)
{
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
      castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("errorCode",sstrerror(e.code())),
      castor::dlf::Param("errorMessage",e.getMessage().str())
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, WORKER_MIG_CANNOT_RDONLY, params,castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logCannotReadOnly (
    Cuuid_t uuid, const requesterInfo& requester,
    const FileMigrationReportList & fileMigrationReportList,
    const std::string & tapePool, const std::string & vid,
    castor::exception::Exception & e)
{
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("mountTransactionId",fileMigrationReportList.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileMigrationReportList.aggregatorTransactionId()),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("tapePool",tapePool),
      castor::dlf::Param("errorCode",sstrerror(e.code())),
      castor::dlf::Param("errorMessage",e.getMessage().str())
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, WORKER_MIG_CANNOT_RDONLY, params);
}

void castor::tape::tapegateway::WorkerThread::logFileError (Cuuid_t uuid,
    const requesterInfo& requester, int logLevel,
    const FileErrorReport & fileError,
    const std::string & migRecContext,
    const fileErrorClassification & classification)
{
  struct Cns_fileid castorFileId;
  std::strncpy (castorFileId.server, fileError.nshost().c_str(),CA_MAXHOSTNAMELEN);
  castorFileId.server[CA_MAXHOSTNAMELEN] = '\0';
  castorFileId.fileid =  fileError.fileid();
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("Context",migRecContext),
      castor::dlf::Param("NSHOSTNAME",fileError.nshost()),
      castor::dlf::Param("NSFILEID",fileError.fileid()),
      castor::dlf::Param("mountTransactionId",fileError.mountTransactionId()),
      castor::dlf::Param("tapebridgeTransId",fileError.aggregatorTransactionId()),
      castor::dlf::Param("fileTransactionId",fileError.fileTransactionId()),
      castor::dlf::Param("errorCode",sstrerror(fileError.errorCode())),
      castor::dlf::Param("errorMessage",fileError.errorMessage()),
      castor::dlf::Param("fileInvolved",classification.fileInvolved?"Involved":"Not involved"),
      castor::dlf::Param("errorRetryable",classification.fileRetryable?"Retryable":"Not retryable"),
      castor::dlf::Param("fseq",fileError.fseq()),
      castor::dlf::Param("positionCommandCode",fileError.positionCommandCode())
  };
  castor::dlf::dlf_writep(uuid, logLevel, WORKER_FILE_ERROR_REPORT, params, &castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logFileError (Cuuid_t uuid,
    const requesterInfo& requester, int logLevel,
    u_signed64 mountTransactionId,
    const FileErrorReportStruct & fileError,
    const std::string & migRecContext,
    const fileErrorClassification & classification)
{
  struct Cns_fileid castorFileId;
  std::strncpy (castorFileId.server, fileError.nshost().c_str(),CA_MAXHOSTNAMELEN);
  castorFileId.server[CA_MAXHOSTNAMELEN] = '\0';
  castorFileId.fileid = fileError.fileid();
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("Context",migRecContext),
      castor::dlf::Param("NSHOSTNAME",fileError.nshost()),
      castor::dlf::Param("NSFILEID",fileError.fileid()),
      castor::dlf::Param("mountTransactionId",mountTransactionId),
      castor::dlf::Param("fileTransactionId",fileError.fileTransactionId()),
      castor::dlf::Param("errorCode",sstrerror(fileError.errorCode())),
      castor::dlf::Param("errorMessage",fileError.errorMessage()),
      castor::dlf::Param("fileInvolved",classification.fileInvolved?"Involved":"Not involved"),
      castor::dlf::Param("errorRetryable",classification.fileRetryable?"Retryable":"Not retryable"),
      castor::dlf::Param("fseq",fileError.fseq()),
      castor::dlf::Param("positionCommandCode",fileError.positionCommandCode())
  };
  castor::dlf::dlf_writep(uuid, logLevel, WORKER_FILE_ERROR_REPORT, params, &castorFileId);
}

void castor::tape::tapegateway::WorkerThread::logSetSessionClosing(Cuuid_t uuid,
    u_signed64 mountTransactionId,
    const requesterInfo& requester,
    std::string tapePool,
    std::string vid, const std::string & migRecContext)
{
  castor::dlf::Param params[] ={
      castor::dlf::Param("IP",castor::dlf::IPAddress(requester.ip)),
      castor::dlf::Param("Port",requester.port),
      castor::dlf::Param("HostName",requester.hostName),
      castor::dlf::Param("Context",migRecContext),
      castor::dlf::Param("mountTransactionId",mountTransactionId),
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("tapePool",tapePool)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, WORKER_SET_SESSION_CLOSING, params);
}
