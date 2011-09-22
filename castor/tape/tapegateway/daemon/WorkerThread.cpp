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
#include <stdlib.h>
#include <unistd.h>
#include <u64subr.h>
#include <memory>
#include <typeinfo>
#include <algorithm>
#include <queue>

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

#include "castor/tape/tapegateway/ScopedTransaction.hpp"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::WorkerThread::WorkerThread():BaseObject(){}

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
        case OBJ_EndNotificationFileErrorReport:
          handler_response = handleFileFailWorker(*obj,*oraSvc,requester);
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
    requesterInfo& requester  ) throw(){

  // Auto-rollback mechanism to cover all execution path (commented out as it is
  // not needed here.
  // ScopedTransaction scpTranns(&oraSvc);

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

    timeval tvStart,tvEnd;
    gettimeofday(&tvStart, NULL);
    
    try {
    
      // GET FROM DB
      // The wrapper function has no hidden effect
      // The SQL commits in all cases.
      oraSvc.startTapeSession(startRequest,*response); 
    
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

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_NO_VOLUME, params);
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
   
    gettimeofday(&tvEnd, NULL);
    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

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
        castor::dlf::Param("ProcessingTime", procTime * 0.000001)
      };
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_VOLUME_FOUND, params0);

  } catch( std::bad_cast &ex) {

    if (response) delete response;
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_INVALID_CAST, 0, NULL);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }

  return response;

}

castor::IObject* castor::tape::tapegateway::WorkerThread::handleRecallUpdate(
    castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc,
    requesterInfo& requester) throw(){

  // Initialize auto-rollback in case of error
  ScopedTransaction scpTrans(&oraSvc);

  // I received a FileRecalledResponse
  NotificationAcknowledge* response = new NotificationAcknowledge();
  try {

    FileRecalledNotification &fileRecalled = dynamic_cast<FileRecalledNotification&>(obj);

    struct Cns_fileid castorFileId;
    memset(&castorFileId,'\0',sizeof(castorFileId));
    strncpy(
	    castorFileId.server,
	    fileRecalled.nshost().c_str(),
	    sizeof(castorFileId.server)-1
	    );
    castorFileId.fileid = fileRecalled.fileid();
    
    char checksumHex[19];
    checksumHex[0] = '0';
    checksumHex[1] = 'x';
    try {
      utils::toHex((uint64_t)fileRecalled.checksum(), &checksumHex[2], 17);
    } catch (castor::exception::Exception& e) {

      castor::dlf::Param paramsError[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId",fileRecalled.aggregatorTransactionId()),
          castor::dlf::Param("fseq",fileRecalled.fseq()),
          castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
          castor::dlf::Param("errorCode",sstrerror(e.code())),
          castor::dlf::Param("errorMessage",e.getMessage().str())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, paramsError, &castorFileId);
      EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
      errorReport->setErrorCode(EINVAL);
      errorReport->setErrorMessage("invalid object");
      errorReport->setMountTransactionId(fileRecalled.mountTransactionId());
      errorReport->setAggregatorTransactionId(fileRecalled.aggregatorTransactionId()); 
      if (response) delete response;
      return errorReport;
    }
    castor::dlf::Param paramsComplete[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId",fileRecalled.aggregatorTransactionId()),
        castor::dlf::Param("fseq",fileRecalled.fseq()),
        castor::dlf::Param("checksum name",fileRecalled.checksumName().c_str()),
        castor::dlf::Param("checksum", checksumHex),
        castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_NOTIFIED, paramsComplete, &castorFileId);
  
    response->setMountTransactionId(fileRecalled.mountTransactionId());
    response->setAggregatorTransactionId(fileRecalled.aggregatorTransactionId());
  
    // GET FILE FROM DB

    NsTapeGatewayHelper nsHelper;
    std::string vid;
    int copyNb;

    timeval tvStart,tvEnd;
    gettimeofday(&tvStart, NULL);

    try {
      // No lock, no update. SQL is read-only.
      oraSvc.getSegmentInfo(fileRecalled,vid,copyNb);
    } catch (castor::exception::Exception& e){
      castor::dlf::Param params[] = {
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId",fileRecalled.aggregatorTransactionId()),
          castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
          castor::dlf::Param("errorCode",sstrerror(e.code())),
          castor::dlf::Param("errorMessage",e.getMessage().str())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_RECALL_FILE_NOT_FOUND, params, &castorFileId);
      return response;
    
    }

    gettimeofday(&tvEnd, NULL);
    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
    
    castor::dlf::Param paramsDb[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId",fileRecalled.aggregatorTransactionId()),
        castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
        castor::dlf::Param("fseq",fileRecalled.fseq()),
        castor::dlf::Param("ProcessingTime", procTime * 0.000001),
        castor::dlf::Param("TPVID",vid),
        castor::dlf::Param("copyNumber",copyNb)
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,WORKER_RECALL_GET_DB_INFO, paramsDb, &castorFileId);

    // CHECK NAMESERVER

    try{

      gettimeofday(&tvStart, NULL);
      nsHelper.checkRecalledFile(fileRecalled, vid, copyNb); 
      gettimeofday(&tvEnd, NULL);

      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
      castor::dlf::Param paramsNs[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId",fileRecalled.aggregatorTransactionId()),
          castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
          castor::dlf::Param("fseq",fileRecalled.fseq()),
          castor::dlf::Param("ProcessingTime", procTime * 0.000001),
          castor::dlf::Param("TPVID",vid),
          castor::dlf::Param("copyNumber",copyNb),
          castor::dlf::Param("checksum name",fileRecalled.checksumName().c_str()),
          castor::dlf::Param("checksum", checksumHex),
      };

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, WORKER_RECALL_NS_CHECK, paramsNs, &castorFileId);
      
      
    } catch (castor::exception::Exception& e){
      
      // nameserver error
      
      castor::dlf::Param params[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId",fileRecalled.aggregatorTransactionId()),
          castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
	      castor::dlf::Param("errorCode",sstrerror(e.code())),
    	  castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_RECALL_NS_FAILURE, params, &castorFileId);

      try {
	FileErrorReport failure;
	failure.setMountTransactionId(fileRecalled.mountTransactionId());
	failure.setAggregatorTransactionId(fileRecalled.aggregatorTransactionId());
	failure.setFileid(fileRecalled.fileid());
	failure.setNshost(fileRecalled.nshost());
	failure.setFseq(fileRecalled.fseq());
	failure.setErrorCode(e.code());
	failure.setErrorMessage(e.getMessage().str());
	// Updates, does not commit. Fine SQL. >-)
	oraSvc.failFileTransfer(failure);
      } catch (castor::exception::Exception& e) {
	// db failure to mark the recall as failed
	castor::dlf::Param params[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
	    castor::dlf::Param("tapebridgeTransId",fileRecalled.aggregatorTransactionId()),
	    castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
	    castor::dlf::Param("errorCode",sstrerror(e.code())),
	    castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_RECALL_CANNOT_UPDATE_DB, params, &castorFileId);
      }
      // The failFileTransfer did not commit. Doing it now.
      scpTrans.commit();
      return response;
    }
      
    try {
      // CHECK FILE SIZE

      gettimeofday(&tvStart, NULL);
      NsTapeGatewayHelper nsHelper;
      nsHelper.checkFileSize(fileRecalled);
      gettimeofday(&tvEnd, NULL);
      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

      castor::dlf::Param paramsNs[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId",fileRecalled.aggregatorTransactionId()),
          castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
          castor::dlf::Param("file size",fileRecalled.fileSize()),
          castor::dlf::Param("fseq",fileRecalled.fseq()),
          castor::dlf::Param("ProcessingTime", procTime * 0.000001),
          castor::dlf::Param("TPVID",vid)
	    };

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, WORKER_RECALL_CHECK_FILE_SIZE, paramsNs, &castorFileId);
      
    } catch (castor::exception::Exception& e) {
      
      castor::dlf::Param params[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId",fileRecalled.aggregatorTransactionId()),
          castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
	      castor::dlf::Param("errorCode",sstrerror(e.code())),
	      castor::dlf::Param("errorMessage",e.getMessage().str())
	    };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_WRONG_FILE_SIZE, params, &castorFileId );
      try {
	FileErrorReport failure;
	failure.setMountTransactionId(fileRecalled.mountTransactionId());
	failure.setAggregatorTransactionId(fileRecalled.aggregatorTransactionId());
	failure.setFileid(fileRecalled.fileid());
	failure.setNshost(fileRecalled.nshost());
	failure.setFseq(fileRecalled.fseq());
	failure.setErrorCode(e.code());
	failure.setErrorMessage(e.getMessage().str());
	// Updates, does not commit. Fine SQL.
	oraSvc.failFileTransfer(failure);
      } catch (castor::exception::Exception& e) {
	// db failure to mark the recall as failed
	castor::dlf::Param params[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
	    castor::dlf::Param("tapebridgeTransId",fileRecalled.aggregatorTransactionId()),
	    castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
	    castor::dlf::Param("errorCode",sstrerror(e.code())),
	    castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_RECALL_CANNOT_UPDATE_DB, params, &castorFileId);
	  
      }
      // The failFileTransfer did not commit. Doing it now.
      scpTrans.commit();
      return response;
    }

    gettimeofday(&tvStart, NULL);

    try {
      // UPDATE DB
      // Here the SQL does the commit in the end. Rollback with be without effet
      // No need for a release.
      oraSvc.setFileRecalled(fileRecalled);
      
      gettimeofday(&tvEnd, NULL);
      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
    
      castor::dlf::Param paramsDbUpdate[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId",fileRecalled.aggregatorTransactionId()),
          castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
          castor::dlf::Param("fseq",fileRecalled.fseq()),
          castor::dlf::Param("ProcessingTime", procTime * 0.000001),
          castor::dlf::Param("TPVID",vid),
          castor::dlf::Param("copyNumber",copyNb),
          castor::dlf::Param("ProcessingTime", procTime * 0.000001)
      };

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_COMPLETED_UPDATE_DB, paramsDbUpdate, &castorFileId);

	
    } catch (castor::exception::Exception& e) {
      // db failure to mark the recall as failed
      castor::dlf::Param params[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId",fileRecalled.aggregatorTransactionId()),
          castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
          castor::dlf::Param("errorCode",sstrerror(e.code())),
          castor::dlf::Param("errorMessage",e.getMessage().str())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_CANNOT_UPDATE_DB, params, &castorFileId );
	
      FileErrorReport failure;
      failure.setMountTransactionId(fileRecalled.mountTransactionId());
      failure.setAggregatorTransactionId(fileRecalled.aggregatorTransactionId());
      failure.setFileid(fileRecalled.fileid());
      failure.setNshost(fileRecalled.nshost());
      failure.setFseq(fileRecalled.fseq());
      failure.setErrorCode(e.code());
      failure.setErrorMessage(e.getMessage().str());
      try {
        // The SQL fails to commit and is the last possible call in the function.
        oraSvc.failFileTransfer(failure);
        // Commit
        scpTrans.commit();
      } catch (castor::exception::Exception& e) {
	// db failure to mark the recall as failed
	castor::dlf::Param params[] ={
	    castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
	    castor::dlf::Param("Port",requester.port),
	    castor::dlf::Param("HostName",requester.hostName),
	    castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
	    castor::dlf::Param("tapebridgeTransId",fileRecalled.aggregatorTransactionId()),
	    castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
	    castor::dlf::Param("errorCode",sstrerror(e.code())),
	    castor::dlf::Param("errorMessage",e.getMessage().str())
	};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_RECALL_CANNOT_UPDATE_DB, params, &castorFileId);
	
      }
      
    }
 
  } catch  (std::bad_cast &ex) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_INVALID_CAST, 0, NULL);
    
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
    
  } 
  
 
  return response;  
    
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleMigrationUpdate(
    castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc,
    requesterInfo& requester ) throw(){
  // Setup the scoped rollback mechanism for proper exceptions handling
  ScopedTransaction scpTrans(&oraSvc);

  // Helper objects
  VmgrTapeGatewayHelper vmgrHelper;
  NsTapeGatewayHelper nsHelper;

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

  // EXTRACT Castor file ID
  struct Cns_fileid castorFileId;
  memset(&castorFileId,'\0',sizeof(castorFileId));
  strncpy(
      castorFileId.server,
      fileMigrated.nshost().c_str(),
      sizeof(castorFileId.server)-1
  );
  castorFileId.fileid = fileMigrated.fileid();
  const std::string blockid = utils::tapeBlockIdToString(fileMigrated.blockId0(),
      fileMigrated.blockId1() ,fileMigrated.blockId2() ,fileMigrated.blockId3());
  char checksumHex[19];
  checksumHex[0] = '0';
  checksumHex[1] = 'x';
  try {
    utils::toHex((uint64_t)fileMigrated.checksum(), &checksumHex[2], 17);
  } catch (castor::exception::Exception& e) {
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
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, paramsError, &castorFileId);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    errorReport->setMountTransactionId(fileMigrated.mountTransactionId());
    errorReport->setAggregatorTransactionId(fileMigrated.aggregatorTransactionId());
    return errorReport;
  }
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
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_NOTIFIED, paramsComplete, &castorFileId);

  // CHECK DB
  timeval tvStart,tvEnd;
  gettimeofday(&tvStart, NULL);
  std::string repackVid;
  int copyNumber;
  std::string vid;
  std::string serviceClass;
  std::string fileClass;
  std::string tapePool;
  u_signed64 lastModificationTime;
  try {
    // This SQL procedure does not do any lock/updates.
    oraSvc.getRepackVidAndFileInfo(fileMigrated,vid,copyNumber,lastModificationTime,repackVid,
        serviceClass, fileClass, tapePool);
  } catch (castor::exception::Exception& e){
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
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_FILE_NOT_FOUND, params, &castorFileId);
    // The migrated file can not be found in the system: this is non-fatal: it could have been deleted by the user
    NotificationAcknowledge* response= new NotificationAcknowledge;
    response->setMountTransactionId(fileMigrated.mountTransactionId());
    response->setAggregatorTransactionId(fileMigrated.aggregatorTransactionId());
    return response;
  }
  gettimeofday(&tvEnd, NULL);
  signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
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
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,WORKER_MIGRATION_GET_DB_INFO, paramsDb, &castorFileId);

  // UPDATE VMGR
  try {
    gettimeofday(&tvStart, NULL);
    vmgrHelper.updateTapeInVmgr(fileMigrated, vid, m_shuttingDown);
    gettimeofday(&tvEnd, NULL);
    procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
    castor::dlf::Param paramsVmgr[] ={
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
        castor::dlf::Param("ProcessingTime", procTime * 0.000001)
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_VMGR_UPDATE, paramsVmgr, &castorFileId);
  } catch (castor::exception::Exception& e){
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
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,WORKER_MIGRATION_VMGR_FAILURE, params, &castorFileId);
    try {
      // File the error for the file (although this is more likely related to the tape).
      FileErrorReport failure;
      failure.setMountTransactionId(fileMigrated.mountTransactionId());
      failure.setAggregatorTransactionId(fileMigrated.aggregatorTransactionId());
      failure.setFileid(fileMigrated.fileid());
      failure.setNshost(fileMigrated.nshost());
      failure.setFseq(fileMigrated.fseq());
      failure.setErrorCode(e.code());
      failure.setErrorMessage(e.getMessage().str());
      // The underlying SQL does NOT commit
      oraSvc.failFileTransfer(failure);
    } catch (castor::exception::Exception& e) {
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
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_MIGRATION_CANNOT_UPDATE_DB, params,&castorFileId);
    }
    // As the previous call did modify things, better committing before returning.
    scpTrans.commit();
    // We could not update the VMGR for this tape: better leave it alone and stop the session
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EIO);
    errorReport->setErrorMessage("Failed to update VMGR");
    errorReport->setMountTransactionId(fileMigrated.mountTransactionId());
    errorReport->setAggregatorTransactionId(fileMigrated.aggregatorTransactionId());
    return errorReport;
  }

  //UPDATE NAMESERVER
  // We'll flag this if the migration turned out to be of an old version of
  // an overwritten file.
  bool fileStale = false;
  try {
    if (repackVid.empty()) {
      gettimeofday(&tvStart, NULL);
      // update the name server (standard migration)
      nsHelper.updateMigratedFile( fileMigrated, copyNumber, vid, lastModificationTime);
      gettimeofday(&tvEnd, NULL);
      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
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
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_NS_UPDATE, paramsNs,&castorFileId);
    } else {
      // update the name server (repacked file)
      gettimeofday(&tvStart, NULL);
      nsHelper.updateRepackedFile( fileMigrated, repackVid, copyNumber, vid, lastModificationTime);
      gettimeofday(&tvEnd, NULL);
      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
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
          castor::dlf::Param("repack vid",repackVid),
          castor::dlf::Param("ProcessingTime", procTime * 0.000001)
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_REPACK_NS_UPDATE, paramsRepack,&castorFileId);
    }
  } catch (castor::tape::tapegateway::NsTapeGatewayHelper::NoSuchFileException) {
    fileStale = true;
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
        castor::dlf::Param("repack vid",repackVid),
        castor::dlf::Param("ProcessingTime", procTime * 0.000001)
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_REPACK_FILE_REMOVED, paramsStaleFile,&castorFileId);
  } catch (castor::tape::tapegateway::NsTapeGatewayHelper::FileMutatedException) {
    fileStale = true;
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
        castor::dlf::Param("repack vid",repackVid),
        castor::dlf::Param("ProcessingTime", procTime * 0.000001)
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_REPACK_STALE_FILE, paramsStaleFile,&castorFileId);
  } catch (castor::tape::tapegateway::NsTapeGatewayHelper::FileMutationUnconfirmedException) {
    fileStale = true;
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
        castor::dlf::Param("repack vid",repackVid),
        castor::dlf::Param("ProcessingTime", procTime * 0.000001)
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, WORKER_REPACK_UNCONFIRMED_STALE_FILE, paramsStaleFile,&castorFileId);
  } catch (castor::exception::Exception& e) {
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
        castor::dlf::Param("repack vid",repackVid),
        castor::dlf::Param("ProcessingTime", procTime * 0.000001),
        castor::dlf::Param("errorCode",sstrerror(e.code())),
        castor::dlf::Param("errorMessage",e.getMessage().str())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_MIGRATION_NS_FAILURE, params,&castorFileId);
    try {
      FileErrorReport failure;
      failure.setMountTransactionId(fileMigrated.mountTransactionId());
      failure.setAggregatorTransactionId(fileMigrated.aggregatorTransactionId());
      failure.setFileid(fileMigrated.fileid());
      failure.setNshost(fileMigrated.nshost());
      failure.setFseq(fileMigrated.fseq());
      failure.setErrorCode(e.code());
      failure.setErrorMessage(e.getMessage().str());
      // The underlying SQL does NOT commit
      oraSvc.failFileTransfer(failure);
    } catch(castor::exception::Exception& e) {
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
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_CANNOT_UPDATE_DB, params,&castorFileId);
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
  gettimeofday(&tvStart, NULL);
  try {
    if (fileStale) {
      // The underlying SQL DOES commit.
      oraSvc.setFileStaleInMigration(fileMigrated);
    } else {
      // The underlying SQL DOES commit.
      oraSvc.setFileMigrated(fileMigrated);
    }
  } catch (castor::exception::Exception& e) {
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
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_CANNOT_UPDATE_DB, params, &castorFileId);
    try {
      FileErrorReport failure;
      failure.setMountTransactionId(fileMigrated.mountTransactionId());
      failure.setAggregatorTransactionId(fileMigrated.aggregatorTransactionId());
      failure.setFileid(fileMigrated.fileid());
      failure.setNshost(fileMigrated.nshost());
      failure.setFseq(fileMigrated.fseq());
      failure.setErrorCode(e.code());
      failure.setErrorMessage(e.getMessage().str());
      // The underlying SQL does NOT commit.
      oraSvc.failFileTransfer(failure);
      // There will be no subsequent call to the DB so better comminting here
      scpTrans.commit();
    } catch(castor::exception::Exception& e) {
      castor::dlf::Param params[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
          castor::dlf::Param("serviceClass",serviceClass),
          castor::dlf::Param("fileClass",fileClass),
          castor::dlf::Param("tapePool",tapePool),
          castor::dlf::Param("tapebridgeTransId",fileMigrated.aggregatorTransactionId()),
          castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
          castor::dlf::Param("errorCode",sstrerror(e.code())),
          castor::dlf::Param("errorMessage",e.getMessage().str())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_CANNOT_UPDATE_DB, params, &castorFileId);
    }
  }
  gettimeofday(&tvEnd, NULL);
  procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
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
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_DB_UPDATE, paramsDbUpdate,&castorFileId);

  // Commit for added safety as the SQL is not to be trusted. (maybe a dupe, though)
  scpTrans.commit();
  // Any problem with the DB does not involve the tape server: it did its job. We failed to keep track of it and will
  // re-migrate later. (Or most likely everything went fine when reaching this point).
  NotificationAcknowledge* response= new NotificationAcknowledge;
  response->setMountTransactionId(fileMigrated.mountTransactionId());
  response->setAggregatorTransactionId(fileMigrated.aggregatorTransactionId());
  return response;
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleRecallMoreWork(
    castor::IObject& obj,castor::tape::tapegateway::ITapeGatewaySvc& oraSvc,
    requesterInfo& requester ) throw(){
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
    
    timeval tvStart,tvEnd;
    gettimeofday(&tvStart, NULL);

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
    gettimeofday(&tvEnd, NULL);
    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
  
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
        castor::dlf::Param("ProcessingTime", procTime * 0.000001)
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

castor::IObject* castor::tape::tapegateway::WorkerThread::handleMigrationMoreWork(
    castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc,
    requesterInfo& requester ) throw(){
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

    timeval tvStart,tvEnd;
    gettimeofday(&tvStart, NULL);

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

    gettimeofday(&tvEnd, NULL);
    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

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
        castor::dlf::Param("ProcessingTime", procTime * 0.000001)
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
    requesterInfo& requester) throw(){

  // Rollback mechanism.
  ScopedTransaction scpTrans(&oraSvc);

  // I received an EndTransferRequest, I send back an EndTransferResponse
  NotificationAcknowledge* response=new NotificationAcknowledge();

    try {
      EndNotification& endRequest = dynamic_cast<EndNotification&>(obj);
	
	  castor::dlf::Param params[] ={
	      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
	      castor::dlf::Param("Port",requester.port),
	      castor::dlf::Param("HostName",requester.hostName),
	      castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
	      castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId())
	    };
	
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_END_NOTIFICATION, params);
	  
	  response->setMountTransactionId(endRequest.mountTransactionId());
	  response->setAggregatorTransactionId(endRequest.aggregatorTransactionId());

	  timeval tvStart,tvEnd;
	  gettimeofday(&tvStart, NULL);
	  

	  try {

	    // ACCESS DB to get tape to release
	    
	    castor::stager::Tape tape;
	    // Strghtforward wrapper, read-only sql.
	    oraSvc.getTapeToRelease(endRequest.mountTransactionId(),tape); 

	    gettimeofday(&tvEnd, NULL);
	    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

	    castor::dlf::Param paramsComplete[] ={
	        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
	        castor::dlf::Param("Port",requester.port),
	        castor::dlf::Param("HostName",requester.hostName),
	        castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
	        castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
	        castor::dlf::Param("TPVID",tape.vid()),
	        castor::dlf::Param("mode",tape.tpmode()),
	        castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	    };
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, WORKER_END_GET_TAPE_TO_RELEASE, paramsComplete);


	    try {
	      
	      // UPDATE VMGR

	      if (tape.tpmode() == castor::stager::TPMODE_WRITE) { // just for write case
	
		gettimeofday(&tvStart, NULL);
     
		VmgrTapeGatewayHelper vmgrHelper;
		vmgrHelper.resetBusyTape(tape, m_shuttingDown);

		gettimeofday(&tvEnd, NULL);
		procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

		castor::dlf::Param paramsVmgr[] ={
		    castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
		    castor::dlf::Param("Port",requester.port),
		    castor::dlf::Param("HostName",requester.hostName),
		    castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
		    castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
		    castor::dlf::Param("TPVID",tape.vid()),
		    castor::dlf::Param("mode",tape.tpmode()),
		    castor::dlf::Param("ProcessingTime", procTime * 0.000001)
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
	          castor::dlf::Param("TPVID", tape.vid()),
	          castor::dlf::Param("errorCode",sstrerror(e.code())),
	          castor::dlf::Param("errorMessage",e.getMessage().str())
	      };
	      
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_CANNOT_RELEASE_TAPE, params);
	    }

	    // ACCESS DB now we mark it as done
	    

	    gettimeofday(&tvStart, NULL);
	    // Wrapper function is straightforward. No trap.
	    // SQL commits in the end but could fail in many selects.
	    // Rollback required for this one.
	    oraSvc.endTapeSession(endRequest);

	    gettimeofday(&tvEnd, NULL);
	    procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);


	    castor::dlf::Param paramsDb[] ={
	        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
	        castor::dlf::Param("Port",requester.port),
	        castor::dlf::Param("HostName",requester.hostName),
	        castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
	        castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
	        castor::dlf::Param("TPVID",tape.vid()),
	        castor::dlf::Param("mode",tape.tpmode()),
	        castor::dlf::Param("ProcessingTime", procTime * 0.000001)
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
	    return response;
	  }

	} catch (std::bad_cast){

	  // "Invalid Request" message
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_INVALID_CAST, 0, NULL);
	  
	  EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
	  errorReport->setErrorCode(EINVAL);
	  errorReport->setErrorMessage("invalid object");
	  if (response) delete response;
	  return errorReport;
	  
	}
	// No release of the scpTrans as all is supposed to be self-contained.
	// Leving the rollback as a safety mechanism (SQL does not catch all exceptions)
	// TODO review SQL.
	return  response;
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleFailWorker(
    castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    requesterInfo& requester ) throw(){
  // Auto-rollback mechanism for exceptions
  // ScopedTransaction scpTrans(&oraSvc); Not needed.

  // I received an EndNotificationErrorReport
	NotificationAcknowledge* response= new NotificationAcknowledge();

	try {
	  EndNotificationErrorReport& endRequest = dynamic_cast<EndNotificationErrorReport&>(obj);
	
	  castor::dlf::Param params[] ={
	      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
	      castor::dlf::Param("Port",requester.port),
	      castor::dlf::Param("HostName",requester.hostName),
	      castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
	      castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
	      castor::dlf::Param("errorcode", endRequest.errorCode()),
	      castor::dlf::Param("errorMessage", endRequest.errorMessage())
	  };
	
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, WORKER_FAIL_NOTIFICATION,params);
	 
	  response->setMountTransactionId(endRequest.mountTransactionId());
	  response->setAggregatorTransactionId(endRequest.aggregatorTransactionId());

	  timeval tvStart,tvEnd;
	  gettimeofday(&tvStart, NULL);

	  try {

	    // ACCESS DB to get tape to release

	    castor::stager::Tape tape;
	    // Safe, read only SQL
	    oraSvc.getTapeToRelease(endRequest.mountTransactionId(),tape);
	    
	    gettimeofday(&tvEnd, NULL);
	    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

	    castor::dlf::Param paramsComplete[] ={
	        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
	        castor::dlf::Param("Port",requester.port),
	        castor::dlf::Param("HostName",requester.hostName),
	        castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
	        castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
	        castor::dlf::Param("TPVID",tape.vid()),
	        castor::dlf::Param("mode",tape.tpmode()),
	        castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	    };
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, WORKER_FAIL_GET_TAPE_TO_RELEASE, paramsComplete);
	   
	      
	    // UPDATE VMGR
	    
	    if (tape.tpmode() == castor::stager::TPMODE_WRITE) { // just for write case
	      VmgrTapeGatewayHelper vmgrHelper;
	

	      // CHECK IF THE ERROR WAS DUE TO A FULL TAPE
	      if (endRequest.errorCode() == ENOSPC ) {

		
		try {
		  
		  gettimeofday(&tvStart, NULL);

		  vmgrHelper.setTapeAsFull(tape, m_shuttingDown);

		  gettimeofday(&tvEnd, NULL);
		  procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

		  castor::dlf::Param paramsVmgr[] ={
		      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
		      castor::dlf::Param("Port",requester.port),
		      castor::dlf::Param("HostName",requester.hostName),
		      castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
		      castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
		      castor::dlf::Param("TPVID",tape.vid()),
		      castor::dlf::Param("mode",tape.tpmode()),
		      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
		    };
		  
		  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_TAPE_MAKED_FULL, paramsVmgr);


		  
		} catch (castor::exception::Exception& e) {
		  castor::dlf::Param params[] ={
		      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
		      castor::dlf::Param("Port",requester.port),
		      castor::dlf::Param("HostName",requester.hostName),
		      castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
		      castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
		      castor::dlf::Param("TPVID", tape.vid()),
		      castor::dlf::Param("errorCode",sstrerror(e.code())),
		      castor::dlf::Param("errorMessage",e.getMessage().str())
		  };
    
		  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_CANNOT_MARK_TAPE_FULL, params);
       
		}
		
	      } else {

		try {
		  // We just release the tape
		  
		  gettimeofday(&tvStart, NULL);

		  vmgrHelper.resetBusyTape(tape, m_shuttingDown);

		  gettimeofday(&tvEnd, NULL);
		  procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

		  castor::dlf::Param paramsVmgr[] ={
		      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
		      castor::dlf::Param("Port",requester.port),
		      castor::dlf::Param("HostName",requester.hostName),
		      castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
		      castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
		      castor::dlf::Param("TPVID",tape.vid()),
		      castor::dlf::Param("mode",tape.tpmode()),
		      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
		  };
		   
		  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_FAIL_RELEASE_TAPE, paramsVmgr);
	      
		} catch (castor::exception::Exception& e) {
		  castor::dlf::Param params[] ={
		      castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
		      castor::dlf::Param("Port",requester.port),
		      castor::dlf::Param("HostName",requester.hostName),
		      castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
		      castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
		      castor::dlf::Param("TPVID", tape.vid()),
		      castor::dlf::Param("errorCode",sstrerror(e.code())),
		      castor::dlf::Param("errorMessage",e.getMessage().str())
		  };
	      
		  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_CANNOT_RELEASE_TAPE, params);
		}
	      }
	    }

	    // ACCESS db now we fail it 
	    gettimeofday(&tvStart, NULL);
	    // Direct wrapper, commiting SQL
	    oraSvc.failTapeSession(endRequest); 

	    gettimeofday(&tvEnd, NULL);
	    procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);


	    castor::dlf::Param paramsDb[] ={
	        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
	        castor::dlf::Param("Port",requester.port),
	        castor::dlf::Param("HostName",requester.hostName),
	        castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
	        castor::dlf::Param("tapebridgeTransId", endRequest.aggregatorTransactionId()),
	        castor::dlf::Param("TPVID",tape.vid()),
	        castor::dlf::Param("mode",tape.tpmode()),
	        castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	    };
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,WORKER_FAIL_DB_UPDATE, paramsDb);
	    

	    
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
	    return response;
	  }

	 

	} catch (std::bad_cast){

	  // "Invalid Request" message
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_INVALID_CAST, 0, NULL);
	  
	  EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
	  errorReport->setErrorCode(EINVAL);
	  errorReport->setErrorMessage("invalid object");
	  if (response) delete response;
	  return errorReport;
	  
	}
	return  response;
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleFileFailWorker(
    castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    requesterInfo& requester ) throw(){
  // Auto-rollback mechanism for exceptions
  // ScopedTransaction scpTrans(&oraSvc); Not needed.

  // I received an EndNotificationFileErrorReport
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
    FileErrorReport fileError;
    fileError.setAggregatorTransactionId(endRequest.aggregatorTransactionId());
    fileError.setErrorCode(endRequest.errorCode());
    fileError.setErrorMessage(endRequest.errorMessage());
    fileError.setFileTransactionId(endRequest.fileTransactionId());
    fileError.setFileid(endRequest.fileId());
    fileError.setFseq(endRequest.fSeq());
    fileError.setMountTransactionId(endRequest.mountTransactionId());
    fileError.setNshost(endRequest.nsHost());
    try {
      // This procedure does not commit
      oraSvc.failFileTransfer(fileError);
      oraSvc.commit();
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
    requesterInfo& requester ) throw(){

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
  // As a first implementation, we just loop-call the two single action handlers
  // The error handler is simple as there is no specific return for errors besides
  // acknowledgement.
  // The success report handling is more tricky, as it can lead to an error
  // due to an issue in the name server or the VMGR, leading to a per-file error (or success).
  // In order to match the current process as closely as possible, we
  // have to handle the success reports in fseq order, and stop the session on the first
  // error.
  // TODO A later implementation pushing the loop deeper towards the DB will then be able to distinguish
  // between the session fatal VMGR errors (checks or updates) and the per file benign NS errors
  // (typically a file being deleted from namespace as it gets migrated)

  // First the hard part: we record the successful migration in fseq order, and stop the session on the
  // first failed one.
  // The non-recorded ones will simply be remigrated on a subsequent request (going back from SELECTED to
  // TOBEMIGRATED.

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
    EndNotificationErrorReport * errorReport = new EndNotificationErrorReport();
    errorReport->setErrorCode(SEINTERNAL);
    errorReport->setErrorMessage(report);
    return errorReport;
  }

  // Get hold of the list and fseq-order it
  std::vector<FileMigratedNotificationStruct *>&successes=fileMigrationReportList.successfulMigrations();
  std::sort (successes.begin(), successes.end(), m_fileMigratedFseqComparator);
  // Loop on the now ordered vector
  std::vector<FileMigratedNotificationStruct *>::iterator migedfile;
  EndNotificationErrorReport * endErrorReport = NULL;
  for (migedfile = successes.begin(); migedfile!=successes.end(); migedfile++) {
    // Copy contents to the old structure
    FileMigratedNotification notif;
    // Session-related info.
    notif.setAggregatorTransactionId(fileMigrationReportList.aggregatorTransactionId());
    notif.setMountTransactionId(fileMigrationReportList.mountTransactionId());
    // File related info
    notif.setFileid((*migedfile)->fileid());
    notif.setNshost((*migedfile)->nshost());
    notif.setFseq((*migedfile)->fseq());
    notif.setFileTransactionId((*migedfile)->fileTransactionId());
    notif.setFileSize((*migedfile)->fileSize());
    notif.setChecksumName((*migedfile)->checksumName());
    notif.setChecksum((*migedfile)->checksum());
    notif.setCompressedFileSize((*migedfile)->compressedFileSize());
    notif.setBlockId0((*migedfile)->blockId0());
    notif.setBlockId1((*migedfile)->blockId1());
    notif.setBlockId2((*migedfile)->blockId2());
    notif.setBlockId3((*migedfile)->blockId3());
    notif.setPositionCommandCode((*migedfile)->positionCommandCode());
    // Call the per-file
    castor::IObject* ret_obj = handleMigrationUpdate(notif, oraSvc, requester);
    // Interpret the result: we expect either NotificationAcknowledge or
    // EndNotificationErrorReport
    if (OBJ_NotificationAcknowledge == ret_obj->type()) {
      // We are happy, nothing to do.
      delete ret_obj;
    } else if (OBJ_EndNotificationErrorReport == ret_obj->type()) {
      // Something went wrong. We stop the session, and we'll pass on the result (later)
      // We are sure of the type, so checks are not necessary
      endErrorReport = dynamic_cast <EndNotificationErrorReport *>(&obj);
      break;
    } else {
      // We got an unexpected message type. Internal error. Better stop here.
      std::string report = "Unexpected return type from handleMigrationUpdate ";
      report += "in handleFileMigrationReportList :";
      report += castor::ObjectsIdStrings[obj.type()];
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
      EndNotificationErrorReport * errorReport = new EndNotificationErrorReport();
      errorReport->setErrorCode(SEINTERNAL);
      errorReport->setErrorMessage(report);
      return errorReport;
    }
  }

  // Second, the easy bit: we fail the migrations that need to be failed.
  // No reply analysis needed here, just blindly loop.
  std::vector<FileErrorReportStruct *>& failures = fileMigrationReportList.failedMigrations();
  std::vector<FileErrorReportStruct *>::iterator failedfile;
  for (failedfile = failures.begin(); failedfile!= failures.end(); failedfile++) {
    // We received a a failed file information. We will fail the file transfer first in the DB.
    FileErrorReport fileError;
    fileError.setAggregatorTransactionId(fileMigrationReportList.aggregatorTransactionId());
    fileError.setMountTransactionId(fileMigrationReportList.mountTransactionId());
    fileError.setErrorCode((*failedfile)->errorCode());
    fileError.setErrorMessage((*failedfile)->errorMessage());
    fileError.setFileTransactionId((*failedfile)->fileTransactionId());
    fileError.setFileid((*failedfile)->fileid());
    fileError.setFseq((*failedfile)->fseq());
    fileError.setNshost((*failedfile)->nshost());
    fileError.setPositionCommandCode((*failedfile)->positionCommandCode());
    try {
      // This SQL does not commit. Commit outside of the loop for efficiency
      oraSvc.failFileTransfer(fileError);
    } catch (castor::exception::Exception& e){
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
  }
  // Commit if there was any failed migration recorded
  if (!failures.empty()) oraSvc.commit();

  // Now send the result of all this (migrations and recalls) to the tape server.
  // If there was an error, the reply is ready
  if (endErrorReport) return endErrorReport;
  // Else return Ack
  NotificationAcknowledge * ack = new NotificationAcknowledge();
  ack->setAggregatorTransactionId(fileMigrationReportList.aggregatorTransactionId());
  ack->setMountTransactionId(fileMigrationReportList.mountTransactionId());

  // Log completion of the processing
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

  return ack;
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleFileRecallReportList(
    castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    requesterInfo& requester ) throw(){
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

  // Record the successes
  std::vector<FileRecalledNotificationStruct *> successes = fileRecallReportList.successfulRecalls();
  std::vector<FileRecalledNotificationStruct *>::iterator recfile;
  for (recfile = successes.begin(); recfile != successes.end(); recfile++) {
    FileRecalledNotification recNotif;
    // Session-related information
    recNotif.setAggregatorTransactionId(fileRecallReportList.aggregatorTransactionId());
    recNotif.setMountTransactionId(fileRecallReportList.mountTransactionId());
    // File-related information
    recNotif.setFileTransactionId((*recfile)->fileTransactionId());
    recNotif.setNshost((*recfile)->nshost());
    recNotif.setFileid((*recfile)->fileid());
    recNotif.setFseq((*recfile)->fseq());
    recNotif.setPath((*recfile)->path());
    recNotif.setChecksumName((*recfile)->checksumName());
    recNotif.setChecksum((*recfile)->checksum());
    recNotif.setFileSize((*recfile)->fileSize());
    recNotif.setPositionCommandCode((*recfile)->positionCommandCode());
    castor::IObject * ret = handleRecallUpdate(recNotif, oraSvc, requester);
    delete ret;
  }

  // Record the failures
  std::vector<FileErrorReportStruct *> failures = fileRecallReportList.failedRecalls();
  std::vector<FileErrorReportStruct *>::iterator failedfile;
  for (failedfile = failures.begin(); failedfile!=failures.end();failedfile++) {
    // We received a a failed file information. We will fail the file transfer first in the DB.
    FileErrorReport fileError;
    fileError.setAggregatorTransactionId(fileRecallReportList.aggregatorTransactionId());
    fileError.setMountTransactionId(fileRecallReportList.mountTransactionId());
    fileError.setErrorCode((*failedfile)->errorCode());
    fileError.setErrorMessage((*failedfile)->errorMessage());
    fileError.setFileTransactionId((*failedfile)->fileTransactionId());
    fileError.setFileid((*failedfile)->fileid());
    fileError.setFseq((*failedfile)->fseq());
    fileError.setNshost((*failedfile)->nshost());
    fileError.setPositionCommandCode((*failedfile)->positionCommandCode());
    try {
      // This SQL does not commit. Commit outside of the loop for efficiency
      oraSvc.failFileTransfer(fileError);
    } catch (castor::exception::Exception& e){
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
    // Commit if there was any failed migration recorded
    if (!failures.empty()) oraSvc.commit();
  }

  // Acknowledge
  NotificationAcknowledge * ack = new NotificationAcknowledge();
  ack->setAggregatorTransactionId(fileRecallReportList.aggregatorTransactionId());
  ack->setMountTransactionId(fileRecallReportList.mountTransactionId());

  // Log completion of the processing
  {
    castor::dlf::Param params[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId", fileRecallReportList.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", fileRecallReportList.aggregatorTransactionId())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_REC_REPORT_LIST_PROCESSED, params);
  }

  return ack;
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleFilesToMigrateListRequest(
    castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    requesterInfo& requester ) throw(){
  // first check we are called with the proper class
  FilesToMigrateListRequest * req = dynamic_cast <FilesToMigrateListRequest *>(&obj);
  if (!req) {
    // We did not get the expected class
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_INVALID_CAST, 0, NULL);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }
  FilesToMigrateListRequest &filesToMigrateListRequest = *req;
  req = NULL;

  // Try to fulfil the request. We count here.
  uint64_t files = 0;
  uint64_t bytes = 0;
  // We try to find filesToMigrateListRequest.maxFiles().
  // If we get over the bytes threshold, we stop before this limit.
  std::queue <FileToMigrateStruct> files_list;
  // We can keep the upstream answer in some situations.
  // It will be stored here:
  castor::IObject * response = NULL;
  while (files < filesToMigrateListRequest.maxFiles()) {
    // Get new file to migrate
    FileToMigrateRequest fileReq;
    fileReq.setAggregatorTransactionId(filesToMigrateListRequest.aggregatorTransactionId());
    fileReq.setMountTransactionId(filesToMigrateListRequest.mountTransactionId());
    castor::IObject * fileResp = handleMigrationMoreWork (fileReq, oraSvc, requester);
    // handleMigrationMoreWork can return:
    // - FileToMigrate (nortmal case)
    // - EndNotificationErrorReport (in case of problems with the DB)
    // - NoMoreFiles (when there is nothing new to return).
    if (OBJ_FileToMigrate == fileResp->type()) {
      // We found a file
      // We are sure of the type, so checks are not necessary
      FileToMigrate * file_response = dynamic_cast<FileToMigrate *>(fileResp);
      FileToMigrateStruct ftm;
      ftm.setFileTransactionId(file_response->fileTransactionId());
      ftm.setNshost(file_response->nshost());
      ftm.setFileid(file_response->fileid());
      ftm.setFseq(file_response->fseq());
      ftm.setFileSize(file_response->fileSize());
      ftm.setLastKnownFilename(file_response->lastKnownFilename());
      ftm.setLastModificationTime(file_response->lastModificationTime());
      ftm.setPath(file_response->path());
      ftm.setUmask(file_response->umask());
      ftm.setPositionCommandCode(file_response->positionCommandCode());
      // Record the file
      files_list.push(ftm);
      // Accounting for the request
      files++;
      bytes+=file_response->fileSize();
      delete file_response;
      // We're done if we reached the byte quota
      if (bytes >= filesToMigrateListRequest.maxBytes()) break;
    } else if (OBJ_NoMoreFiles == fileResp->type()) {
      // We won't get more files.
      // If the list is empty, we can just pass the NoMoreFiles.
      // If not, we'll build the reply now (break without reaching the quota).
      if (files_list.empty()) {
        response = fileResp;
      } else {
        delete fileResp;
      }
      break;
    } else if  (OBJ_EndNotificationErrorReport == fileResp->type()) {
      // We abandon here the few files (if any) that were put in SELECTED, but not yet
      // tranmitted to the tapebridge. They will be recovered at the end of the session,
      // which will happen right now, as we transmit the EndNotitficationErrorReport.
      // Jettison the files!
      while (!files_list.empty()) files_list.pop();
      response = fileResp;
      break;
    } else {
      // Panic! Panic! Unexpected response. Internal error. Better stop here.
      // If some files are now SELECTED, we won't transmit them, like in the previous case.
      // They will eventually be resurrected at the end of the session (which
      // will come soon as we prepare an EndNotification.)
      std::string report = "Unexpected return type from handleMigrationMoreWork ";
      report += "in handleFilesToMigrateListRequest :";
      report += castor::ObjectsIdStrings[obj.type()];
      castor::dlf::Param params[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId", filesToMigrateListRequest.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId", filesToMigrateListRequest.aggregatorTransactionId()),
          castor::dlf::Param("errorCode",SEINTERNAL),
          castor::dlf::Param("errorMessage",report)
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, INTERNAL_ERROR, params);
      // Abandon ship!
      while (!files_list.empty()) files_list.pop();
      EndNotificationErrorReport * endReport = new EndNotificationErrorReport();
      endReport->setAggregatorTransactionId(filesToMigrateListRequest.aggregatorTransactionId());
      endReport->setMountTransactionId(filesToMigrateListRequest.mountTransactionId());
      endReport->setErrorCode(SEINTERNAL);
      endReport->setErrorMessage(report);
      response = endReport;
      break;
    }
  }

  // We got out of the loop. Should now have either a list of files in the files_list
  // queue, or an already cooked response pointed to by response.
  if (response) {
    // already cooked response. Just make sure there is nothing in the files queue
    if (!files_list.empty()) {
      std::string report = "Non-empty files queue ";
      report += "in handleFilesToMigrateListRequest when response is ready.";
      castor::dlf::Param params[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId", filesToMigrateListRequest.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId", filesToMigrateListRequest.aggregatorTransactionId()),
          castor::dlf::Param("errorCode",SEINTERNAL),
          castor::dlf::Param("errorMessage",report)
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, INTERNAL_ERROR, params);
    }
    /* while (!files_list.empty()) files_list.pop(); */ /* This is done automatically on return */
    return response;
  }
  // Standard case: we create the response and populate it with the queue of files.
  FilesToMigrateList * files_response = new FilesToMigrateList();
  files_response->setAggregatorTransactionId(filesToMigrateListRequest.aggregatorTransactionId());
  files_response->setMountTransactionId(filesToMigrateListRequest.mountTransactionId());
  int i=0;
  // We know the number of files. Let's allocate the vector's memory efficiently.
  files_response->filesToMigrate().resize(files);
  while (!files_list.empty()) {
    FileToMigrateStruct * ftm = new FileToMigrateStruct();
    *ftm = files_list.front();
    files_response->filesToMigrate()[i++]=ftm;
    files_list.pop();
  }
  return files_response;
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleFilesToRecallListRequest(
    castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    requesterInfo& requester ) throw(){
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
  FilesToRecallListRequest &filesToRecallListRequest = *req;
  req = NULL;

  // Try to fulfil the request. We count here.
  uint64_t files = 0;
  uint64_t bytes = 0;
  // We will count in a later version. Make the compiler shut up
  bytes = bytes;
  // We try to find filesToRecallListRequest.maxFiles().
  // If we get over the bytes threshold, we stop before this limit. (Not implemented
  // right now: upstream call to not pass the file size (TODO).
  std::queue <FileToRecallStruct> files_list;
  // We can keep the upstream answer in some situations.
  // It will be stored here:
  castor::IObject * response = NULL;
  while (files < filesToRecallListRequest.maxFiles()) {
    // Get new file to recall
    FileToRecallRequest fileReq;
    fileReq.setAggregatorTransactionId(filesToRecallListRequest.aggregatorTransactionId());
    fileReq.setMountTransactionId(filesToRecallListRequest.mountTransactionId());
    castor::IObject * fileResp = handleRecallMoreWork (fileReq, oraSvc, requester);
    // handleRecallMoreWork can return:
    // - FileTorecall (normal case)
    // - EndNotificationErrorReport (in case of problems with the DB)
    // - NoMoreFiles (when there is nothing new to return).
    if (OBJ_FileToRecall == fileResp->type()) {
      // We found a file
      // We are sure of the type, so checks are not necessary
      FileToRecall * file_response = dynamic_cast<FileToRecall *>(fileResp);
      FileToRecallStruct ftr;
      ftr.setFileTransactionId(file_response->fileTransactionId());
      ftr.setNshost(file_response->nshost());
      ftr.setFileid(file_response->fileid());
      ftr.setFseq(file_response->fseq());
      ftr.setPath(file_response->path());
      ftr.setBlockId0(file_response->blockId0());
      ftr.setBlockId1(file_response->blockId1());
      ftr.setBlockId2(file_response->blockId2());
      ftr.setBlockId3(file_response->blockId3());
      ftr.setUmask(file_response->umask());
      ftr.setPositionCommandCode(file_response->positionCommandCode());
      // Record the file
      files_list.push(ftr);
      // Accounting for the request. We can only do it by number right now.
      // File size is missing in the current implementation.
      files++;
      delete file_response;
    } else if (OBJ_NoMoreFiles == fileResp->type()) {
      // We won't get more files.
      // If the list is empty, we can just pass the NoMoreFiles.
      // If not, we'll build the reply now (break without reaching the quota).
      if (files_list.empty()) {
        response = fileResp;
      } else {
        delete fileResp;
      }
      break;
    } else if  (OBJ_EndNotificationErrorReport == fileResp->type()) {
      // We abandon here the few files (if any) that were put in SELECTED, but not yet
      // tranmitted to the tapebridge. They will be recovered at the end of the session,
      // which will happen right now, as we transmit the EndNotitficationErrorReport.
      // Jettison the files!
      while (!files_list.empty()) files_list.pop();
      response = fileResp;
      break;
    } else {
      // Panic! Panic! Unexpected response. Internal error. Better stop here.
      // If some files are now SELECTED, we won't transmit them, like in the previous case.
      // They will eventually be resurrected at the end of the session (which
      // will come soon as we prepare an EndNotification.)
      std::string report = "Unexpected return type from handleRecallMoreWork ";
      report += "in handleFilesToRecallListRequest :";
      report += castor::ObjectsIdStrings[obj.type()];
      castor::dlf::Param params[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId", filesToRecallListRequest.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId", filesToRecallListRequest.aggregatorTransactionId()),
          castor::dlf::Param("errorCode",SEINTERNAL),
          castor::dlf::Param("errorMessage",report)
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, INTERNAL_ERROR, params);
      // Abandon ship!
      while (!files_list.empty()) files_list.pop();
      EndNotificationErrorReport * endReport = new EndNotificationErrorReport();
      endReport->setAggregatorTransactionId(filesToRecallListRequest.aggregatorTransactionId());
      endReport->setMountTransactionId(filesToRecallListRequest.mountTransactionId());
      endReport->setErrorCode(SEINTERNAL);
      endReport->setErrorMessage(report);
      response = endReport;
      break;
    }
  }

  // We got out of the loop. Should now have either a list of files in the files_list
  // queue, or an already cooked response pointed to by response.
  if (response) {
    // already cooked response. Just make sure there is nothing in the files queue
    if (!files_list.empty()) {
      std::string report = "Non-empty files queue ";
      report += "in handleFilesToRecallListRequest when response is ready.";
      castor::dlf::Param params[] ={
          castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
          castor::dlf::Param("Port",requester.port),
          castor::dlf::Param("HostName",requester.hostName),
          castor::dlf::Param("mountTransactionId", filesToRecallListRequest.mountTransactionId()),
          castor::dlf::Param("tapebridgeTransId", filesToRecallListRequest.aggregatorTransactionId()),
          castor::dlf::Param("errorCode",SEINTERNAL),
          castor::dlf::Param("errorMessage",report)
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, INTERNAL_ERROR, params);
    }
    /* while (!files_list.empty()) files_list.pop(); */ /* This is done automatically on return */
    return response;
  }
  // Standard case: we create the response and populate it with the queue of files.
  FilesToRecallList * files_response = new FilesToRecallList();
  files_response->setAggregatorTransactionId(filesToRecallListRequest.aggregatorTransactionId());
  files_response->setMountTransactionId(filesToRecallListRequest.mountTransactionId());
  int i=0;
  // We know the number of files. Let's allocate the vector's memory efficiently.
  files_response->filesToRecall().resize(files);
  while (!files_list.empty()) {
    FileToRecallStruct* ftr = new FileToRecallStruct();
    *ftr = files_list.front();
    files_response->filesToRecall()[i++]=ftr;
    files_list.pop();
  }
  return files_response;
}
