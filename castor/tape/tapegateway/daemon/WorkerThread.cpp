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
 * @author Giulia Taurelli
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
        default:
          //object not valid
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
    vmgrHelper.updateTapeInVmgr(fileMigrated, vid);
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
		vmgrHelper.resetBusyTape(tape);

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

		  vmgrHelper.setTapeAsFull(tape);

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

		  vmgrHelper.resetBusyTape(tape);

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
      oraSvc.failFileTransfer(fileError);
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
