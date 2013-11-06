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
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <errno.h>
#include <getconfent.h>
#include <sys/time.h>
#include <sys/types.h>
#include <limits.h>
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

std::string castor::tape::tapegateway::WorkerThread::requesterInfo::str ()
{
  // Pretty print the requester info into a string
  std::stringstream ctxt;
  ctxt << "HostName=" << hostName << " Port=" << port
      << " IP=" << std::dec;
  // Chopping of the ulong (u32) IP representation into traditional
  // decimal bytes and dots.
  for (int i=0; i<4; i++) {
    // If i is non-zero, add separator.
    if (i) ctxt << ".";
    ctxt << ( ( ip >> (3-i)*8) & 0xFF);
  }
  return ctxt.str();
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

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleEndWorker(
    castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
    requesterInfo& requester) throw(castor::exception::Exception){
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
  return response.release();
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
  try {
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
  if (!fileMigrationReportList.successfulMigrations().empty()) {
    // 2.1 Build the statistics of the successes. Also log the reception of them.
    u_signed64 totalBytes = 0, totalCompressedBytes = 0;
    int highestFseq = -1;
    int lowestFseq = INT_MAX;

    for (std::vector<FileMigratedNotificationStruct *>::iterator sm =
           fileMigrationReportList.successfulMigrations().begin();
           sm < fileMigrationReportList.successfulMigrations().end(); sm++) {
      totalBytes += (*sm)->fileSize();
      totalCompressedBytes += (*sm)->compressedFileSize();
      highestFseq = std::max((*sm)->fseq(), highestFseq);
      lowestFseq =  std::min((*sm)->fseq(), lowestFseq);
      // Log the file notification while we're at it.
      Cns_fileid fileId;
      fileId.fileid = (*sm)->fileid();
      strncpy(fileId.server, (*sm)->nshost().c_str(),sizeof(fileId.server)-1);
      logMigrationNotified(nullCuuid, fileMigrationReportList.mountTransactionId(),
          fileMigrationReportList.aggregatorTransactionId(), &fileId, requester, **sm);
    }

    // 2.2 As mentioned above, the tape server does not carry on on errors.
    // That means the successes should be a continuous streak of fseqs, and all
    // failures should have a fseq higher than any of the successes.
    // 2.2.1 Check continuity
    if (highestFseq - lowestFseq + 1 !=
        (int) fileMigrationReportList.successfulMigrations().size()) {
      castor::exception::Internal e;
      e.getMessage() << "In handleFileMigrationReportList, mismatching "
          << "fSeqs and number of successes: lowestFseq=" << lowestFseq
          << " highestFseq=" << highestFseq << " expectedNumber="
          << highestFseq - lowestFseq + 1 <<  " actualNumber="
          << fileMigrationReportList.successfulMigrations().size();
      logInternalError(e, requester, fileMigrationReportList);
      std::auto_ptr<EndNotificationErrorReport> errorReport (new EndNotificationErrorReport());
      errorReport->setErrorCode(EIO);
      errorReport->setErrorMessage("Wrong successes number/fseq range mismatch");
      errorReport->setMountTransactionId(fileMigrationReportList.mountTransactionId());
      errorReport->setAggregatorTransactionId(fileMigrationReportList.aggregatorTransactionId());
      return errorReport.release();
    }

    // 2.2.2 Check that any failure fseq is above the highest success fSeq
    for (std::vector<FileErrorReportStruct*>::iterator f =
        fileMigrationReportList.failedMigrations().begin();
        f < fileMigrationReportList.failedMigrations().end();
        f++) {
      if ( (*f)->fseq() <= highestFseq) {
        castor::exception::Internal e;
        e.getMessage() << "In handleFileMigrationReportList, mismatching "
            << "fSeqs for failure, lower than highest success fSeq: failureFSeq="
            << (*f)->fseq() << " failureNsFileId=" << (*f)->fileid()
            << " failureFileTransactionId=" << (*f)->fileTransactionId()
            << " failureErrorCode=" << (*f)->errorCode()
            << " failureErrorMessage=" << (*f)->errorMessage()
            << " highestSuccessFseq=" << highestFseq;
        logInternalError(e, requester, fileMigrationReportList);
        std::auto_ptr<EndNotificationErrorReport> errorReport (new EndNotificationErrorReport());
        errorReport->setErrorCode(EIO);
        errorReport->setErrorMessage("Error report interleaved in successes. This is not supported (nor expected)");
        errorReport->setMountTransactionId(fileMigrationReportList.mountTransactionId());
        errorReport->setAggregatorTransactionId(fileMigrationReportList.aggregatorTransactionId());
        return errorReport.release();
      }
    }

    // 2.3 Update the VMGR for fseq. Stop here in case of failure.
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
    // 2.4 Scan all errors to find tape full condition for more update of the
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
    oraSvc.setBulkFileMigrationResult(
        requester.str(),
        fileMigrationReportList.mountTransactionId(),
        fileMigrationReportList.successfulMigrations(),
        fileMigrationReportList.failedMigrations());
  } catch (castor::exception::Exception e) {
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
    // "Worker: received a recall report list"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_REC_REPORT_LIST_RECEIVED, params);
  }

  // We have 2 lists of both successes and errors.
  // The database side of the application will take care of updating the stager database.
  // As opposed to migrations, the VMGR needs no update here.
  // results are also logged by the DB procedures.
  try {
    oraSvc.setBulkFileRecallResult(
        requester.str(),
        fileRecallReportList.mountTransactionId(),
        fileRecallReportList.successfulRecalls(),
        fileRecallReportList.failedRecalls());
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
        castor::dlf::Param("mountTransactionId", fileRecallReportList.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId", fileRecallReportList.aggregatorTransactionId())
    };
    // "Worker: finished processing a recall report list"
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

  // Prepare the file list container
  std::queue <FileToMigrateStruct> files_list;
  bool dbFailure = false;
  double dbServingTime;
  castor::tape::utils::Timer timer;

  // Transmit the request to the DB directly
  try {
    oraSvc.getBulkFilesToMigrate(requester.str(), ftmlr.mountTransactionId(),
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
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_LIST_RETRIEVED,params);
  }
  // and reply to requester.
  return files_response.release();
}

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

  // Prepare the file list container
  std::queue <castor::tape::tapegateway::ITapeGatewaySvc::FileToRecallStructWithContext> files_list;
  bool dbFailure = false;
  double dbServingTime;
  castor::tape::utils::Timer timer;

  // Transmit the request to the DB directly
  try {
    oraSvc.getBulkFilesToRecall(requester.str(), ftrlr.mountTransactionId(),
        ftrlr.maxFiles(), ftrlr.maxBytes(),
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
  files_response->setAggregatorTransactionId(ftrlr.aggregatorTransactionId());
  files_response->setMountTransactionId(ftrlr.mountTransactionId());
  while (!files_list.empty()) {
    std::auto_ptr<FileToRecallStruct> ftr (new FileToRecallStruct);
    castor::tape::tapegateway::ITapeGatewaySvc::FileToRecallStructWithContext & ftrwc = files_list.front();
    *ftr = ftrwc;
    filesCount++;
    // Log the per-file information
    std::stringstream blockid;
    blockid << std::hex << std::uppercase << std::noshowbase << std::setfill('0')
        << std::setw(2) << (int)ftr->blockId0()
        << std::setw(2) << (int)ftr->blockId1()
        << std::setw(2) << (int)ftr->blockId2()
        << std::setw(2) << (int)ftr->blockId3();
    castor::dlf::Param paramsComplete[] ={
        castor::dlf::Param("IP",  castor::dlf::IPAddress(requester.ip)),
        castor::dlf::Param("Port",requester.port),
        castor::dlf::Param("HostName",requester.hostName),
        castor::dlf::Param("mountTransactionId",ftrlr.mountTransactionId()),
        castor::dlf::Param("tapebridgeTransId",ftrlr.aggregatorTransactionId()),
        castor::dlf::Param("fseq",ftr->fseq()),
        castor::dlf::Param("path",ftr->path()),
        castor::dlf::Param("blockId",blockid.str()),
        castor::dlf::Param("fileTransactionId", ftr->fileTransactionId()),
        castor::dlf::Param("copyNb", ftrwc.copyNb),
        castor::dlf::Param("eUid", ftrwc.eUid),
        castor::dlf::Param("eGid", ftrwc.eGid),
        castor::dlf::Param("TPVID", ftrwc.VID),
        castor::dlf::Param("fileSize", ftrwc.fileSize),
        castor::dlf::Param("creationTime", ftrwc.creationTime),
        castor::dlf::Param("nbRetriesWithinMount", ftrwc.nbRetriesInMount),
        castor::dlf::Param("nbMounts", ftrwc.nbMounts)
    };
    struct Cns_fileid fileId;
    memset(&fileId,'\0',sizeof(fileId));
    strncpy(fileId.server, ftr->nshost().c_str(), sizeof(fileId.server)-1);
    fileId.fileid = ftr->fileid();
    // Handle the special case when blockId = 0: we should position by fseq
    // Said to be for fseq=1 in older code, unconfirmed.
    if (!(ftr->blockId0()|ftr->blockId1()|ftr->blockId2()|ftr->blockId3()))
      ftr->setPositionCommandCode(TPPOSIT_FSEQ);
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_RETRIEVED, paramsComplete, &fileId);
    // ... and store in the response.
    files_response->filesToRecall().push_back(ftr.release());
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
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_LIST_RETRIEVED,params);
  }
  // and reply to requester.
  return files_response.release();
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
    oraSvc.endTapeSession(mountTransactionId);
  } catch (castor::exception::Exception e) {
    // If things still go wrong, log an internal error.
    std::stringstream report;
    report <<  "Failed to endTapeSession in the DB "
      "in: castor::tape::tapegateway::WorkerThread::setSessionToClosing: "
      "error=" << e.code() << " errorMessage=" << e.getMessage().str();
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
  castor::dlf::dlf_writep(uuid, DLF_LVL_DEBUG, WORKER_MIGRATION_NOTIFIED, paramsComplete, castorFileId);
}

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
      castor::dlf::Param("TPVID",vid),
      castor::dlf::Param("totalSize",totalBytes),
      castor::dlf::Param("compressedTotalSize",totalCompressedBytes),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
  };
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_VMGR_UPDATE, paramsVmgr);
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

