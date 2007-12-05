/******************************************************************************
 *                      RHThread.cpp
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
 * @(#)$RCSfile: RHThread.cpp,v $ $Revision: 1.16 $ $Release$ $Date: 2007/12/05 14:49:30 $ $Author: riojac3 $
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files

#include "castor/rh/RHThread.hpp"
#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/PermissionDenied.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/PortsConfig.hpp"
#include "castor/server/BaseServer.hpp"
#include "castor/server/ThreadNotification.hpp"

#include "castor/stager/Request.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/QryRequest.hpp"
#include "castor/stager/GCFileList.hpp"
#include "castor/rh/Client.hpp"

#include "castor/io/biniostream.h"

#include "castor/rh/Server.hpp"
#include "castor/MessageAck.hpp"

#include <iostream>
#include <errno.h>
#include <sys/times.h>
#include <unistd.h>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::rh::RHThread::RHThread(bool useAccessLists)
throw (castor::exception::Exception) :
  m_useAccessLists(useAccessLists) {
  m_stagerHost = castor::PortsConfig::getInstance()->
    getHostName(castor::CASTOR_STAGER);
  m_stagerPort = castor::PortsConfig::getInstance()->
    getNotifPort(castor::CASTOR_STAGER);
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::rh::RHThread::run(void* param) {
  MessageAck ack;
  ack.setStatus(true);

  // clock the time to process this request
  struct tms buf;
  clock_t startTime = times(&buf);

  // We know it's a ServerSocket
  castor::io::ServerSocket* sock = (castor::io::ServerSocket*) param;
  castor::stager::Request* fr = 0;

  // Retrieve info on the client
  unsigned short port;
  unsigned long ip;
  try {
    sock->getPeerIp(port, ip);
  } catch(castor::exception::Exception e) {
    // "Exception caught : ignored" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 5, 2, params);
  }
  // "New Request Arrival" message
  castor::dlf::Param peerParams[] =
    {castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
     castor::dlf::Param("Port", port)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 1, 2, peerParams);

  // get the incoming request
  try {
    castor::IObject* obj = sock->readObject();
    fr = dynamic_cast<castor::stager::Request*>(obj);
    if (0 == fr) {
      delete obj;
      // "Invalid Request" message
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 6);
      ack.setStatus(false);
      ack.setErrorCode(EINVAL);
      ack.setErrorMessage("Invalid Request object sent to server.");
    }
  } catch (castor::exception::Exception e) {
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 7, 1, params);
    ack.setStatus(false);
    ack.setErrorCode(EINVAL);
    std::ostringstream stst;
    stst << "Unable to read Request object from socket."
         << std::endl << e.getMessage().str();
    ack.setErrorMessage(stst.str());
  }

  //if the request comes from a secure connection then set Client values in Request object.
  bool secure=false;
  castor::io::AuthServerSocket* authSock = dynamic_cast<castor::io::AuthServerSocket*>(sock);
  if(authSock != 0) {
    fr->setEuid(authSock->getClientEuid());
    fr->setEgid(authSock->getClientEgid());
    secure=true;
  }

  // placeholder for the request uuid if any
  Cuuid_t cuuid = nullCuuid;
  if (ack.status()) {
    try {
      // gives a Cuuid to the request
      // XXX Interface to Cuuid has to be improved !
      // XXX its length has currently to be hardcoded
      // XXX wherever you use it !!!
      Cuuid_create(&cuuid);
      char uuid[CUUID_STRING_LEN+1];
      uuid[CUUID_STRING_LEN] = 0;
      Cuuid2string(uuid, CUUID_STRING_LEN+1, &cuuid);
      fr->setReqId(uuid);
      // "Processing Request" message
      castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, 8);

      // Complete its client field
      castor::rh::Client *client =
        dynamic_cast<castor::rh::Client *>(fr->client());
      if (0 == client) {
        delete fr;
        castor::exception::Internal e;
        e.getMessage() << "Request arrived with no client object.";
        throw e;
      }
      client->setIpAddress(ip);
      if (secure){
        client->setSecure(1);
      }

      // handle the request. Pass the ip:port for logging purposes
      handleRequest(fr, cuuid, ip, port);
      ack.setRequestId(uuid);
      ack.setStatus(true);
      
    } catch (castor::exception::PermissionDenied e) {
      // "Permission Denied"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Euid", fr->euid()),
	 castor::dlf::Param("Egid", fr->egid()),
	 castor::dlf::Param("Reason", e.getMessage().str())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_AUTH, 14, 3, params);
      ack.setStatus(false);
      ack.setErrorCode(e.code());
      ack.setErrorMessage(e.getMessage().str());      
    } catch (castor::exception::Exception e) {
      // "Exception caught" message
      castor::dlf::Param params[] =
        {castor::dlf::Param("Standard Message", sstrerror(e.code())),
         castor::dlf::Param("Precise Message", e.getMessage().str())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 9, 2, params);
      ack.setStatus(false);
      ack.setErrorCode(e.code());
      ack.setErrorMessage(e.getMessage().str());
    } 
  }

  // the process is over, don't include the time to send the ack
  clock_t endTime = times(&buf);
  try {
    sock->sendObject(ack);
    castor::dlf::Param params[] =
      {castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
       castor::dlf::Param("Port", port),
       castor::dlf::Param("ProcTime",    // processing time in ms
         (int)((endTime - startTime) * 1000.0 / (float)sysconf(_SC_CLK_TCK)))};
    // "Reply sent to client" message
    castor::dlf::dlf_writep(cuuid, DLF_LVL_MONITORING, 10, 3, params);
  } catch (castor::exception::Exception e) {
    // "Unable to send Ack to client" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 11, 2, params);
  }

  delete fr;
  delete sock;
}

//------------------------------------------------------------------------------
// handleRequest
//------------------------------------------------------------------------------
void castor::rh::RHThread::handleRequest
(castor::stager::Request* fr, Cuuid_t cuuid, unsigned long peerIP, unsigned short peerPort)
  throw (castor::exception::Exception) {
  // Checks access rights
  if (m_useAccessLists) {
    castor::rh::IRHSvc* m_rhSvc = 0;
    castor::IService* svc = castor::BaseObject::services()->service("DbRhSvc", castor::SVC_DBRHSVC);
    m_rhSvc = dynamic_cast<castor::rh::IRHSvc*>(svc);
    if (0 == m_rhSvc) {
      castor::exception::Internal ex;
      ex.getMessage() << "Couldn't load the request handler service, check the castor.conf for DynamicLib entries" << std::endl;
      throw ex;
    }
    m_rhSvc->checkPermission(fr->svcClassName(), fr->euid(), fr->egid(), fr->type());
  }
  // Number of subrequests (when applicable)
  unsigned int nbSubReqs = 1;
  // Stores it into DB
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);
  try {
    svcs()->createRep(&ad, fr, false);
    // Store files for file requests
    castor::stager::FileRequest* filreq =
      dynamic_cast<castor::stager::FileRequest*>(fr);
    if (0 != filreq) {
      svcs()->fillRep(&ad, fr, OBJ_SubRequest, false);
      // and get number of subrequests
      nbSubReqs = filreq->subRequests().size();
    }
    // Store client for requests
    castor::stager::Request* req =
      dynamic_cast<castor::stager::Request*>(fr);
    if (0 != req) {
      svcs()->createRep(&ad, req->client(), false);
      svcs()->fillRep(&ad, fr, OBJ_IClient, false);
    }
    // Store parameters for query requests
    castor::stager::QryRequest* qryReq =
      dynamic_cast<castor::stager::QryRequest*>(fr);
    if (0 != qryReq) {
      svcs()->fillRep(&ad, qryReq, OBJ_QueryParameter, false);
    }
    // Store deletedFiles for fileList
    castor::stager::GCFileList* fdReq =
      dynamic_cast<castor::stager::GCFileList*>(fr);
    if (0 != fdReq) {
      svcs()->fillRep(&ad, fdReq, OBJ_GCFile, false);
    }
    svcs()->commit(&ad);
    
    // "Request stored in DB" message
    // here we attach all data for monitoring/tracking purposes
    castor::dlf::Param params[] =
      {castor::dlf::Param("IP", castor::dlf::IPAddress(peerIP)),
       castor::dlf::Param("Port", peerPort),
       castor::dlf::Param("DBKey", fr->id()),
       castor::dlf::Param("Type", fr->type())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 12, 4, params);

  } catch (castor::exception::Exception e) {
    svcs()->rollback(&ad);
    throw e;
  }

  // Send an UDP message to the right stager service
  switch (fr->type()) {
  case OBJ_StageGetRequest:
  case OBJ_StagePutRequest:
  case OBJ_StageUpdateRequest:
    // JobRequest Service
    castor::server::BaseServer::sendNotification(m_stagerHost, m_stagerPort, 'J', nbSubReqs);
    break;
  case OBJ_StagePrepareToPutRequest:
  case OBJ_StagePrepareToGetRequest:
  case OBJ_StageRepackRequest:
  case OBJ_StagePrepareToUpdateRequest:
    // PrepareRequest Service
    castor::server::BaseServer::sendNotification(m_stagerHost, m_stagerPort, 'P', nbSubReqs);
    break;
  case OBJ_StageRmRequest:
  case OBJ_StagePutDoneRequest:
  case OBJ_SetFileGCWeight:
    // StagerRequest Service
    castor::server::BaseServer::sendNotification(m_stagerHost, m_stagerPort, 'S', nbSubReqs);
    break;
  case OBJ_StageFileQueryRequest :
  case OBJ_StageFindRequestRequest :
  case OBJ_StageRequestQueryRequest :
  case OBJ_DiskPoolQuery :
    // QueryRequest Service
    castor::server::BaseServer::sendNotification(m_stagerHost, m_stagerPort, 'Q', nbSubReqs);
    break;
  case OBJ_GetUpdateStartRequest:
  case OBJ_Disk2DiskCopyDoneRequest:
  case OBJ_Disk2DiskCopyStartRequest:
  case OBJ_MoverCloseRequest:
  case OBJ_PutStartRequest :
  case OBJ_GetUpdateDone :
  case OBJ_GetUpdateFailed :
  case OBJ_PutFailed :
  case OBJ_PutDoneStart :
    // Job Service
    castor::server::BaseServer::sendNotification(m_stagerHost, m_stagerPort, 'j', 1);
    break;
  case OBJ_Files2Delete:
  case OBJ_FilesDeleted:
  case OBJ_FilesDeletionFailed:
    // Garbage Collector Service
    castor::server::BaseServer::sendNotification(m_stagerHost, m_stagerPort, 'G', 1);
    break;
  default:
    // We should not go this way, this would not be optimal
    // "Waked up all services at once"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Request type", fr->type())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 13, 1, params);
    // XXX not supported for the time being in the new framework,
    // XXX to be seen if needed.
  }
}
