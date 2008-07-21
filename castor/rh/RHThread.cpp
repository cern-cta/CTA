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
 * @(#)$RCSfile: RHThread.cpp,v $ $Revision: 1.31 $ $Release$ $Date: 2008/07/21 09:32:00 $ $Author: waldron $
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include files
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
#include "castor/bwlist/ChangePrivilege.hpp"
#include "castor/io/biniostream.h"
#include "castor/rh/Server.hpp"
#include "castor/MessageAck.hpp"

#include <iostream>
#include <errno.h>
#include <sys/time.h>
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

  // Record the start time for statistical purposes
  timeval tv;
  gettimeofday(&tv, NULL);
  signed64 startTime = ((tv.tv_sec * 1000000) + tv.tv_usec);

  // We know it's a ServerSocket
  castor::io::ServerSocket* sock = (castor::io::ServerSocket*) param;
  castor::stager::Request* fr = 0;

  // Retrieve info on the client
  unsigned short port;
  unsigned long ip;
  try {
    sock->getPeerIp(port, ip);
  } catch(castor::exception::Exception e) {
    // "Exception caught : ignored"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 5, 2, params);
  }

  // Give a uuid to the request
  Cuuid_t cuuid = nullCuuid;
  Cuuid_create(&cuuid);
  char uuid[CUUID_STRING_LEN+1];
  uuid[CUUID_STRING_LEN] = 0;
  Cuuid2string(uuid, CUUID_STRING_LEN+1, &cuuid);

  // "New Request Arrival"
  castor::dlf::Param peerParams[] =
    {castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
     castor::dlf::Param("Port", port)};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 1, 2, peerParams);

  // Get the incoming request
  try {
    castor::IObject* obj = sock->readObject();
    fr = dynamic_cast<castor::stager::Request*>(obj);
    if (0 == fr) {
      delete obj;
      // "Invalid Request"
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 6);
      ack.setStatus(false);
      ack.setErrorCode(EINVAL);
      ack.setErrorMessage("Invalid Request object sent to server.");
    }
  } catch (castor::exception::Exception e) {
    // "Unable to read Request from socket"
    castor::dlf::Param params[] =
      {castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
       castor::dlf::Param("Port", port),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 7, 3, params);
    ack.setStatus(false);
    ack.setErrorCode(EINVAL);
    std::ostringstream stst;
    stst << "Unable to read Request object from socket."
	 << std::endl << e.getMessage().str();
    ack.setErrorMessage(stst.str());
  }

  // If the request comes from a secure connection then set Client values in
  // the Request object.
  bool secure = false;
  castor::io::AuthServerSocket* authSock =
    dynamic_cast<castor::io::AuthServerSocket*>(sock);
  if (authSock != 0) {
    fr->setEuid(authSock->getClientEuid());
    fr->setEgid(authSock->getClientEgid());
    secure = true;
  }

  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);

  // Process request body
  unsigned int nbThreads = 0;
  castor::rh::Client *client = 0;
  if (fr != 0) {
    client = dynamic_cast<castor::rh::Client *>(fr->client());
  }
  if (ack.status()) {
    try {
      fr->setReqId(uuid);

      // "Processing Request"
      castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, 8);

      // Complete its client field
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

      // Handle the request
      nbThreads = handleRequest(fr, ad, cuuid);
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
      // "Exception caught"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Standard Message", sstrerror(e.code())),
         castor::dlf::Param("Precise Message", e.getMessage().str())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 9, 2, params);
      ack.setStatus(false);
      ack.setErrorCode(e.code());
      ack.setErrorMessage(e.getMessage().str());
    }
  }

  // Send acknowledgement. If we fail, rollback the database transaction
  // otherwise commit the request to be processed by the stager
  try {
    sock->sendObject(ack);
    svcs()->commit(&ad);

    if (ack.status()) {
      sendNotification(fr, cuuid, nbThreads);

      // "Request stored in DB"
      castor::dlf::Param params[] =
	{castor::dlf::Param("ID", fr->id())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 12, 1, params);
    }

    // Calculate elapsed time
    timeval tv;
    gettimeofday(&tv, NULL);
    signed64 elapsedTime =
      (((tv.tv_sec * 1000000) + tv.tv_usec) - startTime);

    // "Reply sent to client"
    if (fr == 0) {
      castor::dlf::Param params2[] =
	{castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
	 castor::dlf::Param("Port", port),
	 castor::dlf::Param("ElapsedTime", elapsedTime * 0.000001)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_MONITORING, 10, 3, params2);
    } else {

      // If possible convert the request type to a string for logging
      // purposes
      std::ostringstream type;
      if ((fr->type() > 0) &&
	  ((unsigned int)fr->type() < castor::ObjectsIdsNb)) {
	type << castor::ObjectsIdStrings[fr->type()];
      } else {
	type << fr->type();
      }

      // Convert the client version to a string e.g 2010708 becomes
      // 2.1.7-8
      int version = client->version();
      std::ostringstream clientVersion;
      if (version < 2000000) {
	clientVersion << "Unknown";
      } else {
	int majorversion = version / 1000000;
	int minorversion = (version -= (majorversion * 1000000)) / 10000;
	int majorrelease = (version -= (minorversion * 10000)) / 100;
	int minorrelease = (version -= (majorrelease * 100));
	clientVersion << majorversion << "."
		      << minorversion << "."
		      << majorrelease << "-"
		      << minorrelease;
      }

      castor::dlf::Param params2[] =
	{castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
	 castor::dlf::Param("Type", type.str()),
	 castor::dlf::Param("Euid", fr->euid()),
	 castor::dlf::Param("Egid", fr->egid()),
	 castor::dlf::Param("SvcClass", fr->svcClassName()),
	 castor::dlf::Param("SubRequests", nbThreads),
	 castor::dlf::Param("ClientVersion", clientVersion.str()),
	 castor::dlf::Param("ElapsedTime", elapsedTime * 0.000001)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_MONITORING, 10, 8, params2);
    }
  } catch (castor::exception::Exception e) {
    // "Unable to send Ack to client"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 11, 2, params);

    // Rollback transaction
    try {
      if (ack.status()) {
	svcs()->rollback(&ad);
      }
    } catch (castor::exception::Exception e) {
      // "Exception caught : failed to rollback transaction"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Standard Message", sstrerror(e.code())),
	 castor::dlf::Param("Precise Message", e.getMessage().str())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 15, 2, params);
    }
  }

  delete fr;
  delete sock;
}


//------------------------------------------------------------------------------
// handleRequest
//------------------------------------------------------------------------------
unsigned int castor::rh::RHThread::handleRequest
(castor::stager::Request* fr,
 castor::BaseAddress ad,
 Cuuid_t cuuid)
  throw (castor::exception::Exception) {

  // Check access rights
  if (m_useAccessLists) {
    castor::rh::IRHSvc* m_rhSvc = 0;
    castor::IService* svc =
      castor::BaseObject::services()->service("DbRhSvc", castor::SVC_DBRHSVC);
    m_rhSvc = dynamic_cast<castor::rh::IRHSvc*>(svc);
    if (0 == m_rhSvc) {
      castor::exception::Internal ex;
      ex.getMessage() << "Couldn't load the request handler service, check the castor.conf for DynamicLib entries" << std::endl;
      throw ex;
    }
    m_rhSvc->checkPermission
      (fr->svcClassName(), fr->euid(), fr->egid(), fr->type());
  }

  // Number of subrequests (when applicable)
  unsigned int nbSubReqs = 1;

  // Store request into the DB
  try {
    svcs()->createRep(&ad, fr, false);

    // Store files for file requests
    castor::stager::FileRequest* filreq =
      dynamic_cast<castor::stager::FileRequest*>(fr);
    if (0 != filreq) {
      svcs()->fillRep(&ad, fr, OBJ_SubRequest, false);
      // And get number of subrequests
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

    // Store users and types for changePrivilege requests
    castor::bwlist::ChangePrivilege* cpReq =
      dynamic_cast<castor::bwlist::ChangePrivilege*>(fr);
    if (0 != cpReq) {
      svcs()->fillRep(&ad, cpReq, OBJ_BWUser, false);
      svcs()->fillRep(&ad, cpReq, OBJ_RequestType, false);
    }

  } catch (castor::exception::Exception e) {
    svcs()->rollback(&ad);
    throw e;
  }

  return nbSubReqs;
}


//------------------------------------------------------------------------------
// sendNotification
//------------------------------------------------------------------------------
void castor::rh::RHThread::sendNotification
(castor::stager::Request* fr, Cuuid_t cuuid, unsigned int nbThreads) {

  // Send an UDP message to the right stager service
  switch (fr->type()) {
  case OBJ_StageGetRequest:
  case OBJ_StagePutRequest:
  case OBJ_StageUpdateRequest:
    // JobRequest Service
    castor::server::BaseServer::sendNotification(m_stagerHost, m_stagerPort, 'J', nbThreads);
    break;
  case OBJ_StagePrepareToPutRequest:
  case OBJ_StagePrepareToGetRequest:
  case OBJ_StageRepackRequest:
  case OBJ_StagePrepareToUpdateRequest:
    // PrepareRequest Service
    castor::server::BaseServer::sendNotification(m_stagerHost, m_stagerPort, 'P', nbThreads);
    break;
  case OBJ_StageRmRequest:
  case OBJ_StagePutDoneRequest:
  case OBJ_SetFileGCWeight:
    // StagerRequest Service
    castor::server::BaseServer::sendNotification(m_stagerHost, m_stagerPort, 'S', nbThreads);
    break;
  case OBJ_StageFileQueryRequest:
  case OBJ_StageFindRequestRequest:
  case OBJ_StageRequestQueryRequest:
  case OBJ_DiskPoolQuery:
  case OBJ_VersionQuery:
  case OBJ_ChangePrivilege:
  case OBJ_ListPrivileges:
    // QueryRequest Service
    castor::server::BaseServer::sendNotification(m_stagerHost, m_stagerPort, 'Q');
    break;
  case OBJ_GetUpdateStartRequest:
  case OBJ_Disk2DiskCopyDoneRequest:
  case OBJ_Disk2DiskCopyStartRequest:
  case OBJ_MoverCloseRequest:
  case OBJ_PutStartRequest:
  case OBJ_GetUpdateDone:
  case OBJ_GetUpdateFailed:
  case OBJ_PutFailed:
  case OBJ_PutDoneStart:
  case OBJ_FirstByteWritten:
    // Job Service
    castor::server::BaseServer::sendNotification(m_stagerHost, m_stagerPort, 'j');
    break;
  case OBJ_Files2Delete:
  case OBJ_FilesDeleted:
  case OBJ_FilesDeletionFailed:
  case OBJ_NsFilesDeleted:
  case OBJ_StgFilesDeleted:
    // Garbage Collector Service
    castor::server::BaseServer::sendNotification(m_stagerHost, m_stagerPort, 'G');
    break;
  default:
    // We should not go this way, this would not be optimal
    // "Waked up all services at once"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", fr->type())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 13, 1, params);
    // XXX not supported for the time being in the new framework,
    // XXX to be seen if needed.
  }
}


