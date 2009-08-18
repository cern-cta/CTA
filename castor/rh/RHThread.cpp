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
 * @(#)$RCSfile: RHThread.cpp,v $ $Revision: 1.45 $ $Release$ $Date: 2009/08/18 09:42:53 $ $Author: waldron $
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include files
#include "getconfent.h"
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
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/rh/Client.hpp"
#include "castor/bwlist/ChangePrivilege.hpp"
#include "castor/io/biniostream.h"
#include "castor/MessageAck.hpp"
#include "castor/rh/Server.hpp"
#include "castor/rh/RHThread.hpp"

#include <iostream>
#include <errno.h>
#include <sys/time.h>
#include <unistd.h>


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::rh::RHThread::RHThread()
  throw (castor::exception::Exception) :
  BaseDbThread() {
  m_stagerHost = castor::PortsConfig::getInstance()->
    getHostName(castor::CASTOR_STAGER);
  m_stagerPort = castor::PortsConfig::getInstance()->
    getNotifPort(castor::CASTOR_STAGER);

  // Statically initialize the list of stager service handlers for each
  // request type.
  // XXX This knowledge is already in the db, cf. the Type2Obj table,
  // XXX a it would be better to use it but this requires a substantial
  // XXX change of how the Request Handler works.
  m_svcHandler[OBJ_StageGetRequest] = "JobReqSvc";
  m_svcHandler[OBJ_StagePutRequest] = "JobReqSvc";
  m_svcHandler[OBJ_StageUpdateRequest] = "JobReqSvc";
  m_svcHandler[OBJ_StagePrepareToGetRequest] = "PrepReqSvc";
  m_svcHandler[OBJ_StagePrepareToPutRequest] = "PrepReqSvc";
  m_svcHandler[OBJ_StagePrepareToUpdateRequest] = "PrepReqSvc";
  m_svcHandler[OBJ_StageRepackRequest] = "PrepReqSvc";
  m_svcHandler[OBJ_StagePutDoneRequest] = "StageReqSvc";
  m_svcHandler[OBJ_StageRmRequest] = "StageReqSvc";
  m_svcHandler[OBJ_SetFileGCWeight] = "StageReqSvc";
  m_svcHandler[OBJ_StageFileQueryRequest] = "Q";
  m_svcHandler[OBJ_DiskPoolQuery] = "Q";
  m_svcHandler[OBJ_VersionQuery] = "Q";
  m_svcHandler[OBJ_ChangePrivilege] = "Q";
  m_svcHandler[OBJ_ListPrivileges] = "Q";
  m_svcHandler[OBJ_GetUpdateStartRequest] = "j";
  m_svcHandler[OBJ_Disk2DiskCopyDoneRequest] = "j";
  m_svcHandler[OBJ_Disk2DiskCopyStartRequest] = "j";
  m_svcHandler[OBJ_MoverCloseRequest] = "j";
  m_svcHandler[OBJ_PutStartRequest] = "j";
  m_svcHandler[OBJ_GetUpdateDone] = "j";
  m_svcHandler[OBJ_GetUpdateFailed] = "j";
  m_svcHandler[OBJ_PutFailed] = "j";
  m_svcHandler[OBJ_PutDoneStart] = "j";
  m_svcHandler[OBJ_FirstByteWritten] = "j";
  m_svcHandler[OBJ_Files2Delete] = "G";
  m_svcHandler[OBJ_FilesDeleted] = "G";
  m_svcHandler[OBJ_FilesDeletionFailed] = "G";
  m_svcHandler[OBJ_NsFilesDeleted] = "G";
  m_svcHandler[OBJ_StgFilesDeleted] = "G";

  // Read the list of SRM trusted hosts. Requests coming from
  // these hosts are entitled to override the request UUID.
  char** values;
  int count;
  if(0 == getconfent_multi((char *)castor::rh::CATEGORY_CONF, "SRMHostsList",
                           1, &values, &count)) {
    for (int i = 0; i < count; i++) {
      m_srmHostList.push_back(values[i]);
      free(values[i]);
    }
    free(values);
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::rh::RHThread::~RHThread() throw ()
{
  /*
    This empty destructor has to be implemented here and NOT inline,
    otherwise the following error will happen at linking time:

    RHThread.o(.text+0x469): In function `castor::rh::RHThread::RHThread(bool)':
    .../castor/rh/RHThread.cpp:61: undefined reference to `VTT for castor::rh::RHThread'

    See e.g. http://www.daniweb.com/forums/thread114299.html for more details.
  */
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

  // Give a uuid to the request (this might be overridden later)
  Cuuid_t cuuid = nullCuuid;
  Cuuid_create(&cuuid);
  char uuid[CUUID_STRING_LEN+1];
  uuid[CUUID_STRING_LEN] = 0;
  Cuuid2string(uuid, CUUID_STRING_LEN+1, &cuuid);

  castor::dlf::Param peerParams[] =
    {castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
     castor::dlf::Param("Port", port)};

  // If the request comes from a secure connection then establish the context &
  // map the user.
  bool secure = false;
  std::string client_mech = "Unsecure";
  castor::io::AuthServerSocket* authSock;
  try {
    authSock = dynamic_cast<castor::io::AuthServerSocket*>(sock);
    if (authSock != 0) {
      authSock->initContext();
      authSock->setClientId();
      client_mech = authSock->getSecMech();
      secure = true;
    }

    // Get the incoming request
    castor::IObject* obj = sock->readObject();
    fr = dynamic_cast<castor::stager::Request*>(obj);
    if (0 == fr) {
      delete obj;
      // "Invalid Request"
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 6, 2, peerParams);
      ack.setStatus(false);
      ack.setErrorCode(EINVAL);
      ack.setErrorMessage("Invalid Request object sent to server.");
    }
  }
  catch (castor::exception::Security e) {
    // "Security problem"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str()),
       castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
       castor::dlf::Param("Port", port)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 17, 4, params);
    ack.setStatus(false);
    ack.setErrorCode(e.code());
    ack.setErrorMessage(e.getMessage().str());
  }
  catch (castor::exception::Exception e) {
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
  if (secure && fr != 0) {
    fr->setEuid(authSock->getClientEuid());
    fr->setEgid(authSock->getClientEgid());
    fr->setUserName(authSock->getClientMappedName());
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
      // Look for the client host: if it is SRM, then
      // try to reuse the user tag as a UUID. This enables full tracing
      // of requests coming from SRM. In any case ignore any failure.
      if(find(m_srmHostList.begin(), m_srmHostList.end(), fr->machine())
         != m_srmHostList.end() &&
         0 == string2Cuuid(&cuuid, fr->userTag().c_str())) {
        strncpy(uuid, fr->userTag().c_str(), CUUID_STRING_LEN);
      }
      fr->setReqId(uuid);

      // Log now "New Request Arrival" with the resolved UUID
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 1, 2, peerParams);

      // Complete its client field
      if (0 == client) {
        delete fr;
        castor::exception::Internal e;
        e.getMessage() << "Request arrived with no client object.";
        throw e;
      }
      client->setIpAddress(ip);
      if (secure) {
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
      // notify the appropriate stager service
      std::map<int, std::string>::const_iterator it = m_svcHandler.find(fr->type());
      if (it != m_svcHandler.end()) {
        castor::server::BaseServer::sendNotification
          (m_stagerHost, m_stagerPort, it->second[0], nbThreads);
      }
      // else don't do anything (should never happen)
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
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 10, 3, params2);
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
         castor::dlf::Param("SecurityMechanism", client_mech),
         castor::dlf::Param("ElapsedTime", elapsedTime * 0.000001)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 10, 9, params2);
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

  if(fr->svcClass()) {
    // this object has been allocated as part of the checkPermission() call
    // and needs to be deallocated by hand here
    delete fr->svcClass();
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
  castor::rh::IRHSvc* m_rhSvc = 0;
  castor::IService* svc =
    castor::BaseObject::services()->service("DbRhSvc", castor::SVC_DBRHSVC);
  m_rhSvc = dynamic_cast<castor::rh::IRHSvc*>(svc);
  if (0 == m_rhSvc) {
    castor::exception::Internal ex;
    ex.getMessage() << "Couldn't load the request handler service, check the castor.conf for DynamicLib entries" << std::endl;
    throw ex;
  }
  m_rhSvc->checkPermission(fr);

  // Number of subrequests (when applicable)
  unsigned int nbSubReqs = 1;

  // Store request into the DB
  try {
    svcs()->createRep(&ad, fr, false);
    if(fr->svcClass()) {
      svcs()->fillRep(&ad, fr, OBJ_SvcClass, false);
    }

    // Store files for file requests
    castor::stager::FileRequest* filreq =
      dynamic_cast<castor::stager::FileRequest*>(fr);
    if (0 != filreq) {
      // And get number of subrequests
      nbSubReqs = filreq->subRequests().size();
      for(unsigned i = 0; i < nbSubReqs; i++) {
        filreq->subRequests()[i]->setSvcHandler(m_svcHandler[filreq->type()]);
      }
      svcs()->fillRep(&ad, fr, OBJ_SubRequest, false);
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
