/******************************************************************************
 *                      Server.cpp
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
 * @(#)$RCSfile: Server.cpp,v $ $Revision: 1.41 $ $Release$ $Date: 2005/09/07 16:39:24 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "errno.h"
#include "Cuuid.h"
#include <signal.h>

#include "castor/io/ServerSocket.hpp"
#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/BaseAddress.hpp"

#include "castor/stager/Request.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/QryRequest.hpp"
#include "castor/stager/GCFileList.hpp"
#include "castor/rh/Client.hpp"

#include "castor/io/biniostream.h"

#include "castor/rh/Server.hpp"
#include "castor/MessageAck.hpp"

#include "h/stager_service_api.h"

#include <iostream>

//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------
int main(int argc, char *argv[]) {

  // First ignoring SIGPIPE and SIGXFSZ 
  // to avoid crashing when a file is too big or
  // when the connection is lost with a client
#if ! defined(_WIN32)
	signal (SIGPIPE,SIG_IGN);
	signal (SIGXFSZ,SIG_IGN);
#endif


  try {
    castor::rh::Server server;
    server.parseCommandLine(argc, argv);
    std::cout << "Starting Request Handler" << std::endl;
    server.start();
    return 0;
  } catch (castor::exception::Exception e) {
    std::cerr << "Caught exception : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
  } catch (...) {
    std::cerr << "Caught exception!" << std::endl;
  }
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::rh::Server::Server() :
  castor::BaseServer("RHServer", 20) {
  initLog("RHLog", SVC_DLFMSG);
  // Initializes the DLF logging. This includes
  // defining the predefined messages
  castor::dlf::Message messages[] =
    {{ 0, " - "},
     { 1, "New Request Arrival"},
     { 2, "Could not get Conversion Service for Database"},
     { 3, "Could not get Conversion Service for Streaming"},
     { 4, "Exception caught : server is stopping"},
     { 5, "Exception caught : ignored"},
     { 6, "Invalid Request"},
     { 7, "Unable to read Request from socket"},
     { 8, "Processing Request"},
     { 9, "Exception caught"},
     {10, "Sending reply to client"},
     {11, "Unable to send Ack to client"},
     {12, "Request stored in DB"},
     {13, "Waked up all services at once"},
     {-1, ""}};
  castor::dlf::dlf_init("RHLog", messages);
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::rh::Server::main () {
  try {
    // create oracle and streaming conversion service
    // so that it is not deleted and recreated all the time
    castor::ICnvSvc *svc =
      svcs()->cnvService("DbCnvSvc", castor::SVC_DBCNV);
    if (0 == svc) {
      // "Could not get Conversion Service for Database" message
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2);
      return -1;
    }
    castor::ICnvSvc *svc2 =
      svcs()->cnvService("StreamCnvSvc", castor::SVC_STREAMCNV);
    if (0 == svc2) {
      // "Could not get Conversion Service for Streaming" message
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 3);
      return -1;
    }
    /* Create a socket for the server, bind, and listen */
    castor::io::ServerSocket sock(CSP_RHSERVER_PORT, true);  
    for (;;) {
      /* Accept connexions */
      castor::io::ServerSocket* s = sock.accept();
      /* handle the command. */
      threadAssign(s);
    }
    
    // The code after this pint will never be used.
    // However it can be useful to cleanup evrything correctly
    // if one removes the loop for debug purposes

    // release the Oracle Conversion Service
    svc->release();
    svc2->release();
    
  } catch(castor::exception::Exception e) {
    // "Exception caught : server is stopping" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 4, 2, params);
  }
}

//------------------------------------------------------------------------------
// processRequest
//------------------------------------------------------------------------------
void *castor::rh::Server::processRequest(void *param) throw() {
  MessageAck ack;
  ack.setStatus(true);

  // We know it's a ServerSocket
  castor::io::ServerSocket* sock =
    (castor::io::ServerSocket*) param;
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
  castor::dlf::Param params[] =
    {castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
     castor::dlf::Param("Port", port)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 2, params);

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
      castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 8);

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
      
      // handle the request
      handleRequest(fr, cuuid);
      ack.setRequestId(uuid);
      ack.setStatus(true);
      
    } catch (castor::exception::Exception e) {
    // "Exception caught" message
      castor::dlf::Param params[] =
        {castor::dlf::Param("Standard Message", sstrerror(e.code())),
         castor::dlf::Param("Precise Message", e.getMessage().str())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 9, 2, params);
      ack.setStatus(false);
      ack.setErrorCode(1);
      ack.setErrorMessage(e.getMessage().str());
    }
  }

  // "Sending reply to client" message
  castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 10);
  try {
    sock->sendObject(ack);
  } catch (castor::exception::Exception e) {
    // "Unable to send Ack to client" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 11, 2, params);
  }

  delete fr;
  delete sock;
  return 0;
}

//------------------------------------------------------------------------------
// handleRequest
//------------------------------------------------------------------------------
void castor::rh::Server::handleRequest
(castor::IObject* fr, Cuuid_t cuuid)
  throw (castor::exception::Exception) {
  // Number of subrequests (when applicable)
  unsigned int nbSubReqs = 1;
  // Stores it into Oracle
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
    castor::dlf::Param params[] =
      {castor::dlf::Param("ID", fr->id())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 12, 1, params);

  } catch (castor::exception::Exception e) {
    svcs()->rollback(&ad);
    throw e;
  }

  // Send an UDP message to the right stager service
  switch (fr->type()) {
  case OBJ_StageGetRequest:
  case OBJ_StagePrepareToGetRequest:
  case OBJ_StagePutRequest:
  case OBJ_StagePrepareToPutRequest:
  case OBJ_StageUpdateRequest:
  case OBJ_StagePrepareToUpdateRequest:
  case OBJ_StageRmRequest:
  case OBJ_StagePutDoneRequest:
    // Query Service
    stager_notifyService_v2(::STAGER_SERVICE_DB, nbSubReqs);
    break;
  case OBJ_StageFileQueryRequest :
  case OBJ_StageFindRequestRequest :
  case OBJ_StageRequestQueryRequest :
    // Query Service
    stager_notifyService(::STAGER_SERVICE_QUERY);
    break;
  case OBJ_GetUpdateStartRequest:
  case OBJ_Disk2DiskCopyDoneRequest:
  case OBJ_MoverCloseRequest:
  case OBJ_PutStartRequest :
  case OBJ_GetUpdateDone :
  case OBJ_GetUpdateFailed :
  case OBJ_PutFailed :
  case OBJ_PutDoneStart :
    // Job Service
    stager_notifyService(::STAGER_SERVICE_JOB);
    break;
  case OBJ_Files2Delete:
  case OBJ_FilesDeleted:
  case OBJ_FilesDeletionFailed:
    // Garbage Collector Service
    stager_notifyService(::STAGER_SERVICE_GC);
    break;
  default:
    // We should not go this way, this would not be optimal
    // "Waked up all services at once"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Request type", fr->type())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 13, 1, params);
    stager_notifyServices();
  }
}
