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
 * @(#)$RCSfile: Server.cpp,v $ $Revision: 1.24 $ $Release$ $Date: 2004/12/13 15:30:52 $ $Author: jdurand $
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
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::rh::Server::main () {
  try {
    // create oracle and streaming conversion service
    // so that it is not deleted and recreated all the time
    castor::ICnvSvc *svc =
      svcs()->cnvService("OraCnvSvc", castor::SVC_ORACNV);
    if (0 == svc) {
      clog() << "Could not get Conversion Service for Oracle"
             << std::endl;
      return -1;
    }
    castor::ICnvSvc *svc2 =
      svcs()->cnvService("StreamCnvSvc", castor::SVC_STREAMCNV);
    if (0 == svc2) {
      clog() << "Could not get Conversion Service for Streaming"
             << std::endl;
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
    clog() << sstrerror(e.code())
           << e.getMessage().str() << std::endl;
  }
}

//------------------------------------------------------------------------------
// processRequest
//------------------------------------------------------------------------------
void *castor::rh::Server::processRequest(void *param) throw() {
  MessageAck ack;
  ack.setStatus(true);

  // get the incoming request
  castor::io::ServerSocket* sock =
    (castor::io::ServerSocket*) param;
  castor::stager::Request* fr = 0;
  try {
    castor::IObject* obj = sock->readObject();
    fr = dynamic_cast<castor::stager::Request*>(obj);
    if (0 == fr) {
      delete obj;
      clog() << ERROR
             << "Client did not send a valid Request object."
             << std::endl;
      ack.setStatus(false);
      ack.setErrorCode(EINVAL);
      ack.setErrorMessage("Invalid Request object sent to server.");
    }
  } catch (castor::exception::Exception e) {
    std::ostringstream stst;
    stst << "Unable to read Request object from socket."
         << std::endl << e.getMessage().str();
    clog() << ERROR << stst.str() << std::endl;
    ack.setStatus(false);
    ack.setErrorCode(EINVAL);
    ack.setErrorMessage(stst.str());
  }

  if (ack.status()) {
    clog() << INFO << "Processing request" << std::endl;
    try {
      // Complete its client field
      unsigned short port;
      unsigned long ip;
      try {
        sock->getPeerIp(port, ip);
      } catch(castor::exception::Exception e) {
        clog() << ERROR << "Exception :" << sstrerror(e.code())
               << std::endl << e.getMessage() << std::endl;
      }
      
      clog() << INFO << "Got request from client "
             << castor::ip << ip << ":" << port << std::endl;
      castor::rh::Client *client =
        dynamic_cast<castor::rh::Client *>(fr->client());
      if (0 == client) {
        delete fr;
        castor::exception::Internal e;
        e.getMessage() << "Request arrived with no client object.";
        throw e;
      }
      client->setIpAddress(ip);
      
      // handle the request (and give a Cuuid to it)
      // XXX Interface to Cuuid has to be improved !
      // XXX its length has currently yo be hardcoded
      // XXX wherever you use it !!!
      Cuuid_t cuuid;
      Cuuid_create(&cuuid);
      char uuid[CUUID_STRING_LEN+1];
      uuid[CUUID_STRING_LEN] = 0;
      Cuuid2string(uuid, CUUID_STRING_LEN, &cuuid);
      fr->setReqId(uuid);
      handleRequest(fr);
      ack.setRequestId(uuid);
      ack.setStatus(true);
      
    } catch (castor::exception::Exception e) {
      clog() << "Exception : " << sstrerror(e.code())
             << std::endl << e.getMessage().str() << std::endl;
      ack.setStatus(false);
      ack.setErrorCode(1);
      ack.setErrorMessage(e.getMessage().str());
    }
  }

  clog() << "Sending reply to client !" << std::endl;
  try {
    sock->sendObject(ack);
  } catch (castor::exception::Exception e) {
    clog() << "Could not send ack to client : "
           << sstrerror(e.code()) << std::endl
           << e.getMessage().str() << std::endl;
  }
  delete fr;
  delete sock;
  return 0;
}

//------------------------------------------------------------------------------
// handleRequest
//------------------------------------------------------------------------------
void castor::rh::Server::handleRequest(castor::IObject* fr)
  throw (castor::exception::Exception) {
  // Stores it into Oracle
  castor::BaseAddress ad;
  ad.setCnvSvcName("OraCnvSvc");
  ad.setCnvSvcType(castor::SVC_ORACNV);
  try {
    svcs()->createRep(&ad, fr, false);
    castor::stager::FileRequest* filreq =
      dynamic_cast<castor::stager::FileRequest*>(fr);
    if (0 != filreq) {
      svcs()->fillRep(&ad, fr, OBJ_SubRequest, false);
    }
    castor::stager::Request* req =
      dynamic_cast<castor::stager::Request*>(fr);
    if (0 != req) {
      svcs()->createRep(&ad, req->client(), false);
      svcs()->fillRep(&ad, fr, OBJ_IClient, false);
    }
    castor::stager::updateRepRequest* urReq =
      dynamic_cast<castor::stager::UpdateRepRequest*>(fr);
    if (0 != urReq) {
      svcs()->createRep(&ad, urReq->object(), false);
      svcs()->fillRep(&ad, urReq, OBJ_IObject, false);
      svcs()->createRep(&ad, urReq->address(), false);
      svcs()->fillRep(&ad, urReq, OBJ_IAddress, false);
    }
    svcs()->commit();
    clog() << "request stored in Oracle, id "
           << fr->id() << std::endl;
  } catch (castor::exception::Exception e) {
    svcs()->rollback(&ad);
    throw e;
  }

  // Send an UDP message to the stager
  stager_notifyServices();

}
