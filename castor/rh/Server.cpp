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
 * @(#)$RCSfile: Server.cpp,v $ $Revision: 1.15 $ $Release$ $Date: 2004/11/02 15:25:40 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <signal.h>
#include <errno.h>

#include "castor/io/ServerSocket.hpp"
#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/BaseAddress.hpp"

#include "castor/stager/Request.hpp"
#include "castor/rh/Client.hpp"

#include "castor/io/biniostream.h"

#include "castor/rh/Server.hpp"
#include "castor/MessageAck.hpp"

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
  castor::BaseServer("RHServer", 2) {
  initLog("RHLog", SVC_STDMSG);
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::rh::Server::main () {
  try {
    // create oracle conversion service
    castor::ICnvSvc *svc =
      svcs()->cnvService("OraCnvSvc", castor::SVC_ORACNV);
    if (svc == 0) {
      clog() << "Could not get Conversion Service for Oracle"
             << std::endl;
      return -1;
    }
    /* Create a socket for the server, bind, and listen */
    castor::io::ServerSocket sock(CSP_RHSERVER_PORT, true);  
    /* Accept connexions */
    for (;;) {
      castor::io::ServerSocket* s = sock.accept();
      /* handle the command. */
      threadAssign(s);
    }
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
        castor::exception::Internal e;
        e.getMessage() << "Request arrived with no client object.";
        throw e;
      }
      client->setIpAddress(ip);
      
      // handle the request
      handleRequest(fr);
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

  delete sock;
  return 0;
}

//------------------------------------------------------------------------------
// handleRequest
//------------------------------------------------------------------------------
void castor::rh::Server::handleRequest(castor::IObject* fr)
  throw (castor::exception::Exception) {
  // Stores it into Oracle
  castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
  try {
    svcs()->createRep(&ad, fr, false);
    svcs()->fillRep(&ad, fr, OBJ_SubRequest, false);
    castor::stager::Request* req =
      dynamic_cast<castor::stager::Request*>(fr);
    if (0 != fr) {
      svcs()->createRep(&ad, req->client(), false);
      svcs()->fillRep(&ad, fr, OBJ_IClient, true);
      svcs()->fillRep(&ad, req->client(), OBJ_Request, true);
    }
    clog() << "request stored in Oracle, id "
           << fr->id() << std::endl;
  } catch (castor::exception::Exception e) {
    svcs()->rollback(&ad);
    throw e;
  }

  // Send an UDP message to the stager
  int fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) {
    clog() << "Unable to open socket for UDP notification"
           << std::endl;
    return;
  }
  struct sockaddr_in serverAddress;
  serverAddress.sin_family = AF_INET;
  serverAddress.sin_addr.s_addr = htonl(INADDR_ANY);
  serverAddress.sin_port = htons(CSP_NOTIFICATION_PORT);
  castor::io::biniostream buf;
  buf << CSP_MSG_MAGIC << fr->id();
  if (sendto (fd, buf.str().data(), buf.str().length(),
              0, (struct sockaddr *)&serverAddress,
              sizeof(serverAddress)) < 0) {
    clog() << "Unable to send UDP notification"
           << std::endl;
    close(fd);
    return;
  }
  //  clog() << "UDP notification sent" << std::endl;
  close(fd);
}

//------------------------------------------------------------------------------
// Overriding start() to add logging
//------------------------------------------------------------------------------
/*int castor::rh::Server::start() {
  clog() << "Now starting the threads" << std::endl;
  return this->BaseServer::start();
  }*/

