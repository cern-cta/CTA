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
 * @(#)$RCSfile: Server.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2004/06/01 15:33:28 $ $Author: sponcec3 $
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

#include "castor/io/Socket.hpp"
#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/BaseAddress.hpp"

#include "castor/rh/Request.hpp"
#include "castor/rh/Client.hpp"

#include "castor/io/biniostream.h"

#include "castor/rh/Server.hpp"
#include "castor/MsgSvc.hpp"
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
    std::cout << "Starting Request Handler" << std::endl;
    castor::rh::Server server;
    server.parseCommandLine(argc, argv);
    std::cout << "Command line parsed, now starting" << std::endl;
    server.start();
    return 0;
  } catch (castor::exception::Exception e) {
    std::cout << "Caught exception : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
  }catch (...) {
    std::cout << "Caught exception!" << std::endl;
  }
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::rh::Server::Server() :
  castor::BaseServer("RHServer", 20) { }

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::rh::Server::main () {
  // create oracle conversion service
  castor::ICnvSvc *svc =
    svcs()->cnvService("OraCnvSvc", castor::SVC_ORACNV);
  if (svc == 0) {
    clog() << "Could not get Conversion Service for Oracle"
           << std::endl;
    return -1;
  }
  /* Create a socket for the server, bind, and listen */
  castor::io::Socket sock(CSP_RHSERVER_PORT, true);
  try {
    sock.reusable();
  } catch(castor::exception::Exception e) {
    clog() << sstrerror(e.code())
           << e.getMessage().str() << std::endl;
  }
  
  
  /* Accept connexions */
  for (;;) {
    castor::io::Socket* s = sock.accept();
    /* handle the command. */
    threadAssign(s);
  }
}

//------------------------------------------------------------------------------
// processRequest
//------------------------------------------------------------------------------
void *castor::rh::Server::processRequest(void *param) throw() {
  // get the incoming request
  castor::io::Socket* sock = (castor::io::Socket*) param;
  castor::IObject* obj;
  try {
    obj = sock->readObject();
  } catch (castor::exception::Exception e) {
    clog() << "Could not get incoming request : "
           << sstrerror(e.code()) << std::endl
           << e.getMessage().str() << std::endl;
  }
  castor::rh::Request* fr =
    dynamic_cast<castor::rh::Request*>(obj);

  clog() << " Processing request" << std::endl;

  MessageAck ack;
  try {
    // Complete its client field
    unsigned short port;
    unsigned long ip;
    try {
      sock->getPeerIp(port, ip);
    } catch(castor::exception::Exception e) {
      clog() << "Exception :" << sstrerror(e.code())
             << std::endl << e.getMessage() << std::endl;
    }

    clog() << " Got request from client "
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
  svcs()->createRep(&ad, fr, true);
  clog() << " request stored in Oracle, id "
         << fr->id() << std::endl;
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

