/******************************************************************************
 *                      BaseClient.cpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * A base class for all castor clients. Implements many
 * useful and common parts for all clients.
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <errno.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/poll.h>
#include <sys/stat.h>
#include <Cpwd.h>
#include "castor/io/ClientSocket.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/Constants.hpp"
#include "castor/IClient.hpp"
#include "castor/IObject.hpp"
#include "castor/MessageAck.hpp"
#include "castor/rh/Client.hpp"
#include "castor/rh/Request.hpp"
#include "castor/rh/Response.hpp"
#include "castor/client/IResponseHandler.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "stage_api.h"
extern "C" {
#include <Cgetopt.h>
#include <common.h>
}

// Local includes
#include "BaseClient.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::client::BaseClient::BaseClient() throw() :
  BaseObject(), m_callbackSocket(0), m_rhPort(-1) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::client::BaseClient::~BaseClient() throw() {
  delete m_callbackSocket;
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::client::BaseClient::sendRequest
(castor::rh::Request* req,
 castor::client::IResponseHandler* rh)
  throw(castor::exception::Exception) {
  // Now the common part of the request
  // Request handler host and port
  setRhHost();
  setRhPort();
  // Uid
  uid_t euid;
  if (0 != getenv("STAGE_EUID")) {
    euid = atoi(getenv("STAGE_EUID"));
  } else {
    euid = geteuid();
  }
  req->setEuid(euid);
  // GID
  uid_t egid;
  if (0 != getenv("STAGE_EGID")) {
    egid = atoi(getenv("STAGE_EGID"));
  } else {
    egid = getegid();
  }
  req->setEgid(egid);
  // Username
  errno = 0;
  struct passwd *pw = Cgetpwuid(euid);
  if (0 == pw) {
    std::cout << "Unknown user " << euid << std::endl;
    return;
  } else {
    req->setUserName(pw->pw_name);
  }
  // Mask
  mode_t mask = umask(0);
  umask(mask);
  req->setMask(mask);
  // Pid
  req->setPid(getpid());
  // Machine
  {
    // All this to get the hostname, thanks to C !
    int len = 64;
    char* hostname;
    hostname = (char*) calloc(len, 1);
    if (gethostname(hostname, len) < 0) {
      // Test whether error is due to a name too long
      // The errno depends on the glibc version
      if (EINVAL != errno &&
          ENAMETOOLONG != errno) {
        clog() << "Unable to get hostname : "
               << strerror(errno) << std::endl;
        free(hostname);
        return;
      }
      // So the name was too long
      while (hostname[len - 1] != 0) {
        len *= 2;
        char *hostnameLonger = (char*) realloc(hostname, len);
        if (0 == hostnameLonger) {
          clog() << "Unable to allocate memory for hostname."
                 << std::endl;
          free(hostname);
          return;
        }
        hostname = hostnameLonger;
        memset(hostname, 0, len);
        if (gethostname(hostname, len) < 0) {
          // Test whether error is due to a name too long
          // The errno depends on the glibc version
          if (EINVAL != errno &&
              ENAMETOOLONG != errno) {
            clog() << "Unable to get hostname : "
                   << strerror(errno) << std::endl;
            free(hostname);
            return;
          }
        }
      }
    }
    req->setMachine(hostname);
    if (m_rhHost == "") {
      m_rhHost = hostname;
    }
    free(hostname);
  }
  // create a socket for the callback
  m_callbackSocket = new castor::io::ServerSocket(0, true);
  m_callbackSocket->listen();
  unsigned short port;
  unsigned long ip;
  m_callbackSocket->getPortIp(port, ip);
  clog() << DEBUG << "Opened callback socket on port "
         << port << std::endl;
  m_rhPort = port;
  // set the Client
  castor::IClient *cl = createClient();
  cl->setRequest(req);
  req->setClient(cl);
  // sends the request
  internalSendRequest(*req);
  // waits for callbacks
  bool stop = false;
  while (!stop) {
    IObject* result = waitForCallBack();
    if (OBJ_EndResponse == result->type()) {
      // flush messages
      clog() << std::flush;
      // terminate response handler
      rh->terminate();
      stop = true;
    } else {
      // cast response into Response*
      castor::rh::Response* res =
        dynamic_cast<castor::rh::Response*>(result);
      if (0 == res) {
        castor::exception::Internal e;
        e.getMessage() << "Receive bad response type :"
                       << result->type();
        delete res;
        throw e;
      }
      // Print the request
      rh->handleResponse(*res);
    }
    // delete the result
    delete result;
  }
}

//------------------------------------------------------------------------------
// createClient
//------------------------------------------------------------------------------
castor::IClient* castor::client::BaseClient::createClient()
  throw (castor::exception::Exception) {
  // if no callbackSocket
  if (0 == m_callbackSocket) {
    castor::exception::Internal ex;
    ex.getMessage() << "No call back socket available";
    throw ex;
  }
  // build client
  unsigned short port;
  unsigned long ip;
  m_callbackSocket->getPortIp(port, ip);
  castor::rh::Client *result = new castor::rh::Client();
  result->setIpAddress(ip);
  result->setPort(port);
  return result;
}

//------------------------------------------------------------------------------
// sendRequest
//------------------------------------------------------------------------------
void castor::client::BaseClient::internalSendRequest(castor::rh::Request& request)
  throw (castor::exception::Exception) {
  // creates a socket
  castor::io::ClientSocket s(RHSERVER_PORT, m_rhHost);
  s.connect();
  // sends the request
  s.sendObject(request);
  // wait for acknowledgment
  IObject* obj = s.readObject();
  castor::MessageAck* ack =
    dynamic_cast<castor::MessageAck*>(obj);
  if (0 == ack) {
    castor::exception::InvalidArgument e; // XXX To be changed
    e.getMessage() << "No Acknowledgement from the Server";
    delete ack;
    throw e;
  }
  if (!ack->status()) {
    castor::exception::InvalidArgument e; // XXX To be changed
    e.getMessage() << "Server Error "
                   << ack->errorCode() << " :" << std::endl
                   << ack->errorMessage();
    delete ack;
    throw e;
  }
  delete ack;
}

//------------------------------------------------------------------------------
// waitForCallBack
//------------------------------------------------------------------------------
castor::IObject* castor::client::BaseClient::waitForCallBack()
  throw (castor::exception::Exception) {

  int rc, nonblocking;

  rc = ioctl(m_callbackSocket->socket(),FIONBIO,&nonblocking);
  if (rc == SOCKET_ERROR) {
    castor::exception::InvalidArgument e; // XXX To be changed
    e.getMessage() << "Could not set socket asynchonous";
    throw e;
  }

  struct pollfd pollit;
  int timeout = 1800; // In seconds, half an hour

  pollit.fd = m_callbackSocket->socket();
  pollit.events = POLLIN;
  pollit.revents = 0;

  /* Will return > 0 if the descriptor is readable */
  rc = poll(&pollit,1,timeout*1000);
  if (0 == rc) {
    castor::exception::InvalidArgument e; // XXX To be changed
    e.getMessage() << "Accept timeout";
    throw e;
  } else if (rc < 0) {
    castor::exception::InvalidArgument e; // XXX To be changed
    e.getMessage() << "Poll error";
    throw e;
  }

  castor::io::ServerSocket* s = m_callbackSocket->accept();
  IObject* obj = s->readObject();
  delete s;
  return obj;
}

//------------------------------------------------------------------------------
// setRhPort
//------------------------------------------------------------------------------
void castor::client::BaseClient::setRhPort()
  throw (castor::exception::Exception) {
  char* port;
  // RH server port. Can be given through the environment
  // variable RH_PORT or in the castor.conf file as a
  // RH/PORT entry. If none is given, default is used
  if ((port = getenv ("RH_PORT")) != 0 ||
      (port = getconfent("RH","PORT",0)) != 0) {
    int iport;
    char* dp;
    if (stage_strtoi(&iport, port, &dp, 0) != 0) {
      castor::exception::Exception e(errno);
      e.getMessage() << "Bad port value." << std::endl;
      throw e;
    }
    if (iport < 0) {
      castor::exception::Exception e(errno);
      e.getMessage()
        << "Invalid port value : " << iport
        << ". Must be > 0." << std::endl;
      throw e;
    }
    m_rhPort = iport;
  } else {
    clog() << "Contacting RH server on default port ("
           << CSP_RHSERVER_PORT << ")." << std::endl;
    m_rhPort = CSP_RHSERVER_PORT;
  }
}

//------------------------------------------------------------------------------
// setRhHost
//------------------------------------------------------------------------------
void castor::client::BaseClient::setRhHost()
  throw (castor::exception::Exception) {
  // RH server host. Can be passed given through the
  // RH_HOST environment variable or in the castor.conf
  // file as a RH/HOST entry
  char* host;
  if ((host = getenv ("RH_HOST")) != 0 ||
      (host = getconfent("RH","HOST",0)) != 0) {
    m_rhHost = host;
  } else {
    castor::exception::Exception e(ETPRM);
    e.getMessage()
      << "Unable to deduce the name of the RH server.\n"
      << "No -h option was given, RH_HOST is not set and "
      << "your castor.conf file does not contain a RH/HOST entry."
      << std::endl;
    throw e;
  }
}

