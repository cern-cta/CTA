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
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <pwd.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/poll.h>
#include <sys/utsname.h>
#include <sstream>
#include "castor/io/Socket.hpp"
#include "castor/IClient.hpp"
#include "castor/IObject.hpp"
#include "castor/MessageAck.hpp"
#include "castor/rh/Client.hpp"
#include "castor/rh/Request.hpp"
#include "castor/Services.hpp"
extern "C" {
#include <Cgetopt.h>
}

// Local includes
#include "BaseClient.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::client::BaseClient::BaseClient() throw() :
  BaseObject(), m_rhPort(0), m_callbackSocket(0) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::client::BaseClient::~BaseClient() throw() {
  delete m_callbackSocket;
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::client::BaseClient::run(int argc, char** argv)
  throw() {
  try {
    // parses the command line
    parseInput(argc, argv);
    // builds a request
    castor::rh::Request* req = buildRequest();
    // create a socket for the callback
    m_callbackSocket = new castor::io::Socket(0, true);
    unsigned short port;
    unsigned long ip;
    m_callbackSocket->getPortIp(port, ip);
    clog() << "Opened callback socket on port "
           << port << std::endl;
    // set the Client
    castor::IClient *cl = createClient();
    cl->setRequest(req);
    req->setClient(cl);
    // sends the request
    sendRequest(*req);
    delete req;
    // waits for a callback
    IObject* result = waitForCallBack();
    // Print the request
    printResult(*result);
    // delete the result
    delete result;
  } catch (Exception ex) {
    clog() << ex.getMessage().str();
  }
}

//------------------------------------------------------------------------------
// parseInput
//------------------------------------------------------------------------------
void castor::client::BaseClient::parseInput(int argc, char** argv)
  throw (castor::Exception) {
  Coptind = 1;    /* Required */
  Copterr = 1;    /* Some stderr output if you want */
  Coptreset = 1;  /* In case we are parsing several times the same argv */
  int ch;
  while ((ch = Cgetopt(argc, argv, "h:p:")) != -1) {
    std::string opt = "-";
    opt += (char) ch;
    m_inputFlags[opt] = Coptarg;
  }
  argc -= Coptind;
  argv += Coptind;
  for (int i = 0; i < argc; i++) {
    m_inputArguments.push_back(argv[i]);
  }
}

//------------------------------------------------------------------------------
// getDefaultDescription
//------------------------------------------------------------------------------
std::string castor::client::BaseClient::getDefaultDescription()
  throw (castor::Exception) {
  struct utsname utsname;
  if (uname(&utsname) == -1) {
    Exception ex;
    ex.getMessage() << "Unable to get machine name" << std::endl;
    throw ex;
  }
  uid_t uid = getuid();
  passwd* pwd = getpwuid(uid);
  if (pwd == 0) {
    Exception ex;
    ex.getMessage() << "Unable to get user name" << std::endl;
    throw ex;;
  }
  const time_t stime = time(0);
  struct tm *t = localtime(&stime);
  if (t == 0) {
    Exception ex;
    ex.getMessage() << "Unable to get time" << std::endl;
    throw ex;;
  }
  std::ostringstream desc;
  desc << "Request by " << pwd->pw_name
       << " on " << utsname.nodename
       << " at " << t->tm_hour
       << ":" << t->tm_min
       << ":" << t->tm_sec
       << " on " << t->tm_mon
       << "/" << t->tm_mday
       << "/" << t->tm_year;
  return desc.str();
}

//------------------------------------------------------------------------------
// createClient
//------------------------------------------------------------------------------
castor::IClient* castor::client::BaseClient::createClient()
  throw (castor::Exception) {
  // if no callbackSocket
  if (0 == m_callbackSocket) {
    Exception ex;
    ex.getMessage() << "No call back socket available" << std::endl;
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
void castor::client::BaseClient::sendRequest(castor::rh::Request& request)
  throw (castor::Exception) {
  // creates a socket
  castor::io::Socket s(m_rhPort, m_rhHost);
  // sends the request
  s.sendObject(request);
  // wait for acknowledgment
  IObject* obj = s.readObject();
  castor::MessageAck* ack =
    dynamic_cast<castor::MessageAck*>(obj);
  if (0 == ack) {
    castor::Exception e;
    e.getMessage() << "No Acknowledgement from the Server";
    delete ack;
    throw e;
  }
  if (!ack->status()) {
    castor::Exception e;
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
  throw (Exception) {

  int rc, nonblocking;

  rc = ioctl(m_callbackSocket->socket(),FIONBIO,&nonblocking);
  if (rc == SOCKET_ERROR) {
    Exception e;
    e.getMessage() << "Could not set socket asynchonous: " << strerror(errno)
                   << std::endl;
    throw e;
  }

  struct pollfd pollit;
  int timeout = 2; // In seconds

  pollit.fd = m_callbackSocket->socket();
  pollit.events = POLLIN;
  pollit.revents = 0;

  /* Will return > 0 if the descriptor is readable */
  rc = poll(&pollit,1,timeout*1000);
  if (0 == rc) {
    Exception e;
    e.getMessage() << "Accept timeout" << std::endl;
    throw e;
  } else if (rc < 0) {
    Exception e;
    e.getMessage() << "Poll error: " << strerror(errno)
                   << std::endl;
    throw e;
  }

  castor::io::Socket* s = m_callbackSocket->accept();
  IObject* obj = s->readObject();
  delete s;
  return obj;

}


