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
#include <sys/stat.h>
#include <sys/utsname.h>
#include <sstream>
#include <Cpwd.h>
#include "castor/io/Socket.hpp"
#include "castor/Constants.hpp"
#include "castor/IClient.hpp"
#include "castor/IObject.hpp"
#include "castor/MessageAck.hpp"
#include "castor/rh/Client.hpp"
#include "castor/rh/Request.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
extern "C" {
#include <Cgetopt.h>
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
void castor::client::BaseClient::run(int argc, char** argv)
  throw() {
  try {
    // parses the command line
    if (!parseInput(argc, argv)) {
      return;
    }
    // builds a request
    castor::rh::Request* req;
    try {
      req = buildRequest();
    } catch (castor::exception::Exception e) {
      usage(e.getMessage().str());
      return;
    }
    // Now the common part of the request
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
      int len = 16;
      char* hostname;
      hostname = (char*) calloc(len, 1);
      if (gethostname(hostname, len) < 0) {
        clog() << "Unable to get hostname : "
               << strerror(errno) << std::endl;
        free(hostname);
        return;
      }
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
          clog() << "Unable to get hostname : "
                 << strerror(errno) << std::endl;
          free(hostname);
          return;
        }
      }
      req->setMachine(hostname);
      free(hostname);
    }
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
  } catch (castor::exception::Exception ex) {
    std::cout << ex.getMessage().str() << std::endl;
  }
}

//------------------------------------------------------------------------------
// parseInput
//------------------------------------------------------------------------------
bool castor::client::BaseClient::parseInput(int argc, char** argv)
  throw (castor::exception::Exception) {
  int nowait_flag = 0;  /** --nowait option flag */
  int noretry_flag = 0; /** --noretry option flag */
  int rdonly_flag = 0;  /** --rdonly option flag */
  int silent_flag = 0;  /** --silent option flag */
  /**
   * List of options supported
   */
  struct Coptions longopts[] = {
    {"allocation_mode", REQUIRED_ARGUMENT,  NULL,        'A'},
    {"host",            REQUIRED_ARGUMENT,  NULL,        'h'},
    {"keep",            NO_ARGUMENT,        NULL,        'K'},
    {"poolname",        REQUIRED_ARGUMENT,  NULL,        'p'},
    {"size",            REQUIRED_ARGUMENT,  NULL,        's'},
    {"silent",          NO_ARGUMENT,        &silent_flag,  1},
    {"nowait",          NO_ARGUMENT,        &nowait_flag,  1},
    {"noretry",         NO_ARGUMENT,        &noretry_flag, 1},
    {"rdonly",          NO_ARGUMENT,        &rdonly_flag,  1},
    {NULL,              0,                  NULL,          0}
  };

  Coptind = 1;    /* Required */
  Copterr = 1;    /* Some stderr output if you want */
  Coptreset = 1;  /* In case we are parsing several times the same argv */
  int ch;

	while ((ch = Cgetopt_long (argc, argv, "A:h:Kp:s:", longopts, NULL)) != -1) {
    switch (ch) {
		case 'A':
		case 'h':
		case 's':
		case 'p':
			m_inputFlags[std::string(1, ch)] = Coptarg;
      break;
    case 'K':
			m_inputFlags["K"] = "";
      break;
    case 0:
			// Long option without short option correspondance
      // they will be handled at the end
      break;
    default:
      std::string s("Unknown option : -");
      s += ch;
      usage(s);
      // Unsuccessful completion
      return false;
    }
  }
  argc -= Coptind;
  argv += Coptind;
  // Handling of long opts with no corresponding short option
  if (nowait_flag)  m_inputFlags["nowait"] = "";
  if (noretry_flag) m_inputFlags["noretry"] = "";
  if (rdonly_flag)  m_inputFlags["rdonly"] = "";
  if (silent_flag)  m_inputFlags["silent"] = "";
  // Dealing with arguments
  for (int i = 0; i < argc; i++) {
    m_inputArguments.push_back(argv[i]);
  }
  // Successful completion
  return true;
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
void castor::client::BaseClient::sendRequest(castor::rh::Request& request)
  throw (castor::exception::Exception) {
  // creates a socket
  castor::io::Socket s(RHSERVER_PORT, m_rhHost);
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
  int timeout = 10; // In seconds

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

  castor::io::Socket* s = m_callbackSocket->accept();
  IObject* obj = s->readObject();
  delete s;
  return obj;

}
