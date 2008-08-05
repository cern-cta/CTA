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
 * @(#)BaseClient.cpp,v 1.37 $Release$ 2006/02/16 15:56:58 sponcec3
 *
 * A base class for all castor clients. Implements many
 * useful and common parts for all clients.
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <errno.h>
#if !defined(_WIN32)
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/poll.h>
#include <sys/times.h>
#else
#include <io.h>
#include "poll.h"
#endif
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <Cpwd.h>
#include "patchlevel.h"
#include "castor/io/ClientSocket.hpp"
#include "castor/io/AuthClientSocket.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/io/AuthServerSocket.hpp"
#include "castor/Constants.hpp"
#include "castor/System.hpp"
#include "castor/IClient.hpp"
#include "castor/IObject.hpp"
#include "castor/MessageAck.hpp"
#include "castor/rh/Client.hpp"
#include "castor/rh/Response.hpp"

#include "castor/stager/StageFileQueryRequest.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/FileRequest.hpp"

#include "castor/client/IResponseHandler.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Communication.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "stager_client_api_common.hpp"


extern "C" {
#include <Cgetopt.h>
#include <common.h>
}

// Local includes
#include "BaseClient.hpp"

//------------------------------------------------------------------------------
// String constants
//------------------------------------------------------------------------------
const char *castor::client::HOST_ENV_ALT = "STAGE_HOST";
const char *castor::client::HOST_ENV = "STAGER_HOST";
const char *castor::client::PORT_ENV_ALT = "STAGE_PORT";
const char *castor::client::PORT_ENV = "STAGER_PORT";
const char *castor::client::SEC_PORT_ENV_ALT = "STAGE_SEC_PORT";
const char *castor::client::SEC_PORT_ENV = "STAGER_SEC_PORT";
const char *castor::client::SVCCLASS_ENV_ALT = "SVCCLASS";
const char *castor::client::SVCCLASS_ENV = "STAGE_SVCCLASS";
const char *castor::client::CATEGORY_CONF = "STAGER";
const char *castor::client::HOST_CONF = "HOST";
const char *castor::client::PORT_CONF = "PORT";
const char *castor::client::SEC_PORT_CONF = "SEC_PORT";
const char *castor::client::STAGE_EUID = "STAGE_EUID";
const char *castor::client::STAGE_EGID = "STAGE_EGID";
const char *castor::client::CLIENT_CONF = "CLIENT";
const char *castor::client::LOWPORT_CONF = "LOWPORT";
const char *castor::client::HIGHPORT_CONF = "HIGHPORT";
const char *castor::client::SEC_MECH_ENV = "CSEC_MECH"; // Security protocol GSI, ID, KRB5,KRB4
const char *castor::client::SECURITY_ENV = "SECURE_CASTOR"; // Security enable
const char *castor::client::SECURITY_CONF = "SECURE_CASTOR";


//------------------------------------------------------------------------------
// Numeric constants
//------------------------------------------------------------------------------
const int castor::client::LOW_CLIENT_PORT_RANGE = 30000;
const int castor::client::HIGH_CLIENT_PORT_RANGE = 30100;

//------------------------------------------------------------------------------
// Timing utility
//------------------------------------------------------------------------------
#ifdef SIXMONTHS
#undef SIXMONTHS
#endif
#define SIXMONTHS (6*30*24*60*60)


#if defined(_WIN32)
static char strftime_format_sixmonthsold[] = "%b %d %Y";
static char strftime_format[] = "%b %d %H:%M:%S";
#else /* _WIN32 */
static char strftime_format_sixmonthsold[] = "%b %e %Y";
static char strftime_format[] = "%b %e %H:%M:%S";
#endif /* _WIN32 */

void DLL_DECL BaseClient_util_time(time_t then, char *timestr) {
  time_t this_time = time(NULL);
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
  struct tm tmstruc;
#endif /* _REENTRANT || _THREAD_SAFE */
  struct tm *tp;
  
#if ((defined(_REENTRANT) || defined(_THREAD_SAFE)) && !defined(_WIN32))
  localtime_r(&(then),&tmstruc);
  tp = &tmstruc;
#else
  tp = localtime(&(then));
#endif /* _REENTRANT || _THREAD_SAFE */
  if ((this_time >= then) && ((this_time - then) > SIXMONTHS)) {
    // Too much in past
    strftime(timestr,64,strftime_format_sixmonthsold,tp);
  } else if ((this_time < then) && ((then - this_time) > SIXMONTHS)) {
    // Too much in feature...!
    strftime(timestr,64,strftime_format_sixmonthsold,tp);
  } else {
    strftime(timestr,64,strftime_format,tp);
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::client::BaseClient::BaseClient
(int acceptTimeout, int transferTimeout) throw() :
  BaseObject(),
  m_rhPort(-1),
  m_callbackSocket(0),
  m_requestId(""),
  m_acceptTimeout(acceptTimeout),
  m_transferTimeout(transferTimeout),
  m_hasAuthorizationId(false),
  m_authUid(0),
  m_authGid(0),
  m_hasSecAuthorization(false) {
  setAuthorization();
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::client::BaseClient::~BaseClient() throw() {
  if (0 != m_callbackSocket) delete m_callbackSocket;
}

//------------------------------------------------------------------------------
// sendrequest
//------------------------------------------------------------------------------
std::string castor::client::BaseClient::sendRequest
(castor::stager::Request* req,
 castor::client::IResponseHandler* rh)
  throw(castor::exception::Exception) {

  // Build and send the Request with the Client information
  createClientAndSend(req);

  // Wait for callbacks
  pollAnswersFromStager(req, rh);

  return requestId();
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
  result->setVersion(MAJORVERSION*1000000+MINORVERSION*10000+MAJORRELEASE*100+MINORRELEASE);
  return result;
}

//------------------------------------------------------------------------------
// internalSendRequest
//------------------------------------------------------------------------------
std::string castor::client::BaseClient::internalSendRequest
(castor::stager::Request& request)
  throw (castor::exception::Exception) {
  std::string requestId;

  // The service class has been previously resolved, attach it to the request
  request.setSvcClassName(m_rhSvcClass);

  // Tracing the submit time
  char timestr[64];
  castor::io::ClientSocket* s;
  time_t now = time(NULL);
  BaseClient_util_time(now, timestr);
  stage_trace(3, "%s (%u) Sending request", timestr, now);

  // preparing the timing information
  clock_t startTime;
#if !defined(_WIN32)
  struct tms buf;
  startTime = times(&buf);
#else
  startTime = clock();
#endif

  // Creates a socket
  if (m_hasSecAuthorization) {
    s = new castor::io::AuthClientSocket(m_rhPort, m_rhHost);
    // If the context cannot be established there will be an exception thrown.
  } else {
    s = new castor::io::ClientSocket(m_rhPort, m_rhHost);
  }

  // Set the timeout for the socket, if not defined DEFAULT_NETTIMEOUT will
  // be used
  if (m_transferTimeout > 0) {
    s->setTimeout(m_transferTimeout);
  }
  s->connect();

  // Send the request
  s->sendObject(request);

  // Wait for acknowledgment
  IObject* obj = s->readObject();
  castor::MessageAck* ack =
    dynamic_cast<castor::MessageAck*>(obj);
  if (0 == ack) {
    castor::exception::InvalidArgument e; // XXX To be changed
    e.getMessage() << "No Acknowledgement from the Server";
    delete s;
    throw e;
  }
  requestId = ack->requestId();
  setRequestId(requestId);
  if (!ack->status()) {
    castor::exception::Exception e(ack->errorCode()); // XXX To be changed
    e.getMessage() << ack->errorMessage();
    delete ack;
    delete s;
    throw e;
  }
  delete ack;
#if !defined(_WIN32)
  clock_t endTime = times(&buf);
#else
  clock_t endTime = clock();
#endif
  stage_trace(3, "%s SND %.2f s to send the request", 
              requestId.c_str(),
#if !defined(_WIN32)
              ((float)(endTime - startTime)) / ((float)sysconf(_SC_CLK_TCK)));
#else
              ((float)(endTime - startTime)) / CLOCKS_PER_SEC);
#endif
  delete s;
  return requestId;
}

//------------------------------------------------------------------------------
// waitForCallBack
//------------------------------------------------------------------------------
// This function blocks until one call back has been made
castor::io::ServerSocket* castor::client::BaseClient::waitForCallBack()
  throw (castor::exception::Exception) {

  stage_trace(3, "Waiting for callback from castor");

  clock_t startTime;
#if !defined(_WIN32)
  struct tms buf;
  startTime = times(&buf);
#else
  startTime = clock();
#endif

  // Set the socket to non blocking
  int rc; 
#if !defined(_WIN32)
  int nonblocking = 1;
  rc = ioctl(m_callbackSocket->socket(), FIONBIO, &nonblocking);
#else
  u_long nonblocking = 1;
  rc = ioctlsocket(m_callbackSocket->socket(), FIONBIO, &nonblocking);
#endif
  if (rc == SOCKET_ERROR) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Could not set socket asynchonous";
    throw e;
  }

  struct pollfd pollit;
  
  pollit.fd      = m_callbackSocket->socket();
  pollit.events  = POLLIN;
  pollit.revents = 0;

  // Wait for an incoming connection
  while (1) {
    rc = poll(&pollit, 1, m_acceptTimeout * 1000);
    if (rc == 0) {
      castor::exception::Communication e(requestId(), SETIMEDOUT);
      e.getMessage() << "Accept timeout";
      throw e;
    } else if (rc < 0) {
      if (errno == EINTR) {
	continue; // A signal was caught during poll()
      }
      castor::exception::Communication e(requestId(), errno);
      e.getMessage() << "Poll error:" << strerror(errno);
      throw e;
    } else {
      break; // Success
    }
  }    
  
  // Accept the incoming connection
  castor::io::ServerSocket* socket = m_callbackSocket->accept();

  // Log the ip address of the incoming connection
  unsigned short port;
  unsigned long ip;
  socket->getPeerIp(port, ip);

  std::ostringstream ipToStr;
  ipToStr << ((ip & 0xFF000000) >> 24) << "." << ((ip & 0x00FF0000) >> 16) << "."
	  << ((ip & 0x0000FF00) >> 8) << "." << ((ip & 0x000000FF));
#if !defined(_WIN32)
  clock_t endTime = times(&buf);
#else
  clock_t endTime = clock();
#endif
  stage_trace(3, "%s CBK %.2f s before callback from %s was received", 
              requestId().c_str(),
#if !defined(_WIN32)
              ((float)(endTime - startTime)) / ((float)sysconf(_SC_CLK_TCK)),
#else
              ((float)(endTime - startTime)) / CLOCKS_PER_SEC,
#endif
              ipToStr.str().c_str());
  
  return socket;
}

//------------------------------------------------------------------------------
// setRhPort
//------------------------------------------------------------------------------
void castor::client::BaseClient::setRhPort()
  throw (castor::exception::Exception) {
  setRhPort(0);
}

void castor::client::BaseClient::setRhPort(int optPort)
  throw (castor::exception::Exception) {

  if (optPort > 65535) {
      castor::exception::Exception e(errno);
      e.getMessage() << "Invalid port value : " << optPort
		     << ". Must be < 65535." << std::endl;
      throw e;
  }
  if (optPort > 0) {
    m_rhPort = optPort;
  } else {
    // Resolve RH port:
    // if security mode is used get the RH Secure server port,
    // the value can be given through the environment
    // or in the castor.conf file. If none is given, default is used
    char* port;
    if (m_hasSecAuthorization) {
      if ((port = getenv (castor::client::SEC_PORT_ENV)) != 0 
        || (port = getenv (castor::client::SEC_PORT_ENV_ALT)) != 0
        || (port = getconfent((char *)castor::client::CATEGORY_CONF,
             (char *)castor::client::SEC_PORT_CONF,0)) != 0) {
        m_rhPort = castor::System::porttoi(port);
      } else {
        m_rhPort = CSP_RHSERVER_SEC_PORT;
      }
    } else {
      if ((port = getenv (castor::client::PORT_ENV)) != 0 
        || (port = getenv (castor::client::PORT_ENV_ALT)) != 0
        || (port = getconfent((char *)castor::client::CATEGORY_CONF,
             (char *)castor::client::PORT_CONF,0)) != 0) {
        m_rhPort = castor::System::porttoi(port);
      } else {
        m_rhPort = CSP_RHSERVER_PORT;
      }
    }
  }
  stage_trace(3, "Looking up RH port - Using %d", m_rhPort);
}

//------------------------------------------------------------------------------
// setRhHost
//------------------------------------------------------------------------------
void castor::client::BaseClient::setRhHost()
  throw (castor::exception::Exception) {
  setRhHost("");
}

void castor::client::BaseClient::setRhHost(std::string optHost)
  throw (castor::exception::Exception) {
  // RH server host. Can be passed given through the
  // RH_HOST environment variable or in the castor.conf
  // file as a RH/HOST entry
  if (optHost.compare("")) {
    m_rhHost = optHost;
  }
  else {
    char* host;
    if ((host = getenv (castor::client::HOST_ENV)) != 0
        || (host = getenv (castor::client::HOST_ENV_ALT)) != 0
        || (host = getconfent((char *)castor::client::CATEGORY_CONF,
                              (char *)castor::client::HOST_CONF,0)) != 0) {
      m_rhHost = host;
    } else {
      m_rhHost = RH_HOST;
    }
  }
  stage_trace(3, "Looking up RH host - Using %s", m_rhHost.c_str());
}

//------------------------------------------------------------------------------
// setRhSvcClass
//------------------------------------------------------------------------------
void castor::client::BaseClient::setRhSvcClass()
  throw (castor::exception::Exception) {
  setRhSvcClass("");
}

void castor::client::BaseClient::setRhSvcClass(std::string optSvcClass)
  throw (castor::exception::Exception) {
  // RH server host. Can be passed given through the
  // RH_HOST environment variable or in the castor.conf
  // file as a RH/HOST entry
  if (optSvcClass.compare("")) {
    m_rhSvcClass = optSvcClass;
  } else {
    char* svc;
    if ((svc = getenv ("STAGE_SVCCLASS")) != 0
        || (svc = getconfent("STAGER", "SVCCLASS",0)) != 0) {
      m_rhSvcClass = svc;
    } else {
      m_rhSvcClass = "";
    }
  }
  if (!m_rhSvcClass.empty()) {
    stage_trace(3, "Looking up service class - Using %s", m_rhSvcClass.c_str());
  }
}


//------------------------------------------------------------------------------
// setOptions
//------------------------------------------------------------------------------
void castor::client::BaseClient::setOptions(struct stage_options* opts)
  throw (castor::exception::Exception) {
  if (0 != opts) {
    setRhHost(opts->stage_host);
    setRhPort(opts->stage_port);
    setRhSvcClass(opts->service_class);
  } else {
    setRhHost("");
    setRhPort(0);
    setRhSvcClass("");
  }
}       

//------------------------------------------------------------------------------
// setRequestId
//------------------------------------------------------------------------------
void castor::client::BaseClient::setRequestId(std::string requestId) {
  m_requestId = requestId;
}

//------------------------------------------------------------------------------
// requestId
//------------------------------------------------------------------------------
std::string castor::client::BaseClient::requestId() {
  return m_requestId;
}

//------------------------------------------------------------------------------
// setAutorizationId
//----------------------------------------------------------------------------
void castor::client::BaseClient::setAuthorizationId() 
  throw(castor::exception::Exception) {
  if (stage_getid(&m_authUid, &m_authGid) < 0) {
    castor::exception::Exception e(serrno);
    e.getMessage() << "Error in stage_getid" << std::endl;
    throw e;
  }
  else {
    m_hasAuthorizationId = true;
  } 
}

//------------------------------------------------------------------------------
// setAutorizationId
//------------------------------------------------------------------------------
void castor::client::BaseClient::setAuthorizationId(uid_t uid, gid_t gid) throw() {
  m_authUid = uid;
  m_authGid = gid;
  m_hasAuthorizationId = true;
}

//------------------------------------------------------------------------------
// setAutorization
//------------------------------------------------------------------------------
void castor::client::BaseClient::setAuthorization() throw(castor::exception::Exception) {
  char *security;
  char *mech;
  // Check if security env option is set. 
  if (((security = getenv (castor::client::SECURITY_ENV)) != 0 ||
       (security = getconfent((char *)castor::client::CLIENT_CONF,
                                     (char *)castor::client::SECURITY_CONF,0)) != 0 ) && 
       strcasecmp(security, "YES") == 0) {
    if (( mech = getenv (castor::client::SEC_MECH_ENV)) != 0) {
      if (strlen(mech) > CA_MAXCSECPROTOLEN) {
        serrno = EINVAL;
        castor::exception::Exception e(serrno);
        e.getMessage() << "Supplied security protocol is too long" << std::endl;
        throw e;
      } else {
        m_Sec_mech = mech;
        m_hasSecAuthorization = true;
        stage_trace(3, "Setting security mechanism: %s", mech);
      }
    }
  }
}

//------------------------------------------------------------------------------
// setAutorization
//------------------------------------------------------------------------------
void castor::client::BaseClient::setAuthorization(char *mech, char *id) throw(castor::exception::Exception) {
  if (strlen(mech) > CA_MAXCSECPROTOLEN) {
    serrno = EINVAL;
    castor::exception::Exception e(serrno);
    e.getMessage() << "Supplied security protocol is too long" << std::endl;
    throw e;
  }
  
  m_Sec_mech = mech;
  
  if (strlen(id) > CA_MAXCSECNAMELEN) {
    serrno = EINVAL;
    castor::exception::Exception e(serrno);
    e.getMessage() << "Supplied authorization id is too long" << std::endl;
    throw e;
  }
  
  m_Csec_auth_id = id;
  
  m_voname = NULL;
  m_nbfqan = 0;
  m_fqan = NULL;
  m_hasSecAuthorization = true;
  stage_trace(3, "Setting security mechanism: %s", mech);
}

//------------------------------------------------------------------------------
// createClientAndSend
//------------------------------------------------------------------------------
std::string castor::client::BaseClient::createClientAndSend
(castor::stager::Request *req)
  throw (castor::exception::Exception) {
  // builds the Client (uuid,guid,hostname,etc)
  buildClient(req);

  // sends the request
  std::string requestId = internalSendRequest(*req);
  return requestId;
}

//------------------------------------------------------------------------------
// buildClient
//------------------------------------------------------------------------------
void castor::client::BaseClient::buildClient(castor::stager::Request* req)
  throw (castor::exception::Exception) {

  // Uid
  uid_t euid;
  char *stgeuid = getenv(castor::client::STAGE_EUID);
  if (0 != stgeuid) {
    euid = atoi(stgeuid);
  } else {
    if (m_hasAuthorizationId) {
      euid = m_authUid;
    } else {
      euid = geteuid();
    }
  }
  stage_trace(3, "Setting euid: %d", euid);
  req->setEuid(euid);

  // GID
  uid_t egid;
  char *stgegid = getenv(castor::client::STAGE_EGID);
  if (0 != stgegid) {
    egid = atoi(stgegid);
  } else {
    if (m_hasAuthorizationId) {
      egid = m_authGid;
    } else {
      egid = getegid();
    }
  }
  stage_trace(3, "Setting egid: %d", egid);
  req->setEgid(egid);

  // Username
  errno = 0;
  struct passwd *pw = Cgetpwuid(euid);
  if (0 == pw) {
    castor::exception::Exception e(EINVAL);
    e.getMessage() << "Unknown User" << std::endl;
    throw e;
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
  req->setMachine(castor::System::getHostName());
  if (m_rhHost == "") {
    castor::exception::Exception e(errno);
    e.getMessage() << "Could not get RH host name"
                   <<  strerror(errno);
    throw e;
  }

  // Create a socket for the callback with no port
  // not to let the client to reuse the port
  m_callbackSocket = new castor::io::ServerSocket(false); 
  
  // Get the port range to be used
  int lowPort = LOW_CLIENT_PORT_RANGE;
  int highPort = HIGH_CLIENT_PORT_RANGE;
  char* sport;
  if ((sport = getconfent((char *)castor::client::CLIENT_CONF,
			  (char *)castor::client::LOWPORT_CONF,0)) != 0) {
    lowPort = castor::System::porttoi(sport);
  }
  if ((sport = getconfent((char *)castor::client::CLIENT_CONF,
			  (char *)castor::client::HIGHPORT_CONF,0)) != 0) {
    highPort = castor::System::porttoi(sport);
  }

  // Bind the socket
  m_callbackSocket->bind(lowPort, highPort);
  m_callbackSocket->listen();
  unsigned short port;
  unsigned long ip;
  m_callbackSocket->getPortIp(port, ip);
  stage_trace(3, "Creating socket for castor callback - Using port %d", port);

  // Set the Client
  castor::IClient *cl = createClient();
  req->setClient(cl);
}

//------------------------------------------------------------------------------
// pollAnswersFromStager
//------------------------------------------------------------------------------
void castor::client::BaseClient::pollAnswersFromStager
(castor::stager::Request* req, castor::client::IResponseHandler* rh)
  throw (castor::exception::Exception) {

  // Check parameters
  if ((req == NULL) || (req->client() == NULL)) {
    castor::exception::Internal ex;
    ex.getMessage() << "Passed Request is NULL" << std::endl;
    throw ex;
  }

  // Determine the number of replies we expect to get from the stager. For
  // requests which are subrequest orientated the responses may come from
  // multiple stagers and be spread over time. As a consequence we should
  // expect 1..N number of callbacks. For requests which are not subrequest
  // orientated we expect one and only one callback/connection from the 
  // stager with all the responses.
  unsigned int nbReplies = 1;
  castor::stager::FileRequest *fr =
    dynamic_cast<castor::stager::FileRequest *>(req);
  if (fr != 0) {
    nbReplies = fr->subRequests().size();
  }

  // Loop over the number of replies we expect to get from the stager, once we
  // achieve the correct number of replies we can return control to the calling
  // function.
  castor::io::ServerSocket *socket = 0;
  for (unsigned int replies = 0; replies < nbReplies; ) {

    // Wait for a callback from the request replier of the stager
    socket = waitForCallBack();
    socket->setTimeout(900);

    while ((replies < nbReplies) || (nbReplies == 1)) {

      // Read object from socket
      IObject *obj = 0;      
      try {
	obj = socket->readObject();
      } catch (castor::exception::Exception e) {
	if (e.code() == 1016) {
	  // Reset serrno to 0 as this isn't really an error!
	  serrno = 0;
	  delete socket;
	  socket = 0;
	  break; // Connection closed by remote end
	}
	throw e;
      }

      // Process the objects data
      try {
	if (obj->type() == OBJ_EndResponse) {
	  continue; // Ignored since 2.1.7-7+
	}
	
	// Cast response into Response*
	castor::rh::Response *res =
	  dynamic_cast<castor::rh::Response *>(obj);
	if (res == 0) {
	  castor::exception::Communication e(requestId(), SEINTERNAL);
	  e.getMessage() << "Receivd bad response type :" << obj->type();
	  throw e;
	}
	
	// Check that the request id sent is what we expected
	if ((res->reqAssociated().length() != 0) &&
	    (res->reqAssociated() != requestId())) {
	  castor::exception::Communication e(requestId(), SEINTERNAL);
	  e.getMessage() << "Receieved data for another request: "
			 << res->reqAssociated();
	  throw e;
	}

	// Handle the response
	rh->handleResponse(*res);
	
	// Cleanup for next iteration
	delete obj;
	replies++;
      } catch (castor::exception::Exception e) {
	delete obj;
	delete socket;
	socket = 0;
	throw e;
      }
    }
  }

  // Cleanup the client side socket
  if (socket != 0) delete socket;

  // If we've got this far it means that we haven't thrown an exception and
  // that all responses have been received so we terminate the response handler
  rh->terminate();
}
