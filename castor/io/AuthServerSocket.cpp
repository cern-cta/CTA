/******************************************************************************
 *                      ServerSocket.cpp
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
 * @(#)$RCSfile: AuthServerSocket.cpp,v $ $Revision: 1.10 $ $Release$ $Date: 2008/11/24 17:47:25 $ $Author: waldron $
 *
 * @author Benjamin Couturier
 *****************************************************************************/

// Include Files
#include <net.h>
#include <netdb.h>
#include <errno.h>
#include <serrno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/io/biniostream.h"
#include "castor/io/StreamAddress.hpp"

// Local Includes
#include "AuthServerSocket.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AuthServerSocket::AuthServerSocket(int socket) throw () :
  ServerSocket(socket) {
  if (loader() == -1) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "Dynamic library was not properly loaded";
    throw ex;
  }

  if (getServer_initContext(&m_security_context, CSEC_SERVICE_TYPE_HOST, NULL) < 0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "The initialization of the security context failed";
    throw ex;
  }
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AuthServerSocket::AuthServerSocket(const bool reusable)
  throw (castor::exception::Exception):
  ServerSocket(reusable) {

  if (loader() == -1) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "Dynamic library was not properly loaded";
    throw ex;
  }

  if (getServer_initContext(&m_security_context, CSEC_SERVICE_TYPE_HOST, NULL) < 0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "The initialization of the security context failed";
    throw ex;
  }
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AuthServerSocket::AuthServerSocket(const unsigned short port,
					       const bool reusable)
  throw (castor::exception::Exception) :
  ServerSocket(port, reusable) {
  if (loader() == -1) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "Dynamic library was not properly loaded";
    throw ex;
  }

  if (getServer_initContext(&m_security_context, CSEC_SERVICE_TYPE_HOST, NULL) < 0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "The initialization of the security context failed";
    throw ex;
  }
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AuthServerSocket::AuthServerSocket(const unsigned short port,
					       const std::string host,
					       const bool reusable,
					       int service_type)
  throw (castor::exception::Exception) :
  ServerSocket(port, host, reusable) {
  if (loader() == -1) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "Dynamic library was not properly loaded";
    throw ex;
  }

  if (getServer_initContext(&m_security_context, CSEC_SERVICE_TYPE_HOST, NULL) < 0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "The initialization of the security context failed";
    throw ex;
  }
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AuthServerSocket::AuthServerSocket(const unsigned short port,
					       const unsigned long ip,
					       const bool reusable,
					       int service_type)
  throw (castor::exception::Exception) :
  ServerSocket(port, ip, reusable) {
  if (loader() == -1) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "Dynamic library was not properly loaded";
    throw ex;
  }

  if (getServer_initContext(&m_security_context, CSEC_SERVICE_TYPE_HOST, NULL) < 0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "The initialization of the security context failed";
    throw ex;
  }
}


//------------------------------------------------------------------------------
// constructor
// Initialize a AuthServerSocket from a ServerSocket. It copies the attributes of
// the ServerSocket, reuses the security context and establishes the context with
// the client and maps the user to a local user
//------------------------------------------------------------------------------
castor::io::AuthServerSocket::AuthServerSocket(castor::io::ServerSocket* cs,
                                               const Csec_context_t context)
  throw (castor::exception::Exception) :
  ServerSocket(cs->socket()) {
  cs->resetSocket();
  delete cs;
  m_security_context = context;

  if (getServer_establishContext(&m_security_context, m_socket) < 0) {
    close();
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "The initialization of the security context failed";
    throw ex;
  }

  // map user.
  setClientId();
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::io::AuthServerSocket::~AuthServerSocket() throw () {
  getClearContext(&m_security_context);
  ::close(m_socket);
}


//------------------------------------------------------------------------------
// accept
//------------------------------------------------------------------------------
castor::io::ServerSocket* castor::io::AuthServerSocket::accept()
  throw(castor::exception::Exception) {

  castor::io::ServerSocket* as = castor::io::ServerSocket::accept();
  return new AuthServerSocket(as, m_security_context);
}


//------------------------------------------------------------------------------
// setClientId (That method should go out of this class
//------------------------------------------------------------------------------
void castor::io::AuthServerSocket::setClientId ()
  throw(castor::exception::Exception) {
  char *mech, *name;
  char username[CA_MAXUSRNAMELEN+1];
  getClientId(&m_security_context, &mech, &name);
  // In the name you got the principal it in the previous call from the gridmapfile
  // here you get uid and gid and if you want
  // the name matching the uid then set buf and BUF_SIZE
  if (getMapUser (mech, name, username, CA_MAXUSRNAMELEN, &m_Euid, &m_Egid) != 0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "User cannot be mapped into local user";
  }
  m_secMech = mech;
  m_userName = username;
}


//------------------------------------------------------------------------------
// getClientEuid
//------------------------------------------------------------------------------
uid_t castor::io::AuthServerSocket::getClientEuid () {
  return m_Euid;
}


//------------------------------------------------------------------------------
// getClientEgid
//------------------------------------------------------------------------------
gid_t castor::io::AuthServerSocket::getClientEgid () {
  return m_Egid;
}


//------------------------------------------------------------------------------
// getClientEgid
//------------------------------------------------------------------------------
std::string castor::io::AuthServerSocket::getClientMappedName () {
  return m_userName;
}


//------------------------------------------------------------------------------
// getSecMech
//------------------------------------------------------------------------------
std::string castor::io::AuthServerSocket::getSecMech () {
  return m_secMech;
}

