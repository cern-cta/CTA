/******************************************************************************
 *                      AuthClientSocket.cpp
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
 * @(#)$RCSfile: AuthClientSocket.cpp,v $ $Revision: 1.8 $ $Release$ $Date: 2008/11/24 17:47:25 $ $Author: waldron $
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
#include "AuthClientSocket.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AuthClientSocket::AuthClientSocket(int socket) 
  throw (castor::exception::Exception) :
  ClientSocket(socket) {
 
  if (loader() == -1) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "Dynamic library was not properly loaded";
    throw ex;
  }
  
  if (getClient_initContext(&m_security_context, CSEC_SERVICE_TYPE_HOST, NULL) < 0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "The initialization of the security context failed";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AuthClientSocket::AuthClientSocket(const unsigned short port,
					       const std::string host,
					       int)
  throw (castor::exception::Exception): ClientSocket(port, host) {
  
  if (loader() == -1) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "Dynamic library was not properly loaded";
    throw ex;
  }
  
  if (getClient_initContext(&m_security_context, CSEC_SERVICE_TYPE_HOST, NULL) < 0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "The initialization of the security context failed";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AuthClientSocket::AuthClientSocket(const unsigned short port,
					       const unsigned long ip,
					       int)
  throw (castor::exception::Exception) : ClientSocket(port, ip) {
  
  if (loader() ==-1) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "Dynamic library was not properly loaded";
    throw ex;
  }
  
  if (getClient_initContext(&m_security_context, CSEC_SERVICE_TYPE_HOST, NULL) < 0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "The initialization of the security context failed";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::io::AuthClientSocket::~AuthClientSocket() throw () {
  // Csec_clearContext(&m_security_context);
  
  getClearContext(&m_security_context);
  if (m_socket >= 0) {
    ::close(m_socket);
  }
}

//------------------------------------------------------------------------------
// connect
//------------------------------------------------------------------------------
void castor::io::AuthClientSocket::connect()
  throw (castor::exception::Exception) {
  
  castor::io::ClientSocket::connect();
  
  if (getClient_establishContext(&m_security_context, m_socket) < 0) {
    close();
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "The initialization of the security context failed";
    throw ex;
  } 
}
