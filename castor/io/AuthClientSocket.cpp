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
 * @(#)$RCSfile: AuthClientSocket.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2005/03/15 23:08:44 $ $Author: bcouturi $
 *
 *
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

#include "Csec_api.h"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AuthClientSocket::AuthClientSocket(int socket) throw () :
  ClientSocket(socket) {
    // XXX Check what the default should be, why no exception ?
    Csec_client_initContext(&m_security_context, 0, NULL);
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AuthClientSocket::AuthClientSocket(const unsigned short port,
					       const std::string host,
					       int service_type)
  throw (castor::exception::Exception): ClientSocket(port, host) {

    if (Csec_client_initContext(&m_security_context, 
				 service_type, 
				 NULL) < 0) {
      castor::exception::Exception ex(serrno);
      ex.getMessage() << Csec_geterrmsg();
      throw ex;
    }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AuthClientSocket::AuthClientSocket(const unsigned short port,
					       const unsigned long ip,
					       int service_type)
  throw (castor::exception::Exception) : ClientSocket(port, ip) {
    if (Csec_client_initContext(&m_security_context, 
				 service_type, 
				 NULL) < 0) {
      castor::exception::Exception ex(serrno);
      ex.getMessage() << Csec_geterrmsg();
      throw ex;
    }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::io::AuthClientSocket::~AuthClientSocket() throw () {
  Csec_clearContext(&m_security_context);
  close(m_socket);
}


//------------------------------------------------------------------------------
// connect
//------------------------------------------------------------------------------
void castor::io::AuthClientSocket::connect()
  throw (castor::exception::Exception) {
  
  castor::io::ClientSocket::connect();

  if (Csec_client_establishContext(&m_security_context, 
				    this->socket()) < 0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << Csec_geterrmsg();
    throw ex;
  }  
}

