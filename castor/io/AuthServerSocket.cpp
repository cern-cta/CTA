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
 * @(#)$RCSfile: AuthServerSocket.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2005/03/15 23:08:44 $ $Author: bcouturi $
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
#include "AuthServerSocket.hpp"

#include "Csec_api.h"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AuthServerSocket::AuthServerSocket(int socket) throw () : 
  ServerSocket(socket) {
  // XXX Check what the default should be, why no exception ?
  Csec_server_initContext(&m_security_context, 0, NULL);
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AuthServerSocket::AuthServerSocket(const unsigned short port,
				       const bool reusable,
				       int service_type)
  throw (castor::exception::Exception) : ServerSocket(port, reusable) {
    if (Csec_server_initContext(&m_security_context, 
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
castor::io::AuthServerSocket::AuthServerSocket(const unsigned short port,
				       const std::string host, 
				       const bool reusable,
				       int service_type)
  throw (castor::exception::Exception) :  ServerSocket(port, host, reusable) {
    if (Csec_server_initContext(&m_security_context, 
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
castor::io::AuthServerSocket::AuthServerSocket(const unsigned short port,
				       const unsigned long ip,
				       const bool reusable,
				       int service_type)
  throw (castor::exception::Exception) : ServerSocket(port, ip, reusable) {
    if (Csec_server_initContext(&m_security_context, 
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
castor::io::AuthServerSocket::~AuthServerSocket() throw () {
  Csec_clearContext(&m_security_context);
  close(m_socket);
}


//------------------------------------------------------------------------------
// accept
//------------------------------------------------------------------------------
castor::io::ServerSocket* castor::io::AuthServerSocket::accept()
  throw(castor::exception::Exception) {

  castor::io::ServerSocket* as = castor::io::ServerSocket::accept();
  if (Csec_server_establishContext(&m_security_context, 
				   as->socket()) < 0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << Csec_geterrmsg();
    throw ex;
  }  
  return as; 
}

