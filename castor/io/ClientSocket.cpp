/******************************************************************************
 *                      ClientSocket.cpp
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
 * @(#)$RCSfile: ClientSocket.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/07/19 10:22:25 $ $Author: bcouturi $
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
#include "ClientSocket.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ClientSocket::ClientSocket(int socket) throw () {
  m_socket = socket;
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ClientSocket::ClientSocket(const unsigned short port,
                           const std::string host)
  throw (castor::exception::Exception) {
  m_socket = 0;
  createSocket();
  m_saddr = buildAddress(port, host);
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ClientSocket::ClientSocket(const unsigned short port,
                           const unsigned long ip)
  throw (castor::exception::Exception) {
  m_socket = 0; 
  createSocket();
  m_saddr = buildAddress(port, ip);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::io::ClientSocket::~ClientSocket() throw () {
  close(m_socket);
}


//------------------------------------------------------------------------------
// connect
//------------------------------------------------------------------------------
void castor::io::ClientSocket::connect()
  throw (castor::exception::Exception) {
  // Connects the socket
  if (::connect(m_socket, (struct sockaddr *)&m_saddr, sizeof(m_saddr)) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to connect socket";
    close(m_socket);
    throw ex;
  }
}

