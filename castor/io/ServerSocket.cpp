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
 * @(#)$RCSfile: ServerSocket.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/07/19 10:22:25 $ $Author: bcouturi $
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
#include "ServerSocket.hpp"


// definition of some constants
#define STG_CALLBACK_BACKLOG 2


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ServerSocket::ServerSocket(int socket) throw () {
  m_socket = socket;
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ServerSocket::ServerSocket(const unsigned short port,
				       const bool doListen)
  throw (castor::exception::Exception) {
  m_socket =0;
  createSocket();
  struct sockaddr_in saddr = buildAddress(port);
  bind(saddr);
  if (doListen) listen();
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ServerSocket::ServerSocket(const unsigned short port,
                           const std::string host)
  throw (castor::exception::Exception) {
  m_socket = 0;
  createSocket();
  struct sockaddr_in saddr = buildAddress(port, host);
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ServerSocket::ServerSocket(const unsigned short port,
                           const unsigned long ip)
  throw (castor::exception::Exception) {
  m_socket = 0;
  createSocket();
  struct sockaddr_in saddr = buildAddress(port, ip);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::io::ServerSocket::~ServerSocket() throw () {
  close(m_socket);
}

//------------------------------------------------------------------------------
// Sets the socket to Reusable address
//------------------------------------------------------------------------------
void castor::io::ServerSocket::reusable()
  throw (castor::exception::Exception) {

  int on = 1;
	if (setsockopt (m_socket, SOL_SOCKET, SO_REUSEADDR, 
                  (char *)&on, sizeof(on)) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to set socket to reusable";
    throw ex;    
  }
}



//------------------------------------------------------------------------------
// listen
//------------------------------------------------------------------------------
void castor::io::ServerSocket::listen()
  throw(castor::exception::Exception) {
  if (::listen(m_socket, STG_CALLBACK_BACKLOG) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to listen on socket";
    (void) close(m_socket);
    m_socket = 0;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// accept
//------------------------------------------------------------------------------
castor::io::ServerSocket* castor::io::ServerSocket::accept()
  throw(castor::exception::Exception) {
  // loop until we really get something
  for (;;) {
    struct sockaddr_in saddr;
    int fromlen = sizeof(saddr);
    int fdc = ::accept(m_socket,
                       (struct sockaddr *) &saddr,
                       (socklen_t *)(&fromlen));
    if (fdc == -1) {
      if (errno == EINTR) {
        continue;
      } else {
        castor::exception::Exception ex(errno);
        ex.getMessage() << "Error in accepting on socket";
        throw ex;
      }
    }
    return new ServerSocket(fdc);
  }
}


//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void castor::io::ServerSocket::bind(sockaddr_in saddr)
  throw (castor::exception::Exception) {
  // Binds the socket
  if (::bind(m_socket, (struct sockaddr *)&saddr, sizeof(saddr)) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to bind socket";
    close(m_socket);
    throw ex;
  }
}
