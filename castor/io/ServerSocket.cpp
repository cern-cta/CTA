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
 * @(#)ServerSocket.cpp,v 1.3 $Release$ 2004/07/21 10:43:43 sponcec3
 *
 *
 *
 * @author Benjamin Couturier
 *****************************************************************************/

// Include Files
#if !defined(_WIN32)
#include <net.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#else
#include <winsock2.h>
#endif
#include <errno.h>
#include <serrno.h>
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
castor::io::ServerSocket::ServerSocket(int socket) throw () :
  m_listening(false) {
  m_socket = socket;
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ServerSocket::ServerSocket(const bool reusable)
  throw (castor::exception::Exception) :
  m_listening(false) {
  m_socket =0;
  createSocket();
  if (reusable) this->reusable();
  m_saddr = buildAddress(0);
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ServerSocket::ServerSocket(const unsigned short port,
                                       const bool reusable)
  throw (castor::exception::Exception) :
  m_listening(false) {
  m_socket =0;
  createSocket();
  if (reusable) this->reusable();
  m_saddr = buildAddress(port);
  bind(port, port);
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ServerSocket::ServerSocket(const unsigned short port,
                                       const std::string host,
                                       const bool reusable)
  throw (castor::exception::Exception) :
  m_listening(false) {
  m_socket = 0;
  createSocket();
  if (reusable) this->reusable();
  struct sockaddr_in saddr = buildAddress(port, host);
  bind(port, port);
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ServerSocket::ServerSocket(const unsigned short port,
                                       const unsigned long ip,
                                       const bool reusable)
  throw (castor::exception::Exception) :
  m_listening(false) {
  m_socket = 0;
  createSocket();
  if (reusable) this->reusable();
  m_saddr = buildAddress(port, ip);
  bind(port, port);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::io::ServerSocket::~ServerSocket() throw () {
#if !defined(_WIN32)
  close(m_socket);
#else
  closesocket(m_socket);
#endif
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
#if !defined(_WIN32)
    close(m_socket);
#else
    closesocket(m_socket);
#endif
    m_socket = 0;
    throw ex;
  }
  m_listening = true;
}

//------------------------------------------------------------------------------
// accept
//------------------------------------------------------------------------------
castor::io::ServerSocket* castor::io::ServerSocket::accept()
  throw(castor::exception::Exception) {
  // Check if listen was called, if not, call it
  if (!m_listening) {
    listen();
  }
  // loop until we really get something
  for (;;) {
    struct sockaddr_in saddr;
    memset(&saddr, 0, sizeof(saddr));
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
void castor::io::ServerSocket::bind(int lowPort, int highPort)
  throw (castor::exception::Exception) {
  // let's go through the port range
  int rc = -1;
  for (int port = lowPort;
       (port <= highPort) && 0 != rc;
       port++) {
    // trying to bind the socket
    m_saddr.sin_port = htons(port);
    rc = ::bind(m_socket, (struct sockaddr *)&m_saddr, sizeof(m_saddr));
  }
  if (rc != 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to bind socket";
    if (lowPort == highPort) {
      if (0 != lowPort) {
        ex.getMessage() << " on port " << lowPort;
      }
    } else {
      ex.getMessage() << " in port range ["
                      << lowPort << ", " << highPort << "]";
    }
#if !defined(_WIN32)
    close(m_socket);
#else
    closesocket(m_socket);
#endif
    throw ex;
  }
}
