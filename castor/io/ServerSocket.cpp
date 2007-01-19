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
#include "castor/exception/Communication.hpp"
#include "castor/io/biniostream.h"
#include "castor/io/StreamAddress.hpp"

#include <time.h>

// Local Includes
#include "ServerSocket.hpp"


// definition of some constants
#define STG_CALLBACK_BACKLOG 2


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ServerSocket::ServerSocket(int socket) throw () :
  AbstractTCPSocket(socket),
  m_listening(false) {
  srand(time(NULL));
  m_lowPort = m_highPort = -1;
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ServerSocket::ServerSocket(const bool reusable)
  throw (castor::exception::Exception) :
  AbstractTCPSocket(reusable),
  m_listening(false) {
    srand(time(NULL));
    m_lowPort = m_highPort = -1;
    createSocket();
    setReusable();
  }

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ServerSocket::ServerSocket(const unsigned short port,
                                       const bool reusable)
  throw (castor::exception::Exception) :
  AbstractTCPSocket(port, reusable),
  m_listening(false) {
    srand(time(NULL));
    m_lowPort = m_highPort = -1;
    createSocket();
    setReusable();
    bind(port, port);
  }

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ServerSocket::ServerSocket(const unsigned short port,
                                       const std::string host,
                                       const bool reusable)
  throw (castor::exception::Exception) :
  AbstractTCPSocket(port, host, reusable),
  m_listening(false) {
    srand(time(NULL));
    m_lowPort = m_highPort = -1;
    createSocket();
    setReusable();
    bind(port, port);
  }

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::ServerSocket::ServerSocket(const unsigned short port,
                                       const unsigned long ip,
                                       const bool reusable)
  throw (castor::exception::Exception) :
  AbstractTCPSocket(port, ip, reusable),
  m_listening(false) {
    m_lowPort = m_highPort = -1;
    srand(time(NULL));
    createSocket();
    setReusable();
    bind(port, port);
  }

//------------------------------------------------------------------------------
// listen
//------------------------------------------------------------------------------
void castor::io::ServerSocket::listen()
  throw(castor::exception::Exception) {

  // watch out sometimes for high-stress tests the listen may not return
  // error code  EADDRINUSE

  if (::listen(m_socket, STG_CALLBACK_BACKLOG) < 0) {
    this->close();

    if(errno == EADDRINUSE && m_lowPort > 0) {
      /* it may happen that another bind() successfully got the same port at the
         same time (bind() is not entirely process-safe!). In this case, we
         just reinit, rebind again and recursively retry */
      createSocket();
      setReusable();
      bind();
      listen();
    }
    else {
      castor::exception::Exception ex(errno);
      ex.getMessage() << "Unable to listen on socket";
      throw ex;
    }
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
  // check range validity
  if (lowPort < 1024 || highPort > 65535 || lowPort > highPort) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to use socket in port range ["
                    << lowPort << ", " << highPort << "]";
    throw ex;
  }
  m_lowPort = lowPort;
  m_highPort = highPort;

  bind();
}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void castor::io::ServerSocket::bind()
  throw (castor::exception::Exception) {
  int rc = -1;
  int port;

  if(m_lowPort == -1 || m_highPort == -1) {
    // it won't happen, bind() is private!
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to bind on an uninitialized port range";
    throw ex;
  }

  // randomly select a free port in the allowed port range
  while (0 != rc){
    port=(rand() % (m_highPort-m_lowPort+1)) + m_lowPort;
    m_saddr.sin_port = htons(port);
    rc = ::bind(m_socket, (struct sockaddr *)&m_saddr, sizeof(m_saddr));

    if(0 != rc && errno == EADDRINUSE && m_lowPort == m_highPort) {
      // this is a server-side daemon trying to bind to an used port
      // don't retry and fire exception
      castor::exception::Communication e("Port in use", EADDRINUSE);
      throw e;
    }
  }
}
