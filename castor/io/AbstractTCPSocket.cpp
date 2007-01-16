/******************************************************************************
 *                     AbstractTCPSocket.cpp
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
 * @(#)AbstractTCPSocket.cpp,v 1.6 $Release$ 2006/01/17 09:52:22 itglp
 *
 * Implementation of a TCP abtract socket interface
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <net.h>
#if !defined(_WIN32)
#include <netdb.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#else
#include <winsock2.h>
#endif
#include <errno.h>
#include <serrno.h>
#include <sys/types.h>
#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

// Local Includes
#include "AbstractTCPSocket.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AbstractTCPSocket::AbstractTCPSocket(int socket) throw () :
  AbstractSocket(socket) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AbstractTCPSocket::AbstractTCPSocket(const bool reusable)
  throw (castor::exception::Exception) :
  AbstractSocket(reusable) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AbstractTCPSocket::AbstractTCPSocket(const unsigned short port,
                                                 const bool reusable)
  throw (castor::exception::Exception) :
  AbstractSocket(port, reusable) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AbstractTCPSocket::AbstractTCPSocket(const unsigned short port,
                                                 const std::string host,
                                                 const bool reusable)
  throw (castor::exception::Exception) :
  AbstractSocket(port, host, reusable) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AbstractTCPSocket::AbstractTCPSocket(const unsigned short port,
                                                 const unsigned long ip,
                                                 const bool reusable)
  throw (castor::exception::Exception) :
  AbstractSocket(port, ip, reusable) {}

//------------------------------------------------------------------------------
// createSocket
//------------------------------------------------------------------------------
void castor::io::AbstractTCPSocket::createSocket()
  throw (castor::exception::Exception) {
  // creates the socket
  if ((m_socket = ::socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Can't create socket";
    throw ex;
  }
  int yes = 1;
  if (setsockopt(m_socket,IPPROTO_TCP,TCP_NODELAY,(char *)&yes,sizeof(yes)) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Can't set socket in TCP_NODELAY mode.";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// sendBuffer
//------------------------------------------------------------------------------
void castor::io::AbstractTCPSocket::sendBuffer(const unsigned int magic,
                                               const char* buf,
                                               const int n)
  throw (castor::exception::Exception) {
  // Sends the buffer with a header (magic number + size)
  if (netwrite(m_socket,
               (char*)(&magic),
               sizeof(unsigned int)) != sizeof(unsigned int) ||
      netwrite(m_socket,
               (char*)(&n),
               sizeof(unsigned int)) != sizeof(unsigned int) ||
      netwrite(m_socket, (char *)buf, n) != n) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "Unable to send data";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readBuffer
//------------------------------------------------------------------------------
void castor::io::AbstractTCPSocket::readBuffer(const unsigned int magic,
                                               char** buf,
                                               int& n)
  throw (castor::exception::Exception) {
  // First read the header
  unsigned int header[2];
  int ret = netread(m_socket,
                    (char*)&header,
                    2*sizeof(unsigned int));
  if (ret != 2*sizeof(unsigned int)) {
    if (0 == ret) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to receive header\n"
                      << "The connection was closed by remote end";
      throw ex;
    } else if (-1 == ret) {
      castor::exception::Exception ex(serrno);
      ex.getMessage() << "Unable to receive header";
      throw ex;
    } else {
      castor::exception::Internal ex;
      ex.getMessage() << "Received header is too short : only "
                      << ret << " bytes";
      throw ex;
    }
  }
  if (header[0] != magic) {
    castor::exception::Internal ex;
    ex.getMessage() << "Bad magic number : " << std::ios::hex
                    << header[0] << " instead of "
                    << std::ios::hex << magic;
    throw ex;
  }
  // Now read the data
  n = header[1];
  *buf = (char*) malloc(n);
  ssize_t readBytes = 0;
  while (readBytes < n) {
#if !defined(_WIN32)
    ssize_t nb = ::read(m_socket, (*buf)+readBytes, n - readBytes);
#else
    ssize_t nb = ::recv(m_socket, (*buf)+readBytes, n - readBytes, 0);
#endif
    if (nb == -1) {
      if (errno == EAGAIN) {
        continue;
      } else {
        break;
      }
    }
    readBytes += nb;
  }
  if (readBytes < n) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "Unable to receive all data. "
                    << "Got " << readBytes << " bytes instead of "
                    << n << ".";
    throw ex;
  }
}
