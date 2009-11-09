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
#include "getconfent.h"
#include <limits.h>

// Local Includes
#include "AbstractTCPSocket.hpp"

// Definitions
#define MAX_NETDATA_SIZE 0x1400000 // 20MB


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AbstractTCPSocket::AbstractTCPSocket(int socket) throw () :
  AbstractSocket(socket),
  m_maxNetDataSize(-1) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AbstractTCPSocket::AbstractTCPSocket(const bool reusable)
  throw (castor::exception::Exception) :
  AbstractSocket(reusable),
  m_maxNetDataSize(-1) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AbstractTCPSocket::AbstractTCPSocket(const unsigned short port,
                                                 const bool reusable)
  throw (castor::exception::Exception) :
  AbstractSocket(port, reusable),
  m_maxNetDataSize(-1) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AbstractTCPSocket::AbstractTCPSocket(const unsigned short port,
                                                 const std::string host,
                                                 const bool reusable)
  throw (castor::exception::Exception) :
  AbstractSocket(port, host, reusable),
  m_maxNetDataSize(-1) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AbstractTCPSocket::AbstractTCPSocket(const unsigned short port,
                                                 const unsigned long ip,
                                                 const bool reusable)
  throw (castor::exception::Exception) :
  AbstractSocket(port, ip, reusable),
  m_maxNetDataSize(-1) {}

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
  if (setsockopt(m_socket, IPPROTO_TCP, TCP_NODELAY, (char *)&yes, sizeof(yes)) < 0) {
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
  if (netwrite_timeout(m_socket,
		       (char*)(&magic),
		       sizeof(unsigned int),
		       m_timeout) != sizeof(unsigned int) ||
      netwrite_timeout(m_socket,
		       (char*)(&n),
		       sizeof(unsigned int),
		       m_timeout) != sizeof(unsigned int) ||
      netwrite_timeout(m_socket, (char *)buf, n, m_timeout) != n) {
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
  // Determine the maximum amount of bytes per message that can be read from
  // the socket when readBuffer is called. By default this is 20MB
  if (m_maxNetDataSize == -1) {
    const char *value = getconfent("CLIENT", "MAX_NETDATA_SIZE", 0);
    if (0 != value) {
      errno = 0;
      long bytes = strtol(value, 0, 10);
      // Check that the string converted to an integer is valid
      if (((bytes == 0) && (errno == ERANGE)) || (bytes > INT_MAX)) {
	castor::exception::Exception ex(EINVAL);
	ex.getMessage() << "Invalid CLIENT/MAX_NETDATA_SIZE option, " << strerror(ERANGE);
	throw ex;
      } else if (bytes < MAX_NETDATA_SIZE) {
	castor::exception::Exception ex(EINVAL);
	ex.getMessage() << "Invalid CLIENT/MAX_NETDATA_SIZE option, value too small";
	throw ex;
      }
      m_maxNetDataSize = (int)bytes;
    } else {
      // No value defined so use the default
      m_maxNetDataSize = MAX_NETDATA_SIZE;
    }
  }
  // First read the header
  unsigned int header[2];
  int ret = netread_timeout(m_socket,
			    (char*)&header,
			    2 * sizeof(unsigned int),
			    m_timeout);
  if (ret != 2 * sizeof(unsigned int)) {
    if (0 == ret) {
      castor::exception::Exception ex(serrno);
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
    ex.getMessage() << "Bad magic number : 0x" << std::hex
                    << header[0] << " instead of 0x"
                    << std::hex << magic;
    throw ex;
  }
  // For security purposes we restrict the maximum possible length of the data
  // returned
  n = header[1];
  if ((n > m_maxNetDataSize) || (n < 0)) {
    castor::exception::Exception ex(ESTMEM);
    ex.getMessage() << "Message body is too long";
    throw ex;
  }
  *buf = (char*) malloc(n);
  if (0 == *buf) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Failed to allocate memory for " << n << " bytes";
    throw ex;
  }

  // Now read the data
  ret = netread_timeout(m_socket, *buf, n, m_timeout);
  if (ret != n) {
    if (0 == ret) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to receive message body\n"
                      << "The connection was closed by remote end";
      throw ex;
    } else if (-1 == ret) {
      castor::exception::Exception ex(serrno);
      ex.getMessage() << "Unable to receive message body";
      throw ex;
    } else {
      castor::exception::Internal ex;
      ex.getMessage() << "Received message body is too short : only "
                      << ret << " bytes of " << n << " transferred";
      throw ex;
    }
  }
}
