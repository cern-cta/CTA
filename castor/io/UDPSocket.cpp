/******************************************************************************
 *                     UDPSocket.cpp
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
 * @(#)UDPSocket.cpp,v 1.6 $Release$ 2006/01/17 09:52:22 itglp
 *
 * Implementation of a UDP abtract socket interface
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
#else
#include <winsock2.h>
#endif
#include <errno.h>
#include <string.h>
#include <serrno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/TooBig.hpp"
#include "castor/io/UDPSocket.hpp"

#define MAX_UDP_DATAGRAM_LENGTH 1024

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::UDPSocket::UDPSocket(const unsigned short port,
                                 const bool reusable)
  throw (castor::exception::Exception) :
  AbstractSocket(port, reusable) {
    createSocket();
    setReusable();
  }

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::UDPSocket::UDPSocket(const unsigned short port,
				 const std::string host)
  throw (castor::exception::Exception) :
  AbstractSocket(port, host, false) {
  createSocket();
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::UDPSocket::UDPSocket(const unsigned short port,
				 const unsigned long ip)
  throw (castor::exception::Exception) :
  AbstractSocket(port, ip, false) {
  createSocket();
}

//------------------------------------------------------------------------------
// createSocket
//------------------------------------------------------------------------------
void castor::io::UDPSocket::createSocket()
  throw (castor::exception::Exception) {
  // creates the socket
  if ((m_socket = ::socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Can't create socket";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// sendBuffer
//------------------------------------------------------------------------------
void castor::io::UDPSocket::sendBuffer(const unsigned int magic,
                                       const char* buf,
                                       const int n)
  throw (castor::exception::Exception) {
  // create new buffer to send everything in one go
  int size = n + 2 * sizeof(unsigned int);
  char* newBuf = new char[size];
  strncpy(newBuf, (char*)(&magic), sizeof(unsigned int));
  strncpy(newBuf + sizeof(unsigned int), (char*)(&n), sizeof(unsigned int));
  strncpy(newBuf + 2 * sizeof(unsigned int), buf, n);
  // Sends the buffer with a header (magic number + size)
  if (sendto(m_socket, newBuf, size, MSG_DONTWAIT,
             (struct sockaddr *)(&m_saddr), sizeof(m_saddr)) != n) {
    delete [] newBuf;
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to send data";
    throw ex;
  }
  delete [] newBuf;
}

//------------------------------------------------------------------------------
// readBuffer
//------------------------------------------------------------------------------
void castor::io::UDPSocket::readBuffer(const unsigned int magic,
                                       char** buf,
                                       int& n)
  throw (castor::exception::Exception) {
  // Read everything in one go. Max allowed is 1K
  char* internalBuf = new char[MAX_UDP_DATAGRAM_LENGTH];
  socklen_t fromLen = sizeof(m_saddr);
  int ret = recvfrom(m_socket, internalBuf, MAX_UDP_DATAGRAM_LENGTH, 0,
                     (struct sockaddr *)(&m_saddr), &fromLen);
  if (-1 == ret) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "Unable to read datagram data";
    delete [] internalBuf;
    throw ex;
  } else if (ret < (int)(2*sizeof(unsigned int))) {
    castor::exception::Internal ex;
    ex.getMessage() << "Received datagram is too short : only "
                    << ret << " bytes";
    delete [] internalBuf;
    throw ex;
  }
  // Check magic number
  unsigned int *recvMagic = (unsigned int*)internalBuf;
  if (*recvMagic != magic) {
    castor::exception::Internal ex;
    ex.getMessage() << "Bad magic number : " << std::ios::hex
                    << *recvMagic << " instead of "
                    << std::ios::hex << magic;
    delete [] internalBuf;
    throw ex;
  }
  // Check number of bytes sent
  n = *((unsigned int*)(internalBuf + sizeof(unsigned int)));
  if (n + 2*sizeof(unsigned int) > MAX_UDP_DATAGRAM_LENGTH) {
    castor::exception::TooBig ex;
    ex.getMessage() << "Wrong datagram (longer that max size : "
                    << n << " > "
                    << (MAX_UDP_DATAGRAM_LENGTH - 2*sizeof(unsigned int))
                    << ")";
    delete [] internalBuf;
    throw ex;
  }
  // Now return the data
  *buf = (char*) malloc(n);
  strncpy(*buf, internalBuf, n);
  delete [] internalBuf;
}

#undef MAX_UDP_DATAGRAM_LENGTH
