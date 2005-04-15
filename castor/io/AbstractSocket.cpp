/******************************************************************************
 *                      Socket.cpp
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
 * @(#)$RCSfile: AbstractSocket.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2005/04/15 16:55:38 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
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
#include "AbstractSocket.hpp"

// definition of some constants
#define STG_CALLBACK_BACKLOG 2


//------------------------------------------------------------------------------
// getPortIp
//------------------------------------------------------------------------------
void castor::io::AbstractSocket::getPortIp(unsigned short& port,
                                           unsigned long& ip) const
  throw (castor::exception::Exception) {
  // get address
  unsigned int soutlen = sizeof(struct sockaddr_in);
  struct sockaddr_in sout;
  if (getsockname(m_socket, (struct sockaddr*)&sout, &soutlen) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to get socket name";
    throw ex;
  }
  // extract port and ip
  port = ntohs(sout.sin_port);
  ip = ntohl(sout.sin_addr.s_addr);
}


//------------------------------------------------------------------------------
// getPeerIp
//------------------------------------------------------------------------------
void castor::io::AbstractSocket::getPeerIp(unsigned short& port,
                                           unsigned long& ip) const
  throw (castor::exception::Exception) {
  // get address
  unsigned int soutlen = sizeof(struct sockaddr_in);
  struct sockaddr_in sout;
  if (getpeername(m_socket, (struct sockaddr*)&sout, &soutlen) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to get peer name";
    throw ex;
  }
  // extract port and ip
  port = ntohs(sout.sin_port);
  ip = ntohl(sout.sin_addr.s_addr);
}


//------------------------------------------------------------------------------
// sendObject
//------------------------------------------------------------------------------
void castor::io::AbstractSocket::sendObject(castor::IObject& obj)
  throw(castor::exception::Exception) {
  // marshalls the object
  castor::io::biniostream buffer;
  castor::io::StreamAddress ad(buffer, "StreamCnvSvc", castor::SVC_STREAMCNV);
  svcs()->createRep(&ad, &obj, true);
  // sends the object through the socket
  sendBuffer(SEND_REQUEST_MAGIC,
             buffer.str().data(),
             buffer.str().length());
}

//------------------------------------------------------------------------------
// readObject
//------------------------------------------------------------------------------
castor::IObject* castor::io::AbstractSocket::readObject()
  throw(castor::exception::Exception) {
  // reads from the socket
  char* buffer;
  int length;
  readBuffer(SEND_REQUEST_MAGIC, &buffer, length);
  // package the buffer
  std::string sbuffer(buffer, length);
  castor::io::biniostream input(sbuffer);
  // unmarshalls the object
  castor::io::StreamAddress ad(input, "StreamCnvSvc", castor::SVC_STREAMCNV);
  castor::IObject* obj = svcs()->createObj(&ad);
  free(buffer);
  // return
  return obj;
}

//------------------------------------------------------------------------------
// createAbstractSocket
//------------------------------------------------------------------------------
void castor::io::AbstractSocket::createSocket()
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
// buildAddress
//------------------------------------------------------------------------------
sockaddr_in castor::io::AbstractSocket::buildAddress(const unsigned short port)
  throw (castor::exception::Exception) {
  // Builds the address
  struct sockaddr_in saddr;
  memset(&saddr, 0, sizeof(saddr));
  saddr.sin_addr.s_addr = htonl(INADDR_ANY);
  saddr.sin_port = htons(port);
  saddr.sin_family = AF_INET;
  return saddr;
}


//------------------------------------------------------------------------------
// buildAddress
//------------------------------------------------------------------------------
sockaddr_in castor::io::AbstractSocket::buildAddress(const unsigned short port,
                                                     const std::string host)
  throw (castor::exception::Exception) {
  // get host information
  struct hostent *hp;
  if ((hp = gethostbyname(host.c_str())) == 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unknown host : " << host;
    throw ex;
  }
  // Builds the address
  struct sockaddr_in saddr;
  memset(&saddr, 0, sizeof(saddr));
  memcpy(&saddr.sin_addr, hp->h_addr, hp->h_length);
  saddr.sin_port = htons(port);
  saddr.sin_family = AF_INET;
  return saddr;
}

//------------------------------------------------------------------------------
// buildAddress
//------------------------------------------------------------------------------
sockaddr_in castor::io::AbstractSocket::buildAddress(const unsigned short port,
                                                     const unsigned long ip)
  throw (castor::exception::Exception) {
  // Builds the address
  struct sockaddr_in saddr;
  memset(&saddr, 0, sizeof(saddr));
  saddr.sin_addr.s_addr = htonl(ip);
  saddr.sin_port = htons(port);
  saddr.sin_family = AF_INET;
  return saddr;
}

//------------------------------------------------------------------------------
// sendBuffer
//------------------------------------------------------------------------------
void castor::io::AbstractSocket::sendBuffer(const unsigned int magic,
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
void castor::io::AbstractSocket::readBuffer(const unsigned int magic,
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
    ssize_t nb = ::read(m_socket, (*buf)+readBytes, n - readBytes);
    if (nb <=0 ) {
      if (errno == EAGAIN) continue;
      fprintf(stdout, "Got an error while reading from fd %d. errno = %d\n",
	      m_socket, errno);
    } else {
      fprintf(stdout, "Read %d bytes from fd %d. %d bytes were to read in total\n",
	      nb, m_socket, n);
    }
    if (nb == -1) break;
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
