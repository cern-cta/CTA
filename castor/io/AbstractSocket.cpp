/******************************************************************************
 *                      AbstractSocket.cpp
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
 * @(#)AbstractSocket.cpp,v 1.6 $Release$ 2006/01/17 09:52:22 itglp
 *
 * Implementation of an abtract socket interface
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
#include <serrno.h>
#include <sys/types.h>
#include <string>
#include "castor/exception/Exception.hpp"
#include "castor/io/biniostream.h"
#include "castor/Services.hpp"
#include "castor/io/StreamAddress.hpp"

// Local Includes
#include "AbstractSocket.hpp"

// definition of some constants
#define STG_CALLBACK_BACKLOG 2

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AbstractSocket::AbstractSocket(int socket) throw () :
  m_socket(socket) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AbstractSocket::AbstractSocket(const bool reusable)
  throw (castor::exception::Exception) :
  m_socket(-1), m_reusable(reusable) {
    m_saddr = buildAddress(0);
  }

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AbstractSocket::AbstractSocket(const unsigned short port,
                                           const bool reusable)
  throw (castor::exception::Exception) :
  m_socket(-1), m_reusable(reusable) {
    m_saddr = buildAddress(port);
  }

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AbstractSocket::AbstractSocket(const unsigned short port,
                                           const std::string host,
                                           const bool reusable)
  throw (castor::exception::Exception) :
  m_socket(-1), m_reusable(reusable) {
    m_saddr = buildAddress(port, host);
  }

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::AbstractSocket::AbstractSocket(const unsigned short port,
                                           const unsigned long ip,
                                           const bool reusable)
  throw (castor::exception::Exception) :
  m_socket(-1), m_reusable(reusable) {
    m_saddr = buildAddress(port, ip);
  }

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::io::AbstractSocket::~AbstractSocket() throw () {
  this->close();
}

//------------------------------------------------------------------------------
// getPortIp
//------------------------------------------------------------------------------
void castor::io::AbstractSocket::getPortIp(unsigned short& port,
                                           unsigned long& ip) const
  throw (castor::exception::Exception) {
  // get address
#if !defined(_WIN32)
  unsigned int soutlen = sizeof(struct sockaddr_in);
#else
  int soutlen = sizeof(struct sockaddr_in);
#endif
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
#if !defined(_WIN32)
  unsigned int soutlen = sizeof(struct sockaddr_in);
#else
  int soutlen = sizeof(struct sockaddr_in);
#endif
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
// Sets the socket to reusable address
//------------------------------------------------------------------------------
void castor::io::AbstractSocket::setReusable()
  throw (castor::exception::Exception) {
  if(!m_reusable) return;
  int on = 1;
  if (setsockopt (m_socket, SOL_SOCKET, SO_REUSEADDR,
                  (char *)&on, sizeof(on)) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to set socket to reusable";
    throw ex;
  }
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
// isDataAvailable
//------------------------------------------------------------------------------
bool castor::io::AbstractSocket::isDataAvailable() throw() {
  // which socket to check
  fd_set rd_setcp;
  FD_ZERO(&rd_setcp);
  FD_SET(m_socket,&rd_setcp);
  // setup timeout to 0
  struct timeval timeout;
  timeout.tv_sec = 0;
  timeout.tv_usec = 0;
  // the check
  if (select(1,&rd_setcp,NULL,NULL,&timeout) <= 0) {
    // case whether nothing can be read or where an error occured
    // so we ignore errors
    return false;
  }
  return true;
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
#if !defined(_WIN32)
    errno = EHOSTUNREACH;
#else
    errno = WSAEHOSTUNREACH;
#endif
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unknown host " << host << " (h_errno = " << h_errno << ")";
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
// close
//------------------------------------------------------------------------------
void castor::io::AbstractSocket::close() throw () {
#if !defined(_WIN32)
  ::close(m_socket);
#else
  closesocket(m_socket);
#endif
  m_socket = -1;
}
