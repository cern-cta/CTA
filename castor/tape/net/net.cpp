/******************************************************************************
 *                      castor/tape/net/net.cpp
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
 *
 *
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/utils/SmartFd.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/serrno.h"
#include "h/socket_timeout.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sstream>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>


//-----------------------------------------------------------------------------
// createListenerSock
//-----------------------------------------------------------------------------
int castor::tape::net::createListenerSock(
  const char           *addr,
  const unsigned short lowPort,
  const unsigned short highPort,
  unsigned short       &chosenPort)
  throw(castor::exception::Exception) {

  // Check range validity
  if(lowPort < 1) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      "lowPort must be greater than 0"
      ": lowPort=" << lowPort);
  }
  if(highPort < 1) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      "highPort must be greater than 0"
      ": highPort=" << lowPort);
  }
  if(lowPort > highPort) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      "lowPort must be less than or equal to highPort"
      ": lowPort=" << lowPort << " highPort=" << highPort);
  }

  // Create a socket
  utils::SmartFd sock(socket(PF_INET, SOCK_STREAM, IPPROTO_TCP));
  if(sock.get() < 0) {
    const int savedErrno = errno;

    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to create socket"
      ": " << sstrerror(savedErrno));
  }

  // Set the SO_REUSEADDR socket option before calling bind
  {
    int reuseaddrOptval = 1;
    if(0 > setsockopt(sock.get(), SOL_SOCKET, SO_REUSEADDR,
      (char *)&reuseaddrOptval, sizeof(reuseaddrOptval))) {
      const int savedErrno = errno;
    
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to set socket option"
        ": file-descriptor=" << sock.get() <<
        " level=SOL_SOCKET"
        " optname=SO_REUSEADDR"
        " optval=" << reuseaddrOptval <<
        ": " << sstrerror(savedErrno));
    }
  }

  // Address structure to be used to bind the socket
  struct sockaddr_in address;

  // For each port in the range
  for(unsigned short port=lowPort; port<=highPort; ++port) {

    // Try to bind the socket to the port
    utils::setBytes(address, '\0');
    address.sin_family      = AF_INET;
    address.sin_addr.s_addr = inet_addr(addr);
    address.sin_port        = htons(port);

    const int bindRc = bind(sock.get(), (struct sockaddr *) &address,
      sizeof(address));
    const int bindErrno = errno;

    // If the bind was successful
    if(bindRc == 0) {

      // Record the port number of the succesful bind
      chosenPort = port;

      // Mark the socket as being a listener
      if(listen(sock.get(), LISTENBACKLOG) < 0) {
        const int listenErrno = errno;

        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to mark socket as being a listener"
          ": listenSocketFd=" << sock.get() <<
          ": " << sstrerror(listenErrno));
      }

      // Release and return the socket descriptor
      return(sock.release());

    // Else the bind failed
    } else {

      // If the bind failed because the address was in use, then continue
      if(bindErrno == EADDRINUSE) {
        continue;

      // Else throw an exception
      } else {

        TAPE_THROW_CODE(bindErrno,
          ": Failed to bind listener socket"
          ": listenSocketFd=" << sock.get() <<
          ": " << sstrerror(bindErrno));
      }
    }
  }

  // If this line is reached then all ports in the specified range are in use

  // Throw an exception
  castor::exception::NoPortInRange ex(lowPort, highPort);
  ex.getMessage() <<
    "All ports within the specified range are in use"
    ": listenSocketFd=" << sock.get() <<
    ": lowPort=" << lowPort <<
    ": highPort=" << highPort;

  throw(ex);
}



//-----------------------------------------------------------------------------
// acceptConnection
//-----------------------------------------------------------------------------
int castor::tape::net::acceptConnection(const int listenSocketFd)
  throw(castor::exception::Exception) {

  // Throw an exception if listenSocketFd is invalid
  if(listenSocketFd < 0) {
    TAPE_THROW_EX(exception::InvalidArgument,
      ": Invalid listen socket file-descriptor"
      ": listenSocketFd=" << listenSocketFd);
  }

  struct sockaddr_in peerAddress;
  unsigned int       peerAddressLen = sizeof(peerAddress);

  const int connectedSocketFd = accept(listenSocketFd,
    (struct sockaddr *)&peerAddress, &peerAddressLen);
  const int savedErrno = errno;

  if(connectedSocketFd < 0) {
    std::stringstream reason;

    reason <<
      ": Accept failed"
      ": listenSocketFd=" << listenSocketFd;

    if(savedErrno == EINVAL) {
      reason << ": Socket is not listening for connections";
    } else {
      reason << ": " << sstrerror(savedErrno);
    }

    TAPE_THROW_CODE(savedErrno, reason.str());
  }

  return connectedSocketFd;
}


//-----------------------------------------------------------------------------
// acceptConnection
//-----------------------------------------------------------------------------
int castor::tape::net::acceptConnection(const int listenSocketFd,
  const time_t timeout) throw(castor::exception::TimeOut,
  castor::exception::TapeNetAcceptInterrupted, castor::exception::Exception) {

  // Throw an exception if listenSocketFd is invalid
  if(listenSocketFd < 0) {
    TAPE_THROW_EX(exception::InvalidArgument,
      "Invalid listen socket file-descriptor"
      ": listenSocketFd=" << listenSocketFd);
  }

  const time_t startTime = time(NULL);
  fd_set       fdSet;
  timeval      selectTimeout;

  FD_ZERO(&fdSet);
  FD_SET(listenSocketFd, &fdSet);

  selectTimeout.tv_sec  = timeout;
  selectTimeout.tv_usec = 0;

  const int selectRc = select(listenSocketFd + 1, &fdSet, NULL, NULL,
    &selectTimeout);
  const int selectErrno = errno;

  switch(selectRc) {
  case 0: // Select timed out
    {
      TAPE_THROW_EX(castor::exception::TimeOut,
           ": Timed out after " << timeout
        << " seconds whilst trying to accept a connection");
    }
    break;
  case -1: // Select encountered an error
    // If select was interrupted
    if(selectErrno == EINTR) {
      const time_t remainingTime = timeout - (time(NULL) - startTime);

      castor::exception::TapeNetAcceptInterrupted ex(remainingTime);

      throw(ex);
    } else {
      TAPE_THROW_CODE(selectErrno,
        ": Failed to accept connection"
        ": Select failed: " << sstrerror(selectErrno));
    }
    break;
  default: // Select found a file descriptor awaiting attention
    // If it is not the expected connection request
    if(!FD_ISSET(listenSocketFd, &fdSet)) {
      TAPE_THROW_CODE(selectErrno,
        ": Failed to accept connection "
        ": Invalid file descriptor set");
    }
  }

  struct sockaddr_in peerAddress;
  unsigned int       peerAddressLen = sizeof(peerAddress);

  const int connectedSocketFd = accept(listenSocketFd,
    (struct sockaddr *)&peerAddress, &peerAddressLen);
  const int acceptErrno = errno;

  if(connectedSocketFd < 0) {
    std::stringstream reason;

    reason <<
      ": Accept failed"
      ": listenSocketFd=" << listenSocketFd;

    if(acceptErrno == EINVAL) {
      reason << ": Socket is not listening for connections";
    } else {
      reason << ": " << sstrerror(acceptErrno);
    }

    TAPE_THROW_CODE(acceptErrno, reason.str());
  }

  return connectedSocketFd;
}


//-----------------------------------------------------------------------------
// getSockIpPort
//-----------------------------------------------------------------------------
void castor::tape::net::getSockIpPort(const int socketFd,
  unsigned long& ip, unsigned short& port)
  throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    TAPE_THROW_EX(exception::InvalidArgument,
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd);
  }

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getsockname(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    const int savedErrno = errno;

    TAPE_THROW_CODE(savedErrno,
      ": Failed to get socket name"
      ": socketFd=" << socketFd <<
      ": " << sstrerror(savedErrno));
  }

  ip   = ntohl(address.sin_addr.s_addr);
  port = ntohs(address.sin_port);
}


//-----------------------------------------------------------------------------
// getPeerIpPort
//-----------------------------------------------------------------------------
void castor::tape::net::getPeerIpPort(const int socketFd,
  unsigned long& ip, unsigned short& port)
  throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    TAPE_THROW_EX(exception::InvalidArgument,
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd);
  }

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getpeername(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    const int savedErrno = errno;

    TAPE_THROW_CODE(savedErrno,
      ": Failed to get peer name"
      ": socketFd=" << socketFd <<
      ": " << sstrerror(savedErrno));
  }

  ip   = ntohl(address.sin_addr.s_addr);
  port = ntohs(address.sin_port);
}


//------------------------------------------------------------------------------
// getSockHostName
//------------------------------------------------------------------------------
void castor::tape::net::getSockHostName(const int socketFd,
  char *buf, size_t len) throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    TAPE_THROW_EX(exception::InvalidArgument,
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd);
  }

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getsockname(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    const int savedErrno = errno;

    TAPE_THROW_CODE(savedErrno,
      ": Failed to get socket name"
      ": socketFd=" << socketFd <<
      ": " << sstrerror(savedErrno));
  }

  char hostName[HOSTNAMEBUFLEN];
  char serviceName[SERVICENAMEBUFLEN];
  const int error = getnameinfo((const struct sockaddr*)&address, addressLen,
    hostName, sizeof(hostName), serviceName, sizeof(serviceName), 0);

  if(error != 0) {
    TAPE_THROW_CODE(SENOSHOST,
      ": Failed to get host information by address"
      ": socketFd=" << socketFd <<
      ": " << gai_strerror(error));
  }

  utils::copyString(buf, len, hostName);
}


//------------------------------------------------------------------------------
// getSockIpHostnamePort
//------------------------------------------------------------------------------
void castor::tape::net::getSockIpHostnamePort(const int socketFd,
  unsigned long& ip, char *hostName, size_t hostNameLen,
  unsigned short& port) throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    TAPE_THROW_EX(exception::InvalidArgument,
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd);
  }

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getsockname(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    const int savedErrno = errno;

    TAPE_THROW_CODE(savedErrno,
      ": Failed to get socket name"
      ": socketFd=" << socketFd <<
      ": " << sstrerror(savedErrno));
  }

  ip   = ntohl(address.sin_addr.s_addr);
  port = ntohs(address.sin_port);

  {
    char serviceName[SERVICENAMEBUFLEN];
    const int rc = getnameinfo((const struct sockaddr*)&address, addressLen,
      hostName, hostNameLen, serviceName, sizeof(serviceName), 0);

    if(rc != 0) {
      TAPE_THROW_CODE(SENOSHOST,
        ": Failed to get host information by address"
        ": socketFd=" << socketFd <<
        ": " << gai_strerror(rc));
    }
  }
}


//------------------------------------------------------------------------------
// getPeerHostName
//------------------------------------------------------------------------------
void castor::tape::net::getPeerHostName(const int socketFd,
  char *buf, size_t len) throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    TAPE_THROW_EX(exception::InvalidArgument,
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd);
  }

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getpeername(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    const int savedErrno = errno;

    TAPE_THROW_CODE(savedErrno,
      ": Failed to get peer name"
      ": socketFd=" << socketFd <<
      ": " << sstrerror(savedErrno));
  }

  {
    char hostName[HOSTNAMEBUFLEN];
    char serviceName[SERVICENAMEBUFLEN];
    const int rc = getnameinfo((const struct sockaddr*)&address, addressLen,
      hostName, sizeof(hostName), serviceName, sizeof(serviceName), 0);

    if(rc != 0) {
      TAPE_THROW_CODE(SENOSHOST,
        ": Failed to get host information by address"
        ": socketFd=" << socketFd <<
        ": " << gai_strerror(rc));
    }

    utils::copyString(buf, len, hostName);
  }
}


//------------------------------------------------------------------------------
// writeIp
//------------------------------------------------------------------------------
void castor::tape::net::writeIp(std::ostream &os,
  const unsigned long ip) throw() {
  os << ((ip >> 24) & 0x000000FF) << "."
     << ((ip >> 16) & 0x000000FF) << "."
     << ((ip >>  8) & 0x000000FF) << "."
     << ( ip        & 0x000000FF);
}


//------------------------------------------------------------------------------
// writeSockDescription
//------------------------------------------------------------------------------
void castor::tape::net::writeSockDescription(std::ostream &os,
  const int socketFd) throw() {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    TAPE_THROW_EX(exception::InvalidArgument,
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd);
  }

  unsigned long  localIp   = 0;
  unsigned short localPort = 0;
  unsigned long  peerIp    = 0;
  unsigned short peerPort  = 0;

  try {
    getSockIpPort(socketFd, localIp, localPort);
  } catch(castor::exception::Exception &e) {
    localIp   = 0;
    localPort = 0;
  }

  try {
    getPeerIpPort(socketFd, peerIp, peerPort);
  } catch(castor::exception::Exception &e) {
    peerIp   = 0;
    peerPort = 0;
  }

  os << "{local=";
  writeIp(os, localIp);
  os << ":" << localPort;
  os << ",peer=";
  writeIp(os, peerIp);
  os << ":" << peerPort;
  os << "}";
}


//------------------------------------------------------------------------------
// readBytes
//------------------------------------------------------------------------------
void castor::tape::net::readBytes(const int socketFd, const int timeout,
  const int nbBytes, char *buf) throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    TAPE_THROW_EX(exception::InvalidArgument,
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd);
  }

  bool connClosed = false;

  readBytesFromCloseable(connClosed, socketFd, timeout, nbBytes, buf);

  if(connClosed) {
    std::stringstream oss;
    oss << "Failed to read " << nbBytes << " bytes from socket: ";
    writeSockDescription(oss, socketFd);
    oss << ": Connection was closed by the remote end";

    TAPE_THROW_CODE(SECONNDROP, oss.str());
  }
}


//------------------------------------------------------------------------------
// readBytesFromCloseable
//------------------------------------------------------------------------------
void castor::tape::net::readBytesFromCloseable(bool &connClosed, 
  const int socketFd, const int timeout, const int nbBytes, char *buf)
  throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    TAPE_THROW_EX(exception::InvalidArgument,
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd);
  }

  connClosed = false;
  const int rc = netread_timeout(socketFd, buf, nbBytes, timeout);
  int savedSerrno = serrno;

  switch(rc) {
  case -1:
    {
      std::stringstream oss;
      oss << ": Failed to read " << nbBytes << " bytes from socket: " <<
        " socketFd=" << socketFd << ": ";
      writeSockDescription(oss, socketFd);
      // netread_timeout can return -1 with serrno set to 0
      if(0 == savedSerrno) {
        savedSerrno = SEINTERNAL;
        oss << ": Unknown error";
      } else {
        oss << ": " << sstrerror(savedSerrno);
      }
      if(SETIMEDOUT == savedSerrno) {
        oss << ": timeout=" << timeout;
      }

      TAPE_THROW_CODE(savedSerrno, oss.str());
    }
    break;
  case 0:
    {
      connClosed = true;
    }
    break;
  default:
    if (rc != nbBytes) {

      std::stringstream oss;
      oss << "Failed to read " << nbBytes << " bytes from socket: ";
      writeSockDescription(oss, socketFd);
      oss
        << ": Read the wrong number of bytes"
        << ": Expected: " << nbBytes
        << ": Read: " << rc;

      TAPE_THROW_CODE(SECOMERR, oss.str());
    }
  }
}


//------------------------------------------------------------------------------
// writeBytes
//------------------------------------------------------------------------------
void castor::tape::net::writeBytes(const int socketFd, const int timeout,
  const int nbBytes, char *const buf) throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    TAPE_THROW_EX(exception::InvalidArgument,
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd);
  }

  const int rc = netwrite_timeout(socketFd, buf, nbBytes, timeout);
  int savedSerrno = serrno;

  switch(rc) {
  case -1:
    {
      std::stringstream oss;
      oss << ": Failed to write " << nbBytes << " bytes to socket: " <<
        " socketFd=" << socketFd << ": ";
      writeSockDescription(oss, socketFd);
      // netwrite_timeout can return -1 with serrno set to 0
      if(0 == savedSerrno) {
        savedSerrno = SEINTERNAL;
        oss << ": Unknown error";
      } else {
        oss << ": " << sstrerror(savedSerrno);
      }
      if(savedSerrno == SETIMEDOUT) {
        oss << ": timeout=" << timeout;
      }

      TAPE_THROW_CODE(SECOMERR, oss.str());
    }
  case 0:
    {
      std::stringstream oss;
      oss << ": Failed to write " << nbBytes << " bytes to socket: ";
      writeSockDescription(oss, socketFd);
      oss << ": Connection dropped";

      TAPE_THROW_CODE(SECONNDROP, oss.str());
    }
  default:
    if(rc != nbBytes) {
      std::stringstream oss;
      oss << ": Failed to write " << nbBytes << " bytes to socket: ";
      writeSockDescription(oss, socketFd);
      oss
        << ": Wrote the wrong number of bytes"
        << ": Expected: " << nbBytes
        << ": Wrote: " << rc;
      TAPE_THROW_CODE(SECOMERR, oss.str());
    }
  }
}
