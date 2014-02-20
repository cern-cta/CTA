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
#include "castor/io/io.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/serrno.h"
#include "h/net.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <sstream>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>

//-----------------------------------------------------------------------------
// createListenerSock
//-----------------------------------------------------------------------------
int castor::io::createListenerSock(
  const unsigned short port)
  throw(castor::exception::Exception) {
  const unsigned short lowPort = port;
  const unsigned short highPort = port;
  unsigned short chosenPort = 0;

  struct in_addr networkAddress;
  networkAddress.s_addr = INADDR_ANY;

  return createListenerSock(networkAddress, lowPort, highPort, chosenPort);
}

//-----------------------------------------------------------------------------
// createListenerSock
//-----------------------------------------------------------------------------
int castor::io::createListenerSock(
  const unsigned short lowPort,
  const unsigned short highPort,
  unsigned short       &chosenPort)
  throw(castor::exception::Exception) {

  struct in_addr networkAddress;
  networkAddress.s_addr = INADDR_ANY;

  return createListenerSock(networkAddress, lowPort, highPort, chosenPort);
}

//-----------------------------------------------------------------------------
// createListenerSock
//-----------------------------------------------------------------------------
int castor::io::createListenerSock(
  const std::string    &addr,
  const unsigned short lowPort,
  const unsigned short highPort,
  unsigned short       &chosenPort)
  throw(castor::exception::Exception) {

  struct in_addr networkAddress;

  const int rc = inet_pton(AF_INET, addr.c_str(), &networkAddress);
  if(0 >= rc) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Failed to create listener socket:"
      " Failed to convert string to network address: value=" << addr;
    throw ex;
  }

  return createListenerSock(networkAddress, lowPort, highPort, chosenPort);
}

//-----------------------------------------------------------------------------
// createListenerSock
//-----------------------------------------------------------------------------
int castor::io::createListenerSock(
  const struct in_addr &addr,
  const unsigned short lowPort,
  const unsigned short highPort,
  unsigned short       &chosenPort)
  throw(castor::exception::Exception) {

  // Check range validity
  if(lowPort < 1) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() <<  "lowPort must be greater than 0"
      ": lowPort=" << lowPort;
    throw ex;
  }
  if(highPort < 1) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "highPort must be greater than 0"
      ": highPort=" << lowPort;
    throw ex;
  }
  if(lowPort > highPort) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() <<  "lowPort must be less than or equal to highPort"
      ": lowPort=" << lowPort << " highPort=" << highPort;
    throw ex;
  }

  // Create a socket
  utils::SmartFd sock(socket(PF_INET, SOCK_STREAM, IPPROTO_TCP));
  if(sock.get() < 0) {
    const int savedErrno = errno;

    castor::exception::Internal ex;
    ex.getMessage() << ": Failed to create socket"
      ": " << sstrerror(savedErrno);
    throw ex;
  }

  // Set the SO_REUSEADDR socket option before calling bind
  {
    int reuseaddrOptval = 1;
    if(0 > setsockopt(sock.get(), SOL_SOCKET, SO_REUSEADDR,
      (char *)&reuseaddrOptval, sizeof(reuseaddrOptval))) {
      const int savedErrno = errno;
    
      castor::exception::Internal ex;
      ex.getMessage() <<
        ": Failed to set socket option"
        ": file-descriptor=" << sock.get() <<
        " level=SOL_SOCKET"
        " optname=SO_REUSEADDR"
        " optval=" << reuseaddrOptval <<
        ": " << sstrerror(savedErrno);
      throw ex;
    }
  }

  // Address structure to be used to bind the socket
  struct sockaddr_in address;

  // For each port in the range
  for(unsigned short port=lowPort; port<=highPort; ++port) {

    // Try to bind the socket to the port
    utils::setBytes(address, '\0');
    address.sin_family = AF_INET;
    address.sin_addr   = addr;
    address.sin_port   = htons(port);

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

        castor::exception::Internal ex;
        ex.getMessage() <<
          ": Failed to mark socket as being a listener"
          ": listenSocketFd=" << sock.get() <<
          ": " << sstrerror(listenErrno);
        throw ex;
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
        castor::exception::Exception ex(bindErrno);
        ex.getMessage() <<
          ": Failed to bind listener socket"
          ": listenSocketFd=" << sock.get() <<
          ": " << sstrerror(bindErrno);
        throw ex;
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

  throw ex;
}

//-----------------------------------------------------------------------------
// acceptConnection
//-----------------------------------------------------------------------------
int castor::io::acceptConnection(const int listenSocketFd)
  throw(castor::exception::Exception) {

  // Throw an exception if listenSocketFd is invalid
  if(listenSocketFd < 0) {
    exception::InvalidArgument ex;
    ex.getMessage() <<
      ": Invalid listen socket file-descriptor"
      ": listenSocketFd=" << listenSocketFd;
    throw ex;
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

    castor::exception::Exception ex(savedErrno);
    ex.getMessage() << reason.str();
    throw ex;
  }

  return connectedSocketFd;
}

//-----------------------------------------------------------------------------
// acceptConnection
//-----------------------------------------------------------------------------
int castor::io::acceptConnection(const int listenSocketFd,
  const time_t timeout) throw(
    castor::exception::TimeOut,
    castor::exception::AcceptConnectionInterrupted,
    castor::exception::Exception) {

  // Throw an exception if listenSocketFd is invalid
  if(listenSocketFd < 0) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid listen socket file-descriptor"
      ": listenSocketFd=" << listenSocketFd;
    throw ex;
  }

  const time_t   startTime = time(NULL);
  fd_set         fdSet;
  struct timeval selectTimeout;

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
      castor::exception::TimeOut ex;
      ex.getMessage() <<
           ": Timed out after " << timeout
        << " seconds whilst trying to accept a connection";
      throw ex;
    }
    break;
  case -1: // Select encountered an error
    // If select was interrupted
    if(selectErrno == EINTR) {
      const time_t remainingTime = timeout - (time(NULL) - startTime);

      castor::exception::AcceptConnectionInterrupted ex(remainingTime);

      throw(ex);
    } else {
      castor::exception::Exception ex(selectErrno);
      ex.getMessage() <<
        ": Failed to accept connection"
        ": Select failed: " << sstrerror(selectErrno);
      throw ex;
    }
    break;
  default: // Select found a file descriptor awaiting attention
    // If it is not the expected connection request
    if(!FD_ISSET(listenSocketFd, &fdSet)) {
      castor::exception::Exception ex(selectErrno);
      ex.getMessage() <<
        ": Failed to accept connection "
        ": Invalid file descriptor set";
      throw ex;
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

    castor::exception::Exception ex(acceptErrno);
    ex.getMessage() << reason.str();
    throw ex;
  }

  return connectedSocketFd;
}

//-----------------------------------------------------------------------------
// getSockIpPort
//-----------------------------------------------------------------------------
castor::io::IpAndPort castor::io::getSockIpPort(
  const int socketFd) throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  struct sockaddr_in address;
  memset(&address, '\0', sizeof(address));
  socklen_t addressLen = sizeof(address);

  if(getsockname(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    const int savedErrno = errno;

    castor::exception::Exception ex(savedErrno);
    ex.getMessage() << 
      ": Failed to get socket name"
      ": socketFd=" << socketFd <<
      ": " << sstrerror(savedErrno);
    throw ex;
  }

  return IpAndPort(ntohl(address.sin_addr.s_addr), ntohs(address.sin_port));
}

//-----------------------------------------------------------------------------
// getPeerIpPort
//-----------------------------------------------------------------------------
castor::io::IpAndPort  castor::io::getPeerIpPort(
  const int socketFd) throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  struct sockaddr_in address;
  memset(&address, '\0', sizeof(address));
  socklen_t addressLen = sizeof(address);

  if(getpeername(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    const int savedErrno = errno;

    castor::exception::Exception ex(savedErrno);
    ex.getMessage() <<
      ": Failed to get peer name"
      ": socketFd=" << socketFd <<
      ": " << sstrerror(savedErrno);
    throw ex;
  }

  return IpAndPort(ntohl(address.sin_addr.s_addr), ntohs(address.sin_port));
}

//------------------------------------------------------------------------------
// getSockHostName
//------------------------------------------------------------------------------
void castor::io::getSockHostName(
  const int    socketFd,
  char *const  buf,
  const size_t len)
  throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getsockname(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    const int savedErrno = errno;

    castor::exception::Exception ex(savedErrno);
    ex.getMessage() <<
      ": Failed to get socket name"
      ": socketFd=" << socketFd <<
      ": " << sstrerror(savedErrno);
    throw ex;
  }

  char hostName[HOSTNAMEBUFLEN];
  char serviceName[SERVICENAMEBUFLEN];
  const int error = getnameinfo((const struct sockaddr*)&address, addressLen,
    hostName, sizeof(hostName), serviceName, sizeof(serviceName), 0);

  if(error != 0) {
    castor::exception::Exception ex(SENOSHOST);
    ex.getMessage() <<
      ": Failed to get host information by address"
      ": socketFd=" << socketFd <<
      ": " << gai_strerror(error);
    throw ex;
  }

  utils::copyString(buf, len, hostName);
}

//------------------------------------------------------------------------------
// getSockIpHostnamePort
//------------------------------------------------------------------------------
void castor::io::getSockIpHostnamePort(
  const int      socketFd,
  unsigned long  &ip,
  char *const    hostName,
  const size_t   hostNameLen,
  unsigned short &port)
  throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getsockname(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    const int savedErrno = errno;

    castor::exception::Exception ex(savedErrno);
    ex.getMessage() <<
      ": Failed to get socket name"
      ": socketFd=" << socketFd <<
      ": " << sstrerror(savedErrno);
    throw ex;
  }

  ip   = ntohl(address.sin_addr.s_addr);
  port = ntohs(address.sin_port);

  {
    char serviceName[SERVICENAMEBUFLEN];
    const int rc = getnameinfo((const struct sockaddr*)&address, addressLen,
      hostName, hostNameLen, serviceName, sizeof(serviceName), 0);

    if(rc != 0) {
      castor::exception::Exception ex(SENOSHOST);
      ex.getMessage() <<
        ": Failed to get host information by address"
        ": socketFd=" << socketFd <<
        ": " << gai_strerror(rc);
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// getPeerHostName
//------------------------------------------------------------------------------
void castor::io::getPeerHostName(
  const int    socketFd,
  char *const  buf,
  const size_t len)
  throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getpeername(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    const int savedErrno = errno;

    castor::exception::Exception ex(savedErrno);
    ex.getMessage() <<
      ": Failed to get peer name"
      ": socketFd=" << socketFd <<
      ": " << sstrerror(savedErrno);
    throw ex;
  }

  {
    char hostName[HOSTNAMEBUFLEN];
    char serviceName[SERVICENAMEBUFLEN];
    const int rc = getnameinfo((const struct sockaddr*)&address, addressLen,
      hostName, sizeof(hostName), serviceName, sizeof(serviceName), 0);

    if(rc != 0) {
      castor::exception::Exception ex(SENOSHOST);
      ex.getMessage() <<
        ": Failed to get host information by address"
        ": socketFd=" << socketFd <<
        ": " << gai_strerror(rc);
      throw ex;
    }

    utils::copyString(buf, len, hostName);
  }
}

//------------------------------------------------------------------------------
// writeIp
//------------------------------------------------------------------------------
void castor::io::writeIp(
  std::ostream        &os,
  const unsigned long ip)
  throw() {
  os << ((ip >> 24) & 0x000000FF) << "."
     << ((ip >> 16) & 0x000000FF) << "."
     << ((ip >>  8) & 0x000000FF) << "."
     << ( ip        & 0x000000FF);
}

//------------------------------------------------------------------------------
// writeSockDescription
//------------------------------------------------------------------------------
void castor::io::writeSockDescription(
  std::ostream &os,
  const int    socketFd)
  throw() {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  IpAndPort localIpAndPort(0, 0);
  try {
    localIpAndPort = getSockIpPort(socketFd);
  } catch(castor::exception::Exception &e) {
    localIpAndPort.setIp(0);
    localIpAndPort.setPort(0);
  }

  IpAndPort peerIpAndPort(0, 0);
  try {
    peerIpAndPort = getPeerIpPort(socketFd);
  } catch(castor::exception::Exception &e) {
    peerIpAndPort.setIp(0);
    peerIpAndPort.setPort(0);
  }

  os << "{local=";
  writeIp(os, localIpAndPort.getIp());
  os << ":" << localIpAndPort.getPort();
  os << ",peer=";
  writeIp(os, peerIpAndPort.getIp());
  os << ":" << peerIpAndPort.getPort();
  os << "}";
}

//------------------------------------------------------------------------------
// readBytes
//------------------------------------------------------------------------------
void castor::io::readBytes(
  const int   socketFd,
  const int   timeout,
  const int   nbBytes,
  char *const buf)
  throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  const bool connClosed = readBytesFromCloseable(socketFd, timeout, nbBytes,
    buf);

  if(connClosed) {
    std::stringstream oss;
    oss << "Failed to read " << nbBytes << " bytes from socket: ";
    writeSockDescription(oss, socketFd);
    oss << ": Connection was closed by the remote end";

    castor::exception::Exception ex(SECONNDROP);
    ex.getMessage() << oss.str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readBytesFromCloseable
//------------------------------------------------------------------------------
bool castor::io::readBytesFromCloseable(
  const int   socketFd,
  const int   timeout,
  const int   nbBytes,
  char *const buf)
  throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  bool connClosed = false;
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

      castor::exception::Exception ex(savedSerrno);
      ex.getMessage() << oss.str();
      throw ex;
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

      castor::exception::Exception ex(SECOMERR);
      ex.getMessage() << oss.str();
      throw ex;
    }
  }

  return connClosed;
}

//------------------------------------------------------------------------------
// writeBytes
//------------------------------------------------------------------------------
void castor::io::writeBytes(
  const int   socketFd,
  const int   timeout,
  const int   nbBytes,
  char *const buf)
  throw(castor::exception::Exception) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
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

      castor::exception::Exception ex(SECOMERR);
      ex.getMessage() << oss.str();
      throw ex;
    }
  case 0:
    {
      std::stringstream oss;
      oss << ": Failed to write " << nbBytes << " bytes to socket: ";
      writeSockDescription(oss, socketFd);
      oss << ": Connection dropped";

      castor::exception::Exception ex(SECONNDROP);
      ex.getMessage() << oss.str();
      throw ex;
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
      castor::exception::Exception ex(SECOMERR);
      ex.getMessage() << oss.str();
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// connectWithTimeout
//-----------------------------------------------------------------------------
int castor::io::connectWithTimeout(
  const int             sockDomain,
  const int             sockType,
  const int             sockProtocol,
  const struct sockaddr *address,
  const socklen_t       address_len,
  const int             timeout)
  throw(castor::exception::TimeOut, castor::exception::Exception) {

  // Create the socket for the new connection
  utils::SmartFd smartSock(socket(sockDomain, sockType, sockProtocol));
  if(-1 == smartSock.get()) {
    const int socketErrno = errno;
    castor::exception::Exception ex(errno);
    ex.getMessage() <<
      ": Failed to create socket for new connection"
      ": Call to socket() failed"
      ": " << sstrerror(socketErrno);
    throw ex;
  }

  // Get the orginal file-control flags of the socket
  const int orginalFileControlFlags = fcntl(smartSock.get(), F_GETFL, 0);
  if(-1 == orginalFileControlFlags) {
    const int fcntlErrno = errno;
    castor::exception::Exception ex(fcntlErrno);
    ex.getMessage() <<
      ": Failed to get the original file-control flags of the socket"
      ": Call to fcntl() failed"
      ": " << sstrerror(fcntlErrno);
    throw ex;
  }

  // Set the O_NONBLOCK file-control flag of the socket
  if(-1 == fcntl(smartSock.get(), F_SETFL,
    orginalFileControlFlags | O_NONBLOCK)) {
    const int fcntlErrno = errno;
    castor::exception::Exception ex(fcntlErrno);
    ex.getMessage() <<
      ": Failed to set the O_NONBLOCK file-control flag"
      ": Call to fcntl() failed"
      ": " << sstrerror(fcntlErrno);
    throw ex;
  }

  // Start connecting
  const int connectRc = connect(smartSock.get(), address, address_len);
  const int connectErrno = errno;

  // If the connection completed immediately then restore the original
  // file-control flags of the socket and return it
  if(0 == connectRc) {
    if(-1 == fcntl(smartSock.get(), F_SETFL, orginalFileControlFlags)) {
      const int fcntlErrno = errno;
      castor::exception::Exception ex(fcntlErrno);
      ex.getMessage() <<
        ": Failed to restore the file-control flags of the socket"
        ": " << sstrerror(fcntlErrno);
      throw ex;
    }
    return smartSock.release();
  }

  // Throw an exception if there was any other error than
  // "operation in progress" when trying to start to connect
  if(EINPROGRESS != connectErrno) {
    castor::exception::Exception ex(connectErrno);
    ex.getMessage() <<
      ": Call to connect() failed"
      ": " << sstrerror(connectErrno);
    throw ex;
  }

  // Create a read set and a write set for select() putting the
  // socket in both sets
  fd_set readFds, writeFds;
  FD_ZERO(&readFds);
  FD_SET(smartSock.get(), &readFds);
  writeFds = readFds;

  // Wait for the connection to complete using select with a timeout
  struct timeval selectTimeout;
  selectTimeout.tv_sec = timeout;
  selectTimeout.tv_usec = 0;
  const int selectRc = select(smartSock.get() + 1, &readFds, &writeFds, NULL,
    &selectTimeout);
  if(-1 == selectRc) {
    const int selectErrno = errno;
    castor::exception::Exception ex(selectErrno);
    ex.getMessage() <<
      ": Call to select() failed"
      ": " << sstrerror(selectErrno);
    throw ex;
  }

  // Throw a timed-out exception if select timed-out
  if(0 == selectRc) {
    castor::exception::TimeOut ex;
    ex.getMessage() <<
      ": Failed to connect"
      ": Timed out after " << timeout << " seconds";
    throw ex;
  }

  // Throw an exception if no file descriptor was set
  if(!FD_ISSET(smartSock.get(), &readFds) &&
    !FD_ISSET(smartSock.get(), &writeFds)) {
    castor::exception::Exception ex(ECANCELED);
    ex.getMessage() <<
      ": Failed to connect"
      ": select() returned with no timneout or any descriptors set";
    throw ex;
  }

  // Use getsockopt() to check whether or not the connection completed
  // successfully
  //
  // Take into account the fact that if there is an error then Solaris will
  // return -1 and set errno, whereas BSD will return 0 and set sockoptError
  int sockoptError = 0;
  socklen_t sockoptErrorLen = sizeof(sockoptError);
  const int getsockoptRc = getsockopt(smartSock.get(), SOL_SOCKET, SO_ERROR,
    &sockoptError, &sockoptErrorLen);
  const int getsockoptErrno = errno;
  if(-1 == getsockoptRc) { // Solaris
    castor::exception::Exception ex(getsockoptErrno);
    ex.getMessage() <<
      ": Connection did not complete successfully"
      ": " << sstrerror(getsockoptErrno);
    throw ex;
  }
  if(0 != sockoptError) { // BSD
    castor::exception::Exception ex(sockoptError);
    ex.getMessage() <<
      ": Connection did not complete successfully"
      ": " << sstrerror(sockoptError);
    throw ex;
  }

  // Restore the original file-control flags of the socket
  if(-1 == fcntl(smartSock.get(), F_SETFL, orginalFileControlFlags)) {
    const int fcntlErrno = errno;
    castor::exception::Exception ex(fcntlErrno);
    ex.getMessage() <<
      ": Failed to restore the file-control flags of the socket"
      ": " << sstrerror(fcntlErrno);
    throw ex;
  }

  return smartSock.release();
}
