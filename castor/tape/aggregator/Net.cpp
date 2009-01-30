/******************************************************************************
 *                      Net.cpp
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

#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/Net.hpp"
#include "castor/tape/aggregator/Utils.hpp"
#include "h/serrno.h"
#include "h/socket_timeout.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <time.h>


//-----------------------------------------------------------------------------
// createListenerSocket
//-----------------------------------------------------------------------------
int castor::tape::aggregator::Net::createListenerSocket(
  const unsigned short port) throw(castor::exception::Exception) {
  int    socketFd = 0;
  struct sockaddr_in address;

  if ((socketFd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    castor::exception::Exception ex(errno);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to create a listener socket";

    throw ex;
  }

  Utils::setBytes(address, 0);
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_ANY);
  address.sin_port = htons(port);

  if(bind(socketFd, (struct sockaddr *) &address, sizeof(address)) < 0) {
    castor::exception::Exception ex(errno);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to bind listener socket";

    throw ex;
  }

  if(listen(socketFd, LISTENBACKLOG) < 0) {
    castor::exception::Exception ex(errno);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to mark listener socket as being a listener socket";

    throw ex;
  }

  return socketFd;
}


//-----------------------------------------------------------------------------
// acceptConnection
//-----------------------------------------------------------------------------
int castor::tape::aggregator::Net::acceptConnection(
  const int listenSocketFd, const int netReadWriteTimeout)
  throw(castor::exception::Exception) {

  const time_t startTime     = time(NULL);
  time_t       remainingTime = netReadWriteTimeout;
  time_t       elapsedTime   = netReadWriteTimeout;
  bool         selectAgain   = true;
  int          selectRc      = 0;
  int          selectErrno   = 0;
  fd_set       fdSet;
  timeval      timeout;

  // While a connection request has not arrived and the timeout has not expired
  while(selectAgain) {
    FD_ZERO(&fdSet);
    FD_SET(listenSocketFd, &fdSet);

    timeout.tv_sec  = remainingTime;
    timeout.tv_usec = 0;

    selectRc = select(listenSocketFd + 1, &fdSet, NULL, NULL, &timeout);
    selectErrno = errno;

    switch(selectRc) {
    case 0: // Select timed out
      {
        castor::exception::Exception ex(ETIMEDOUT);

        ex.getMessage() << __PRETTY_FUNCTION__
          << ": Timed out after " << netReadWriteTimeout
          << " seconds whilst trying to accept a connection";

        throw ex;
      }
      break;
    case -1: // Select encountered an error
      // If select was interrupted
      if(errno == EINTR) {
        elapsedTime = time(NULL) - startTime;

        // Recalculate the amount of remaining time ready for another call to
        // select
        if(elapsedTime >= netReadWriteTimeout) {
          remainingTime = 0; // One last non-blocking select
        } else {
          remainingTime = netReadWriteTimeout - elapsedTime;
        }
      } else {
        char strerrorBuf[STRERRORBUFLEN];
        char *const errorStr = strerror_r(selectErrno, strerrorBuf,
          sizeof(strerrorBuf));

        castor::exception::Exception ex(selectErrno);

        ex.getMessage() << __PRETTY_FUNCTION__
          << ": Failed to accept connection"
             ": Select failed: " << errorStr;

        throw ex;
      }
      break;
    default: // Select found a file descriptor awaiting attention
      // If there is a connection request
      if(FD_ISSET(listenSocketFd, &fdSet)) {

        // The select loop should now finish
        selectAgain = false;

      // Else the file descriptor set does not make sense
      } else {
        castor::exception::Exception ex(selectErrno);

        ex.getMessage() << __PRETTY_FUNCTION__
          << ": Failed to accept connection "
             ": Invalid file descriptor set";

        throw ex;
      }
    }
  }

  struct sockaddr_in peerAddress;
  unsigned int       peerAddressLen = sizeof(peerAddress);

  const int connectedSocketFd = accept(listenSocketFd,
    (struct sockaddr *)&peerAddress, &peerAddressLen);

  if(connectedSocketFd < 0) {
    char strerrorBuf[STRERRORBUFLEN];
    char *const errorStr = strerror_r(selectErrno, strerrorBuf,
      sizeof(strerrorBuf));

    castor::exception::Exception ex(selectErrno);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to accept connection"
         ": Accept failed: " << errorStr;

    throw ex;
  }

  return connectedSocketFd;
}


//-----------------------------------------------------------------------------
// getSocketIpAndPort
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Net::getSocketIpAndPort(const int socketFd,
  unsigned long& ip, unsigned short& port)
  throw(castor::exception::Exception) {

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getsockname(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    const int getsocknameErrno = errno;

    char strerrorBuf[STRERRORBUFLEN];
    char *const errorStr = strerror_r(getsocknameErrno, strerrorBuf,
      sizeof(strerrorBuf));

    castor::exception::Exception ex(getsocknameErrno);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get socket name"
         ": " << errorStr;
    throw ex;
  }

  ip   = ntohl(address.sin_addr.s_addr);
  port = ntohs(address.sin_port);
}


//-----------------------------------------------------------------------------
// getPeerIpAndPort
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Net::getPeerIpAndPort(const int socketFd,
  unsigned long& ip, unsigned short& port)
  throw(castor::exception::Exception) {

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getpeername(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    const int getpeernameErrno = errno;

    char strerrorBuf[STRERRORBUFLEN];
    char *const errorStr = strerror_r(getpeernameErrno, strerrorBuf,
      sizeof(strerrorBuf));

    castor::exception::Exception ex(getpeernameErrno);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get peer name"
         ": " << errorStr;

    throw ex;
  }

  ip   = ntohl(address.sin_addr.s_addr);
  port = ntohs(address.sin_port);
}


//------------------------------------------------------------------------------
// getSocketHostName
//------------------------------------------------------------------------------
void castor::tape::aggregator::Net::getSocketHostName(const int socketFd,
  char *buf, size_t len) throw(castor::exception::Exception) {

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getsockname(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    const int getsocknameErrno = errno;

    char strerrorBuf[STRERRORBUFLEN];
    char *const errorStr = strerror_r(getsocknameErrno, strerrorBuf,
      sizeof(strerrorBuf));

    castor::exception::Exception ex(getsocknameErrno);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get socket name"
         ": " << errorStr;

    throw ex;
  }

  char hostName[HOSTNAMEBUFLEN];
  char serviceName[SERVICENAMEBUFLEN];
  const int error = getnameinfo((const struct sockaddr*)&address, addressLen,
    hostName, sizeof(hostName), serviceName, sizeof(serviceName), 0);

  if(error != 0) {
    castor::exception::Exception ex(SENOSHOST);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get host information by address"
         ": " << gai_strerror(error);

    throw ex;
  }

  Utils::copyString(buf, hostName, len);
}


//------------------------------------------------------------------------------
// getPeerHostName
//------------------------------------------------------------------------------
void castor::tape::aggregator::Net::getPeerHostName(const int socketFd,
  char *buf, size_t len) throw(castor::exception::Exception) {

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getpeername(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    const int getpeernameErrno = errno;

    char strerrorBuf[STRERRORBUFLEN];
    char *const errorStr = strerror_r(getpeernameErrno, strerrorBuf,
      sizeof(strerrorBuf));

    castor::exception::Exception ex(getpeernameErrno);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get peer name"
         ": " << errorStr;

    throw ex;
  }

  char hostName[HOSTNAMEBUFLEN];
  char serviceName[SERVICENAMEBUFLEN];
  const int error = getnameinfo((const struct sockaddr*)&address, addressLen,
    hostName, sizeof(hostName), serviceName, sizeof(serviceName), 0);

  if(error != 0) {
    castor::exception::Exception ex(SENOSHOST);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get host information by address"
         ": " << gai_strerror(error);

    throw ex;
  }

  Utils::copyString(buf, hostName, len);
}


//------------------------------------------------------------------------------
// printIp
//------------------------------------------------------------------------------
void castor::tape::aggregator::Net::printIp(std::ostream &os,
  const unsigned long ip) throw() {
  os << ((ip >> 24) & 0x000000FF) << "."
     << ((ip >> 16) & 0x000000FF) << "."
     << ((ip >>  8) & 0x000000FF) << "."
     << ( ip        & 0x000000FF);
}


//------------------------------------------------------------------------------
// printSocketDescription
//------------------------------------------------------------------------------
void castor::tape::aggregator::Net::printSocketDescription(std::ostream &os,
  const int socketFd) throw() {
  unsigned long  localIp   = 0;
  unsigned short localPort = 0;
  unsigned long  peerIp    = 0;
  unsigned short peerPort  = 0;

  try {
    getSocketIpAndPort(socketFd, localIp, localPort);
  } catch(castor::exception::Exception &e) {
    localIp   = 0;
    localPort = 0;
  }

  try {
    getPeerIpAndPort(socketFd, peerIp, peerPort);
  } catch(castor::exception::Exception &e) {
    peerIp   = 0;
    peerPort = 0;
  }

  os << "local=";
  printIp(os, localIp);
  os << ":" << localPort;
  os << " peer=";
  printIp(os, peerIp);
  os << ":" << peerPort;
}


//------------------------------------------------------------------------------
// readBytes
//------------------------------------------------------------------------------
void castor::tape::aggregator::Net::readBytes(const int socketFd,
  const int netReadWriteTimeout, const int nbBytes, char *buf)
  throw(castor::exception::Exception) {

  const int netreadRc = netread_timeout(socketFd, buf, nbBytes,
    netReadWriteTimeout);
  const int netreadErrno = errno;

  switch(netreadRc) {
  case -1:
    {
      char strerrorBuf[STRERRORBUFLEN];
      char *const errorStr = strerror_r(netreadErrno, strerrorBuf,
        sizeof(strerrorBuf));

      castor::exception::Exception ex(serrno);

      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << ": Failed to read " << nbBytes << " bytes from socket: ";
      printSocketDescription(os, socketFd);
      os << ": " << errorStr;
      throw ex;
    }
  case 0:
    {
      castor::exception::Exception ex(SECONNDROP);

      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << "Failed to read " << nbBytes << " bytes from socket: ";
      printSocketDescription(os, socketFd);
      os << ": Connection was closed by the remote end";
      throw ex;
    }
  default:
    if (netreadRc != nbBytes) {
      castor::exception::Exception ex(SECOMERR);

      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << "Failed to read " << nbBytes << " bytes from socket: ";
      printSocketDescription(os, socketFd);
      os << "Read the wrong number of bytes"
        << ": Expected: " << nbBytes
        << ": Read: " << netreadRc;
      throw ex;
    }
  }
}


//------------------------------------------------------------------------------
// writeBytes
//------------------------------------------------------------------------------
void castor::tape::aggregator::Net::writeBytes(const int socketFd,
  const int netReadWriteTimeout, const int nbBytes, char *const buf)
  throw(castor::exception::Exception) {

  const int netwriteRc = netwrite_timeout(socketFd, buf, nbBytes,
    netReadWriteTimeout);

  switch(netwriteRc) {
  case -1:
    {
      castor::exception::Exception ex(SECOMERR);
      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << ": Failed to write " << nbBytes << " bytes to socket: ";
      printSocketDescription(os, socketFd);
      os << ": " << neterror();
      throw ex;
    }
  case 0:
    {
      castor::exception::Exception ex(SECONNDROP);
      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << ": Failed to write " << nbBytes << " bytes to socket: ";
      printSocketDescription(os, socketFd);
      os << ": Connection dropped";
      throw ex;
    }
  default:
    if(netwriteRc != nbBytes) {
      castor::exception::Exception ex(SECOMERR);

      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << "Failed to write " << nbBytes << " bytes to socket: ";
      printSocketDescription(os, socketFd);
      os << "Wrote the wrong number of bytes"
        << ": Expected: " << nbBytes
        << ": Wrote: " << netwriteRc;
      throw ex;
    }
  }
}
