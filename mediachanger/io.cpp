/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/exception/InvalidArgument.hpp"
#include "common/exception/Exception.hpp"
#include "common/SmartFd.hpp"
#include "common/Timer.hpp"
#include "common/utils/utils.hpp"
#include "common/exception/Errnum.hpp"
#include "mediachanger/io.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sstream>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/poll.h>
#include <time.h>
#include <list>

namespace cta {
namespace mediachanger {

//------------------------------------------------------------------------------
// createListenerSock
//------------------------------------------------------------------------------
int createListenerSock(const unsigned short port) {
  const unsigned short lowPort = port;
  const unsigned short highPort = port;
  unsigned short chosenPort = 0;

  struct in_addr networkAddress;
  memset(&networkAddress, '\0', sizeof(networkAddress));
  networkAddress.s_addr = INADDR_ANY;

  return createListenerSock(networkAddress, lowPort, highPort, chosenPort);
}

//------------------------------------------------------------------------------
// createListenerSock
//------------------------------------------------------------------------------
int createListenerSock(
  const unsigned short lowPort,
  const unsigned short highPort,
  unsigned short       &chosenPort)
   {

  struct in_addr networkAddress;
  memset(&networkAddress, '\0', sizeof(networkAddress));
  networkAddress.s_addr = INADDR_ANY;

  return createListenerSock(networkAddress, lowPort, highPort, chosenPort);
}

//------------------------------------------------------------------------------
// createListenerSock
//------------------------------------------------------------------------------
int createListenerSock(
  const std::string    &addr,
  const unsigned short lowPort,
  const unsigned short highPort,
  unsigned short       &chosenPort) {

  struct in_addr networkAddress;

  const int rc = inet_pton(AF_INET, addr.c_str(), &networkAddress);
  if(0 >= rc) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to create listener socket:"
      " Failed to convert string to network address: value=" << addr;
    throw ex;
  }

  return createListenerSock(networkAddress, lowPort, highPort, chosenPort);
}

//------------------------------------------------------------------------------
// createListenerSock
//------------------------------------------------------------------------------
int createListenerSock(
  const struct in_addr &addr,
  const unsigned short lowPort,
  const unsigned short highPort,
  unsigned short       &chosenPort) {

  // Check range validity
  if(lowPort < 1) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() <<  "lowPort must be greater than 0"
      ": lowPort=" << lowPort;
    throw ex;
  }
  if(highPort < 1) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() << "highPort must be greater than 0"
      ": highPort=" << lowPort;
    throw ex;
  }
  if(lowPort > highPort) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() <<  "lowPort must be less than or equal to highPort"
      ": lowPort=" << lowPort << " highPort=" << highPort;
    throw ex;
  }

  // Create a socket
  SmartFd sock(socket(PF_INET, SOCK_STREAM, IPPROTO_TCP));
  if(sock.get() < 0) {
    cta::exception::Exception ex;
    ex.getMessage() << ": Failed to create socket: "
      << cta::utils::errnoToString(errno);
    throw ex;
  }

  // Set the SO_REUSEADDR socket option before calling bind
  {
    int reuseaddrOptval = 1;
    if(0 > setsockopt(sock.get(), SOL_SOCKET, SO_REUSEADDR,
      (char *)&reuseaddrOptval, sizeof(reuseaddrOptval))) {
      cta::exception::Exception ex;
      ex.getMessage() <<
        ": Failed to set socket option"
        ": file-descriptor=" << sock.get() <<
        " level=SOL_SOCKET"
        " optname=SO_REUSEADDR"
        " optval=" << reuseaddrOptval <<
        ": " << cta::utils::errnoToString(errno);
      throw ex;
    }
  }

  // Address structure to be used to bind the socket
  struct sockaddr_in address;

  // For each port in the range
  for(unsigned short port=lowPort; port<=highPort; ++port) {

    // Try to bind the socket to the port
    memset(&address, '\0', sizeof(address));
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
        cta::exception::Exception ex;
        ex.getMessage() <<
          ": Failed to mark socket as being a listener"
          ": listenSocketFd=" << sock.get() <<
          ": " << cta::utils::errnoToString(errno);
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
        cta::exception::Exception ex;
        ex.getMessage() <<
          ": Failed to bind listener socket"
          ": listenSocketFd=" << sock.get() <<
          ": " << cta::utils::errnoToString(bindErrno);
        throw ex;
      }
    }
  }

  // If this line is reached then all ports in the specified range are in use

  // Throw an exception
  cta::exception::NoPortInRange ex(lowPort, highPort);
  ex.getMessage() <<
    "All ports within the specified range are in use"
    ": listenSocketFd=" << sock.get() <<
    ": lowPort=" << lowPort <<
    ": highPort=" << highPort;

  throw ex;
}

//------------------------------------------------------------------------------
// createLocalhostListenerSockBoundToLocalhost
//------------------------------------------------------------------------------
int createLocalhostListenerSock(const unsigned short port) {
  const unsigned short lowPort = port;
  const unsigned short highPort = port;
  unsigned short chosenPort = 0;

  const char *addr = "127.0.0.1";
  struct in_addr networkAddress;
  const int rc = inet_pton(AF_INET, addr, &networkAddress);
  if(0 >= rc) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to create listener socket:"
      " Failed to convert string to network address: value=" << addr;
    throw ex;
  }

  return createListenerSock(networkAddress, lowPort, highPort, chosenPort);
}

//------------------------------------------------------------------------------
// acceptConnection
//------------------------------------------------------------------------------
int acceptConnection(const int listenSocketFd) {

  // Throw an exception if listenSocketFd is invalid
  if(listenSocketFd < 0) {
    cta::exception::InvalidArgument ex;
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
      reason << ": " << cta::utils::errnoToString(savedErrno);
    }

    cta::exception::Exception ex;
    ex.getMessage() << reason.str();
    throw ex;
  }

  return connectedSocketFd;
}

//------------------------------------------------------------------------------
// acceptConnection
//------------------------------------------------------------------------------
int acceptConnection(const int listenSocketFd,
  const time_t timeout) {

  // Throw an exception if listenSocketFd is invalid
  if(listenSocketFd < 0) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid listen socket file-descriptor"
      ": listenSocketFd=" << listenSocketFd;
    throw ex;
  }

  const time_t startTime = time(nullptr);

  pollfd pollFd;
  pollFd.fd = listenSocketFd;
  pollFd.events = POLLIN;
  pollFd.revents = 0;

  const int pollRc = poll(&pollFd, 1, 1000 * timeout);
  const int pollErrno = errno;

  switch(pollRc) {
  case 0: // poll() timed out
    {
      cta::exception::TimeOut ex;
      ex.getMessage() <<
           "Failed to accept connection: poll() timed out after " << timeout
        << " seconds whilst trying to accept a connection";
      throw ex;
    }
  case -1: // poll() encountered an error
    // If poll() was interrupted
    if(pollErrno == EINTR) {
      const time_t remainingTime = timeout - (time(nullptr) - startTime);

      cta::exception::AcceptConnectionInterrupted ex(remainingTime);

      throw ex;
    } else {
      cta::exception::Exception ex;
      ex.getMessage() << "Failed to accept connection: poll() failed: " <<
        cta::utils::errnoToString(pollErrno);
      throw ex;
    }
  default: // poll() found a file descriptor awaiting attention
    if(pollFd.revents & POLLERR) {
      cta::exception::Exception ex;
      ex.getMessage() << "Failed to accept connection"
        ": POLLERR - Error condition";

      if(pollFd.revents & POLLHUP) {
        ex.getMessage() << ": POLLHUP - Connection closed by peer";
      }

      if(pollFd.revents & POLLNVAL) {
        ex.getMessage() << ": POLLNVAL - File descriptor not open";
      }

      ex.getMessage() << ": pollFd.fd=" << pollFd.fd << ",pollFd.events=" <<
        pollFd.events << ",pollFd.revents=" << pollFd.revents;
      throw ex;
    }

    if(!(pollFd.revents & POLLIN)) {
      cta::exception::Exception ex;
      ex.getMessage() << "Failed to accept connection "
        ": POLLIN event not set";
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
      "Failed to accept connection"
      ": listenSocketFd=" << listenSocketFd;

    if(acceptErrno == EINVAL) {
      reason << ": Socket is not listening for connections";
    } else {
      reason << ": " << cta::utils::errnoToString(acceptErrno);
    }

    cta::exception::Exception ex;
    ex.getMessage() << reason.str();
    throw ex;
  }

  return connectedSocketFd;
}

//------------------------------------------------------------------------------
// getSockIpPort
//------------------------------------------------------------------------------
mediachanger::IpAndPort getSockIpPort(const int socketFd)  {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  struct sockaddr_in address;
  memset(&address, '\0', sizeof(address));
  socklen_t addressLen = sizeof(address);

  if(getsockname(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to get socket name: socketFd=" << socketFd <<
      ": " << cta::utils::errnoToString(errno);
    throw ex;
  }

  return IpAndPort(ntohl(address.sin_addr.s_addr), ntohs(address.sin_port));
}

//------------------------------------------------------------------------------
// getPeerIpPort
//------------------------------------------------------------------------------
mediachanger::IpAndPort getPeerIpPort(const int socketFd)  {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  struct sockaddr_in address;
  memset(&address, '\0', sizeof(address));
  socklen_t addressLen = sizeof(address);

  if(getpeername(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    cta::exception::Exception ex;
    ex.getMessage() << ": Failed to get peer name: socketFd=" << socketFd <<
      ": " << cta::utils::errnoToString(errno);
    throw ex;
  }

  return IpAndPort(ntohl(address.sin_addr.s_addr), ntohs(address.sin_port));
}

//------------------------------------------------------------------------------
// getSockHostName
//------------------------------------------------------------------------------
std::string getSockHostName(const int socketFd) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() << "Failed to get socket hostname"
      ": Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getsockname(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to get socket hostname"
      ": socketFd=" << socketFd << ": " << cta::utils::errnoToString(errno);
    throw ex;
  }

  char hostName[HOSTNAMEBUFLEN];
  char serviceName[SERVICENAMEBUFLEN];
  const int error = getnameinfo((const struct sockaddr*)&address, addressLen,
    hostName, sizeof(hostName), serviceName, sizeof(serviceName), 0);

  if(error != 0) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      ": Failed to get host information by address"
      ": socketFd=" << socketFd <<
      ": " << gai_strerror(error);
    throw ex;
  }

  return hostName;
}

//------------------------------------------------------------------------------
// getSockIpHostnamePort
//------------------------------------------------------------------------------
void getSockIpHostnamePort(
  const int      socketFd,
  unsigned long  &ip,
  char *const    hostName,
  const size_t   hostNameLen,
  unsigned short &port) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getsockname(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      ": Failed to get socket name"
      ": socketFd=" << socketFd <<
      ": " << cta::utils::errnoToString(errno);
    throw ex;
  }

  ip   = ntohl(address.sin_addr.s_addr);
  port = ntohs(address.sin_port);

  {
    char serviceName[SERVICENAMEBUFLEN];
    const int rc = getnameinfo((const struct sockaddr*)&address, addressLen,
      hostName, hostNameLen, serviceName, sizeof(serviceName), 0);

    if(rc != 0) {
      cta::exception::Exception ex;
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
std::string getPeerHostName(const int socketFd) {
  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getpeername(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      ": Failed to get peer name"
      ": socketFd=" << socketFd <<
      ": " << cta::utils::errnoToString(errno);
    throw ex;
  }

  {
    char hostName[HOSTNAMEBUFLEN];
    char serviceName[SERVICENAMEBUFLEN];
    const int rc = getnameinfo((const struct sockaddr*)&address, addressLen,
      hostName, sizeof(hostName), serviceName, sizeof(serviceName), 0);

    if(rc != 0) {
      cta::exception::Exception ex;
      ex.getMessage() <<
        ": Failed to get host information by address"
        ": socketFd=" << socketFd <<
        ": " << gai_strerror(rc);
      throw ex;
    }

    return hostName;
  }
}

//------------------------------------------------------------------------------
// writeIp
//------------------------------------------------------------------------------
void writeIp(
  std::ostream        &os,
  const unsigned long ip) noexcept {
  os << ((ip >> 24) & 0x000000FF) << "."
     << ((ip >> 16) & 0x000000FF) << "."
     << ((ip >>  8) & 0x000000FF) << "."
     << ( ip        & 0x000000FF);
}

//------------------------------------------------------------------------------
// writeSockDescription
//------------------------------------------------------------------------------
void writeSockDescription(
  std::ostream &os,
  const int    socketFd) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() <<
      "Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  IpAndPort localIpAndPort(0, 0);
  try {
    localIpAndPort = getSockIpPort(socketFd);
  } catch(cta::exception::Exception &e) {
    localIpAndPort.ip = 0;
    localIpAndPort.port = 0;
  }

  IpAndPort peerIpAndPort(0, 0);
  try {
    peerIpAndPort = getPeerIpPort(socketFd);
  } catch(cta::exception::Exception &e) {
    peerIpAndPort.ip = 0;
    peerIpAndPort.port = 0;
  }

  os << "{local=";
  writeIp(os, localIpAndPort.ip);
  os << ":" << localIpAndPort.port;
  os << ",peer=";
  writeIp(os, peerIpAndPort.ip);
  os << ":" << peerIpAndPort.port;
  os << "}";
}

//------------------------------------------------------------------------------
// readBytes
//------------------------------------------------------------------------------
void readBytes(
  const int   socketFd,
  const int   timeout,
  const int   nbBytes,
  char *const buf) {
  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() <<
      "In io::readBytes: Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  if (timeout < 0) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() <<
      "In io::readBytes: Invalid timeout value: " << timeout;
    throw ex;
  }

  cta::utils::Timer timer;
  size_t bytesRemaining = nbBytes;
  char * readPtr = buf;
  while (bytesRemaining > 0) {
    {
      struct ::pollfd pollDescr;
      pollDescr.fd = socketFd;
      pollDescr.events = POLLIN;
      pollDescr.revents = 0;
      int pollRet = poll(&pollDescr, 1, (timeout * 1000) - (timer.usecs()/1000));
      cta::exception::Errnum::throwOnMinusOne(pollRet, "In io::readBytes: failed to poll socket");
      if (!pollRet)
        throw cta::exception::Exception("In io::readBytes: timeout");
      if (pollRet != 1) {
        std::stringstream err;
        err << "In io::readBytes: unexpected return value from poll: " << pollRet;
        throw cta::exception::Exception(err.str());
      }
    }
    {
      int recvRet = recv(socketFd, readPtr, bytesRemaining, 0);
      cta::exception::Errnum::throwOnMinusOne(recvRet, "In io::readBytes: failed to receive data: ");
      if (recvRet > 0) {
        // We did read more data...
        readPtr += recvRet;
        bytesRemaining -= recvRet;
      } else if (0 == recvRet) {
        throw cta::exception::Exception("In io::readBytes: recv(): connection closed.");
      } else if (-1 == recvRet) {
        throw cta::exception::Errnum("In io::readBytes: error calling recv():");
      } else {
        std::stringstream err;
        err << "In io::readBytes: unexpected return value from recv: " << recvRet;
        throw cta::exception::Exception(err.str());
      }
    }
  }
}

//------------------------------------------------------------------------------
// writeBytes
//------------------------------------------------------------------------------
void writeBytes(
  const int   socketFd,
  const int   timeout,
  const int   nbBytes,
  char *const buf) {

  // Throw an exception if socketFd is invalid
  if(socketFd < 0) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() <<
      "In io::writeBytes: Invalid socket file-descriptor"
      ": socketFd=" << socketFd;
    throw ex;
  }

  if (timeout < 0) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() <<
      "In io::writeBytes: Invalid timeout value: " << timeout;
    throw ex;
  }

  cta::utils::Timer timer;
  size_t bytesRemaining = nbBytes;
  char * writePtr = buf;
  while (bytesRemaining > 0) {
    {
      struct ::pollfd pollDescr;
      pollDescr.fd = socketFd;
      pollDescr.events = POLLOUT;
      pollDescr.revents = 0;
      int pollRet = poll(&pollDescr, 1, (timeout * 1000) - (timer.usecs()/1000));
      cta::exception::Errnum::throwOnMinusOne(pollRet, "In io::writeBytes: failed to poll socket");
      if (!pollRet)
        throw cta::exception::Exception("In io::writeBytes: timeout");
      if (pollRet != 1) {
        std::stringstream err;
        err << "In io::writeBytes: unexpected return value from poll: " << pollRet;
        throw cta::exception::Exception(err.str());
      }
    }
    {
      int sendRet = send(socketFd, writePtr, bytesRemaining, 0);
      cta::exception::Errnum::throwOnMinusOne(sendRet, "In io::writeBytes: failed to send data: ");
      if (sendRet > 0) {
        // We did read more data...
        writePtr += sendRet;
        bytesRemaining -= sendRet;
      } else {
        std::stringstream err;
        err << "In io::writeBytes: unexpected return value from send: " << sendRet;
        throw cta::exception::Exception(err.str());
      }
    }
  }
}

//------------------------------------------------------------------------------
// getAddrInfoErrorString
//
// Helper function used to know which string is associated with a specific errno
// returned by getaddrinfo. This function is needed because the standard one
// (gai_strerror) is not thread-safe on all systems.
//------------------------------------------------------------------------------
static std::string getAddrInfoErrorString(const int rc) {
  switch(rc) {
    case EAI_BADFLAGS:
      return "Invalid value for `ai_flags' field.";
    case EAI_NONAME:
      return "NAME or SERVICE is unknown.";
    case EAI_AGAIN:
      return "Temporary failure in name resolution.";
    case EAI_FAIL:
      return "Non-recoverable failure in name res.";
    case EAI_NODATA:
      return "No address associated with NAME.";
    case EAI_FAMILY:
      return "`ai_family' not supported.";
    case EAI_SOCKTYPE:
      return "`ai_socktype' not supported.";
    case EAI_SERVICE:
      return "SERVICE not supported for `ai_socktype'.";
    case EAI_ADDRFAMILY:
      return "Address family for NAME not supported.";
    case EAI_MEMORY:
      return "Memory allocation failure.";
    case EAI_SYSTEM:
      return "System error returned in `errno'.";
    case EAI_OVERFLOW:
      return "Argument buffer overflow.";
    case EAI_INPROGRESS:
      return "Processing request in progress.";
    case EAI_CANCELED:
      return "Request canceled.";
    case EAI_NOTCANCELED:
      return "Request not canceled.";
    case EAI_ALLDONE:
      return "All requests done.";
    case EAI_INTR:
      return "Interrupted by a signal.";
    case EAI_IDN_ENCODE:
      return "IDN encoding failed.";
    default:
      return "Unknown error";
  }
}

//------------------------------------------------------------------------------
// connectWithTimeout
//------------------------------------------------------------------------------
int connectWithTimeout(const std::string& hostName, const unsigned short port, const int timeout) {
  try {
    std::ostringstream portStream;
    portStream << port;
    struct addrinfo hints;
    memset(&hints, '\0', sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *res = nullptr;
    if(const int rc = getaddrinfo(hostName.c_str(), portStream.str().c_str(), &hints, &res); rc != 0) {
      cta::exception::Exception ex;
      ex.getMessage() << "getaddrinfo() failed: " << getAddrInfoErrorString(rc);
      throw ex;
    }
    struct sockaddr_in s_in;
    if(AF_INET != res->ai_family or SOCK_STREAM != res->ai_socktype or sizeof(s_in) != res->ai_addrlen) {
      cta::exception::Exception ex;
      ex.getMessage() << "getaddrinfo() bad result: either ai_family or ai_socktype or ai_addrlen are wrong";
      freeaddrinfo(res);
      throw ex;
    }
    memcpy(&s_in, res->ai_addr, sizeof(s_in));
    int protocol = res->ai_protocol;
    socklen_t length = res->ai_addrlen;
    freeaddrinfo(res);

    return connectWithTimeout(AF_INET, SOCK_STREAM, protocol, reinterpret_cast<struct sockaddr*>(&s_in), length, timeout);
  } catch(const cta::exception::Exception& ce) {
    cta::exception::Exception ex;
    ex.getMessage() << ce.getMessage().str() << ": hostName=" << hostName << ", port=" << port << ", timeout=" << timeout;
    throw ex;
  } catch(const std::exception& se) {
    cta::exception::Exception ex;
    ex.getMessage() << se.what() << ": hostName=" << hostName << ", port=" << port << ", timeout=" << timeout;
    throw ex;
  } catch(...) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to connect: Caught an unknown exception: hostName=" << hostName << ", port=" << port << ", timeout=" << timeout;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// connectWithTimeout
//------------------------------------------------------------------------------
int connectWithTimeout(
  const int             sockDomain,
  const int             sockType,
  const int             sockProtocol,
  const struct sockaddr *address,
  const socklen_t       address_len,
  const int             timeout) {

  // Create the socket for the new connection
  SmartFd smartSock(socket(sockDomain, sockType, sockProtocol));
  if(-1 == smartSock.get()) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to create socket for new connection"
      ": Call to socket() failed: " << cta::utils::errnoToString(errno);
    throw ex;
  }

  // Get the orginal file-control flags of the socket
  const int orginalFileControlFlags = fcntl(smartSock.get(), F_GETFL, 0);
  if(-1 == orginalFileControlFlags) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to get the original file-control flags of the socket"
      ": Call to fcntl() failed: " << cta::utils::errnoToString(errno);
    throw ex;
  }

  // Set the O_NONBLOCK file-control flag of the socket
  if(-1 == fcntl(smartSock.get(), F_SETFL,
    orginalFileControlFlags | O_NONBLOCK)) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to set the O_NONBLOCK file-control flag"
      ": Call to fcntl() failed: " << cta::utils::errnoToString(errno);
    throw ex;
  }

  // Start connecting
  const int connectRc = connect(smartSock.get(), address, address_len);
  const int connectErrno = errno;

  // If the connection completed immediately then restore the original
  // file-control flags of the socket and return it
  if(0 == connectRc) {
    if(-1 == fcntl(smartSock.get(), F_SETFL, orginalFileControlFlags)) {
      cta::exception::Exception ex;
      ex.getMessage() <<
        "Failed to restore the file-control flags of the socket"
        ": " << cta::utils::errnoToString(errno);
      throw ex;
    }
    return smartSock.release();
  }

  // Throw an exception if there was any other error than
  // "operation in progress" when trying to start to connect
  if(EINPROGRESS != connectErrno) {
    cta::exception::Exception ex;
    ex.getMessage() << "Call to connect() failed: "
      << cta::utils::errnoToString(connectErrno);
    throw ex;
  }

  pollfd pollFd;
  pollFd.fd = smartSock.get();
  pollFd.events = POLLIN | POLLOUT;
  pollFd.revents = 0;

  // Wait for the connection to complete using poll() with a timeout
  const int pollRc = poll(&pollFd, 1, 1000 * timeout);
  if(-1 == pollRc) {
    cta::exception::Exception ex;
    ex.getMessage() << "Call to poll() failed: "
      << cta::utils::errnoToString(errno);
    throw ex;
  }

  // Throw a timed-out exception if poll() timed-out
  if(0 == pollRc) {
    cta::exception::TimeOut ex;
    ex.getMessage() <<
      "Failed to connect"
      ": poll() timed out after " << timeout << " seconds";
    throw ex;
  }

  if(pollFd.revents & POLLNVAL) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to connect"
      ": File descriptor " << pollFd.fd << " is not open";
    throw ex;
  }

  // Use getsockopt() to check whether or not the connection completed
  // successfully
  //
  // Take into account the fact that if there is an error then Solaris will
  // return -1 and set errno, whereas BSD will return 0 and set sockoptError
  int sockoptError = 0;
  socklen_t sockoptErrorLen = sizeof(sockoptError);
  cta::exception::Errnum::throwOnMinusOne(
    getsockopt(smartSock.get(), SOL_SOCKET, SO_ERROR, &sockoptError,
      &sockoptErrorLen),
    "In io::connectWithTimeout: failed to getsockopt: ");
  if(0 != sockoptError) { // BSD
    cta::exception::Exception ex;
    ex.getMessage()
      << "In io::connectWithTimeout: Connection did not complete successfully: "
      << cta::utils::errnoToString(sockoptError);
    throw ex;
  }

  // Restore the original file-control flags of the socket
  cta::exception::Errnum::throwOnMinusOne(
    fcntl(smartSock.get(), F_SETFL, orginalFileControlFlags),
    "In io::connectWithTimeout: failed to restore flags with fcntl: ");

  return smartSock.release();
}

//------------------------------------------------------------------------------
// marshalUint8
//------------------------------------------------------------------------------
size_t marshalUint8(const uint8_t src, char*& dst) {
  if(dst == nullptr) {
    throw exception::Exception("Failed to marshal uint8_t: Pointer to destination buffer is nullptr");
  }

  *dst = src;
  dst += sizeof(src);

  return sizeof(src);
}

//------------------------------------------------------------------------------
// marshalInt16
//------------------------------------------------------------------------------
size_t marshalInt16(const int16_t src, char*& dst) {
  if(dst == nullptr) {
    throw exception::Exception("Failed to marshal int16_t: Pointer to destination buffer is nullptr");
  }

  const int16_t netByteOrder = htons(src);
  memcpy(dst, &netByteOrder, sizeof(src));
  dst += sizeof(src);

  return sizeof(src);
}

//------------------------------------------------------------------------------
// marshalUint16
//------------------------------------------------------------------------------
size_t marshalUint16(const uint16_t src, char*& dst) {
  if(dst == nullptr) {
    throw exception::Exception("Failed to marshal uint16_t: Pointer to destination buffer is nullptr");
  }

  const uint16_t netByteOrder = htons(src);
  memcpy(dst, &netByteOrder, sizeof(src));
  dst += sizeof(src);

  return sizeof(src);
}

//------------------------------------------------------------------------------
// marshalInt32
//------------------------------------------------------------------------------
size_t marshalInt32(const int32_t src, char*& dst) {
  if(dst == nullptr) {
    throw exception::Exception("Failed to marshal int32_t: Pointer to destination buffer is nullptr");
  }

  const int32_t netByteOrder = htonl(src);
  memcpy(dst, &netByteOrder, sizeof(src));
  dst += sizeof(src);

  return sizeof(src);
}

//------------------------------------------------------------------------------
// marshalUint32
//------------------------------------------------------------------------------
size_t marshalUint32(const uint32_t src, char*& dst) {
  if(dst == nullptr) {
    throw exception::Exception("Failed to marshal uint32_t: Pointer to destination buffer is nullptr");
  }

  const uint32_t netByteOrder = htonl(src);
  memcpy(dst, &netByteOrder, sizeof(src));
  dst += sizeof(src);

  return sizeof(src);
}

//------------------------------------------------------------------------------
// marshalUint64
//------------------------------------------------------------------------------
size_t marshalUint64(const uint64_t src, char*& dst) {
  if(dst == nullptr) {
    throw exception::Exception("Failed to marshal uint64_t: Pointer to destination buffer is nullptr");
  }

  // Be independent of the host's byte representation of a multi-byte integer by
  // determining the value of each byte to be marshalled using the left-shift
  // arithmetic-operator
  dst[0] = src >> 56 & 0xFF;
  dst[1] = src >> 48 & 0xFF;
  dst[2] = src >> 40 & 0xFF;
  dst[3] = src >> 32 & 0xFF;
  dst[4] = src >> 24 & 0xFF;
  dst[5] = src >> 16 & 0xFF;
  dst[6] = src >>  8 & 0xFF;
  dst[7] = src       & 0xFF;
  dst += sizeof(src);

  return sizeof(src);
}

//------------------------------------------------------------------------------
// marshalString
//------------------------------------------------------------------------------
void marshalString(const std::string& src, char*& dst, size_t& dstSize) {
  if(dst == nullptr) {
    throw exception::Exception("Failed to marshal string: Pointer to destination buffer is nullptr");
  }
  if(src.size()+1 > dstSize) {
    throw exception::Exception("Failed to marshal string: Source string is too long");
  }
  strcpy(dst, src.c_str());

  dst     += src.size()+1;
  dstSize -= src.size()+1;
}

//------------------------------------------------------------------------------
// unmarshalUint8
//------------------------------------------------------------------------------
void unmarshalUint8(const char * &src, size_t &srcLen, uint8_t &dst) {

  if(src == nullptr) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal uint8_t"
      ": Pointer to source buffer is nullptr";
    throw ex;
  }

  if(srcLen < sizeof(dst)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal uint8_t"
      ": Source buffer length is too small: expected="
      << sizeof(dst) << " actual=" << srcLen;
    throw ex;
  }

  dst = *src;
  src += sizeof(dst);
  srcLen -= sizeof(dst);
}

//------------------------------------------------------------------------------
// unmarshalInt16
//------------------------------------------------------------------------------
void unmarshalInt16(const char * &src, size_t &srcLen, int16_t &dst) {

  if(src == nullptr) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal int16_t"
      ": Pointer to source buffer is nullptr";
    throw ex;
  }

  if(srcLen < sizeof(dst)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal int16_t"
      ": Source buffer length is too small: expected="
      << sizeof(dst) << " actual=" << srcLen;
    throw ex;
  }

  int16_t netByteOrder = 0;
  memcpy(&netByteOrder, src, sizeof(dst));
  dst = ntohs(netByteOrder);
  src += sizeof(dst);
  srcLen -= sizeof(dst);
}

//------------------------------------------------------------------------------
// unmarshalUint16
//------------------------------------------------------------------------------
void unmarshalUint16(const char * &src, size_t &srcLen, uint16_t &dst) {

  if(src == nullptr) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal uint16_t"
      ": Pointer to source buffer is nullptr";
    throw ex;
  }

  if(srcLen < sizeof(dst)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal uint16_t"
      ": Source buffer length is too small: expected="
      << sizeof(dst) << " actual=" << srcLen;
    throw ex;
  }

  uint16_t netByteOrder = 0;
  memcpy(&netByteOrder, src, sizeof(dst));
  dst = ntohs(netByteOrder);
  src += sizeof(dst);
  srcLen -= sizeof(dst);
}

//------------------------------------------------------------------------------
// unmarshalUint32
//------------------------------------------------------------------------------
void unmarshalUint32(const char * &src, size_t &srcLen,
  uint32_t &dst)  {

  if(src == nullptr) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal uint32_t"
      ": Pointer to source buffer is nullptr";
    throw ex;
  }

  if(srcLen < sizeof(dst)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal uint32_t"
      ": Source buffer length is too small: expected="
      << sizeof(dst) << " actual=" << srcLen;
    throw ex;
  }

  uint32_t netByteOrder = 0;
  memcpy(&netByteOrder, src, sizeof(dst));
  dst = ntohl(netByteOrder);
  src += sizeof(dst);
  srcLen -= sizeof(dst);
}

//------------------------------------------------------------------------------
// unmarshalInt32
//------------------------------------------------------------------------------
void unmarshalInt32(const char * &src, size_t &srcLen,
  int32_t &dst)  {

  if(src == nullptr) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal int32_t"
      ": Pointer to source buffer is nullptr";
    throw ex;
  }

  if(srcLen < sizeof(dst)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal int32_t"
      ": Source buffer length is too small: expected="
      << sizeof(dst) << " actual=" << srcLen;
    throw ex;
  }

  int32_t netByteOrder = 0;
  memcpy(&netByteOrder, src, sizeof(dst));
  dst = ntohl(netByteOrder);
  src += sizeof(dst);
  srcLen -= sizeof(dst);
}

//------------------------------------------------------------------------------
// unmarshalUint64
//------------------------------------------------------------------------------
void unmarshalUint64(const char * &src, size_t &srcLen, uint64_t &dst) {

  if(src == nullptr) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal uint64_t"
      ": Pointer to source buffer is nullptr";
    throw ex;
  }

  if(srcLen < sizeof(dst)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal uint64_t"
      ": Source buffer length is too small: expected="
      << sizeof(dst) << " actual=" << srcLen;
    throw ex;
  }

  dst  = ((uint64_t)src[0] << 56) & 0xFF00000000000000ULL;
  dst |= ((uint64_t)src[1] << 48) & 0x00FF000000000000ULL;
  dst |= ((uint64_t)src[2] << 40) & 0x0000FF0000000000ULL;
  dst |= ((uint64_t)src[3] << 32) & 0x000000FF00000000ULL;
  dst |= ((uint64_t)src[4] << 24) & 0x00000000FF000000ULL;
  dst |= ((uint64_t)src[5] << 16) & 0x0000000000FF0000ULL;
  dst |= ((uint64_t)src[6] << 8)  & 0x000000000000FF00ULL;
  dst |=  (uint64_t)src[7]        & 0x00000000000000FFULL;
  src += sizeof(dst);
  srcLen -= sizeof(dst);
}

//------------------------------------------------------------------------------
// unmarshalString
//------------------------------------------------------------------------------
void unmarshalString(const char*& src, size_t& srcLen, char* dst, const size_t dstLen) {
  if(src == nullptr) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal string"
      ": Pointer to source buffer is nullptr";
    throw ex;
  }

  if(srcLen == 0) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal string"
      ": Source buffer length is 0";
    throw ex;
  }

  if(dst == nullptr) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal string"
      ": Pointer to destination buffer is nullptr";
    throw ex;
  }

  if(dstLen == 0) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal string"
      ": Destination buffer length is 0";
    throw ex;
  }

  // Calculate the maximum number of bytes that could be unmarshalled
  size_t maxlen = 0;
  if(srcLen < dstLen) {
    maxlen = srcLen;
  } else {
    maxlen = dstLen;
  }

  bool strTerminatorReached = false;

  // While there are potential bytes to copy and the string terminator has not
  // been reached
  for(size_t i=0; i<maxlen && !strTerminatorReached; i++) {
    // If the string terminator has been reached
    if((*dst++ = *src++) == '\0') {
      strTerminatorReached = true;
    }

    srcLen--;
  }

  // If all potential bytes were copied but the string terminator was not
  // reached
  if(!strTerminatorReached) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal string"
      ": String terminator of source buffer was not reached";
    throw ex;
  }
}

} // namespace mediachanger
} // namespace cta
