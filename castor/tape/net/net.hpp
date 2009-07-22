/******************************************************************************
 *                      castor/tape/net/net.hpp
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

#ifndef CASTOR_TAPE_NET_NET_HPP
#define CASTOR_TAPE_NET_NET_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/exception/TimeOut.hpp"
#include "castor/tape/net/Constants.hpp"

#include <errno.h>
#include <string.h>
#include <iostream>

namespace castor {
namespace tape   {
namespace net    {
	
/**
 * Creates a listener socket including, creation, binding and marking as a
 * listener socket.
 *
 * @param addr The IP address in dotted quad notation to be used by
 * inet_addr().
 * @param port The port number to listen on or 0 if one should be allocated.
 * @return The socket file descriptor.
 */
int createListenerSock(const char *addr,
  const unsigned short port) throw(castor::exception::Exception);

/**
 * Accepts a connection on the specified listener socket and returns the
 * socket file descriptor of the newly created connected socket.
 *
 * @param listenSockFd The file descriptor of the listener socket.
 */
int acceptConnection(const int listenSockFd)
  throw(castor::exception::Exception);

/**
 * Accepts a connection on the specified listener socket and returns the
 * socket file descriptor of the newly created connected socket.
 *
 * This method accepts a timeout parameter.  If the timeout is exceeded, then
 * this method raises a castor::exception::TimeOut exception.  All other
 * errors result in a castor::exception::Exception being raised.  Note that
 * castor::exception::TimeOut inherits from castor::exception::Exception so one
 * must catch castor::exception::TimeOut before catching
 * castor::exception::Exception.
 *
 * @param listenSockFd The file descriptor of the listener socket.
 * @param timeout The timeout in seconds to be used when waiting for a
 * connection.
 */
int acceptConnection(const int listenSockFd,
  const int timeout) throw(castor::exception::TimeOut,
    castor::exception::Exception);

/**
 * Gets the locally-bound IP and port number of the specified socket.
 *
 * @param socketFd The socket file descriptor.
 * @param ip The IP to be filled.
 * @param port The port to be filled.
 */
void getSockIpPort(const int socketFd, unsigned long& ip,
  unsigned short& port) throw(castor::exception::Exception);

/**
 * Gets the peer IP and port number of the specified socket.
 *
 * @param socketFd The socket file descriptor.
 * @param ip The IP to be filled.
 * @param port The port to be filled.
 */
void getPeerIpPort(const int socketFd, unsigned long& ip,
  unsigned short& port) throw(castor::exception::Exception);

/**
 * Gets the locally-bound host name of the specified socket.
 *
 * @param socketFd The socket file descriptor.
 * @param buf The buffer into which the hostname should written to.
 * @param len The length of the buffer into which the host name should be
 * written to.
 */
void getSockHostName(const int socketFd, char *buf, size_t len)
  throw(castor::exception::Exception);

/**
 * Gets the locally-bound host name of the specified socket.
 *
 * @param socketFd The socket file descriptor.
 * @param buf The buffer into which the hostname should written to.
 */
template<int n> static void getSockHostName(const int socketFd,
  char (&buf)[n]) throw(castor::exception::Exception) {
  getSockHostName(socketFd, buf, n);
}


/**
 * Gets the locally-bound IP, host name and port of the specified socket.
 *
 * @param socketFd The socket file descriptor.
 * @param ip The IP to be filled.
 * @param hostName The buffer into which the hostname should written to.
 * @param hostNameLen The length of the buffer into which the host name
 * should be written to.
 * @param port The port to be filled.
 * written to.
 */
void getSockIpHostnamePort(const int socketFd,
  unsigned long& ip, char *hostName, size_t hostNameLen,
  unsigned short& port) throw(castor::exception::Exception);

/**
 * Gets the locally-bound IP, host name and port of the specified socket.
 *
 * @param socketFd The socket file descriptor.
 * @param ip The IP to be filled.
 * @param hostName The buffer into which the hostname should written to.
 * @param port The port to be filled.
 * written to.
 */
template<int n> static void getSockIpHostnamePort(const int socketFd,
  unsigned long& ip, char (&hostName)[n], unsigned short& port)
  throw(castor::exception::Exception) {
  getSockIpHostnamePort(socketFd, ip, hostName, n, port);
}

/**
 * Gets the peer host name of the specified connection.
 *
 * @param socketFd The socket file descriptor of the connection.
 * @param buf The buffer into which the hostname should written to.
 * @param len The length of the buffer into which the host name should be
 * written to.
 */
void getPeerHostName(const int socketFd, char *buf, size_t len)
  throw(castor::exception::Exception);

/**
 * Gets the peer host name of the specified connection.
 *
 * @param socketFd The socket file descriptor of the connection.
 * @param buf The buffer into which the hostname should written to.
 */
template<int n> static void getPeerHostName(const int socketFd,
  char (&buf)[n]) throw(castor::exception::Exception) {
  getPeerHostName(socketFd, buf, n);
}

/**
 * Writes the string form of specified IP using the specified output
 * stream.
 *
 * @param os The output stream.
 * @param ip The IP address in host byte order.
 */
void writeIp(std::ostream &os, const unsigned long ip) throw();

/**
 * Writes a textual description of the specified socket to the specified
 * output stream.
 *
 * @param os The output stream to which the string is to be printed.
 * @param socketFd The file descriptor of the socket whose textual
 * description is to be printed to the stream.
 */
void writeSockDescription(std::ostream &os, const int socketFd)
  throw();

/**
 * Reads the specified number of bytes from the specified socket and writes
 * the result into the specified buffer.
 *
 * This operation assumes that all of the bytes can be read in.  Failure
 * to read in all the bytes or a closed connection will result in an
 * exception being thrown.
 *
 * If it is normal that the connection can be closed by the peer, for
 * example you are using select, then please use
 * readBytesFromCloseable().
 *
 * @param socketFd The file descriptor of the socket to be read from.
 * @param netReadWriteTimeout The timeout to be used when performing
 * network reads and writes.
 * @param nbBytes The number of bytes to be read.
 * @param buf The buffer into which the bytes will be written.
 */
void readBytes(const int socketFd, const int netReadWriteTimeout,
  const int nbBytes, char *buf) throw(castor::exception::Exception);

/**
 * Reads the specified number of bytes from the specified closable socket
 * and writes the result into the specified buffer.
 *
 * @param connClosed Output parameter: True if the connection was closed
 * by the peer.
 * @param socketFd The file descriptor of the socket to be read from.
 * @param netReadWriteTimeout The timeout to be used when performing
 * network reads and writes.
 * @param nbBytes The number of bytes to be read.
 * @param buf The buffer into which the bytes will be written.
 */
void readBytesFromCloseable(bool &connClosed, const int socketFd, 
  const int netReadWriteTimeout, const int nbBytes, char *buf) 
  throw(castor::exception::Exception);

/**
 * Writes the specified number of bytes from the specified buffer to the
 * specified socket.
 *
 * @param socketFd The file descriptor of the socket to be written to
 * @param netReadWriteTimeout The timeout to be used when performing
 * network reads and writes.
 * @param nbBytes The number of bytes to be written.
 * @param buf The buffer of bytes to be written to the socket.
 */
void writeBytes(const int socketFd, const int netReadWriteTimeout,
  const int nbBytes, char *const buf) throw(castor::exception::Exception);

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_NET_NET_HPP
