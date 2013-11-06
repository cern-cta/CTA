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
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoPortInRange.hpp"
#include "castor/exception/TapeNetAcceptInterrupted.hpp"
#include "castor/exception/TimeOut.hpp"
#include "castor/tape/net/Constants.hpp"
#include "castor/tape/net/IpAndPort.hpp"

#include <errno.h>
#include <string.h>
#include <iostream>
#include <sys/socket.h>
#include <sys/types.h>

namespace castor {
namespace tape   {
namespace net    {
	

/**
 * Creates a listener socket with a port number within the specified range.
 * This method creates the socket, binds it and marks it as a listener.
 *
 * This method raises a castor::exception::InvalidArgument exception if one or
 * more of its input parameters are invalid.
 *
 * This method raises a castor::exception::NoPortInRange exception if it cannot
 * find a free port to bind to within the specified range.
 *
 * @param addr       The IP address in dotted quad notation to be used by
 *                   inet_addr().
 * @param lowPort    The inclusive low port of the port number range.  This
 *                   parameter must be an unsigned integer greater than 0.
 * @param highPort   The inclusive high port of the port number range.  This
 *                   parameter must be an unsigned integer greater than 0.
 * @param chosenPort Out parameter: The actual port that this method binds the
 *                   socket to.
 * @return           The socket descriptor.
 */
int createListenerSock(
  const char *const    addr,
  const unsigned short lowPort,
  const unsigned short highPort,
  unsigned short       &chosenPort)
  throw(castor::exception::Exception);

/**
 * Accepts a connection on the specified listener socket and returns the
 * socket descriptor of the newly created and connected socket.
 *
 * @param listenSockFd The file descriptor of the listener socket.
 * @return             The socket descriptor of the newly created and connected
 *                     socket.
 */
int acceptConnection(const int listenSockFd)
  throw(castor::exception::Exception);

/**
 * Accepts a connection on the specified listener socket and returns the
 * socket descriptor of the newly created and connected socket.
 *
 * This method accepts a timeout parameter.  If the timeout is exceeded, then
 * this method raises a castor::exception::TimeOut exception.  If this method
 * is interrupted, then this method raises a
 * castor::exception::TapeNetAcceptInterrupted exception which gives the number
 * of remaining seconds when the interrupt occured.  All other errors result in
 * a castor::exception::Exception being raised.  Note that both
 * castor::exception::TimeOut and castor::exception::TapeNetAcceptInterrupted
 * inherit from castor::exception::Exception so callers of the this method must
 * catch castor::exception::TimeOut and
 * castor::exception::TapeNetAcceptInterrupted before catching
 * castor::exception::Exception.
 *
 * @param listenSockFd The file descriptor of the listener socket.
 * @param timeout      The timeout in seconds to be used when waiting for a
 *                     connection.
 * @return             The socket descriptor of the newly created and connected
 *                     socket.
 */
int acceptConnection(
  const int    listenSockFd,
  const time_t timeout)
  throw(castor::exception::TimeOut,
    castor::exception::TapeNetAcceptInterrupted, castor::exception::Exception); 

/**
 * Gets the locally-bound IP and port number of the specified socket.
 *
 * @param socketFd The socket file descriptor.
 * @return         The IP and port number of the specified socket.
 */
IpAndPort getSockIpPort(const int socketFd)
  throw(castor::exception::Exception);

/**
 * Gets the peer IP and port number of the specified socket.
 *
 * @param socketFd The socket file descriptor.
 * @return         The IP and port number of the specified socket.
 */
IpAndPort getPeerIpPort(const int socketFd)
  throw(castor::exception::Exception);

/**
 * Gets the locally-bound host name of the specified socket.
 *
 * @param socketFd The socket file descriptor.
 * @param buf      The buffer into which the hostname should written to.
 * @param len      The length of the buffer into which the host name should be
 *                 written to.
 */
void getSockHostName(
  const int    socketFd,
  char *const  buf,
  const size_t len)
  throw(castor::exception::Exception);

/**
 * Gets the locally-bound host name of the specified socket.
 *
 * @param socketFd The socket file descriptor.
 * @param buf      The buffer into which the hostname should written to.
 */
template<int n> static void getSockHostName(
  const int socketFd,
  char (&buf)[n])
  throw(castor::exception::Exception) {
  getSockHostName(socketFd, buf, n);
}

/**
 * Gets the locally-bound IP, host name and port of the specified socket.
 *
 * @param socketFd    The socket file descriptor.
 * @param ip          The IP to be filled.
 * @param hostName    The buffer into which the hostname should written to.
 * @param hostNameLen The length of the buffer into which the host name
 *                    should be written to.
 * @param port        The port to be filled.
 */
void getSockIpHostnamePort(
  const int      socketFd,
  unsigned long  &ip,
  char           *const hostName,
  const size_t   hostNameLen,
  unsigned short &port) throw(castor::exception::Exception);

/**
 * Gets the locally-bound IP, host name and port of the specified socket.
 *
 * @param socketFd The socket file descriptor.
 * @param ip       The IP to be filled.
 * @param hostName The buffer into which the hostname should written to.
 * @param port     The port to be filled.
 */
template<int n> static void getSockIpHostnamePort(
  const int      socketFd,
  unsigned long  &ip,
  char           (&hostName)[n],
  unsigned short &port)
  throw(castor::exception::Exception) {
  getSockIpHostnamePort(socketFd, ip, hostName, n, port);
}

/**
 * Gets the peer host name of the specified connection.
 *
 * @param socketFd The socket file descriptor of the connection.
 * @param buf      The buffer into which the hostname should written to.
 * @param len      The length of the buffer into which the host name should be
 *                 written to.
 */
void getPeerHostName(
  const int    socketFd,
  char *const  buf,
  const size_t len)
  throw(castor::exception::Exception);

/**
 * Gets the peer host name of the specified connection.
 *
 * @param socketFd The socket file descriptor of the connection.
 * @param buf      The buffer into which the hostname should written to.
 */
template<int n> static void getPeerHostName(
  const int socketFd,
  char (&buf)[n])
  throw(castor::exception::Exception) {
  getPeerHostName(socketFd, buf, n);
}

/**
 * Writes the string form of specified IP using the specified output
 * stream.
 *
 * @param os The output stream.
 * @param ip The IP address in host byte order.
 */
void writeIp(
  std::ostream        &os,
  const unsigned long ip)
  throw();

/**
 * Writes a textual description of the specified socket to the specified
 * output stream.
 *
 * @param os       The output stream to which the string is to be printed.
 * @param socketFd The file descriptor of the socket whose textual
 *                 description is to be printed to the stream.
 */
void writeSockDescription(
  std::ostream &os,
  const int socketFd)
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
 * @param timeout  The timeout in seconds.
 * @param nbBytes  The number of bytes to be read.
 * @param buf      The buffer into which the bytes will be written.
 */
void readBytes(
  const int   socketFd,
  const int   timeout,
  const int   nbBytes,
  char *const buf)
  throw(castor::exception::Exception);

/**
 * Reads the specified number of bytes from the specified closable socket
 * and writes the result into the specified buffer.
 *
 * @param socketFd   The file descriptor of the socket to be read from.
 * @param timeout    The timeout in seconds.
 * @param nbBytes    The number of bytes to be read.
 * @param buf        The buffer into which the bytes will be written.
 * @return           True if the connection was closed by the peer, else false.
 */
bool readBytesFromCloseable(
  const int   socketFd, 
  const int   timeout,
  const int   nbBytes,
  char *const buf) 
  throw(castor::exception::Exception);

/**
 * Writes the specified number of bytes from the specified buffer to the
 * specified socket.
 *
 * @param socketFd The file descriptor of the socket to be written to
 * @param timeout  The timeout in seconds.
 * @param nbBytes  The number of bytes to be written.
 * @param buf      The buffer of bytes to be written to the socket.
 */
void writeBytes(
  const int   socketFd,
  const int   timeout,
  const int   nbBytes,
  char *const buf)
  throw(castor::exception::Exception);

/**
 * Creates the specified socket and uses it to connects to the specified
 * address within the constraints of the specified timeout.
 *
 * This method throws a castor::exception::TimeOut exception if a timeout
 * occurs.
 *
 * This method throws a castor::exception::Exception exception if an error
 * other than a timeout occurs.
 *
 * @param sockDomain   The communications domain of the socket, see
 *                     'man 2 socket'.
 * @param sockType     The type of the socket, see 'man 2 socket'.
 * @param sockProtocol The protocol to be used with the socket, see
 *                     'man 2 socket'.
 * @param address      The address to connect to, see 'man 2 connect'.
 * @param address_len  The length of the address to connect to, see
 *                     'man 2 connect'.
 * @param timeout      The maximum amount of time in seconds to wait for the
 *                     connect to finish.
 * @return             A file-descriptor referencing the newly created and
 *                     connected socket.
 */
int connectWithTimeout(
  const int             sockDomain,
  const int             sockType,
  const int             sockProtocol,
  const struct sockaddr *address,
  const socklen_t       address_len,
  const int             timeout)
  throw(castor::exception::TimeOut, castor::exception::Exception);

} // namespace net
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_NET_NET_HPP
