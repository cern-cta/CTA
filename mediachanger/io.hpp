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

#pragma once

#include "common/exception/AcceptConnectionInterrupted.hpp"
#include "common/exception/InvalidArgument.hpp"
#include "common/exception/NoPortInRange.hpp"
#include "common/exception/TimeOut.hpp"
#include "common/exception/Exception.hpp"
#include "mediachanger/Constants.hpp"
#include "mediachanger/IpAndPort.hpp"

#include <errno.h>
#include <iostream>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdint.h>
#include <string>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <list>

namespace cta::mediachanger {

/**
 * Creates a listener socket with the specified port number.
 *
 * This method creates the socket, binds it and marks it as a listener.  The
 * listening port will accept connections on any of the hosts incomming
 * interfaces.
 *
 * This method raises a cta::exception::InvalidArgument exception if one or
 * more of its input parameters are invalid.
 *
 * This method raises a cta::exception::NoPortInRange exception if the
 * specified port is not free.
 *
 * @param port The port number.
 * @return     The socket descriptor.
 */
int createListenerSock(const unsigned short port);

/**
 * Creates a listener socket with a port number within the specified range.
 *
 * This method creates the socket, binds it and marks it as a listener.  The
 * listening port will accept connections on any of the hosts incomming
 * interfaces.
 *
 * This method raises a cta::exception::InvalidArgument exception if one or
 * more of its input parameters are invalid.
 *
 * This method raises a cta::exception::NoPortInRange exception if it cannot
 * find a free port to bind to within the specified range.
 *
 * @param lowPort    The inclusive low port of the port number range.  This
 *                   parameter must be an unsigned integer greater than 0.
 * @param highPort   The inclusive high port of the port number range.  This
 *                   parameter must be an unsigned integer greater than 0.
 * @param chosenPort Out parameter: The actual port that this method binds the
 *                   socket to.
 * @return           The socket descriptor.
 */
int createListenerSock(
  const unsigned short lowPort,
  const unsigned short highPort,
  unsigned short       &chosenPort);
	
/**
 * Creates a listener socket with a port number within the specified range.
 *
 * This method creates the socket, binds it and marks it as a listener.
 *
 * This method raises a cta::exception::InvalidArgument exception if one or
 * more of its input parameters are invalid.
 *
 * This method raises a cta::exception::NoPortInRange exception if it cannot
 * find a free port to bind to within the specified range.
 *
 * @param addr       The IP address as a string in dotted quad notation.
 * @param lowPort    The inclusive low port of the port number range.  This
 *                   parameter must be an unsigned integer greater than 0.
 * @param highPort   The inclusive high port of the port number range.  This
 *                   parameter must be an unsigned integer greater than 0.
 * @param chosenPort Out parameter: The actual port that this method binds the
 *                   socket to.
 * @return           The socket descriptor.
 */
int createListenerSock(
  const std::string    &addr,
  const unsigned short lowPort,
  const unsigned short highPort,
  unsigned short       &chosenPort);

/**
 * Creates a listener socket with a port number within the specified range.
 * This method creates the socket, binds it and marks it as a listener.
 *
 * This method raises a cta::exception::InvalidArgument exception if one or
 * more of its input parameters are invalid.
 *
 * This method raises a cta::exception::NoPortInRange exception if it cannot
 * find a free port to bind to within the specified range.
 *
 * @param addr       The IP address.
 * @param lowPort    The inclusive low port of the port number range.  This
 *                   parameter must be an unsigned integer greater than 0.
 * @param highPort   The inclusive high port of the port number range.  This
 *                   parameter must be an unsigned integer greater than 0.
 * @param chosenPort Out parameter: The actual port that this method binds the
 *                   socket to.
 * @return           The socket descriptor.
 */
int createListenerSock(
  const struct in_addr &addr,
  const unsigned short lowPort,
  const unsigned short highPort,
  unsigned short       &chosenPort);

/**
 * Creates a listener socket with the specified port number that is bound to
 * localhost, in other words the socket can only accept connections originating
 * from the local host.
 *
 * This method creates the socket, binds it and marks it as a listener.  The
 * listening port will accept connections on any of the hosts incomming
 * interfaces.
 *
 * This method raises a cta::exception::InvalidArgument exception if one or
 * more of its input parameters are invalid.
 *
 * This method raises a cta::exception::NoPortInRange exception if the
 * specified port is not free.
 *
 * @param port The port number.
 * @return     The socket descriptor.
 */
int createLocalhostListenerSock(const unsigned short port);

/**
 * Accepts a connection on the specified listener socket and returns the
 * socket descriptor of the newly created and connected socket.
 *
 * @param listenSockFd The file descriptor of the listener socket.
 * @return             The socket descriptor of the newly created and connected
 *                     socket.
 */
int acceptConnection(const int listenSockFd);

/**
 * Accepts a connection on the specified listener socket and returns the
 * socket descriptor of the newly created and connected socket.
 *
 * This method accepts a timeout parameter.  If the timeout is exceeded, then
 * this method raises a cta::exception::TimeOut exception.  If this method
 * is interrupted, then this method raises a
 * cta::exception::AcceptConnectionInterrupted exception which gives the
 * number of remaining seconds when the interrupt occured.  All other errors
 * result in a cta::exception::Exception being raised.  Note that both
 * cta::exception::TimeOut and cta::exception::AcceptConnectionInterrupted
 * inherit from cta::exception::Exception so callers of the this method must
 * catch cta::exception::TimeOut and
 * cta::exception::AcceptConnectionInterrupted before catching
 * cta::exception::Exception.
 *
 * @param listenSockFd The file descriptor of the listener socket.
 * @param timeout      The timeout in seconds to be used when waiting for a
 *                     connection.
 * @return             The socket descriptor of the newly created and connected
 *                     socket.
 */
int acceptConnection(
  const int    listenSockFd,
  const time_t timeout);

/**
 * Gets the locally-bound IP and port number of the specified socket.
 *
 * @param socketFd The socket file descriptor.
 * @return         The IP and port number of the specified socket.
 */
IpAndPort getSockIpPort(const int socketFd);

/**
 * Gets the peer IP and port number of the specified socket.
 *
 * @param socketFd The socket file descriptor.
 * @return         The IP and port number of the specified socket.
 */
IpAndPort getPeerIpPort(const int socketFd);

/**
 * Gets the locally-bound host name of the specified socket.
 *
 * @param socketFd The socket file descriptor.
 * @return         The host name.
 */
std::string getSockHostName(const int socketFd);

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
  unsigned short &port);

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
   {
  getSockIpHostnamePort(socketFd, ip, hostName, n, port);
}

/**
 * Gets the peer host name of the specified connection.
 *
 * @param socketFd The socket file descriptor of the connection.
 *                 written to.
 * @return         The peer host name.
 */
std::string getPeerHostName(const int socketFd);

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
  noexcept;

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
  const int socketFd);

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
  char *const buf);

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
  char *const buf);

/**
 * Creates the specified socket and uses it to connect to the specified
 * address within the constraint of the specified timeout.
 *
 * This method throws a cta::exception::TimeOut exception if a timeout
 * occurs.
 *
 * This method throws a cta::exception::Exception exception if an error
 * other than a timeout occurs.
 *
 * @param hostName The name of the host to connect to.
 * @param port     The TCP/IP port number to connect to.
 * @param timeout  The maximum amount of time in seconds to wait for the
 *                 connect to finish.
 * @return         A file-descriptor referencing the newly created and
 *                 connected socket.
 */
int connectWithTimeout(
  const std::string    &hostName,
  const unsigned short port,
  const int            timeout);

/**
 * Creates the specified socket and uses it to connect to the specified
 * address within the constraint of the specified timeout.
 *
 * This method throws a cta::exception::TimeOut exception if a timeout
 * occurs.
 *
 * This method throws a cta::exception::Exception exception if an error
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
  const int             timeout);

/**
 * Marshals the specified src value into the specified destination buffer.
 *
 * @param src The source value be marshalled.
 * @param dst In/out parameter, before invocation points to the destination
 *            buffer where the source value should be marshalled to and on
 * return     The number of bytes marshalled
 */
size_t marshalUint8(const uint8_t src, char*& dst);

/**
 * Marshals the specified src value into the specified destination buffer.
 *
 * @param src The source value be marshalled.
 * @param dst In/out parameter, before invocation points to the destination
 *            buffer where the source value should be marshalled to and on
 * return     The number of bytes marshalled
 */
size_t marshalInt16(const int16_t src, char*& dst);

/**
 * Marshals the specified src value into the specified destination buffer.
 *
 * @param src The source value be marshalled.
 * @param dst In/out parameter, before invocation points to the destination
 *            buffer where the source value should be marshalled to and on
 * return     The number of bytes marshalled
 */
size_t marshalUint16(const uint16_t src, char*& dst);

/**
 * Marshals the specified src value into the specified destination buffer.
 *
 * @param src The source value be marshalled.
 * @param dst In/out parameter, before invocation points to the destination
 *            buffer where the source value should be marshalled to and on
 * return     The number of bytes marshalled
 */
size_t marshalInt32(const int32_t src, char*& dst);

/**
 * Marshals the specified src value into the specified destination buffer.
 *
 * @param src The source value be marshalled.
 * @param dst In/out parameter, before invocation points to the destination
 *            buffer where the source value should be marshalled to and on
 * return     The number of bytes marshalled
 */
size_t marshalUint32(const uint32_t src, char*& dst);

/**
 * Marshals the specified src value into the specified destination buffer.
 *
 * @param src The source value be marshalled.
 * @param dst In/out parameter, before invocation points to the destination
 *            buffer where the source value should be marshalled to and on
 * return     The number of bytes marshalled
 */
size_t marshalUint64(const uint64_t src, char*& dst);

/**
 * Marshals the specified string into the specified destination buffer.
 *
 * @param src[in]            The string to be marshalled
 * @param dst[in,out]        Pointer to the destination buffer where the string should be marshalled.
 *                           On return, points to the byte in the destination buffer immediately after
 *                           the marshalled string.
 * @param dstSize[in,out]    Contains the size of the buffer pointed to by dst. Value is reduced by
 *                           the number of bytes written to dst.
 */
void marshalString(const std::string& src, char*& dst, size_t& dstSize);

/**
 * Unmarshals a value from the specified source buffer into the specified
 * destination.
 *
 * @param src    In/out parameter: Before invocation points to the source
 *               buffer where the value should be unmarshalled from and on
 *               return points to the byte in the source buffer immediately
 *               after the unmarshalled value.
 * @param srcLen In/our parameter: Before invocation is the length of the
 *               source buffer from where the value should be unmarshalled and
 *               on return is the number of bytes remaining in the source
 *               buffer.
 * @param dst    Out parameter: The destination.
 */
void unmarshalUint8(const char * &src, size_t &srcLen, uint8_t &dst);

/**
 * Unmarshals a value from the specified source buffer into the specified
 * destination.
 *
 * @param src    In/out parameter: Before invocation points to the source
 *               buffer where the value should be unmarshalled from and on
 *               return points to the byte in the source buffer immediately
 *               after the unmarshalled value.
 * @param srcLen In/our parameter: Before invocation is the length of the
 *               source buffer from where the value should be unmarshalled and
 *               on return is the number of bytes remaining in the source
 *               buffer.
 * @param dst    Out parameter: The destination.
 */
void unmarshalInt16(const char * &src, size_t &srcLen, int16_t &dst);

/**
 * Unmarshals a value from the specified source buffer into the specified
 * destination.
 *
 * @param src    In/out parameter: Before invocation points to the source
 *               buffer where the value should be unmarshalled from and on
 *               return points to the byte in the source buffer immediately
 *               after the unmarshalled value.
 * @param srcLen In/our parameter: Before invocation is the length of the
 *               source buffer from where the value should be unmarshalled and
 *               on return is the number of bytes remaining in the source
 *               buffer.
 * @param dst    Out parameter: The destination.
 */
void unmarshalUint16(const char * &src, size_t &srcLen, uint16_t &dst);

/**
 * Unmarshals a value from the specified source buffer into the specified
 * destination.
 *
 * @param src    In/out parameter: Before invocation points to the source
 *               buffer where the value should be unmarshalled from and on
 *               return points to the byte in the source buffer immediately
 *               after the unmarshalled value.
 * @param srcLen In/our parameter: Before invocation is the length of the
 *               source buffer from where the value should be unmarshalled and
 *               on return is the number of bytes remaining in the source
 *               buffer.
 * @param dst    Out parameter: The destination.
 */
void unmarshalUint32(const char * &src, size_t &srcLen, uint32_t &dst);

/**
 * Unmarshals a value from the specified source buffer into the specified
 * destination.
 *
 * @param src    In/out parameter: Before invocation points to the source
 *               buffer where the value should be unmarshalled from and on
 *               return points to the byte in the source buffer immediately
 *               after the unmarshalled value.
 * @param srcLen In/our parameter: Before invocation is the length of the
 *               source buffer from where the value should be unmarshalled and
 *               on return is the number of bytes remaining in the source
 *               buffer.
 * @param dst    Out parameter: The destination.
 */
void unmarshalInt32(const char * &src, size_t &srcLen, int32_t &dst);

/**
 * Unmarshals a value from the specified source buffer into the specified
 * destination.
 *
 * @param src    In/out parameter: Before invocation points to the source
 *               buffer where the value should be unmarshalled from and on
 *               return points to the byte in the source buffer immediately
 *               after the unmarshalled value.
 * @param srcLen In/our parameter: Before invocation is the length of the
 *               source buffer from where the value should be unmarshalled and
 *               on return is the number of bytes remaining in the source
 *               buffer.
 * @param dst    Out parameter: The destination.
 */
void unmarshalUint64(const char * &src, size_t &srcLen, uint64_t &dst);

/**
 * Unmarshals a string from the specified source buffer into the specified
 * destination buffer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the string should be unmarshalled from and on return points
 * to the byte in the source buffer immediately after the unmarshalled
 * string.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the string should be unmarshalled and on return
 * is the number of bytes remaining in the source buffer
 * @param dst In/out parameter, before invocation points to the destination
 * buffer where the string should be unmarshalled to and on return points
 * to the byte in the destination buffer immediately after the unmarshalled
 * string .
 * @param dstLen The length of the destination buffer where the string
 * should be unmarshalled to.
 */
void unmarshalString(const char * &src, size_t &srcLen, char *dst,
  const size_t dstLen);

/**
 * Unmarshals a string from the specified source buffer into the specified
 * destination buffer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the string should be unmarshalled from and on return points
 * to the byte in the source buffer immediately after the unmarshalled
 * string.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the string should be unmarshalled and on return
 * is the number of bytes remaining in the source buffer.
 * @param dst In/out parameter, before invocation points to the destination
 * buffer where the string should be unmarshalled to and on return points
 * to the byte in the destination buffer immediately after the unmarshalled
 * string.
 */
template<int n> void unmarshalString(const char * &src,
  size_t &srcLen, char (&dst)[n])  {
  unmarshalString(src, srcLen, dst, n);
}

} // namespace cta::mediachanger
