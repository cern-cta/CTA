/******************************************************************************
 *                      Net.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_NET_HPP
#define CASTOR_TAPE_AGGREGATOR_NET_HPP 1

#include "castor/exception/Exception.hpp"

#include <errno.h>
#include <string.h>
#include <iostream>

namespace castor     {
namespace tape       {
namespace aggregator {
  	
/**
 * Class of static network utility functions.
 */
class Net {
public:

  /**
   * Creates a listener socket including, creation, binding and marking as a
   * listener socket.
   *
   * @param port The port number to listen on or 0 if one should be allocated.
   * @return The socket file descriptor.
   */
  static int createListenerSocket(const unsigned short port)
    throw(castor::exception::Exception);

  /**
   * Accepts a connection on the specified listener socket and returns the
   * socket file descriptor of the newly created connected soket.
   *
   * @param listenSocketFd The file descriptor of the listener socket.
   * @param netReadWriteTimeout The timeout to be used when performing
   * network reads and writes.  Accepting a connection is classed a network
   * read operation in the case of this function.
   */
  static int acceptConnection(const int listenSocketFd,
    const int netReadWriteTimeout) throw(castor::exception::Exception);

  /**
   * Gets the IP and port number of the specified socket.
   *
   * @param socketFd The socket file descriptor.
   * @param ip The IP to be filled.
   * @param port The port to be filled.
   */
  static void getSocketIpAndPort(const int socketFd, unsigned long& ip,
    unsigned short& port) throw(castor::exception::Exception);

  /**
   * Gets the peer IP and port number of the specified socket.
   *
   * @param socketFd The socket file descriptor.
   * @param ip The IP to be filled.
   * @param port The port to be filled.
   */
  static void getPeerIpAndPort(const int socketFd, unsigned long& ip,
    unsigned short& port) throw(castor::exception::Exception);

  /**
   * Gets the host name of the specified socket.
   *
   * @param socketFd The socket file descriptor.
   * @param buf The buffer into which the hostname should written to.
   * @param len The length of the buffer into which the host name should be
   * written to.
   */
  static void getSocketHostName(const int socketFd, char *buf, size_t len)
    throw(castor::exception::Exception);

  /**
   * Gets the host name of the specified socket.
   *
   * @param socketFd The socket file descriptor.
   * @param buf The buffer into which the hostname should written to.
   */
  template<int n> static void getSocketHostName(const int socketFd,
    char (&buf)[n]) throw(castor::exception::Exception) {
    getSocketHostName(socketFd, buf, n);
  }

  /**
   * Gets the peer host name of the specified connection.
   *
   * @param socketFd The socket file descriptor of the connection.
   * @param buf The buffer into which the hostname should written to.
   * @param len The length of the buffer into which the host name should be
   * written to.
   */
  static void getPeerHostName(const int socketFd, char *buf, size_t len)
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
   * Prints the string form of specified IP using the specified output
   * stream.
   *
   * @param os The output stream.
   * @param ip The IP address in host byte order.
   */
  static void printIp(std::ostream &os, const unsigned long ip) throw();

  /**
   * Prints a textual description of the specified socket to the specified
   * output stream.
   *
   * @param os The output stream to which the string is to be printed.
   * @param socketFd The file descriptor of the socket whose textual
   * description is to be printed to the stream.
   */
  static void printSocketDescription(std::ostream &os, const int socketFd)
    throw();

  /**
   * Reads the specified number of bytes from the specified socket and writes
   * the result into the specified buffer.
   *
   * @param socketFd The file descriptor of the socket to be read from.
   * @param netReadWriteTimeout The timeout to be used when performing
   * network reads and writes.
   * @param nbBytes The number of bytes to be read.
   * @param buf The buffer into which the bytes will be written.
   */
  static void readBytes(const int socketFd, const int netReadWriteTimeout,
    const int nbBytes, char *buf) throw(castor::exception::Exception);

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
  static void writeBytes(const int socketFd, const int netReadWriteTimeout,
    const int nbBytes, char *const buf) throw(castor::exception::Exception);

private:

  /**
   * Private constructor to inhibit instances of this class from being
   * instantiated.
   */
  Net() {}

}; // class Net

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_NET_HPP
