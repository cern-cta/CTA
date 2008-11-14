/******************************************************************************
 *                      SocketHelper.hpp
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
 * @author castor dev team
 *****************************************************************************/
 
#ifndef CASTOR_TAPE_AGGREGATOR_SOCKETHELPER_HPP
#define CASTOR_TAPE_AGGREGATOR_SOCKETHELPER_HPP 1

#include "castor/exception/PermissionDenied.hpp"

#include <iostream>
#include <stdint.h>


namespace castor {

  namespace io {
    // Forward declaration
    class ServerSocket;
  }

namespace tape {
namespace aggregator {


  /**
   * Provides helper functions to work with sockets.
   */
  class SocketHelper {

  public:

    /**
     * Prints the string form of specified IP using the specified output
     * stream.
     *
     * @param os the output stream.
     * @param ip the IP address in host byte order.
     */
    static void printIp(std::ostream &os, const unsigned long ip) throw();

    /**
     * Prints a textual description of the specified socket to the specified
     * output stream.
     *
     * @param os the output stream to which the string is to be printed.
     * @param the socket whose textual description is to be printed to the
     * stream.
     */
    static void printSocketDescription(std::ostream &os,
      castor::io::ServerSocket &socket) throw();

    /**
     * Reads an unsigned 32-bit integer from the specified socket.
     *
     * @param the socket to be read from.
     * @return the unsigned 32-bit integer.
     */
    static uint32_t readUint32(castor::io::ServerSocket &socket)
      throw (castor::exception::Exception);

  }; // class SocketHelper

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_SOCKETHELPER_HPP
