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
 
#ifndef CASTOR_VDQM_SOCKETHELPER_HPP
#define CASTOR_VDQM_SOCKETHELPER_HPP 1

#include "castor/exception/PermissionDenied.hpp"

#include <iostream>


namespace castor {

  namespace io {
    // Forward declaration
    class ServerSocket;
  }

  namespace vdqm {

    /**
     * Provides VDQM specific helper functions to work with sockets used by the
     * VDQM.
     */
    class SocketHelper {

    public:

      /**
       * Reads the first four bytes of the header. This function was added to 
       * support also the older VDQM protocol. The magic number defines, which
       * protocol is used.
       * 
       * @param socket the socket to be read from.
       * @param timeout the timeout to be used in seconds.
       * @return the magic number.
       */
      static unsigned int readMagicNumber(
        castor::io::ServerSocket &socket,
        const int timeout)
        throw (castor::exception::Exception);

      /**
       * Writes the contents of the specified header buffer to the specified
       * socket.
       * 
       * @param socket the socket to be written to.
       * @param hdrbuf The header buffer, which contains the data for the client
       * @exception In case of error
       */
      static void netWriteVdqmHeader(castor::io::ServerSocket &socket,
        void *hdrbuf) throw (castor::exception::Exception);
          
      /**
       * Reads the VDQM header from the specified socket into the specified
       * buffer.
       * 
       * @param socket the socket to be read from.
       * @param hdrbuf the header buffer where the data will be written to
       */
      static void netReadVdqmHeader(castor::io::ServerSocket &socket,
        void *hdrbuf) throw (castor::exception::Exception);       

      /**
       * Throws a permission denied exception if the specified action is
       * not authorised.
       *
       * @param sock The socket used to receive the message.  This socket
       * should only be used for CUPV purposes as all message data has
       * already been read out.
       * @param uid The uid of the client.
       * @param gid The gid of the client.
       * @param privilege The required privilege.
       * @param privilegeName The name string of the required privilege
       * @param messageType Then type string of the message.
       */
      static void checkCupvPermissions(castor::io::ServerSocket &sock,
        const uid_t uid, const gid_t gid, const int privilege,
        const char *privilegeName, const char *messageType)
        throw (castor::exception::PermissionDenied);

    }; // class SocketHelper

  } // namespace vdqm

} // namespace castor

#endif // CASTOR_VDQM_SOCKETHELPER_HPP
