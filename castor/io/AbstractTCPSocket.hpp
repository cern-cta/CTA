/******************************************************************************
 *                   AbstractTCPSocket.hpp
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
 * defines a dedicated socket that handles most of the network
 * calls
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#pragma once

// Include Files
#include "castor/io/AbstractSocket.hpp"

namespace castor {

  namespace io {

    /**
     * TCP version of the abstract socket class, able
     * to handle sending and receiving of IObjects in TCP mode
     */
    class AbstractTCPSocket : public AbstractSocket {

    public:

      /**
       * Constructor building a Socket objet around a regular socket
       * @param socket the regular socket used
       */
      AbstractTCPSocket(int socket) throw();

      /**
       * Constructor building a socket with no port. As a consequence,
       * the used port will be 0 and the socket will not be bound.
       * The bind method should be call independently
       * @param reusable whether the socket should be reusable
       * @exception Exception in case of error
       */
      AbstractTCPSocket(const bool reusable)
	;

      /**
       * Constructor building a socket on a given local port
       * @param port the local port for this socket. Use 0 if
       * you want the system to allocate a port
       * @param reusable whether the socket should be reusable
       * @exception Exception in case of error
       */
      AbstractTCPSocket(const unsigned short port,
                        const bool reusable)
        ;

      /**
       * Constructor building a socket on a given port of a given host
       * @param port the port on which the socket should be opened on
       * remote host
       * @param host the host to connect to, given by its name
       * @param reusable whether the socket should be reusable
       * @exception Exception in case of error
       */
      AbstractTCPSocket(const unsigned short port,
                        const std::string host,
                        const bool reusable)
        ;

      /**
       * Constructor building a socket on a given port of a given host
       * @param port the port on which the socket should be opened on
       * remote host
       * @param ip the host to connect to, given as an ip address
       * @param reusable whether the socket should be reusable
       * @exception Exception in case of error
       */
      AbstractTCPSocket(const unsigned short port,
                        const unsigned long ip,
                        const bool reusable)
        ;

    protected:

      /**
       * internal method to create the inner socket
       */
      virtual void createSocket() ;

      /**
       * Internal method to send the content of a buffer
       * over the socket.
       * @param magic the magic number to be used as first
       * 4 bytes of the data sent
       * @param buf the data to send
       * @param n the length of buf
       * @exception Exception in case of error
       */
      virtual void sendBuffer(const unsigned int magic,
                              const char* buf,
                              const int n)
        ;

      /**
       * Internal method to read from a socket into a buffer.
       * @param magic the magic number expected as the first 4 bytes read.
       * An exception is sent if the correct magic number is not found
       * @param buf allocated by the method, it contains the data read.
       * Note that the deallocation of this buffer is the responsability
       * of the caller
       * @param n the length of the allocated buffer
       * @exception Exception in case of error
       */
      virtual void readBuffer(const unsigned int magic,
                              char** buf,
                              int& n)
        ;

    protected:

      /// The maximum amount of data in bytes that can be read from the
      /// socket in the readBuffer method
      int m_maxNetDataSize;

    };

  } // end of namespace io

} // end of namespace castor

