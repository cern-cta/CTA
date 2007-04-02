/******************************************************************************
 *                   UDPSocket.hpp
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
 * @(#)$RCSfile: UDPSocket.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/04/02 15:26:01 $ $Author: sponcec3 $
 *
 * defines a dedicated socket that handles most of the network
 * calls
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_IO_ABSTRACTUDPSOCKET_HPP
#define CASTOR_IO_ABSTRACTUDPSOCKET_HPP 1

// Include Files
#include "castor/io/AbstractSocket.hpp"

namespace castor {

  namespace io {

    /**
     * UDP version of the abstract socket class, able
     * to handle sending and receiving of IObjects in UDP mode
     */
    class UDPSocket : public AbstractSocket {

    public:

      /**
       * Constructor building a socket on a given local port
       * @param port the local port for this socket.
       * @param reusable whether the socket should be reusable
       */
      UDPSocket(const unsigned short port,
                const bool reusable)
        throw (castor::exception::Exception);

      /**
       * Constructor building a socket on a given port
       * in order to connect to a given host
       * @param port the port for this socket.
       * @param port the host for this socket, given as machine name.
       */
      UDPSocket(const unsigned short port,
		const std::string host)
        throw (castor::exception::Exception);

      /**
       * Constructor building a socket on a given port
       * in order to connect to a given host
       * @param port the port for this socket.
       * @param port the host for this socket, given as ip address.
       */
      UDPSocket(const unsigned short port,
		const unsigned long ip)
        throw (castor::exception::Exception);

    protected:

      /**
       * internal method to create the inner socket
       */
      virtual void createSocket() throw (castor::exception::Exception);

      /**
       * Internal method to send the content of a buffer
       * over the socket.
       * @param magic the magic number to be used as first
       * 4 bytes of the data sent
       * @param buf the data to send
       * @param n the length of buf
       */
      virtual void sendBuffer(const unsigned int magic,
                              const char* buf,
                              const int n)
        throw (castor::exception::Exception);

      /**
       * Internal method to read from a socket into a buffer.
       * @param magic the magic number expected as the first 4 bytes read.
       * An exception is sent if the correct magic number is not found
       * @param buf allocated by the method, it contains the data read.
       * Note that the deallocation of this buffer is the responsability
       * of the caller
       * @param n the length of the allocated buffer
       */
      virtual void readBuffer(const unsigned int magic,
                              char** buf,
                              int& n)
        throw (castor::exception::Exception);

    };

  } // end of namespace io

} // end of namespace castor

#endif // CASTOR_IO_ABSTRACTUDPSOCKET_HPP
