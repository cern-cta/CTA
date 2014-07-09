/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

// Include Files
#include <net.h>
#include <string>
#include <netinet/in.h>
#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"
#include "AbstractTCPSocket.hpp"

namespace castor {

  // Forward declaration
  class IObject;

  namespace io {

    /**
     * A dedicated socket class, able to deal with socket manipulation
     * and to handle sending and receiving of IObjects
     */
    class ServerSocket : public AbstractTCPSocket {

    public:

      /**
       * Constructor building a Socket objet around a regular socket
       * @param socket the regular socket used
       */
      ServerSocket(int socket) throw();

      /**
       * Constructor building a socket with no port. As a consequence,
       * the used port will be 0 and the socket will not be bound.
       * The bind method should be call independently
       * @param reusable whether the socket should be reusable
       */
      ServerSocket(const bool reusable) ;

      /**
       * Constructor building a socket on a given local port
       * @param port the local port for this socket. Use 0 if
       * you want the system to allocate a port
       * @param reusable whether the socket should be reusable
       */
      ServerSocket(const unsigned short port,
                   const bool reusable)
        ;

      /**
       * Constructor building a socket on a given port of a given host
       * @param port the port on which the socket should be opened on
       * remote host
       * @param host the host to connect to, given by its name
       * @param reusable whether the socket should be reusable
       */
      ServerSocket(const unsigned short port,
                   const std::string host,
                   const bool reusable)
        ;

      /**
       * Constructor building a socket on a given port of a given host
       * @param port the port on which the socket should be opened on
       * remote host
       * @param host the host to connect to, given as an ip address
       * @param reusable whether the socket should be reusable
       */
      ServerSocket(const unsigned short port,
                   const unsigned long ip,
                   const bool reusable)
        ;

      /**
       * Start listening on the socket.
       * In case the socket has been bound by another process (it may
       * happen because bind() is not process/thread-safe), a new bind()
       * is performed, so to make this class process-safe.
       */
      virtual void listen() ;

      /**
       * accept a connection and return the correponding Socket.
       * The deallocation of the new socket is the responsability
       * of the caller.
       */
      virtual ServerSocket* accept() ;

      /**
       * binds the socket and let the system choose a port
       * in a given range. Giving 0 for both ends of the
       * range allows to system to freely choose the port
       * @param lowPort the low value in the port range (included)
       * @param highPort the high value in the port range (included)
       */
      void bind(int lowPort, int highPort)
        ;

    private:

      /**
       * Tells whether listen was already called
       */
      bool m_listening;
      
      /**
       * Low and high value in the port range
       */
      int m_lowPort, m_highPort;

      /**
       * binds the socket and let the system choose a port
       * in a range. The range must already have been initialized
       * by a previous call to bind(lowPort, highPort)
       */
      void bind()
        ;
        
    };

  } // end of namespace io

} // end of namespace castor

