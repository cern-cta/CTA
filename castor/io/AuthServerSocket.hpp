/******************************************************************************
 *                      AuthServerSocket.hpp
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
 * @(#)$RCSfile: AuthServerSocket.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/07/19 15:13:59 $ $Author: bcouturi $
 *
 * defines a dedicated socket that handles most of the network
 * calls
 *
 * @author Benjamin Couturier
 *****************************************************************************/

#ifndef CASTOR_AUTH_SERVER_SOCKET_HPP
#define CASTOR_AUTH_SERVER_SOCKET_HPP 1

// Include Files
#include <net.h>
#include <string>
#include <netinet/in.h>
#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"
#include "ServerSocket.hpp"

extern "C" {
  #include "Csec_api.h"
}


namespace castor {

  // Forward declaration
  class IObject;

  namespace io {

    /**
     * A dedicated socket class, able to deal with socket manipulation
     * and to handle sending and receiving of IObjects
     */
    class AuthServerSocket : public ServerSocket {

    public:

      /**
       * Constructor building a Socket objet around a regular socket
       * @param socket the regular socket used
       */
      AuthServerSocket(int socket) throw ();

      /**
       * Constructor building a socket on a given local port
       * @param port the local port for this socket. Use 0 if
       * you want the system to allocate a port
       * @param doListen whether to start listening on the socket.
       */
      AuthServerSocket(const unsigned short port,
		       const bool reusable,
		       int service_type = 0)
        throw (castor::exception::Exception);

      /**
       * Constructor building a socket on a given port of a given host
       * @param port the port on which the socket should be opened on
       * remote host
       * @param host the host to connect to, given by its name
       */
      AuthServerSocket(const unsigned short port,
		       const std::string host,
		       const bool reusable,
		       int service_type = 0)
        throw (castor::exception::Exception);

      /**
       * Constructor building a socket on a given port of a given host
       * @param port the port on which the socket should be opened on
       * remote host
       * @param host the host to connect to, given as an ip address
       */
      AuthServerSocket(const unsigned short port,
		       const unsigned long ip,
		       const bool reusable,
		       int service_type = 0)
        throw (castor::exception::Exception);

      /**
       * destructor
       */
      ~AuthServerSocket() throw();

      /**
       * accept a connection and return the correponding Socket.
       * The deallocation of the new socket is the responsability
       * of the caller.
       */
      virtual ServerSocket* accept() throw(castor::exception::Exception);


    protected:
      Csec_context_t m_security_context;
    };

  } // end of namespace io

} // end of namespace castor

#endif // CASTOR_AUTH_SERVER_SOCKET_HPP
