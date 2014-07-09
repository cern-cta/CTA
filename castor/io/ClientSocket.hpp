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
 * Defines a dedicated socket that handles most of the network calls
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

// Include Files
#include <net.h>
#include <string>
#include <netinet/in.h>
#include "castor/exception/Exception.hpp"
#include "castor/io/AbstractTCPSocket.hpp"

namespace castor {

  // Forward declaration
  class IObject;

  namespace io {

    /**
     * A dedicated socket class, able to deal with socket manipulation
     * and to handle sending and receiving of IObjects
     */
    class ClientSocket : public AbstractTCPSocket {

    public:

      /**
       * Constructor building a Socket objet around a regular socket
       * @param socket the regular socket used
       */
      ClientSocket(int socket) throw();

      /**
       * Constructor building a socket on a given port of a given host
       * @param port the port on which the socket should be opened on
       * remote host
       * @param host the host to connect to, given by its name
       */
      ClientSocket(const unsigned short port,
             const std::string host)
        ;

      /**
       * Constructor building a socket on a given port of a given host
       * @param port the port on which the socket should be opened on
       * remote host
       * @param host the host to connect to, given as an ip address
       */
      ClientSocket(const unsigned short port,
             const unsigned long ip)
        ;

      /**
       * Connects the socket to the given address
       */
       virtual void connect()
	 ;

    };

  } // end of namespace io

} // end of namespace castor

