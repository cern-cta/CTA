/******************************************************************************
 *                  UDPListenerThreadPool.hpp
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
 * @(#)$RCSfile: UDPListenerThreadPool.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/01/16 15:46:51 $ $Author: sponcec3 $
 *
 * Listener thread pool based on UDP
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef CASTOR_SERVER_UDPLISTENERTHREADPOOL_HPP
#define CASTOR_SERVER_UDPLISTENERTHREADPOOL_HPP 1

#include <iostream>
#include <string>
#include "castor/server/ListenerThreadPool.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  // Forward declaration
  namespace io {
    class UDPSocket;
  }

  namespace server {

    /**
     * UDP Listener thread pool: handles a UDPSocket and allows
     * processing upcoming requests in dedicated threads.
     */
    class UDPListenerThreadPool : public ListenerThreadPool {

    public:

      /**
       * empty constructor
       */
      UDPListenerThreadPool() throw() : ListenerThreadPool() {};

      /**
       * constructor
       * @param poolName, thread as in BaseThreadPool
       * @param listenPort the TCP port to which to attach the ServerSocket.
       * @param listenereOnOwnThread if false the listener loop is run directly. See run().
       */
      UDPListenerThreadPool(const std::string poolName, castor::server::IThread* thread,
                            int listenPort, bool listenerOnOwnThread = true) throw();

      /**
       * Binds a standard UDPSocket to the given port.
       * @throw castor::exception::Internal if the port is busy.
       */
      virtual void init() throw (castor::exception::Exception);

    protected:

      /**
       * The listening loop implementation for this Listener, based on standard UDPSocket.
       * Child classes must override this method to provide different listener behaviors;
       * it is expected that this method implements a never-ending loop.
       */
      virtual void listenLoop();

      /// The server socket used to accept connections
      castor::io::UDPSocket* m_sock;

    };

  } // end of namespace server

} // end of namespace castor

#endif // CASTOR_SERVER_UDPLISTENERTHREADPOOL_HPP

