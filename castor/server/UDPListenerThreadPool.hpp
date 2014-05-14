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
 *
 * Listener thread pool based on UDP
 *
 * @author castor dev team
 *****************************************************************************/

#pragma once

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
       * Constructor
       * @param poolName, thread as in BaseThreadPool
       * @param listenPort the TCP port to which to attach the ServerSocket.
       * @param waitIfBusy true to wait on dispatching to a worker thread
       * even if all threads are busy, as opposed to reject the connection.
       * False by default for this UDP-based listener (note there's no
       * default for the generic ListenerThreadPool).
       * @param nbThreads as in ListenerThreadPool. This listener is not dynamic.
       */
      UDPListenerThreadPool(const std::string poolName, castor::server::IThread* thread,
                            const int listenPort, bool waitIfBusy = false,
                            unsigned nbThreads = castor::server::DEFAULT_THREAD_NUMBER) throw();

    protected:

      /**
       * Binds a standard UDPSocket to the given port.
       * @throw castor::exception::Exception if the port is busy.
       */
      virtual void bind() throw (castor::exception::Exception);

      /**
       * The listening loop implementation for this Listener, based on standard UDPSocket.
       * Child classes must override this method to provide different listener behaviors;
       * it is expected that this method implements a never-ending loop.
       */
      virtual void listenLoop();

      /**
       * Terminates the work to be done when the thread pool is exhausted,
       * by simply freeing the received object.
       * @param param user parameter that would have been passed to a thread
       */
      virtual void terminate(void* param) throw();

    };

  } // end of namespace server

} // end of namespace castor


