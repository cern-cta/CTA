/******************************************************************************
 *                   TCPListenerThreadPool.hpp
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
 * @(#)$RCSfile: TCPListenerThreadPool.hpp,v $ $Revision: 1.4 $ $Release$ $Date: 2009/08/10 15:27:31 $ $Author: itglp $
 *
 * Listener thread pool based on TCP
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef CASTOR_SERVER_TCPLISTENERTHREADPOOL_HPP
#define CASTOR_SERVER_TCPLISTENERTHREADPOOL_HPP 1

#include <iostream>
#include <string>
#include "castor/server/ListenerThreadPool.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  // Forward declaration
  namespace io {
    class ServerSocket;
  }

  namespace server {

    /**
     * TCP Listener thread pool: handles a (TCP) ServerSocket and allows
     * processing upcoming requests in dedicated threads.
     */
    class TCPListenerThreadPool : public ListenerThreadPool {

    public:

      /**
       * Constructor for a TCP listener with a fixed number of threads.
       * @param poolName, thread as in BaseThreadPool
       * @param listenPort the port to be used by the listening socket.
       * @param waitIfBusy true to wait on dispatching to a worker thread
       * even if all threads are busy, as opposed to reject the connection.
       * True by default for this TCP-based listener (note there's no
       * default for the generic ListenerThreadPool).
       * @param nbThreads as in DynamicThreadPool
       */
      TCPListenerThreadPool(const std::string poolName, castor::server::IThread* thread,
                            unsigned int listenPort,
                            bool waitIfBusy = true,
                            unsigned int nbThreads = DEFAULT_THREAD_NUMBER)
        throw (castor::exception::Exception);
  
      /**
       * Constructor for a TCP listener with a dynamic number of threads.
       * @param poolName, thread as in BaseThreadPool
       * @param listenPort the port to be used by the listening socket.
       * @param waitIfBusy true to wait on dispatching to a worker thread
       * even if all threads are busy, as opposed to reject the connection.
       * @param initThreads, maxThreads, threshold, maxTasks as in DynamicThreadPool
       */
      TCPListenerThreadPool(const std::string poolName, castor::server::IThread* thread,
                            unsigned int listenPort,
                            bool waitIfBusy,
                            unsigned int initThreads, unsigned int maxThreads,
                            unsigned int threshold = DEFAULT_THRESHOLD,
                            unsigned int maxTasks  = DEFAULT_MAXTASKS)      
        throw (castor::exception::Exception);
                       
      /**
       * Destructor
       */
      virtual ~TCPListenerThreadPool() throw();
  
    protected:

      /**
       * Binds a standard ServerSocket to the given port.
       * @throw castor::exception::Internal if the port is busy.
       */
      virtual void bind() throw (castor::exception::Exception);

      /**
       * The listening loop implementation for this Listener, based on standard ServerSocket.
       * Child classes must override this method to provide different listener behaviors;
       * it is expected that this method implements a never-ending loop.
       */
      virtual void listenLoop();

      /**
       * Terminates the work to be done when the thread pool is exhausted,
       * by simply closing the connection to the client.
       * @param param user parameter that would have been passed to a thread
       */
      virtual void terminate(void* param) throw();
      
    };

  } // end of namespace server

} // end of namespace castor

#endif // CASTOR_SERVER_TCPLISTENERTHREADPOOL_HPP

