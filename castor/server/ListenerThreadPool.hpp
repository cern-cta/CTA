/******************************************************************************
 *                      ListenerThreadPool.hpp
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
 * Abstract class defining a listener thread pool
 *
 * @author castor dev team
 *****************************************************************************/

#pragma once

#include <iostream>
#include <string>
#include "castor/server/DynamicThreadPool.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/io/AbstractSocket.hpp"

namespace castor {

 namespace server {

  /**
   * Listener thread pool: allows processing upcoming
   * requests in dedicated threads.
   */
  class ListenerThreadPool : public DynamicThreadPool {

  /// This is the producer thread for this pool
  friend class ListenerProducerThread;

  public:

    /**
     * Constructor for a listener with a fixed number of threads.
     * @param poolName, thread as in BaseThreadPool
     * @param listenPort the port to be used by the listening socket.
     * @param waitIfBusy true to wait on dispatching to a worker thread
     * even if all threads are busy, as opposed to reject the connection
     * @param nbThreads number of threads in the pool
     */
    ListenerThreadPool(const std::string poolName, castor::server::IThread* thread,
                       unsigned int listenPort,
                       bool waitIfBusy,
                       unsigned int nbThreads = DEFAULT_THREAD_NUMBER)
      throw (castor::exception::Exception);

    /**
     * Constructor for a listener with a dynamic number of threads.
     * @param poolName, thread as in BaseThreadPool
     * @param listenPort the port to be used by the listening socket.
     * @param waitIfBusy true to wait on dispatching to a worker thread
     * even if all threads are busy, as opposed to reject the connection
     * @param initThreads, maxThreads, threshold, maxTasks as in DynamicThreadPool
     */
    ListenerThreadPool(const std::string poolName, castor::server::IThread* thread,
                       unsigned int listenPort,
                       bool waitIfBusy,
                       unsigned int initThreads,
                       unsigned int maxThreads,
                       unsigned int threshold = DEFAULT_THRESHOLD,
                       unsigned int maxTasks  = DEFAULT_MAXTASKS)      
      throw (castor::exception::Exception);
                       
    /**
     * Destructor
     */
    virtual ~ListenerThreadPool() throw();
    
    /**
     * Starts the pool and the listener loop to accept connections.
     */
    virtual void run() throw (castor::exception::Exception);

    /**
     * Shutdowns the pool by calling the ancestor's shutdown
     * and by closing the listening socket.
     * @return true if the pool has stopped.
     */
    virtual bool shutdown(bool wait = true) throw();
      
    /**
     * Sets the port on which this ThreadPool should listen
     */
    inline void setPort(unsigned int port)
      { m_port = port; }
	
  protected:
  
    /**
     * Binds a socket to the given port. Children classes must implement
     * this method according to the type of socket they need to use.
     * @throw castor::exception::Internal if the port is busy.
     */
    virtual void bind() throw (castor::exception::Exception) = 0;

    /**
     * The listening loop implementation for this Listener.
     * Children classes must override this method to provide different listener behaviors;
     * it is expected that this method implements a never-ending loop.
     */
    virtual void listenLoop() = 0;

    /**
     * Forks and assigns work to a thread from the pool.
     * @param param user parameter passed to thread->run().
     */
    virtual void threadAssign(void* param);
    
    /**
     * Terminates the work to be done when the thread pool is exhausted.
     * By default, this does nothing, but on a TCP-based pool it is adviced to
     * at least close the connection to the client.
     * @param param user parameter that would have been passed to a thread
     */
    virtual void terminate(void*) throw() {};

    /// The socket used to accept connections
    castor::io::AbstractSocket* m_sock;
    
    /// TCP port to listen for
    unsigned int m_port;

    /**
     * Flag to decide whether to wait for dispatching in case
     * no thread is idle as opposed to reject the incoming connection
     */
    bool m_waitIfBusy;
    
  };
  
  /// A simple producer helper class, which runs the listening loop
  class ListenerProducerThread : public IThread {

    /// Empty init
    virtual void init() {};

    /// Runs the listening loop of the pool passed as argument
    virtual void run(void* param);
    
    /// Empty stop
    virtual void stop() {};
  };
  
 } // end of namespace server

} // end of namespace castor



