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
 * @(#)$RCSfile: ListenerThreadPool.hpp,v $ $Revision: 1.12 $ $Release$ $Date: 2008/02/01 11:20:27 $ $Author: itglp $
 *
 * Abstract class defining a listener thread pool
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef CASTOR_SERVER_LISTENERTHREADPOOL_HPP
#define CASTOR_SERVER_LISTENERTHREADPOOL_HPP 1

#include <iostream>
#include <string>
#include "castor/server/BaseThreadPool.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/io/AbstractSocket.hpp"

namespace castor {

 namespace server {

  /**
   * Listener thread pool: allows processing upcoming
   * requests in dedicated threads.
   */
  class ListenerThreadPool : public BaseThreadPool {

  public:

    /**
     * empty constructor
     */
    ListenerThreadPool() throw() : 
      BaseThreadPool() {};

    /**
     * constructor
     * @param poolName, thread as in BaseThreadPool
     * @param listenPort the port to which to attach the listening socket.
     * @param listenereOnOwnThread if false the listener loop is run directly. See run().
     */
    ListenerThreadPool(const std::string poolName, castor::server::IThread* thread,
                       int listenPort, bool listenerOnOwnThread = true) throw();

    /**
     * destructor
     */
    virtual ~ListenerThreadPool() throw() {};
	
    /**
     * Pre-daemonization initialization. Empty for this pool.
     */
    virtual void init() throw (castor::exception::Exception) {};

    /**
     * Starts the listener loop to accept connections.
     * If m_spawnListener (default), the loop is started on a separate thread,
     * otherwise it is run directly: in the latter case the method does NOT return,
     * thus it does NOT obey the contract of BaseThreadPool; however, for simple
     * listening servers like the RH, this results in less overhead.
     */
    virtual void run() throw (castor::exception::Exception);
    
    /**
     * Shutdowns the pool by closing the underlying server socket.
     * @return true.
     */
    virtual bool shutdown() throw();

    /**
     * Sets the port on which this ThreadPool should listen
     */
    inline void setPort(int port) { m_port = port; }
	
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

    /// The socket used to accept connections
    castor::io::AbstractSocket* m_sock;
    
    /// TCP port to listen for
    int m_port;
    
    /// flag to decide whether the listener loop has to run in a separate thread
    bool m_spawnListener;
    
  private:

    /// Thread entrypoint made friend to access private fields.
    friend void* _listener_run(void* param);
  };


  // Entrypoint for the listener loop
  void* _listener_run(void* param);

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_LISTENERTHREADPOOL_HPP

