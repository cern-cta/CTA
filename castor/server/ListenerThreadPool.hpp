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
 * @(#)$RCSfile: ListenerThreadPool.hpp,v $ $Revision: 1.7 $ $Release$ $Date: 2006/11/27 16:54:51 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_LISTENERTHREADPOOL_HPP
#define CASTOR_SERVER_LISTENERTHREADPOOL_HPP 1

#include <iostream>
#include <string>
#include "castor/server/BaseThreadPool.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/io/ServerSocket.hpp"

namespace castor {

 namespace server {

  /**
   * Listener thread pool: handles a ServerSocket and allows
   * processing upcoming requests in dedicated threads.
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
     * @param listenPort the TCP port to which to attach the ServerSocket.
     * @param listenereOnOwnThread if false the listener loop is run directly. See run().
     */
    ListenerThreadPool(const std::string poolName, castor::server::IThread* thread,
       int listenPort, bool listenerOnOwnThread = true) throw();

    /*
     * destructor
     */
     virtual ~ListenerThreadPool() throw() {};
	
    /**
     * Binds a standard ServerSocket to the given port.
     * @throw castor::exception::Internal if the port is busy.
     */
    virtual void init() throw (castor::exception::Exception);

    /**
     * Starts the listener loop to accept connections.
     * If m_spawnListener (default), the loop is started on a separate thread,
     * otherwise it is run directly: in the latter case the method does NOT return,
     * thus it does NOT obey the contract of BaseThreadPool; however, for simple
     * listening servers like the RH, this results in less overhead.
     */
    virtual void run();
	
  protected:
  
    /**
     * The listening loop implementation for this Listener, based on standard ServerSocket.
     * Child classes must override this method to provide different listener behaviors;
     * it is expected that this method implements a never-ending loop.
     */
    virtual void listenLoop();

    /**
     * Forks and assigns work to a thread from the pool.
     * @param param user parameter passed to thread->run().
     */
    virtual int threadAssign(void* param);
    
    /// TCP port to listen for
    int m_port;
    
    /// flag to decide whether the listener loop has to run in a separate thread
    bool m_spawnListener;
	
    /// The server socket used to accept connections
  	castor::io::ServerSocket* m_sock;

  private:

    /// Thread entrypoint made friend to access private fields.
    friend void* _listener_run(void* param);
  };


  // Entrypoint for the listener loop
  void* _listener_run(void* param);

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_LISTENERTHREADPOOL_HPP

