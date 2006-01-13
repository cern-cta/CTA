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
 * @(#)$RCSfile: ListenerThreadPool.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2006/01/13 17:21:36 $ $Author: itglp $
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
     */
    ListenerThreadPool(const std::string poolName,
                   castor::server::IThread* thread, int listenPort) throw();

    /*
     * destructor
     */
    virtual ~ListenerThreadPool() throw();

    /**
     * User function to run the pool.
     * Default implementation forks a single thread by
     * calling threadAssign(0).
     */
    virtual void run();

  protected:
  
    /**
     * Forks and assigns work to a thread from the pool.
     * @param param user parameter passed to thread->init().
     */
    virtual int threadAssign(void *param);

    /// TCP port to listen for
    int m_port;

  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_LISTENERTHREADPOOL_HPP
