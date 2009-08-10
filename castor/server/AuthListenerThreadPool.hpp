/******************************************************************************
 *                      AuthListenerThreadPool.hpp
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
 * @(#)$RCSfile: AuthListenerThreadPool.hpp,v $ $Revision: 1.5 $ $Release$ $Date: 2009/08/10 15:27:31 $ $Author: itglp $
 *
 * A ListenerThreadPool which uses AuthSockets to handle the connections
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_AUTHLISTENERTHREADPOOL_HPP
#define CASTOR_SERVER_AUTHLISTENERTHREADPOOL_HPP 1

#include <iostream>
#include <string>
#include "castor/server/TCPListenerThreadPool.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/io/AuthServerSocket.hpp"

namespace castor {

 namespace server {

  /**
   * A ListenerThreadPool able to handle secure connections
   * as implemented in AuthServerSocket, by means of the Csec API
   */
  class AuthListenerThreadPool : public TCPListenerThreadPool {

  public:

    /**
     * Empty constructor
     */
    AuthListenerThreadPool() throw() : 
       TCPListenerThreadPool() {};

    /**
     * Inherited constructor, see TCPListenerThreadPool
     */
    AuthListenerThreadPool(const std::string poolName, castor::server::IThread* thread,
                           unsigned int listenPort, unsigned int nbThreads = DEFAULT_THREAD_NUMBER)
      throw (castor::exception::Exception);

    /**
     * Inherited constructor, see TCPListenerThreadPool
     */
    AuthListenerThreadPool(const std::string poolName, castor::server::IThread* thread,
                           unsigned int listenPort,
                           unsigned int initThreads, unsigned int maxThreads,
                           unsigned int threshold = DEFAULT_THRESHOLD,
                           unsigned int maxTasks  = DEFAULT_MAXTASKS)      
      throw (castor::exception::Exception);               

    /**
     * Destructor
     */
    virtual ~AuthListenerThreadPool() throw();

  protected:

    /**
     * Performs the bind with an AuthServerSocket here
     */
    virtual void bind() throw (castor::exception::Exception);

  };

 } // end of namespace server

} // end of namespace castor

#endif // CASTOR_SERVER_AUTHLISTENERTHREADPOOL_HPP
