/******************************************************************************
 *                      BaseDaemon.hpp
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
 * @(#)$RCSfile: BaseDaemon.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2006/02/20 14:39:14 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_BASEDAEMON_HPP
#define CASTOR_SERVER_BASEDAEMON_HPP 1

#include <iostream>
#include <string>
#include <map>
#include "castor/exception/Exception.hpp"
#include "castor/BaseObject.hpp"
#include "castor/server/BaseThreadPool.hpp"
#include "castor/server/BaseServer.hpp"
#include "castor/server/Mutex.hpp"


namespace castor {

 namespace server {

  // Forward declaration
  class BaseServer;

  /**
   * Basic CASTOR multithreaded daemon.
   */
  class BaseDaemon : public castor::server::BaseServer {

  public:

    /**
     * constructor
     */
    BaseDaemon(const std::string serverName);

    /*
     * destructor
     */
    virtual ~BaseDaemon() throw() {};

    /**
     * Starts the thread pools in detached mode.
     * Then starts also the signal thread (one per daemon) and waits
     * for incoming signals forever.
     */
    virtual void start() throw (castor::exception::Exception);

  protected:

    /**
     * Initializes the server as daemon
     */
    virtual void init() throw (castor::exception::Exception);

    /**
     * wait all threads to terminate before exiting.
     * This implements a graceful kill and is triggered by kill -2.
     * XXX To be implemented later.
     */
    void waitAllThreads() throw ();

  private:

    /// set of catched signal
    sigset_t m_signalSet;

    /// a mutex for the signal handler thread
    Mutex* m_signalMutex;

    /// signal handler; friend function to access private fields.
    friend void* _signalThread_run(void* arg);

  };

  // entrypoint for the signal handler thread
  void* _signalThread_run(void* arg);

 } // end of namespace server

} // end of namespace castor



#endif // CASTOR_SERVER_BASEDAEMON_HPP
