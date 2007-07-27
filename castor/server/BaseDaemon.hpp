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
 * @(#)$RCSfile: BaseDaemon.hpp,v $ $Revision: 1.8 $ $Release$ $Date: 2007/07/27 11:58:02 $ $Author: waldron $
 *
 * A base multithreaded daemon supporting signal handling
 * Credits to Jean-Damien Durand for the original C code
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
#include "castor/server/SignalThreadPool.hpp"
#include "castor/server/BaseServer.hpp"
#include "castor/server/Mutex.hpp"


namespace castor {

 namespace server {

  // Forward declaration
  class BaseServer;

  ///  handled signals - see the signal handler thread
  const int RESTART_GRACEFULLY = 1;
  const int STOP_GRACEFULLY = 2;
  const int STOP_NOW = 3;
  const int CHILD_STOPPED = 4;
	
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
	
    /**
     * Handles signals and performs graceful/immediate stop.
     * Called by start()
     */
    virtual void handleSignals();

  protected:

    /**
     * Initializes the server as daemon
     */
    virtual void init() throw (castor::exception::Exception);

    /**
     * wait all threads to terminate before exiting.
     * This implements a graceful kill if the signal is STOP_GRACEFULLY
     * otherwise it simply calls the shutdown method of all thread/
     * process pools.
     * @param signal The signal to handle
     */
    void waitAllThreads(const int signal) throw ();

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
