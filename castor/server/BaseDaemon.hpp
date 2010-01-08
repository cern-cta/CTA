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
 * @(#)$RCSfile: BaseDaemon.hpp,v $ $Revision: 1.16 $ $Release$ $Date: 2009/08/04 08:15:00 $ $Author: murrayc3 $
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
#include "castor/server/BaseServer.hpp"
#include "castor/server/Mutex.hpp"
#include "castor/server/NotifierThread.hpp"


namespace castor {

 namespace server {

  // Forward declaration
  class BaseServer;
  class NotifierThread;

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
     * Constructor
     * @param serverName as in BaseServer
     */
    BaseDaemon(const std::string serverName);

    /**
     * Destructor
     */
    virtual ~BaseDaemon() throw() {};

    /**
     * Adds a dedicated UDP thread pool for getting wakeup notifications
     * from other Castor daemons. Those notifications are supposed to be
     * sent using the BaseServer::sendNotification method.
     * @param port the UDP port where to listen
     */
    void addNotifierThreadPool(int port);
    
    /**
     * Parses a command line to set the server options
     */
    virtual void parseCommandLine(int argc, char *argv[]);
      
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
     * Prints out the online help
     */
    virtual void help(std::string programName);

    /**
     * Sends a shutdown message to all thread pools, then
     * waits for all threads to terminate before returning.
     * This implements a graceful kill and is triggered by SIGTERM.
     */
    virtual void waitAllThreads() throw ();

  private:

    /// set of caught signals
    sigset_t m_signalSet;

    /// a mutex for the signal handler thread
    Mutex* m_signalMutex;

    /// entrypoint for the signal handler thread
    static void* _signalHandler(void* arg);
	
  };

 } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_BASEDAEMON_HPP
