/*******************************************************************************
 *                      castor/server/MultiThreadedDaemon.hpp
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
 * @author castor dev team
 ******************************************************************************/

#ifndef CASTOR_SERVER_MULTITHREADEDDAEMON_HPP
#define CASTOR_SERVER_MULTITHREADEDDAEMON_HPP 1

#include "castor/server/BaseThreadPool.hpp"
#include "castor/server/Daemon.hpp"
#include "castor/server/Mutex.hpp"

#include <sstream>

namespace castor {
namespace server {

/**
 * This concrete class represents a multi-threaded daemon.
 */
class MultiThreadedDaemon: public Daemon {

public:

  /**
   * Constants for handling signals - see the signal handler thread.
   */
  static const int RESTART_GRACEFULLY = 1;
  static const int STOP_GRACEFULLY = 2;
  static const int STOP_NOW = 3;
  static const int CHILD_STOPPED = 4;

  /**
   * Constructor
   *
   * @param logger Object representing the API of the CASTOR logging system.
   */
  MultiThreadedDaemon(log::Logger &logger);

  /**
   * Destructor.
   */
  virtual ~MultiThreadedDaemon() throw();

  /**
   * Gets a pool by its name initial.
   * @param nameIn the name initial
   * @throw castor::exception::Exception in case it was not found
   */
  BaseThreadPool* getThreadPool(const char nameIn)
    throw (castor::exception::Exception);

  /**
   * Calls resetMetrics on all registered thread pools
   */
  void resetAllMetrics() throw();

  /**
   * Adds a thread pool to this server
   *
   * @param tpool The thread pool to be added.
   */
  void addThreadPool(BaseThreadPool *const pool) throw();

protected:

  /**
   * Parses a command line to set the server options.
   *
   * @param argc The size of the command-line vector.
   * @param argv The command-line vector.
   */
  virtual void parseCommandLine(int argc, char *argv[]);

  /**
   * Starts all the thread pools
   */
  void start() throw (castor::exception::Exception);

  /**
   * Adds a dedicated UDP thread pool for getting wakeup notifications
   * from other Castor daemons. Those notifications are supposed to be
   * sent using the BaseServer::sendNotification method.
   * @param port the UDP port where to listen
   */
  void addNotifierThreadPool(const int port);

  /**
   * Shuts down the daemon gracefully.
   */
  void shutdownGracefully() throw();

private:

  /**
   * Prints out the online help
   */
  void help(const std::string &programName);

  /**
   * Sets up the signal handling for this multi-threaded daemon.
   */
  void setupMultiThreadedSignalHandling() throw(castor::exception::Internal);

  /**
   * Handles signals and performs graceful/immediate stop.
   * Called by start()
   */
  void handleSignals();

  /**
   * Sends a shutdown message to all thread pools, then
   * waits for all threads to terminate before returning.
   * This implements a graceful kill and is triggered by SIGTERM.
   */
  virtual void waitAllThreads() throw ();

  /**
   * Command line parameters. Includes by default a parameter
   * per each thread pool to specify the number of threads.
   */
  std::ostringstream m_cmdLineParams;

  /**
   * List of thread pools running on this server,
   * identified by their name initials (= cmd line parameter).
   */
  std::map<const char, BaseThreadPool*> m_threadPools;

  /**
   * Set of caught signals.
   */
  sigset_t m_signalSet;

  /**
   * A mutex for the signal handler thread.
   */
  Mutex* m_signalMutex;

  /**
   * Entry point for the signal handler thread.
   */
  static void* s_signalHandler(void* arg);

}; // class MultiThreadedDaemon

} // namespace server
} // namespace castor

#endif // CASTOR_SERVER_MULTITHREADEDDAEMON_HPP
