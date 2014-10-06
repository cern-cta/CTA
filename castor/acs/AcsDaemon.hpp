/******************************************************************************
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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/server/Daemon.hpp"
#include "castor/server/ProcessCap.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"
#include "castor/acs/Constants.hpp"

namespace castor     {
namespace acs        {

/**
 * CASTOR ACS daemon responsible for mounting and dismounting tapes for ACS.
 */
class AcsDaemon : public castor::server::Daemon {

public:
  /**
   *  Subclass holding all the configuration parameters
   */
  class CastorConf {
  public:
    CastorConf(): 
      ascServerInternalListeningPort(DEFAULT_ACS_SERVER_INTERNAL_LISTENING_PORT),
      acsQueryLibraryInterval(DEFAULT_ACS_QUERY_LIBRARY_INTERVAL), 
      acsCommandTimeout(DEFAULT_ACS_COMMAND_TIMEOUT) {} 
    /**
     * The TCP/IP port on which the CASTOR ACS daemon listens for incoming Zmq
     * connections from the tape server.
     */
    unsigned short ascServerInternalListeningPort;
    /**
     * Time to wait in seconds between queries to ACS Library for responses.
     */
    const int acsQueryLibraryInterval; 
    /**
     * Time to wait in seconds for the mount or dismount to conclude.
     */
    const int acsCommandTimeout;
  };
  /**
   * Constructor.
   *
   * @param argc The argc of main().
   * @param argv The argv of main().
   * @param stdOut Stream representing standard out.
   * @param stdErr Stream representing standard error.
   * @param log The object representing the API of the CASTOR logging system.
   * @param reactor The reactor responsible for dispatching the I/O requests to
   * the CASTOR ACS daemon.
   * @param capUtils Object providing utilities for working UNIX capabilities.
   * @param castorConf The configuration for the CASTOR ACS daemon.
   */
  AcsDaemon(
    const int argc,
    char **const argv,
    std::ostream &stdOut,
    std::ostream &stdErr,
    log::Logger &log,
    castor::tape::reactor::ZMQReactor &reactor,
    castor::server::ProcessCap &capUtils,
    const CastorConf &castorConf);

  /**
   * Destructor.
   */
  ~AcsDaemon() throw();

  /**
   * The main entry function of the daemon.
   *
   * @return The return code of the process.
   */
  int main() throw();
  
protected:

  /**
   * Returns the name of the host on which the daemon is running.
   */
  std::string getHostName() const;

  /**
   * Exception throwing main() function.
   *
   * @param argc The number of command-line arguments.
   * @param argv The array of command-line arguments.
   */
  void exceptionThrowingMain(const int argc, char **const argv);

  /**
   * Logs the start of the daemon.
   */
  void logStartOfDaemon(const int argc, const char *const *const argv) throw();

  /**
   * Creates a string that contains the specified command-line arguments
   * separated by single spaces.
   *
   * @param argc The number of command-line arguments.
   * @param argv The array of command-line arguments.
   */
  std::string argvToString(const int argc, const char *const *const argv)
    throw();

  /**
   * Idempotent method that destroys the ZMQ context.
   */
  void destroyZmqContext() throw();

  /**
   * Sets the dumpable attribute of the current process to true.
   */
  void setDumpable();

  /**
   * Sets the capabilities of the current process.
   *
   * @text The string representation the capabilities that the current
   * process should have.
   */
  void setProcessCapabilities(const std::string &text);

  /**
   * Blocks the signals that should not asynchronously disturb the daemon.
   */
  void blockSignals() const;
  
  /**
   * Initialises the ZMQ context.
   */
  void initZmqContext();
  /**
   * Sets up the reactor.
   */
  void setUpReactor();
 
  /**
   * Creates the handler to handle messages for the acs Zmq requests.
   */
  void createAndRegisterAcsMessageHandler();
  
  /**
   * The main event loop of the daemon.
   */
  void mainEventLoop();

  /**
   * Handles any pending events.
   *
   * @return True if the main event loop should continue, else false.
   */
  bool handleEvents();

  /**
   * Handles any pending signals.
   *
   * @return True if the main event loop should continue, else false.
   */
  bool handlePendingSignals() throw();

  
  /**
   * The argc of main().
   */
  const int m_argc;

  /**
   * The argv of main().
   */
  char **const m_argv;

  /**
   * The reactor responsible for dispatching the file-descriptor event-handlers
   * of the CASTOR ACS daemon.
   */
  castor::tape::reactor::ZMQReactor &m_reactor;

  /**
   * Object providing utilities for working UNIX capabilities.
   */
  castor::server::ProcessCap &m_capUtils;

  /**
   * The program name of the daemon.
   */
  const std::string m_programName;

  /**
   * The name of the host on which the daemon is running. 
   */
  const std::string m_hostName;

  /**
   * The ZMQ context.
   */
  void *m_zmqContext;
  
  /**
   * The configuration parameters for the CASTOR ACS daemon.
   */
  const CastorConf & m_castorConf;

}; // class AcsDaemon

} // namespace acs
} // namespace castor
