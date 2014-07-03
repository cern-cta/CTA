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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/exception/Exception.hpp"
#include "castor/legacymsg/CupvProxy.hpp"
#include "castor/server/Daemon.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"

#include <string>

namespace castor {
namespace tape {
namespace rmc {

/**
 * Daemon responsible for mounting and unmount tapes.
 */
class RmcDaemon : public castor::server::Daemon {

public:

  /**
   * Constructor.
   *
   * @param stdOut Stream representing standard out.
   * @param stdErr Stream representing standard error.
   * @param log The object representing the API of the CASTOR logging system.
   * @param reactor The reactor responsible for dispatching the I/O events of
   * the parent process of the rmcd daemon.
   * @param cupv Proxy object representing the cupvd daemon.
   */
  RmcDaemon(
    std::ostream &stdOut,
    std::ostream &stdErr,
    log::Logger &log,
    reactor::ZMQReactor &reactor,
    legacymsg::CupvProxy &cupv) ;

  /**
   * Destructor.
   */
  ~RmcDaemon() throw();

  /**
   * The main entry function of the daemon.
   *
   * @param argc The number of command-line arguments.
   * @param argv The array of command-line arguments.
   * @return     The return code of the process.
   */
  int main(const int argc, char **const argv) throw();

protected:

  /**
   * Returns the name of the host on which the daemon is running.
   */
  std::string getHostName() const ;

  /**
   * Determines and returns the TCP/IP port onwhich the rmcd daemon should
   * listen for client connections.
   */
  unsigned short getRmcPort() ;

  /**
   * Tries to get the value of the specified parameter from parsing
   * /etc/castor/castor.conf.
   *
   * @param category The category of the configuration parameter.
   * @param name The name of the configuration parameter.
   */
  std::string getConfigParam(const std::string &category, const std::string &name) ;

  /**
   * Exception throwing main() function.
   *
   * @param argc The number of command-line arguments.
   * @param argv The array of command-line arguments.
   */
  void exceptionThrowingMain(const int argc, char **const argv)
    ;

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
   * Blocks the signals that should not asynchronously disturb the daemon.
   */
  void blockSignals() const ;

  /**
   * Sets up the reactor.
   */
  void setUpReactor() ;

  /**
   * Creates the handler to accept client connections and registers it with
   * the reactor.
   */
  void createAndRegisterAcceptHandler() ;

  /**
   * The main event loop of the daemon.
   */
  void mainEventLoop() ;

  /**
   * Handles any pending events.
   *
   * @return True if the main event loop should continue, else false.
   */
  bool handleEvents() ;

  /**
   * Handles any pending signals.
   *
   * @return True if the main event loop should continue, else false.
   */
  bool handlePendingSignals() throw();

  /**
   * Reaps any zombie processes.
   */
  void reapZombies() throw();

  /**
   * Reaps the specified zombie process.
   *
   * @param childPid The process ID of the zombie child-process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void reapZombie(const pid_t childPid, const int waitpidStat) throw();

  /**
   * Logs the fact that the specified child-process has terminated.
   *
   * @param childPid The process ID of the child-process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void logChildProcessTerminated(const pid_t childPid, const int waitpidStat) throw();

  /**
   * Forks a child-process for every pending tape mount/unmount request
   * handled by the previous call to m_reactor.handleEvebts().
   */
  void forkChildProcesses() throw();

  /**
   * Forks a child-process to mount/unmount a tape.
   */ 
  void forkChildProcess() throw();

  /**
   * The reactor responsible for dispatching the file-descriptor event-handlers
   * of the rmcd daemon.
   */
  reactor::ZMQReactor &m_reactor;

  /**
   * Proxy object representing the cupvd daemon.
   */
  legacymsg::CupvProxy &m_cupv;

  /**
   * The program name of the tape daemon.
   */
  const std::string m_programName;

  /**
   * The name of the host on which the daemon is running.
   */
  const std::string m_hostName;

  /**
   * The TCP/IP port on which the rmcd daemon listens for client connections.
   */
  const unsigned short m_rmcPort;

}; // class RmcDaemon

} // namespace rmc
} // namespace tape
} // namespace castor

