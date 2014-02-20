/******************************************************************************
 *                 castor/tape/tapeserver/daemon/TapeDaemon.hpp
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

#ifndef CASTOR_TAPE_TAPESERVER_DAEMON_TAPEDAEMON_HPP
#define CASTOR_TAPE_TAPESERVER_DAEMON_TAPEDAEMON_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidConfigEntry.hpp"
#include "castor/server/Daemon.hpp"

#include <stdint.h>
#include <iostream>
#include <list>
#include <map>
#include <string>


namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Daemon responsible for reading and writing data from and to one or more tape
 * drives drives connected to a tape server.
 */
class TapeDaemon : public castor::server::Daemon {

public:

  /**
   * Constructor.
   *
   * @param stdOut Stream representing standard out.
   * @param stdErr Stream representing standard error.
   * @param log The object representing the API of the CASTOR logging system.
   */
  TapeDaemon(std::ostream &stdOut, std::ostream &stdErr, log::Logger &log)
    throw(castor::exception::Exception);

  /**
   * Destructor.
   */
  ~TapeDaemon() throw();

  /**
   * The main entry function of the daemon.
   *
   * @param argc The number of command-line arguments.
   * @param argv The array of command-line arguments.
   * @return     The return code of the process.
   */
  int main(const int argc, char **const argv) throw();

private:

  /**
   * The TCP/IP port on which the tape server daemon listens for incoming
   * connections from the VDQM server.
   */
  static const unsigned short TAPE_SERVER_LISTENING_PORT = 5003;

  /**
   * Exception throwing main() function.
   */
  void exceptionThrowingMain(const int argc, char **const argv)
    throw(castor::exception::Exception);

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
   * Blocks the signals that should not asynchronously disturb the tape-server
   * daemon.
   */
  void blockSignals() const throw(castor::exception::Exception);

  /**
   * The main event loop of the tape-server daemon.
   */
  void mainEventLoop(const unsigned short listenSock)
    throw(castor::exception::Exception);

  /**
   * Handles any pending signals.
   *
   * @return True if the main event lopp should continue, else false.
   */
  bool handlePendingSignals() throw();

  /**
   * Reaps any zombie processes.
   */
  void reapZombies() throw();

  /**
   * The program name of the tape daemon.
   */
  const std::string m_programName;

}; // class TapeDaemon

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TAPESERVER_DAEMON_TAPEDAEMON_HPP
