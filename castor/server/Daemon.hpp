/*******************************************************************************
 *                      castor/server/Daemon.hpp
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

#pragma once

#include "castor/dlf/Message.hpp"
#include "castor/exception/CommandLineNotParsed.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/log/Logger.hpp"

#include <ostream>

namespace castor {
namespace server {

/**
 * This class contains the code common to all daemon classes.
 *
 * The code common to all daemon classes includes daemonization and logging.
 */
class Daemon {

public:

  /**
   * Constructor
   *
   * @param stdOut Stream representing standard out.
   * @param stdErr Stream representing standard error.
   * @param log Object representing the API of the CASTOR logging system.
   */
  Daemon(std::ostream &stdOut, std::ostream &stdErr, log::Logger &log)
    throw();

  /**
   * Destructor.
   */
  virtual ~Daemon() throw();

  /**
   * Parses a command line to set the server options.
   *
   * @param argc The size of the command-line vector.
   * @param argv The command-line vector.
   */
  virtual void parseCommandLine(int argc, char *argv[])
    ;

  /**
   * Prints out the online help
   */
  virtual void help(const std::string &programName) throw();

  /**
   * Returns this server's name as used by the CASTOR logging system.
   */
  const std::string &getServerName() const throw();

  /**
   * Sets the runAsStagerSuperuser flag to true.
   *
   * The default value of the runAsStagerSuperuser flag at construction time is
   * false.
   */
  void runAsStagerSuperuser() throw();

  /**
   * Returns true if the daemon is configured to run in the foreground.
   */
  bool getForeground() const ;

protected:

  /**
   * Tells the daemon object that the command-line has been parsed.  This
   * method allows subclasses to implement their own command-line parsing logic,
   * whilst enforcing the fact that they must provide values for the options and
   * arguments this class requires.
   *
   * @param foreground Set to true if the daemon should run in the foreground.
   */
  void setCommandLineHasBeenParsed(const bool foreground) throw();

  /**
   * Initializes the DLF, both for streaming and regular messages
   * Does not create the DLF thread, this is created after daemonization
   * @param messages the messages to be passed to dlf_init
   */
  void dlfInit(castor::dlf::Message messages[])
    ;

  /**
   * Daemonizes the daemon if it has not been configured to run in the
   * foreground.
   *
   * Please make sure that the setForeground() and runAsStagerSuperuser()
   * methods have been called as appropriate before this method is called.
   * This method takes into account whether the dameon should run in foregreound
   * or background mode (m_foreground) and whether or not the user of daemon
   * should be changed to the stager superuser (m_runAsStagerSuperuser).
   */
  void daemonizeIfNotRunInForeground() ;

  /**
   * Sends a notification message to the given host,port
   * to wake up nbThreads threads to handle pending requests.
   * @param host the destination host
   * @param port the destination port
   * @param tpName the name of the thread pool to be signaled
   * @param nbThreads the number of threads to be signaled
   */
  static void sendNotification(const std::string &host, const int port,
    const char tpName, const int nbThreads = 1) throw();

  /**
   * Stream representing standard out.
   */
  std::ostream &m_stdOut;

  /**
   * Stream representing standard in.
   */
  std::ostream &m_stdErr;

  /**
   * Object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

private:

  /**
   * Flag indicating whether the server should run in foreground or background
   * mode.
   */
  bool m_foreground;

  /**
   * True if the command-line has been parsed.
   */
  bool m_commandLineHasBeenParsed;

  /**
   * Flag indicating whether the server should
   * change identity at startup and run as STAGERSUPERUSER
   * (normally defined as stage:st)
   */
  bool m_runAsStagerSuperuser;

}; // class Daemon

} // namespace server
} // namespace castor

