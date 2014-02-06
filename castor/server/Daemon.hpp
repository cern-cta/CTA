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

#ifndef CASTOR_SERVER_DAEMON_HPP
#define CASTOR_SERVER_DAEMON_HPP 1

#include "castor/dlf/Message.hpp"
#include "castor/exception/CommandLineNotParsed.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/log/Logger.hpp"

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
   * @param logger Object representing the API of the CASTOR logging system.
   */
  Daemon(log::Logger &logger) throw();

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
    throw(castor::exception::Exception);

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
  bool getForeground() const throw(castor::exception::CommandLineNotParsed);

protected:

  /**
   * Initializes the DLF, both for streaming and regular messages
   * Does not create the DLF thread, this is created after daemonization
   * @param messages the messages to be passed to dlf_init
   */
  void dlfInit(castor::dlf::Message messages[])
    throw (castor::exception::Exception);

  /**
   * Daemonizes the daemon.
   *
   * Please make sure that the setForeground() and runAsStagerSuperuser()
   * methods have been called as appropriate before this method is called.
   * This method takes into account whether the dameon should run in foregreound
   * or background mode (m_foreground) and whether or not the user of daemon
   * should be changed to the stager superuser (m_runAsStagerSuperuser).
   */
  void daemonize() throw(castor::exception::Exception);

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
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of logMsg() allows the caller to specify the
   * time stamp of the log message.
   *
   * @param priority the priority of the message as defined by the syslog
   * API.
   * @param msg the message.
   * @param numParams the number of parameters in the message.
   * @param params the parameters of the message.
   * @param timeStamp the time stamp of the log message.
   */
  void logMsg(
    const int priority,
    const std::string &msg,
    const int numParams,
    const log::Param params[],
    const struct timeval &timeStamp) throw();

  /**
   * A template function that wraps logMsg in order to get the compiler
   * to automatically determine the size of the params parameter, therefore
   *
   * Note that this version of logMsg() allows the caller to specify the
   * time stamp of the log message.
   *
   * @param priority the priority of the message as defined by the syslog
   * API.
   * @param msg the message.
   * @param params the parameters of the message.
   * @param timeStamp the time stamp of the log message.
   */
  template<int numParams> void logMsg(
    const int priority,
    const std::string &msg,
    castor::log::Param(&params)[numParams],
    const struct timeval &timeStamp) throw() {
    logMsg(priority, msg, numParams, params, timeStamp);
  }

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of logMsg() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog
   * API.
   * @param msg the message.
   * @param numParams the number of parameters in the message.
   * @param params the parameters of the message.
   */
  void logMsg(
    const int priority,
    const std::string &msg,
    const int numParams,
    const castor::log::Param params[]) throw();

  /**
   * A template function that wraps logMsg in order to get the compiler
   * to automatically determine the size of the params parameter, therefore
   * removing the need for the devloper to provide it explicity.
   *
   * Note that this version of logMsg() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog
   * API.
   * @param msg the message.
   * @param params the parameters of the message.
   */
  template<int numParams> void logMsg(
    const int priority,
    const std::string &msg,
    castor::log::Param(&params)[numParams]) throw() {
    logMsg(priority, msg, numParams, params);
  }

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of logMsg() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog
   * API.
   * @param msg the message.
   */
  void logMsg(
    const int priority,
    const std::string &msg) throw();

  /**
   * Flag indicating whether the server should run in foreground or background
   * mode.
   */
  bool m_foreground;

  /**
   * True if the command-line has been parsed.
   */
  bool m_commandLineHasBeenParsed;

private:

  /**
   * Flag indicating whether the server should
   * change identity at startup and run as STAGERSUPERUSER
   * (normally defined as stage:st)
   */
  bool m_runAsStagerSuperuser;

  /**
   * Object representing the API of the CASTOR logging system.
   */
  log::Logger &m_logger;

}; // class Daemon

} // namespace server
} // namespace castor

#endif // CASTOR_SERVER_DAEMON_HPP
