/******************************************************************************
 *                      castor/log/LogImplementation.hpp
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
 * Interface to the CASTOR logging system
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_LOG_LOGIMPLEMENTATION_HPP
#define CASTOR_LOG_LOGIMPLEMENTATION_HPP 1

#include "castor/log/Log.hpp"

#include <map>
#include <pthread.h>
#include <syslog.h>
#include <sys/time.h>

namespace castor {
namespace log {

/**
 * Class implementaing the API of the CASTOR logging system.
 */
class LogImplementation: public Log {
public:

  /**
   * Constructor
   *
   * @param programName The name of the program to be prepended to every log
   * message.
   */
  LogImplementation(const std::string &programName)
    throw(castor::exception::Internal, castor::exception::InvalidArgument);

  /**
   * Destructor.
   */
  ~LogImplementation() throw();

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of writeMsg() allows the caller to specify the
   * time stamp of the log message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param numParams the number of parameters in the message.
   * @param params the parameters of the message.
   * @param timeStamp the time stamp of the log message.
   */
  void writeMsg(
    const int priority,
    const std::string &msg,
    const int numParams,
    const Param params[],
    const struct timeval &timeStamp) throw();

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of writeMsg() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param numParams the number of parameters in the message.
   * @param params the parameters of the message.
   */
  void writeMsg(
    const int priority,
    const std::string &msg,
    const int numParams,
    const castor::log::Param params[]) throw();

private:

  /**
   * Default size of a syslog message.
   */
  static const size_t DEFAULT_SYSLOG_MSGLEN = 1024;

  /**
   * Default size of a rsyslog message.
   */
  static const size_t DEFAULT_RSYSLOG_MSGLEN = 2000;

  /**
   * Maximum length of a parameter name.
   */
  static const size_t LOG_MAX_PARAMNAMELEN = 20;

  /**
   * Maximum length of a string value.
   */
  static const size_t LOG_MAX_PARAMSTRLEN = 1024;

  /**
   * Maximum length of a log message.
   */
  static const size_t LOG_MAX_LINELEN = 8192;

  /**
   * The name of the program to be prepended to every log message.
   */
  const std::string m_programName;

  /**
   * The maximum message length that the client syslog server can handle.
   */
  const size_t m_maxMsgLen;

  /**
   * Mutex used to protect the critical section of the LogImplementation object.
   */
  pthread_mutex_t m_mutex;

  /**
   * The file descriptor of the socket used to send messages to syslog.
   */
  int m_logFile;

  /**
   * True if there is currrently a connection with syslog.
   */
  bool m_connected;

  /**
   * Map from syslog integer priority to textual representation.
   */
  const std::map<int, std::string> m_priorityToText;

  /**
   * Throws castor::exception::InvalidArgument if the specified program name is
   * too long.
   */
  void checkProgramNameLen(const std::string &programName)
    throw(castor::exception::InvalidArgument);

  /**
   * Determines the maximum message length that the client syslog server can
   * handle.
   *
   * @return The maximum message length that the client syslog server can
   * handle.
   */
  size_t determineMaxMsgLen() const throw();

  /**
   * Generates and returns the mapping between syslog priorities and their
   * textual representations.
   */
  std::map<int, std::string> generatePriorityToTextMap() const
    throw(castor::exception::Internal);

  /**
   * Initializes the mutex used to protect the critical section of the
   * LogImplementation object.
   */
  void initMutex() throw(castor::exception::Internal);

  /**
   * Connects to syslog.
   *
   * Please note that this method must be called from within the critical
   * section of the LogImplementation object.
   *
   * If the connection with syslog is already open then this method does
   * nothing.
   *
   * This method does not throw an exception if the connection cannot be made
   * to syslog.  In this case the internal state of the LogImplementation
   * object reflects the fact that no connection was made.
   */
  void openLog() throw();

  /**
   * Build the header of a syslog message.
   */
  int buildSyslogHeader(
    char *const buffer,
    const int buflen,
    const int priority,
    const struct timeval &timeStamp,
    const int pid) const throw();

  /**
   * Creates a clean version of the specified string ready for use with syslog.
   *
   * @param s The string to be cleaned.
   * @param replaceSpaces Set to true if spaces should be replaced by
   * underscores.
   * @return A cleaned version of the string.
   */
  std::string cleanString(const std::string &s, const bool replaceSpaces)
    throw();

  /**
   * A reduced version of syslog.  This method is able to set the message
   * timestamp.  This is necessary when logging messages asynchronously of there
   * creation, such as when retrieving logs from the DB.
   *
   * @param msg The message to be logged.
   * @param msgLen The length of the message top be logged.
   */
  void reducedSyslog(const char *const msg, const int msgLen) throw();

  /**
   * Closes the connection to syslog.
   *
   * Please note that this method must be called from within the critical
   * section of the LogImplementation object.
   *
   * If the connection to syslog is already closed then this method does
   * nothing.
   */
  void closeLog() throw();

}; // class LogImplementation

} // namespace log
} // namespace castor

#endif // CASTOR_LOG_LOGIMPLEMENTATION_HPP
