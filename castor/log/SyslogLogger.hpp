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
 * Interface to the CASTOR logging system
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/log/Logger.hpp"

#include <map>
#include <pthread.h>
#include <syslog.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef __APPLE__
#include <mach/mach.h>
#endif

namespace castor {
namespace log {

/**
 * Class implementaing the API of the CASTOR logging system.
 */
class SyslogLogger: public Logger {
public:

  /**
   * Constructor
   *
   * @param programName The name of the program to be prepended to every log
   * message.
   */
  SyslogLogger(const std::string &programName)
    throw(castor::exception::Exception, castor::exception::InvalidArgument);

  /**
   * Destructor.
   */
  ~SyslogLogger() throw();

  /**
   * Prepares the logger object for a call to fork().
   *
   * No further calls to operator() should be made after calling this
   * method until the call to fork() has completed.
   */
  void prepareForFork() ;

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of operator() allows the caller to specify the
   * time stamp of the log message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param params the parameters of the message.
   * @param timeStamp the time stamp of the log message.
   */
  void operator() (
    const int priority,
    const std::string &msg,
    const std::vector<Param> &params,
    const struct timeval &timeStamp) throw();

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of operator() allows the caller to specify the
   * time stamp of the log message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param params the parameters of the message.
   * @param timeStamp the time stamp of the log message.
   */
  void operator() (
    const int priority,
    const std::string &msg,
    const std::list<Param> &params,
    const struct timeval &timeStamp) throw();

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of operator() allows the caller to specify the
   * time stamp of the log message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param numParams the number of parameters in the message.
   * @param params the parameters of the message.
   * @param timeStamp the time stamp of the log message.
   */
  void operator() (
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
   * Note that this version of operator() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param params the parameters of the message.
   */
  void operator() (
    const int priority,
    const std::string &msg,
    const std::vector<Param> &params) throw();

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of operator() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param params the parameters of the message.
   */
  void operator() (
    const int priority,
    const std::string &msg,
    const std::list<Param> &params) throw();

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of operator() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param numParams the number of parameters in the message.
   * @param params the parameters of the message.
   */
  void operator() (
    const int priority,
    const std::string &msg,
    const int numParams,
    const Param params[]) throw();

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of operator() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   */
  void operator() (
    const int priority,
    const std::string &msg) throw();

  /**
   * A template function that wraps operator() in order to get the compiler
   * to automatically determine the size of the params parameter, therefore
   *
   * Note that this version of operator() allows the caller to specify the
   * time stamp of the log message.
   *
   * @param priority the priority of the message as defined by the syslog
   * API.
   * @param msg the message.
   * @param params the parameters of the message.
   * @param timeStamp the time stamp of the log message.
   */
  template<int numParams> void operator() (
    const int priority,
    const std::string &msg,
    castor::log::Param(&params)[numParams],
    const struct timeval &timeStamp) throw() {
    operator() (priority, msg, numParams, params, timeStamp);
  }

  /**
   * A template function that wraps operator() in order to get the compiler
   * to automatically determine the size of the params parameter, therefore
   * removing the need for the devloper to provide it explicity.
   *
   * Note that this version of operator() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog
   * API.
   * @param msg the message.
   * @param params the parameters of the message.
   */
  template<int numParams> void operator() (
    const int priority,
    const std::string &msg,
    castor::log::Param(&params)[numParams]) throw() {
    operator() (priority, msg, numParams, params);
  }

protected:

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
   * The maximum message length that the client syslog server can handle.
   */
  const size_t m_maxMsgLen;

  /**
   * Mutex used to protect the critical section of the SyslogLogger
   * object.
   */
  pthread_mutex_t m_mutex;

  /**
   * The file descriptor of the socket used to send messages to syslog.
   */
  int m_logFile;

  /**
   * Map from syslog integer priority to textual representation.
   */
  const std::map<int, std::string> m_priorityToText;

  /**
   * Map from the possible string values of the LogMask parameters of
   * /etc/castor.conf and their equivalent syslog priorities.
   */
  const std::map<std::string, int> m_configTextToPriority;

  /**
   * Returns the log mask for the program.
   */
  int logMask() const throw();

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
    ;

  /**
   * Generates and returns the mapping between the possible string values
   * of the LogMask parameters of /etc/castor.conf and their equivalent
   * syslog priorities.
   */
  std::map<std::string, int> generateConfigTextToPriorityMap() const
    ;

  /**
   * Initializes the mutex used to protect the critical section of the
   * SyslogLogger object.
   */
  void initMutex() ;

  /**
   * Connects to syslog.
   *
   * Please note that this method must be called from within the critical
   * section of the SyslogLogger object.
   *
   * If the connection with syslog is already open then this method does
   * nothing.
   *
   * This method does not throw an exception if the connection cannot be made
   * to syslog.  In this case the internal state of the SyslogLogger
   * object reflects the fact that no connection was made.
   */
  void openLog() throw();

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of operator() allows the caller to specify the
   * time stamp of the log message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param paramsBegin the first parameters of the message.
   * @param paramsEnd one past the end of the parameters of the message.
   * @param timeStamp the time stamp of the log message.
   */
  template<typename ParamIterator> void operator() (
    const int priority,
    const std::string &msg,
    ParamIterator paramsBegin,
    ParamIterator paramsEnd,
    const struct timeval &timeStamp) throw() {
    //-------------------------------------------------------------------------
    // Note that we do here part of the work of the real syslog call, by
    // building the message ourselves. We then only call a reduced version of
    // syslog (namely reducedSyslog). The reason behind it is to be able to set
    // the message timestamp ourselves, in case we log messages asynchronously,
    // as we do when retrieving logs from the DB
    //-------------------------------------------------------------------------

    // Ignore messages whose priority is not of interest
    if(priority > logMask()) {
      return;
    }

    // Try to find the textual representation of the syslog priority
    std::map<int, std::string>::const_iterator priorityTextPair =
      m_priorityToText.find(priority);

    // Do nothing if the log priority is not valid
    if(m_priorityToText.end() == priorityTextPair) {
      return;
    }

    // Safe to get a reference to the textual representation of the priority
    const std::string &priorityText = priorityTextPair->second;

    std::ostringstream logMsg;

    // Start message with priority, time, program and PID (syslog standard
    // format)
    logMsg << buildSyslogHeader(priority | LOG_LOCAL3, timeStamp, getpid());

    // Determine the thread id
#ifdef __APPLE__
    const int tid = mach_thread_self();
#else
    const int tid = syscall(__NR_gettid);
#endif

    // Append the log level, the thread id and the message text
    logMsg << "LVL=" << priorityText << " TID=" << tid << " MSG=\"" << msg <<
      "\" ";

    // Process parameters
    for(ParamIterator itor = paramsBegin; itor != paramsEnd; itor++) {
      const Param &param = *itor;

      // Check the parameter name, if it's an empty string set the value to
      // "Undefined".
      const std::string name = param.getName() == "" ? "Undefined" :
        cleanString(param.getName(), true);

      // Process the parameter value
      const std::string value = cleanString(param.getValue(), false);

      // Write the name and value to the buffer
      logMsg << name << "=\"" << value << "\" ";
    }

    // Terminate the string
    logMsg << "\n";

    reducedSyslog(logMsg.str());
  }

  /**
   * Build the header of a syslog message.
   *
   * @param priority The priority of the message.
   * @param timeStamp The time stamp of the message.
   * @param pid The process ID of the process logging the message.
   * @return The header of the syslog message.
   */
  std::string buildSyslogHeader(
    const int priority,
    const struct timeval &timeStamp,
    const int pid) const throw();

  /**
   * Creates a clean version of the specified string ready for use with syslog.
   *
   * @param s The string to be cleaned.
   * @param replaceUnderscores Set to true if spaces should be replaced by
   * underscores.
   * @return A cleaned version of the string.
   */
  std::string cleanString(const std::string &s, const bool replaceUnderscores)
    const throw();

  /**
   * A reduced version of syslog.  This method is able to set the message
   * timestamp.  This is necessary when logging messages asynchronously of there
   * creation, such as when retrieving logs from the DB.
   *
   * @param msg The message to be logged.
   */
  void reducedSyslog(std::string msg) throw();

  /**
   * Closes the connection to syslog.
   *
   * Please note that this method must be called from within the critical
   * section of the SyslogLogger object.
   *
   * If the connection to syslog is already closed then this method does
   * nothing.
   */
  void closeLog() throw();

}; // class SyslogLogger

} // namespace log
} // namespace castor

