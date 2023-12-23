/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "common/log/Constants.hpp"
#include "common/log/Param.hpp"

#include "common/json/object/JSONCObject.hpp"
#include "common/log/JSONLogger.hpp"

using namespace cta::utils::json::object;//::JSONCObject;;

// The header file for atomic was is actually called cstdatomic in gcc 4.4
#if __GNUC__ == 4 && (__GNUC_MINOR__ == 4)
    #include <cstdatomic>
#else
  #include <atomic>
#endif

#include <list>
#include <map>

/**
 * It is a convention of CASTOR to use syslog level of LOG_NOTICE to label
 * user errors.  This macro helps enforce that convention and document it in
 * the code.
 */
#define LOG_USERERR LOG_NOTICE

namespace cta::log {

/**
 * Abstract class representing the API of the CASTOR logging system.
 *
 * The intended way to use the CASTOR logging API is as follows:
 *
 * 1. Keep a reference to a Logger object, for example:
 * \code{.cpp}
 *
 * class MyClassThatWillLog {
 * protected:
 *   Logger & m_log;
 *
 * public:
 *   MyClassThatWillLog(Logger &log): m_log(log) {
 *     ....
 *   }
 * }
 *
 * \endcode
 *
 * 2. To log a message, use the reference to the Logger object like a function.
 *    In other words the Logger object implements operator() and therefore
 *    behaves like a functor:
 * \code{.cpp}
 *
 * void MyClassThatWillLog::aMethodThatWillLog() {
 *   ....
 *   m_log(cta::log::INFO, "My log message");
 *   ....
 * }
 *
 * \endcode
 *
 * The Logger object implements operator() in order to avoid the following long
 * winded syntax (which does not work by the way, so please do NOT copy and
 * paste the following example):
 * \code{.cpp}
 *
 * m_log.logMsg(cta::log::INFO, "My log message");
 *
 * \endcode
 */
class Logger {
public:

  /**
   * Constructor
   *
   * @param hostName The name of the host to be prepended to every log
   * @param programName The name of the program to be prepended to every log
   * @param logMask The log mask.
   * message.
   */
  Logger(std::string_view hostName, std::string_view programName, const int logMask);

  /**
   * Destructor.
   */
  virtual ~Logger() = 0;

  /**
   * Prepares the logger object for a call to fork().
   *
   * No further calls to operator() should be made after calling this
   * method until the call to fork() has completed.
   */
  virtual void prepareForFork() = 0;

  /**
   * Returns the name of the program.
   */
  const std::string &getProgramName() const;

  /**
   * Writes a message into the CTA logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of operator() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param params optional parameters of the message.
   */
  virtual void operator() (int priority, std::string_view msg, const std::list<Param>& params = std::list<Param>());

  /**
   * Sets the log mask.
   *
   * @param logMask The log mask.
   */
  void setLogMask(std::string_view logMask);

  /**
   * Sets the log mask.
   *
   * @param logMask The log mask.
   */
  void setLogMask(const int logMask);

  /**
   * Creates a clean version of the specified string ready for use with syslog.
   *
   * @param s The string to be cleaned.
   * @param replaceUnderscores Set to true if spaces should be replaced by
   * underscores.
   * @return A cleaned version of the string.
   */
  static std::string cleanString(const std::string &s,
    const bool replaceUnderscores);

protected:
  /**
   * The name of the host to be prepended to every log message.
   */
  const std::string m_hostName;

  /**
   * The name of the program to be prepended to every log message.
   */
  const std::string m_programName;
  
  /**
   * Writes the specified msg to the underlying logging system.
   *
   * This method is to be implemented by concrete sub-classes of the Logger
   * class.
   *
   * Please note it is the responsibility of a concrete sub-class to decide
   * whether or not to use the specified log message header.  For example, the
   * SysLogLogger sub-class does not use the header.  Instead it relies on
   * rsyslog to provide a header.
   *
   * @param header The header of the message to be logged.  It is the
   * esponsibility of the concrete sub-class 
   * @param body The body of the message to be logged.
   */
  virtual void writeMsgToUnderlyingLoggingSystem(std::string_view header, std::string_view body) = 0;
  virtual void writeMsgToUnderlyingLoggingSystemJson(const std::string &jsonOut) = 0;

  /**
   * The log mask.
   */
  std::atomic<int> m_logMask;

  /**
   * Map from syslog integer priority to textual representation.
   */
  const std::map<int, std::string> m_priorityToText;

  /**
   * Map from the possible string values of the LogMask parameters and
   * their equivalent syslog priorities.
   */
  const std::map<std::string, int> m_configTextToPriority;
  
  /**
   * Generates and returns the mapping between syslog priorities and their
   * textual representations.
   */
  static std::map<int, std::string> generatePriorityToTextMap();

  /**
   * Generates and returns the mapping between the possible string values
   * of the LogMask parameters their equivalent syslog priorities.
   */
  static std::map<std::string, int> generateConfigTextToPriorityMap();

private:
  inline static cta::log::JSONLogger m_jsonLog = cta::log::JSONLogger();
  /**
   * Creates and returns the header of a log message.
   *
   * Concrete subclasses of the Logger class can decide whether or not to use
   * message headers created by this method.  The SysLogger sub-class for example
   * relies on rsyslog to provide message headers and therefore does not call
   * this method.
   *
   * @param timeStamp The time stamp of the message.
   * @param hostName The name of the host.
   * @param programName the program name of the log message.
   * @param pid The process ID of the process logging the message.
   * @return The message header.
   */
  static std::string createMsgHeader(
    //const struct timeval &timeStamp,
    //char epoch_time[],
    //const float epoch_time,
    //const float epoch_time_nano,
    //const float nano,
    uint64_t nanoTime,
    uint64_t seconds,
    uint64_t nanoseconds,
    char* local_time,
    std::string_view hostName,
    std::string_view programName,
    const int pid);

  /**
   * Creates and returns the body of a log message
   *
   * @param epoch_time
   * @param local_time
   * @param priority      the priority of the message as defined by the syslog API
   * @param priorityText
   * @param msg           the message
   * @param params        the parameters of the message
   * @param rawParams     preprocessed parameters of the message
   * @param pid           the pid of the log message
   * @return the message body
   */
  static std::string createMsgBody(
    //char epoch_time[],
    //const float epoch_time,
    char* local_time,
    const int priority,
    std::string_view priorityText, std::string_view msg,
    const std::list<Param> &params, std::string_view rawParams, int pid);

  static std::string createMsgJsonOut(
    uint64_t nanoTime,
    uint64_t nanoseconds,
    uint64_t seconds,
    char* local_time,
    const std::string &hostName,
    const std::string &programName,
    const int pid,
    const int priority,
    const std::string &priorityText,
    const std::string &msg,
    const std::list<Param> &params,
    const std::string &rawParams);
};

} // namespace cta::log

