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

#include <chrono>
#include <atomic>
#include <list>
#include <map>

namespace cta::log {

/**
 * Format for log lines
 *
 * Log lines are output as key-value pairs
 */
enum class LogFormat {
  DEFAULT,  //!< Text format
  JSON      //!< JSON format
};

/**
 * Abstract class representing the API of the CTA logging system
 *
 * The intended way to use the CTA logging API is as follows:
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
   * @param hostName    The name of the host to be prepended to every log message
   * @param programName The name of the program to be prepended to every log message
   * @param logMask     The log mask
   */
  Logger(std::string_view hostName, std::string_view programName, int logMask);

  /**
   * Destructor
   */
  virtual ~Logger() = 0;

  /**
   * Prepares the logger object for a call to fork()
   *
   * No further calls to operator() should be made after calling this
   * method until the call to fork() has completed.
   */
  virtual void prepareForFork() = 0;

  /**
   * Writes a message into the CTA logging system
   *
   * Exceptions are not thrown in case of failure. Failures are silently ignored in order to not impact the processing.
   *
   * This version of operator() implicitly uses the current time as the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog API
   * @param msg      the message
   * @param params   optional parameters of the message
   */
  virtual void operator() (int priority, std::string_view msg, const std::list<Param>& params = std::list<Param>()) noexcept;

  /**
   * Sets the log mask
   *
   * @param logMask The log mask
   */
  void setLogMask(std::string_view logMask);

  /**
   * Sets the log format
   *
   * @param logFormat The log format
   */
  void setLogFormat(std::string_view logFormat);

  /**
   * Sets the map of static headers to be included in every log line.
   */
  void setStaticParams(const std::map<std::string, std::string> &staticHeaders);

  /**
   * Creates a clean version of the specified string ready for use with syslog
   *
   * @param s                  The string to be cleaned
   * @param replaceUnderscores Set to true if spaces should be replaced by underscores
   * @return                   A cleaned version of the string
   */
  static std::string cleanString(std::string_view s, bool replaceUnderscores);

protected:
  /**
   * `is_optional_type<T>` will be used to detect, at compile time, if a variable is of type `std::optional`
   */
  // Assume T is not std::optional by default
  template<typename T>
  struct is_optional_type : std::false_type {};

  // Specialization for std::optional<T>
  template<typename T>
  struct is_optional_type<std::optional<T>> : std::true_type {};

  // A helper template that is always false
  template<typename T>
  struct always_false : std::false_type {};

  // A helper trait to check for operator<<
  template <typename, typename = void>
  struct has_ostream_operator : std::false_type {};
  template <typename T>
  struct has_ostream_operator<T, std::void_t<decltype(std::declval<std::ostream&>() << std::declval<T>())>> : std::true_type {};

  /**
   * Helper class to format floating-point values
   */
  template<typename T>
  class floatingPointFormatting {
  public:
    explicit floatingPointFormatting(T value) : m_value(value) {
      static_assert(std::is_floating_point_v<T>, "Template parameter must be a floating point type.");
    }

    friend std::ostream& operator<<(std::ostream& oss, const floatingPointFormatting& fp) {
      constexpr int nr_digits = std::numeric_limits<T>::digits10;
      std::ostringstream oss_tmp;
      oss_tmp << std::setprecision(nr_digits) << fp.m_value;
      // Find if value contains a '.' or 'e/E', making it clear that it's a floating-point value
      if (oss_tmp.str().find_first_of(".eE") == std::string::npos) {
        // If not, append ".0" to make it clear that it's a floating point
        oss_tmp << ".0";
      }
      oss << oss_tmp.str();
      return oss;
    }

  private:
    T m_value;
  };

  /**
   * The name of the host to be prepended to every log message
   */
  const std::string m_hostName;

  /**
   * The name of the program to be prepended to every log message
   */
  const std::string m_programName;

  /**
   * Log mask
   */
  std::atomic<int> m_logMask;

  /**
   * Log output format
   */
  std::atomic<LogFormat> m_logFormat = LogFormat::DEFAULT;

  /**
   * String containing static parameters, expected to always be printed in a log massage.
   * There parameters can be set with setStaticParams.
   */
  std::string m_staticParamsStr;

 /**
   * Map from syslog integer priority to textual representation
   */
  const std::map<int, std::string> m_priorityToText;

  /**
   * Map from the possible string values of the LogMask parameters and their equivalent syslog priorities
   */
  const std::map<std::string, int> m_configTextToPriority;

  /**
   * Writes the specified msg to the underlying logging system
   *
   * It is the responsibility of the concrete sub-classes to decide whether or not to use the specified log message
   * header.
   *
   * @param header The header of the message to be logged
   * @param body   The body of the message to be logged
   */
  virtual void writeMsgToUnderlyingLoggingSystem(std::string_view header, std::string_view body) = 0;

  /**
   * Generates and returns the mapping between syslog priorities and their textual representations
   */
  static std::map<int, std::string> generatePriorityToTextMap();

  /**
   * Generates the mapping between possible string values of the LogMask parameters and their equivalent syslog priorities
   */
  static std::map<std::string, int> generateConfigTextToPriorityMap();

private:

  /**
   * Timestamp type
   */
  using TimestampT = decltype(std::chrono::system_clock::now());

  /**
   * Helper class to format string values in JSON
   */
  class stringFormattingJSON {
  public:
      explicit stringFormattingJSON(std::string_view str): m_value(str) {};
    friend std::ostream& operator<<(std::ostream& oss, const stringFormattingJSON& fp);
  private:
    std::string_view m_value;
  };

  friend std::ostream& operator<<(std::ostream& oss, const Logger::stringFormattingJSON& fp);

  /**
   * Creates and returns the header of a log message
   *
   * Concrete subclasses of the Logger class can decide whether or not to use message headers created by this method.
   *
   * @param timeStamp   Timestamp of the message
   * @return            Message header
   */
  std::string createMsgHeader(const TimestampT& timeStamp) const;

  /**
   * Creates and returns the body of a log message
   *
   * @param logLevel    Log level
   * @param msg         Message text
   * @param params      Message parameters
   * @param pid         Process ID of the process logging the message
   * @return            Message body
   */
  std::string createMsgBody(std::string_view logLevel, std::string_view msg, const std::list<Param> &params, int pid) const;
};

} // namespace cta::log
