/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/log/Logger.hpp"
#include "common/threading/Mutex.hpp"

namespace cta {
namespace log {

/**
 * Class implementaing the API of the CASTOR logging system.
 */
class StringLogger: public Logger {
public:

  /**
   * Constructor
   *
   * @param programName The name of the program to be prepended to every log
   * message.
   * @param logMask The log mask.
   */
  StringLogger(const std::string &programName, const int logMask);

  /**
   * Destructor.
   */
  ~StringLogger();

  /**
   * Prepares the logger object for a call to fork().
   *
   * No further calls to operator() should be made after calling this
   * method until the call to fork() has completed.
   */
  void prepareForFork() ;

  /**
   * Extractor for the resulting logs.
   */
  std::string getLog() { return m_log.str(); }

protected:

  /**
   * Mutex used to protect the critical section of the StringLogger
   * object.
   */
  threading::Mutex m_mutex;

  /**
   * The file descriptor of the socket used to send messages to syslog.
   */
  std::stringstream m_log;

  /**
   * Writes the specified log message to the underlying logging system.
   *
   * This method is to be implemented by concrete sub-classes of the Logger
   * class.
   *
   * @param msg The message to be logged.
   */
  void writeMsgToUnderlyingLoggingSystem(const std::string &msg) override;

}; // class StringLogger

} // namespace log
} // namespace cta
