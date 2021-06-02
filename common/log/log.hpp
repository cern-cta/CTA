/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "Logger.hpp"
#include "common/log/Param.hpp"
#include "common/log/IPAddress.hpp"
#include "common/log/TimeStamp.hpp"
//#include "common/Cuuid.h"

#include <list>
#include <syslog.h>
#include <sys/time.h>

// more meaningful alias to NOTICE log level
#define LOG_USER_ERROR LOG_NOTICE

namespace cta {
namespace log {

  /**
   * Initialises the logging system with the specified logger which should be
   * allocated on the heap and will be owned by the logging system;
   *
   * This method is not thread safe.
   *
   * @logger The logger to be used by the logging system.
   */
  void init(cta::log::Logger *logger);

  /**
   * Deallocates the logger.
   *
   * This method is not thread safe.
   */
  void shutdown();

  /**
   * Returns a reference to the logger if it exists else throws an exception.
   */
  Logger &instance();

  /**
   * Prepares the logger object for a call to fork().
   *
   * No further calls to operator() should be made after calling this
   * method until the call to fork() has completed.
   */
  void prepareForFork();

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of write() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param params optionally the parameters of the message.
   */
  void write(
    const int priority,
    const std::string &msg,
    const std::list<cta::log::Param> &params =
      std::list<cta::log::Param>());

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of write() is very specific and should not be
   * used for general purpose. It allows the caller to specify the
   * time stamp, the program name and the pid of the log message and
   * takes preprocessed param
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param rawParams preprocessed parameters of the message.
   * @param timeStamp the time stamp of the log message.
   * @param progName the program name of the log message.
   * @param pid the pid of the log message.
   */
  void write(
    const int priority,
    const std::string &msg,
    const std::string &rawParams,
    const struct timeval &timeStamp,
    const std::string &progName,
    const int pid);

  /**
   * Returns the program name if known or the empty string if not.
   *
   * @return the program name if known or the empty string if not.
   */
  std::string getProgramName();

} // namespace log
} // namespace cta

/**
 * non-member operator to stream a Cuuid_t
 */
//////std::ostream& operator<<(std::ostream& out, const Cuuid_t& uuid);
