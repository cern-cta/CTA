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

#include "Logger.hpp"
#include "common/log/Param.hpp"
#include "common/log/IPAddress.hpp"
#include "common/log/TimeStamp.hpp"
//#include "common/Cuuid.h"

#include <list>
#include <syslog.h>
#include <sys/time.h>

namespace cta::log {

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
    std::string_view msg,
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
    std::string_view msg,
    std::string_view rawParams,
    const struct timeval &timeStamp,
    std::string_view progName,
    const int pid);

  /**
   * Returns the program name if known or the empty string if not.
   *
   * @return the program name if known or the empty string if not.
   */
  std::string getProgramName();

} // namespace cta::log

/**
 * non-member operator to stream a Cuuid_t
 */
//////std::ostream& operator<<(std::ostream& out, const Cuuid_t& uuid);
