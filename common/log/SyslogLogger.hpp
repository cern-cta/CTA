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

#include <map>
#include <syslog.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/log/Logger.hpp"
#include "common/threading/Mutex.hpp"

namespace cta::log {

/**
 * Class implementaing the API of the CASTOR logging system.
 */
class SyslogLogger: public Logger {
public:
  /**
   * Constructor
   *
   * @param hostName The name of the host to be prepended to every log message.
   * @param programName The name of the program to be prepended to every log message.
   * @param logMask The log mask.
   */
  SyslogLogger(std::string_view hostName, std::string_view programName, const int logMask);

  /**
   * Destructor
   */
  ~SyslogLogger() override = default;

  /**
   * Prepares the logger object for a call to fork()
   *
   * No further calls to operator() should be made after calling this
   * method until the call to fork() has completed.
   */
  void prepareForFork() final { /* intentionally-blank override of pure virtual method */ }

  /**
   * Update the underlying logger settings
   */
  void refresh() final { /* intentionally-blank override of pure virtual method */ }

protected:
  /**
   * Writes the specified msg to the underlying logging system
   *
   * This method is to be implemented by concrete sub-classes of the Logger class.
   *
   * Please note it is the responsibility of a concrete sub-class to decide
   * whether or not to use the specified log message header.  For example, the
   * SysLogLogger sub-class does not use the header.  Instead it relies on
   * rsyslog to provide a header.
   *
   * @param header The header of the message to be logged. It is the responsibility of the concrete sub-class.
   * @param body The body of the message to be logged.
   */
  void writeMsgToUnderlyingLoggingSystem(std::string_view header, std::string_view body) final;
};

} // namespace cta::log
