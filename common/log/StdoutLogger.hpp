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

#include "common/log/Logger.hpp"

namespace cta {
namespace log {

/**
 * Class implementaing the API of the CASTOR logging system.
 */
class StdoutLogger : public Logger {
public:
  /**
   * Constructor
   *
   * @param hostName The name of the host to be prepended to every log message.
   * @param programName The name of the program to be prepended to every log
   * message.
   * @param simple If true, then logging header is not included.
   */
  StdoutLogger(const std::string& hostName, const std::string& programName, bool simple = false);

  /**
   * Destructor.
   */
  ~StdoutLogger();

  /**
   * Prepares the logger object for a call to fork().
   *
   * No further calls to operator() should be made after calling this
   * method until the call to fork() has completed.
   */
  void prepareForFork();

protected:
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
  void writeMsgToUnderlyingLoggingSystem(const std::string& header, const std::string& body) override;

private:
  bool m_simple;

};  // class StringLogger

}  // namespace log
}  // namespace cta
