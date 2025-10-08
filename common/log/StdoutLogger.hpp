/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/Logger.hpp"

namespace cta::log {

/**
 * Class implementaing the API of the CASTOR logging system.
 */
class StdoutLogger: public Logger {
public:
  /**
   * Constructor
   *
   * @param hostName The name of the host to be prepended to every log message.
   * @param programName The name of the program to be prepended to every log
   * message.
   * @param simple If true, then logging header is not included.
   */
  StdoutLogger(std::string_view hostName, std::string_view programName, bool simple = false) :
    Logger(hostName, programName, DEBUG), m_simple(simple) { }

  /**
   * Destructor
   */
  ~StdoutLogger() final = default;

  /**
   * Prepares the logger object for a call to fork()
   *
   * No further calls to operator() should be made after calling this
   * method until the call to fork() has completed.
   */
  void prepareForFork() final { /* intentionally-blank override of pure virtual method */ }

  /**
   * Refresh the underlying logger setup
   */
  void refresh() final { /* intentionally-blank override of pure virtual method */ }

protected:
  /**
   * Writes the specified msg to the underlying logging system
   *
   * This method is to be implemented by concrete sub-classes of the Logger class.
   *
   * Please note it is the responsibility of a concrete sub-class to decide
   * whether or not to use the specified log message header.
   *
   * @param header The header of the message to be logged. It is the responsibility of the concrete sub-class.
   * @param body The body of the message to be logged.
   */
  void writeMsgToUnderlyingLoggingSystem(std::string_view header, std::string_view body) final;

private:
    bool m_simple;
};

} // namespace cta::log
