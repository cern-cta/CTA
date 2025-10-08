/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/log/Logger.hpp"
#include "common/threading/Mutex.hpp"

namespace cta::log {

/**
 * Class implementaing the API of the CTA logging system
 */
class StringLogger : public Logger {
  using Logger::Logger;

public:
  /**
   * Destructor
   */
  ~StringLogger() final = default;

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

  /**
   * Extractor for the resulting logs
   */
  std::string getLog() { return m_log.str(); }

  /**
   * Clear the log
   */
  void clearLog() { m_log.str(""); }

protected:
  /**
   * Mutex used to protect the critical section of the StringLogger object
   */
  threading::Mutex m_mutex;

  /**
   * The file descriptor of the socket used to send messages to syslog
   */
  std::stringstream m_log;

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
};

} // namespace cta::log
