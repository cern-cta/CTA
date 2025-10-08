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
class FileLogger: public Logger {
public:
  /**
   * Constructor
   *
   * @param hostName The host name to be prepended to every log message.
   * @param programName The name of the program to be prepended to every log message.
   * @param filePath path to the log file.
   * @param logMask The log mask.
   */
  FileLogger(std::string_view hostName, std::string_view programName, const std::string& filePath, int logMask);

  /**
   * Destructor
   */
  ~FileLogger();

  /**
   * Prepares the logger object for a call to fork().
   *
   * No further calls to operator() should be made after calling this
   * method until the call to fork() has completed.
   */
  void prepareForFork() final { /* intentionally-blank override of pure virtual method */ }

  /**
   * Refresh the underlying logger setup
   */
  void refresh() final;

protected:
  /**
   * Mutex used to protect the critical section of the StringLogger object.
   */
  threading::Mutex m_mutex;

  /**
   * The output file handle
   */
  int m_fd = -1;

  /**
   * Log file path
   */
  const std::string m_filePath;

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
