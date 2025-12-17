/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/log/FileLogger.hpp"

#include "common/exception/Errnum.hpp"
#include "common/process/threading/MutexLocker.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace cta::log {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
FileLogger::FileLogger(std::string_view hostName,
                       std::string_view programName,
                       const std::string& filePath,
                       int logMask)
    : Logger(hostName, programName, logMask),
      m_filePath(filePath) {
  m_fd = ::open(m_filePath.data(), O_APPEND | O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  exception::Errnum::throwOnMinusOne(m_fd,
                                     std::string("In FileLogger::FileLogger(): failed to open log file: ")
                                       + std::string(m_filePath));
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
FileLogger::~FileLogger() {
  if (-1 != m_fd) {
    ::close(m_fd);
  }
}

//-----------------------------------------------------------------------------
// writeMsgToUnderlyingLoggingSystem
//-----------------------------------------------------------------------------
void FileLogger::writeMsgToUnderlyingLoggingSystem(std::string_view header, std::string_view body) {
  if (-1 == m_fd) {
    throw exception::Exception("In FileLogger::writeMsgToUnderlyingLoggingSystem(): file is not properly initialised");
  }

  // Prepare the string to print
  std::ostringstream logLine;
  logLine << (m_logFormat == LogFormat::JSON ? "{" : "") << header << body
          << (m_logFormat == LogFormat::JSON ? "}" : "") << std::endl;

  // Append the message to the file
  threading::MutexLocker lock(m_mutex);
  exception::Errnum::throwOnMinusOne(::write(m_fd, logLine.str().c_str(), logLine.str().size()),
                                     "In FileLogger::writeMsgToUnderlyingLoggingSystem(): failed to write to file");
}

//------------------------------------------------------------------------------
// refresh
//------------------------------------------------------------------------------
void FileLogger::refresh() {
  // In the case of FileLogger this means getting a new fd (to rotate the log file).
  operator()(INFO, "Refreshing log file descriptor");
  {
    threading::MutexLocker lock(m_mutex);
    if (-1 != m_fd) {
      ::close(m_fd);
    }
    m_fd = ::open(m_filePath.data(), O_APPEND | O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    exception::Errnum::throwOnMinusOne(m_fd,
                                       std::string("In FileLogger::FileLogger(): failed to refresh log file: ")
                                         + std::string(m_filePath));
  }
  operator()(INFO, "Log file descriptor reopened");
}

}  // namespace cta::log
