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

#include "common/log/FileLogger.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/exception/Errnum.hpp"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace cta::log {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
FileLogger::FileLogger(std::string_view hostName, std::string_view programName, const std::string& filePath, int logMask) :
  Logger(hostName, programName, logMask), m_filePath(filePath) {
  m_fd = ::open(m_filePath.data(), O_APPEND | O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH );
  exception::Errnum::throwOnMinusOne(m_fd, std::string("In FileLogger::FileLogger(): failed to open log file: ") + std::string(m_filePath));
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
  logLine << (m_logFormat == LogFormat::JSON ? "{" : "")
          << header << body
          << (m_logFormat == LogFormat::JSON ? "}" : "")
          << std::endl;

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
    m_fd = ::open(m_filePath.data(), O_APPEND | O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH );
    exception::Errnum::throwOnMinusOne(m_fd, std::string("In FileLogger::FileLogger(): failed to refresh log file: ") +
                                             std::string(m_filePath));
  }
  operator()(INFO, "Log file descriptor reopened");
}

} // namespace cta::log
