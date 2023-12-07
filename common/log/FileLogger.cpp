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
FileLogger::FileLogger(const std::string &hostName, const std::string &programName, const std::string &filePath, const int logMask):
  Logger(hostName, programName, logMask) {
  m_fd = ::open(filePath.c_str(), O_APPEND | O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP);
  cta::exception::Errnum::throwOnMinusOne(m_fd, std::string("In FileLogger::FileLogger(): failed to open log file: ") + filePath);
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
void FileLogger::writeMsgToUnderlyingLoggingSystem(const std::string &header, const std::string &body) {
  if (-1 == m_fd) {
    throw cta::exception::Exception("In FileLogger::writeMsgToUnderlyingLoggingSystem(): file is not properly initialized");
  }

  const std::string headerPlusBody = header + body;

  // Prepare the string to print
  std::string m = headerPlusBody + "\n";
  
  // enter critical section
  threading::MutexLocker lock(m_mutex);
  
  // Append the message to the file
  cta::exception::Errnum::throwOnMinusOne(::write(m_fd, m.c_str(), m.size()), 
      "In FileLogger::writeMsgToUnderlyingLoggingSystem(): failed to write to file");
}

} // namespace cta::log
