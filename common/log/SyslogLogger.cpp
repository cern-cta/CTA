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

#include "common/exception/Exception.hpp"
#include "common/log/SyslogLogger.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/utils/utils.hpp"

#include <errno.h>
#include <fstream>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>

namespace cta::log {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SyslogLogger::SyslogLogger(std::string_view hostName, std::string_view programName, const int logMask):
  Logger(hostName, programName, logMask) {
  const int option = 0;
  const int facility = 0;
  openlog(m_programName.data(), option, facility);
}

//-----------------------------------------------------------------------------
// writeMsgToLoggingSystem
//-----------------------------------------------------------------------------
void SyslogLogger::writeMsgToUnderlyingLoggingSystem(std::string_view header, std::string_view body) {
  // Prepare the string to print. Explicitly ignore the message header as this will be provided by rsyslog.
  std::ostringstream logLine;
  logLine << (m_logFormat == LogFormat::JSON ? "{" : "")
          << body
          << (m_logFormat == LogFormat::JSON ? "}" : "");

  syslog(LOG_LOCAL3|INFO, "%s", logLine.str().c_str());
}

} // namespace cta::log
