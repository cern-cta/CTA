/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

namespace cta {
namespace log {
  

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SyslogLogger::SyslogLogger(const std::string &hostName, const std::string &programName, const int logMask):
  Logger(hostName, programName, logMask) {
  const int option = 0;
  const int facility = 0;
  openlog(m_programName.c_str(), option, facility);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
SyslogLogger::~SyslogLogger() {
}

//------------------------------------------------------------------------------
// prepareForFork
//------------------------------------------------------------------------------
void SyslogLogger::prepareForFork() {
}

//-----------------------------------------------------------------------------
// writeMsgToLoggingSystem
//-----------------------------------------------------------------------------
void SyslogLogger::writeMsgToUnderlyingLoggingSystem(const std::string &header, const std::string &body) {
  // Explicitly ignore the message header as this will be provided by rsyslog
  syslog(LOG_LOCAL3|INFO, body.c_str());
}

} // namespace log
} // namespace cta
