/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * Interface to the CASTOR logging system
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "common/exception/Exception.hpp"
#include "common/log/SyslogLogger.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/utils/utils.hpp"

#include <errno.h>
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

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::log::SyslogLogger::SyslogLogger(const std::string &programName, const int logMask):
  Logger(programName, logMask) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::log::SyslogLogger::~SyslogLogger() {
}

//------------------------------------------------------------------------------
// prepareForFork
//------------------------------------------------------------------------------
void cta::log::SyslogLogger::prepareForFork() {
}

//-----------------------------------------------------------------------------
// reducedSyslog
//-----------------------------------------------------------------------------
void cta::log::SyslogLogger::reducedSyslog(const std::string& msg) {
  // Truncate the log message if it exceeds the permitted maximum
  std::string truncatedMsg = msg.substr(0, m_maxMsgLen);

  syslog(LOG_LOCAL3|LOG_INFO, truncatedMsg.c_str());
}
