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

#include "common/log/StringLogger.hpp"
#include "common/log/SyslogLogger.hpp"
#include "common/threading/MutexLocker.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::log::StringLogger::StringLogger(const std::string &programName,
  const int logMask):
  Logger(programName, logMask) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::log::StringLogger::~StringLogger() {
}

//------------------------------------------------------------------------------
// prepareForFork
//------------------------------------------------------------------------------
void cta::log::StringLogger::prepareForFork() {
}

//-----------------------------------------------------------------------------
// reducedSyslog
//-----------------------------------------------------------------------------
void cta::log::StringLogger::reducedSyslog(const std::string & msg) {
  // enter critical section
  threading::MutexLocker lock(m_mutex);

  // Append the message to the log (truncated to the maximum length)
  m_log << msg.substr(0, m_maxMsgLen) << std::endl;

  // Uncomment this to get the logs printed to stdout during unit tests.
  // printf (msg.substr(0, m_maxMsgLen).c_str());
}
