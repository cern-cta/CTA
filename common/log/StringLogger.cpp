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

#include "common/log/StringLogger.hpp"
#include "common/log/SyslogLogger.hpp"
#include "common/threading/MutexLocker.hpp"

namespace cta {
namespace log {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
StringLogger::StringLogger(const std::string &programName,
  const int logMask):
  Logger(programName, logMask) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
StringLogger::~StringLogger() {
}

//------------------------------------------------------------------------------
// prepareForFork
//------------------------------------------------------------------------------
void StringLogger::prepareForFork() {
}

//-----------------------------------------------------------------------------
// reducedSyslog
//-----------------------------------------------------------------------------
void StringLogger::reducedSyslog(const std::string & msg) {
  // enter critical section
  threading::MutexLocker lock(m_mutex);

  // Append the message to the log (truncated to the maximum length)
  m_log << msg.substr(0, m_maxMsgLen) << std::endl;

  // Uncomment this to get the logs printed to stdout during unit tests.
  // printf ("%s\n", msg.substr(0, m_maxMsgLen).c_str());
}

} // namespace log
} // namespace cta