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

#include "common/log/StringLogger.hpp"
#include "common/threading/MutexLocker.hpp"

namespace cta::log {

//-----------------------------------------------------------------------------
// writeMsgToUnderlyingLoggingSystem
//-----------------------------------------------------------------------------
void StringLogger::writeMsgToUnderlyingLoggingSystem(std::string_view header, std::string_view body) {
  // enter critical section
  threading::MutexLocker lock(m_mutex);

  // Append the message to the log
  m_log << (m_logFormat == LogFormat::JSON ? "{" : "")
        << header << body
        << (m_logFormat == LogFormat::JSON ? "}" : "")
        << std::endl;
}

} // namespace cta::log
