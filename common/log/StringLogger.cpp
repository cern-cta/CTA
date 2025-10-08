/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
