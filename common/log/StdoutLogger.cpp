/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <iostream>

#include "common/log/StdoutLogger.hpp"

namespace cta::log {

//------------------------------------------------------------------------------
// writeMsgToUnderlyingLoggingSystem
//------------------------------------------------------------------------------
void StdoutLogger::writeMsgToUnderlyingLoggingSystem(std::string_view header, std::string_view body) {
  std::cout << (m_logFormat == LogFormat::JSON ? "{" : "")
            << (m_simple ? "" : header)
            << body
            << (m_logFormat == LogFormat::JSON ? "}" : "")
            << std::endl;
}

} // namespace cta::log
