/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/log/StdoutLogger.hpp"

#include <iostream>
#include <syncstream>

namespace cta::log {

//------------------------------------------------------------------------------
// writeMsgToUnderlyingLoggingSystem
//------------------------------------------------------------------------------
void StdoutLogger::writeMsgToUnderlyingLoggingSystem(std::string_view header, std::string_view body) {
  const bool json = m_logFormat == LogFormat::JSON;

  // use an osyncstream, which accumulates the output until `.emit()` is called
  // (or the object goes out of scope)
  std::osyncstream out {std::cout};

  if (json) {
    out << '{';
  }
  if (!m_simple) {
    out << header;
  }
  out << body;
  if (json) {
    out << '}';
  }
  out << '\n' << std::flush;

  // this shouldn't be needed, as it happens on destruction, but
  // for the sake of explicitness...
  out.emit();
}

}  // namespace cta::log
