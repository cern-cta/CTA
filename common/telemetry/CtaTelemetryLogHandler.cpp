/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
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

#include "CtaTelemetryLogHandler.hpp"
#include "common/log/Constants.hpp"

namespace cta::telemetry {

int toSyslogLevel(opentelemetry::sdk::common::internal_log::LogLevel level) noexcept {
  switch (level) {
    using enum opentelemetry::sdk::common::internal_log::LogLevel;
    case Error:
      return cta::log::ERR;
    case Warning:
      return cta::log::WARNING;
    case Info:
      return cta::log::INFO;
    case Debug:
      return cta::log::DEBUG;
    case None:
    default:
      return cta::log::INFO;
  }
}

CtaTelemetryLogHandler::CtaTelemetryLogHandler(log::Logger& log) : m_log(log) {}

CtaTelemetryLogHandler::~CtaTelemetryLogHandler() = default;

void CtaTelemetryLogHandler::Handle(opentelemetry::sdk::common::internal_log::LogLevel level,
                                    const char* file,
                                    int line,
                                    const char* msg,
                                    const opentelemetry::sdk::common::AttributeMap& attributes) noexcept {
  // We ignore the file, line numbers and attributes as they just add unnecessary noise
  m_log(toSyslogLevel(level), msg);
}

}  // namespace cta::telemetry
