/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
