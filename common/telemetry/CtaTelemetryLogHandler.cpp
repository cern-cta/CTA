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

#include <variant>
#include <opentelemetry/sdk/common/attribute_utils.h>

#include "CtaTelemetryLogHandler.hpp"
#include "common/log/Constants.hpp"
#include "common/log/Logger.hpp"
#include "common/log/LogContext.hpp"

namespace cta::telemetry {

int toSyslogLevel(opentelemetry::sdk::common::internal_log::LogLevel level) noexcept {
  switch (level) {
    using enum opentelemetry::sdk::common::internal_log::LogLevel;
    case Error:
      return cta::log::WARNING;  // Telemetry errors do not affect the service, so we emit them as warnings
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

// Copied from https://github.com/open-telemetry/opentelemetry-cpp/blob/main/exporters/prometheus/src/exporter_utils.cc#L743
// For some reason this functionality is not defined in a common place
std::string AttributeValueToString(const opentelemetry::sdk::common::OwnedAttributeValue& value) {
  std::string result;
  if (std::holds_alternative<bool>(value)) {
    result = std::get<bool>(value) ? "true" : "false";
  } else if (std::holds_alternative<int>(value)) {
    result = std::to_string(std::get<int>(value));
  } else if (std::holds_alternative<int64_t>(value)) {
    result = std::to_string(std::get<int64_t>(value));
  } else if (std::holds_alternative<unsigned int>(value)) {
    result = std::to_string(std::get<unsigned int>(value));
  } else if (std::holds_alternative<uint64_t>(value)) {
    result = std::to_string(std::get<uint64_t>(value));
  } else if (std::holds_alternative<double>(value)) {
    result = std::to_string(std::get<double>(value));
  } else if (std::holds_alternative<std::string>(value)) {
    result = std::get<std::string>(value);
  } else {
    return "<unsupported nested value>";
  }
  return result;
}

CtaTelemetryLogHandler::CtaTelemetryLogHandler(log::Logger& log) : m_log(log) {}

CtaTelemetryLogHandler::~CtaTelemetryLogHandler() = default;

void CtaTelemetryLogHandler::Handle(opentelemetry::sdk::common::internal_log::LogLevel level,
                                    const char* file,
                                    int line,
                                    const char* msg,
                                    const opentelemetry::sdk::common::AttributeMap& attributes) noexcept {
  cta::log::LogContext lc(m_log);
  cta::log::ScopedParamContainer params(lc);
  params.add("otlpMessage", msg);
  const auto& attrs = attributes.GetAttributes();
  for (const auto& [key, val] : attrs) {
    params.add(key, AttributeValueToString(val));
  }
  lc.log(toSyslogLevel(level), "OTLP " + opentelemetry::sdk::common::internal_log::LevelToString(level));
}

}  // namespace cta::telemetry
