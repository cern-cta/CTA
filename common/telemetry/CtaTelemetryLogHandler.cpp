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

template<typename T>
constexpr bool is_supported_scalar_v =
  std::is_same_v<T, bool> || std::is_same_v<T, int32_t> || std::is_same_v<T, uint32_t> || std::is_same_v<T, int64_t> ||
  std::is_same_v<T, double> || std::is_same_v<T, std::string>;

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
  for (const auto& [key, ownedAttribute] : attrs) {
    std::visit(
      [&](const auto& val) {
        if constexpr (is_supported_scalar_v<std::decay_t<decltype(val)>>) {
          params.add(key, val);
        } else {
          params.add(key, "<unsupported nested value>");
        }
      },
      ownedAttribute);
  }
  lc.log(toSyslogLevel(level), "OTLP " + opentelemetry::sdk::common::internal_log::LevelToString(level));
}

}  // namespace cta::telemetry
