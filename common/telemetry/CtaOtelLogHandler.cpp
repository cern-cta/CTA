/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "CtaOtelLogHandler.hpp"

#include "common/log/Constants.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"

#include <opentelemetry/sdk/common/attribute_utils.h>
#include <variant>

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

// All scalar types of OwnedAttributeValue
// See: https://github.com/open-telemetry/opentelemetry-cpp/blob/main/sdk/include/opentelemetry/sdk/common/attribute_utils.h#L36
template<typename T>
constexpr bool is_supported_scalar_v =
  std::is_same_v<T, bool> || std::is_same_v<T, int32_t> || std::is_same_v<T, uint32_t> || std::is_same_v<T, int64_t>
  || std::is_same_v<T, uint64_t> || std::is_same_v<T, double> || std::is_same_v<T, std::string>;

CtaOtelLogHandler::CtaOtelLogHandler(log::Logger& log) : m_log(log) {}

CtaOtelLogHandler::~CtaOtelLogHandler() = default;

void CtaOtelLogHandler::Handle(opentelemetry::sdk::common::internal_log::LogLevel level,
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
