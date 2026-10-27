/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/server/RequestLogger.hpp"

#include "common/log/LogLevel.hpp"

#include <cctype>
#include <utility>

namespace cta::grpc_server {

namespace {

/// @brief Copy of the string with its first letter in upper case, to start a log message with it
std::string capitalized(std::string str) {
  if (!str.empty()) {
    str.front() = static_cast<char>(std::toupper(static_cast<unsigned char>(str.front())));
  }
  return str;
}

}  // namespace

RequestLogger::RequestLogger(const log::LogContext& lc, std::string requestName, std::source_location location)
    : m_lc(lc),
      m_requestName(capitalized(requestName)),
      m_handlingMessage("Handling " + std::move(requestName)) {
  m_lc.log(log::INFO, m_handlingMessage, location);
}

void RequestLogger::succeeded(std::source_location location) {
  logOutcome(log::INFO, m_requestName + " succeeded", "success", std::nullopt, location);
}

void RequestLogger::rejected(const std::string& reason, std::source_location location) {
  logOutcome(log::INFO, m_requestName + " rejected due to a user error", "user_error", reason, location);
}

void RequestLogger::failed(const std::string& reason, std::source_location location) {
  logOutcome(log::ERR, m_requestName + " failed", "failure", reason, location);
}

void RequestLogger::logOutcome(int priority,
                               const std::string& message,
                               const std::string& status,
                               const std::optional<std::string>& reason,
                               std::source_location location) {
  log::ScopedParamContainer params(m_lc);
  params.add("status", status);

  if (reason.has_value()) {
    params.add("reason", reason.value());
  }

  params.add("requestTime", m_timer.secs());
  m_lc.log(priority, message, location);
}

}  // namespace cta::grpc_server
