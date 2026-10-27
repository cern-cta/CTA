/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "common/utils/Timer.hpp"

#include <optional>
#include <source_location>
#include <string>

namespace cta::grpc_server {

/// @brief Logs the start and the outcome of a single request, along with its processing time
///
/// The logger keeps its own copy of the log context: the logged params are the ones present in the context when the
/// logger is created. Being copyable, it can follow a request which outlives the scope it was started in (e.g. a
/// streaming RPC, handed over to its reactor).
class RequestLogger {
public:
  /// @brief Start the timer and log that the request is being handled
  ///
  /// @param lc           Log context carrying the request params
  /// @param requestName  Short description of the request in the log messages (in lower case)
  /// @param location     Source location reported in the log message
  RequestLogger(const log::LogContext& lc,
                std::string requestName,
                std::source_location location = std::source_location::current());

  /// @brief Log that the request succeeded
  void succeeded(std::source_location location = std::source_location::current());

  /// @brief Log that the request was rejected because of a user error
  ///
  /// @param reason  Error message returned to the client
  void rejected(const std::string& reason, std::source_location location = std::source_location::current());

  /// @brief Log that the request failed
  ///
  /// @param reason  Error message returned to the client
  void failed(const std::string& reason, std::source_location location = std::source_location::current());

private:
  /// @brief Log the outcome of the request, with its status, reason (if any) and processing time
  void logOutcome(int priority,
                  const std::string& message,
                  const std::string& status,
                  const std::optional<std::string>& reason,
                  std::source_location location);

  /// Copy of the context the request was started with
  log::LogContext m_lc;
  /// Name of the request, capitalized to start the outcome log messages
  std::string m_requestName;
  /// Message logged when the request starts
  std::string m_handlingMessage;
  /// Measures the request processing time, started on construction
  utils::Timer m_timer;
};

}  // namespace cta::grpc_server
