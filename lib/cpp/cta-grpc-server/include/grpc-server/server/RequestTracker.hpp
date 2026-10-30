/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/utils/Timer.hpp"

#include <optional>
#include <string>

namespace cta::grpc_server {

/// @brief RAII helper recording telemetry for a single frontend request.
///
/// Increments ctaFrontendActiveRequests on construction and decrements it on destruction, at which point
/// the request duration is also recorded in ctaFrontendRequestDuration. The tracker can be moved, to follow a request
/// which outlives the scope it was started in (e.g. a streaming RPC, handed over to its reactor).
class RequestTracker {
public:
  /// @brief Start tracking a request and increment the active request counter.
  ///
  /// @param eventName Name of the request event, used as the event name attribute of the duration metric
  /// @param requesterName Name of the requester, used as the requester name attribute of both metrics
  RequestTracker(std::string_view eventName, std::string_view requesterName);

  /// @brief Stop tracking the request: decrement the active request counter and record the request duration,
  /// tagged with the error type if one was set.
  ~RequestTracker();

  RequestTracker(const RequestTracker&) = delete;
  RequestTracker& operator=(const RequestTracker&) = delete;

  /// @brief Take over the tracking of a request: the moved-from tracker no longer records anything
  RequestTracker(RequestTracker&& other) noexcept;
  /// Deleted: assigning to a live tracker would silently end its own request
  RequestTracker& operator=(RequestTracker&&) = delete;

  /// @brief Mark the request as failed.
  ///
  /// @param errorType Error type attached to the duration metric when the tracker is destroyed
  void setErrorType(std::string_view errorType);

private:
  /// Measures the request duration, started on construction
  utils::Timer m_timer;
  /// Event name attribute of the duration metric
  std::string m_eventName;
  /// Requester name attribute of the active requests and duration metrics
  std::string m_requesterName;
  /// Error type attribute of the duration metric; unset on success
  std::optional<std::string> m_errorType;
  /// False once moved from: the destructor then leaves the metrics alone
  bool m_isActive = true;
};

}  // namespace cta::grpc_server
