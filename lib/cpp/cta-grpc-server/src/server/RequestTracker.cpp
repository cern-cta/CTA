/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/server/RequestTracker.hpp"

#include "common/semconv/Attributes.hpp"
#include "telemetry/metrics/FrontendMetrics.hpp"

#include <opentelemetry/context/runtime_context.h>
#include <utility>

namespace cta::grpc_server {

RequestTracker::RequestTracker(std::string_view eventName, std::string_view requesterName)
    : m_eventName(eventName),
      m_requesterName(requesterName) {
  cta::telemetry::metrics::ctaFrontendActiveRequests->Add(
    1,
    {
      {cta::semconv::attr::kFrontendRequesterName, m_requesterName}
  });
}

RequestTracker::RequestTracker(RequestTracker&& other) noexcept
    : m_timer(other.m_timer),
      m_eventName(std::move(other.m_eventName)),
      m_requesterName(std::move(other.m_requesterName)),
      m_errorType(std::move(other.m_errorType)),
      m_isActive(std::exchange(other.m_isActive, false)) {}

RequestTracker::~RequestTracker() {
  if (!m_isActive) {
    return;
  }
  cta::telemetry::metrics::ctaFrontendActiveRequests->Add(
    -1,
    {
      {cta::semconv::attr::kFrontendRequesterName, m_requesterName}
  });
  if (m_errorType) {
    cta::telemetry::metrics::ctaFrontendRequestDuration->Record(
      m_timer.msecs(),
      {
        {cta::semconv::attr::kEventName,             m_eventName        },
        {cta::semconv::attr::kFrontendRequesterName, m_requesterName    },
        {cta::semconv::attr::kErrorType,             m_errorType.value()}
    },
      opentelemetry::context::RuntimeContext::GetCurrent());

  } else {
    cta::telemetry::metrics::ctaFrontendRequestDuration->Record(
      m_timer.msecs(),
      {
        {cta::semconv::attr::kEventName,             m_eventName    },
        {cta::semconv::attr::kFrontendRequesterName, m_requesterName}
    },
      opentelemetry::context::RuntimeContext::GetCurrent());
  }
}

void RequestTracker::setErrorType(std::string_view errorType) {
  m_errorType = errorType;
}

}  // namespace cta::grpc_server
