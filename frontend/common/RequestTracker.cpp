/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RequestTracker.hpp"

#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/FrontendInstruments.hpp"

#include <opentelemetry/context/runtime_context.h>

namespace cta::frontend {

RequestTracker::RequestTracker(std::string_view eventName, std::string_view requesterName)
    : m_eventName(eventName),
      m_requesterName(requesterName) {
  cta::telemetry::metrics::ctaFrontendActiveRequests->Add(
    1,
    {
      {cta::semconv::attr::kFrontendRequesterName, m_requesterName}
  });
}

RequestTracker::~RequestTracker() {
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

}  // namespace cta::frontend
