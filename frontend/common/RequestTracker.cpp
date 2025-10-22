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

#include <opentelemetry/context/runtime_context.h>

#include "RequestTracker.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/FrontendInstruments.hpp"

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
