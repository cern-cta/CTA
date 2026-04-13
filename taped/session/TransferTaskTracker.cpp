/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TransferTaskTracker.hpp"

#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/TapedInstruments.hpp"

#include <opentelemetry/context/runtime_context.h>

namespace castor::tape::tapeserver::daemon {

TransferTaskTracker::TransferTaskTracker(std::string_view ioDirection, std::string_view ioMedium)
    : m_ioDirection(ioDirection),
      m_ioMedium(ioMedium) {
  cta::telemetry::metrics::ctaTapedTransferActive->Add(1,
                                                       {
                                                         {cta::semconv::attr::kCtaIoDirection, m_ioDirection},
                                                         {cta::semconv::attr::kCtaIoMedium,    m_ioMedium   }
  });
}

TransferTaskTracker::~TransferTaskTracker() {
  cta::telemetry::metrics::ctaTapedTransferActive->Add(-1,
                                                       {
                                                         {cta::semconv::attr::kCtaIoDirection, m_ioDirection},
                                                         {cta::semconv::attr::kCtaIoMedium,    m_ioMedium   }
  });
}

}  // namespace castor::tape::tapeserver::daemon
