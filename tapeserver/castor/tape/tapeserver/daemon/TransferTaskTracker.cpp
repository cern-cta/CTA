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

#include "TransferTaskTracker.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/TapedInstruments.hpp"

namespace castor::tape::tapeserver::daemon {

TransferTaskTracker::TransferTaskTracker(std::string_view ioDirection, std::string_view ioMedium)
    : m_ioDirection(ioDirection),
      m_ioMedium(ioMedium) {
  cta::telemetry::metrics::ctaTapedTransferActive->Add(
    1,
    {
      {cta::semconv::attr::kCtaIoDirection, m_ioDirection},
      {cta::semconv::attr::kCtaIoDirection, m_ioMedium}
  });
}

TransferTaskTracker::~TransferTaskTracker() {
  cta::telemetry::metrics::ctaTapedTransferActive->Add(
    -1,
    {
      {cta::semconv::attr::kCtaIoDirection, m_ioDirection},
      {cta::semconv::attr::kCtaIoDirection, m_ioMedium}
  });
}

}  // namespace castor::tape::tapeserver::daemon
