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
#include "RdbmsInstruments.hpp"

#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> dbClientOperationDuration;
std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> dbClientConnectionCount;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaRdbms, CTA_VERSION);

  cta::telemetry::metrics::dbClientOperationDuration =
    meter->CreateUInt64Histogram(cta::semconv::metrics::kMetricDbClientOperationDuration,
                                 cta::semconv::metrics::descrDbClientOperationDuration,
                                 cta::semconv::metrics::unitDbClientOperationDuration);

  cta::telemetry::metrics::dbClientConnectionCount =
    meter->CreateInt64UpDownCounter(cta::semconv::metrics::kMetricDbClientConnectionCount,
                                    cta::semconv::metrics::descrDbClientConnectionCount,
                                    cta::semconv::metrics::unitDbClientConnectionCount);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
