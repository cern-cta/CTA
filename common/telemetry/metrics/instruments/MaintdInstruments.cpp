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
#include "MaintdInstruments.hpp"

#include <opentelemetry/metrics/provider.h>

#include "version.h"
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"

namespace cta::telemetry::metrics {
  std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaMaintdGarbageCollectorCount;
  std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaMaintdDiskReporterCount;
  std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaMaintdQueueCleanupCount;
  std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaMaintdRepackReportingCount;
  std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaMaintdRepackExpansionCount;
}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaMaintd, CTA_VERSION);

  cta::telemetry::metrics::ctaMaintdGarbageCollectorCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaMaintdGarbageCollectorCount,
                               cta::semconv::metrics::descrCtaMaintdGarbageCollectorCount,
                               cta::semconv::metrics::unitCtaMaintdGarbageCollectorCount);

  cta::telemetry::metrics::ctaMaintdDiskReporterCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaMaintdDiskReporterCount,
                               cta::semconv::metrics::descrCtaMaintdDiskReporterCount,
                               cta::semconv::metrics::unitCtaMaintdDiskReporterCount);

  cta::telemetry::metrics::ctaMaintdQueueCleanupCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaMaintdRepackReportingCount,
                               cta::semconv::metrics::descrCtaMaintdRepackReportingCount,
                               cta::semconv::metrics::unitCtaMaintdRepackReportingCount);

  cta::telemetry::metrics::ctaMaintdRepackReportingCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaMaintdRepackExpansionCount,
                               cta::semconv::metrics::descrCtaMaintdRepackExpansionCount,
                               cta::semconv::metrics::unitCtaMaintdRepackExpansionCount);

  cta::telemetry::metrics::ctaMaintdRepackExpansionCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaMaintdQueueCleanupCount,
                               cta::semconv::metrics::descrCtaMaintdQueueCleanupCount,
                               cta::semconv::metrics::unitCtaMaintdQueueCleanupCount);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);

} // namespace
