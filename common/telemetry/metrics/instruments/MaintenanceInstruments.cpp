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
#include "MaintenanceInstruments.hpp"

#include <opentelemetry/metrics/provider.h>

#include "version.h"
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"

namespace cta::telemetry::metrics {
  std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaMaintenanceGarbageCollectorCount; 
  std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaMaintenanceDiskReporterCount;
  std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaMaintenanceQueueCleanupCount;
  std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaMaintenanceRepackReportingCount;
  std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaMaintenanceRepackExpansionCount;
}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaMaintenance, CTA_VERSION);

  cta::telemetry::metrics::ctaMaintenanceGarbageCollectorCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaMaintenanceGarbageCollectorCount,
                               cta::semconv::metrics::descrCtaMaintenanceGarbageCollectorCount,
                               cta::semconv::metrics::unitCtaMaintenanceGarbageCollectorCount);

  cta::telemetry::metrics::ctaMaintenanceDiskReporterCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaMaintenanceDiskReporterCount,
                               cta::semconv::metrics::descrCtaMaintenanceDiskReporterCount,
                               cta::semconv::metrics::unitCtaMaintenanceDiskReporterCount);

  cta::telemetry::metrics::ctaMaintenanceQueueCleanupCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaMaintenanceRepackReportingCount,
                               cta::semconv::metrics::descrCtaMaintenanceRepackReportingCount,
                               cta::semconv::metrics::unitCtaMaintenanceRepackReportingCount);

  cta::telemetry::metrics::ctaMaintenanceRepackReportingCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaMaintenanceRepackExpansionCount,
                               cta::semconv::metrics::descrCtaMaintenanceRepackExpansionCount,
                               cta::semconv::metrics::unitCtaMaintenanceRepackExpansionCount);

  cta::telemetry::metrics::ctaMaintenanceRepackExpansionCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaMaintenanceQueueCleanupCount,
                               cta::semconv::metrics::descrCtaMaintenanceQueueCleanupCount,
                               cta::semconv::metrics::unitCtaMaintenanceQueueCleanupCount);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
  
} // namespace
