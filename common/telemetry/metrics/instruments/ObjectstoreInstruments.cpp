/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "ObjectstoreInstruments.hpp"

#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaObjectstoreLockAcquireDuration;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaObjectstoreGcAgentCount;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaObjectstoreGcObjectCount;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaObjectstoreCleanupQueueCount;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaObjectstoreCleanupFileCount;

}  // namespace cta::telemetry::metrics

namespace {

void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaObjectstore, CTA_VERSION);

  cta::telemetry::metrics::ctaObjectstoreLockAcquireDuration =
    meter->CreateUInt64Histogram(cta::semconv::metrics::kMetricCtaObjectstoreLockAcquireDuration,
                                 cta::semconv::metrics::descrCtaObjectstoreLockAcquireDuration,
                                 cta::semconv::metrics::unitCtaObjectstoreLockAcquireDuration);

  cta::telemetry::metrics::ctaObjectstoreGcAgentCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaObjectstoreGcAgentCount,
                               cta::semconv::metrics::descrCtaObjectstoreGcAgentCount,
                               cta::semconv::metrics::unitCtaObjectstoreGcAgentCount);

  cta::telemetry::metrics::ctaObjectstoreGcObjectCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaObjectstoreGcObjectCount,
                               cta::semconv::metrics::descrCtaObjectstoreGcObjectCount,
                               cta::semconv::metrics::unitCtaObjectstoreGcObjectCount);

  cta::telemetry::metrics::ctaObjectstoreCleanupQueueCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaObjectstoreCleanupQueueCount,
                               cta::semconv::metrics::descrCtaObjectstoreCleanupQueueCount,
                               cta::semconv::metrics::unitCtaObjectstoreCleanupQueueCount);

  cta::telemetry::metrics::ctaObjectstoreCleanupFileCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaObjectstoreCleanupFileCount,
                               cta::semconv::metrics::descrCtaObjectstoreCleanupFileCount,
                               cta::semconv::metrics::unitCtaObjectstoreCleanupFileCount);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
