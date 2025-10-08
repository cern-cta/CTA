/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "ObjectstoreInstruments.hpp"

#include <opentelemetry/metrics/provider.h>

#include "version.h"
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaObjectstoreLockAcquireDuration;

}  // namespace cta::telemetry::metrics

namespace {

void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaObjectstore, CTA_VERSION);

  cta::telemetry::metrics::ctaObjectstoreLockAcquireDuration =
    meter->CreateUInt64Histogram(cta::semconv::metrics::kMetricCtaObjectstoreLockAcquireDuration,
                                 cta::semconv::metrics::descrCtaObjectstoreLockAcquireDuration,
                                 cta::semconv::metrics::unitCtaObjectstoreLockAcquireDuration);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
