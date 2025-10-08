/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "SchedulerInstruments.hpp"

#include <opentelemetry/metrics/provider.h>

#include "version.h"
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaSchedulerOperationDuration;

}  // namespace cta::telemetry::metrics

namespace {

void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaScheduler, CTA_VERSION);

  cta::telemetry::metrics::ctaSchedulerOperationDuration =
    meter->CreateUInt64Histogram(cta::semconv::metrics::kMetricCtaSchedulerOperationDuration,
                                 cta::semconv::metrics::descrCtaSchedulerOperationDuration,
                                 cta::semconv::metrics::unitCtaSchedulerOperationDuration);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
