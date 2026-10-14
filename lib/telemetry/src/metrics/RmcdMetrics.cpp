/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "telemetry/metrics/RmcdMetrics.hpp"

#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"
#include "telemetry/metrics/InstrumentRegistry.hpp"
#include "telemetry/metrics/MetricsUtils.hpp"
#include "version.hpp"

#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {
std::unique_ptr<opentelemetry::metrics::Histogram<double>> ctaRmcdRequestDuration;
std::unique_ptr<opentelemetry::metrics::Histogram<double>> ctaRmcdScsiCommandDuration;
}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaRmcd, CTA_VERSION);

  cta::telemetry::metrics::ctaRmcdRequestDuration =
    meter->CreateDoubleHistogram(cta::semconv::metrics::kMetricCtaRmcdRequestDuration,
                                  cta::semconv::metrics::descrCtaRmcdRequestDuration,
                                  cta::semconv::metrics::unitCtaRmcdRequestDuration);

  cta::telemetry::metrics::ctaRmcdScsiCommandDuration =
    meter->CreateDoubleHistogram(cta::semconv::metrics::kMetricCtaRmcdScsiCommandDuration,
                                  cta::semconv::metrics::descrCtaRmcdScsiCommandDuration,
                                  cta::semconv::metrics::unitCtaRmcdScsiCommandDuration);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);

}  // namespace
