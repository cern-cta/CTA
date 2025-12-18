/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "FrontendInstruments.hpp"

#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaFrontendRequestDuration;
std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> ctaFrontendActiveRequests;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaFrontend, CTA_VERSION);

  cta::telemetry::metrics::ctaFrontendRequestDuration =
    meter->CreateUInt64Histogram(cta::semconv::metrics::kMetricCtaFrontendRequestDuration,
                                 cta::semconv::metrics::descrCtaFrontendRequestDuration,
                                 cta::semconv::metrics::unitCtaFrontendRequestDuration);

  cta::telemetry::metrics::ctaFrontendActiveRequests =
    meter->CreateInt64UpDownCounter(cta::semconv::metrics::kMetricCtaFrontendActiveRequests,
                                    cta::semconv::metrics::descrCtaFrontendActiveRequests,
                                    cta::semconv::metrics::unitCtaFrontendActiveRequests);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
