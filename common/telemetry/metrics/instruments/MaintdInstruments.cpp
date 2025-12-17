/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "MaintdInstruments.hpp"

#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {
std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaMaintdRoutineDuration;
}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaMaintd, CTA_VERSION);

  cta::telemetry::metrics::ctaMaintdRoutineDuration =
    meter->CreateUInt64Histogram(cta::semconv::metrics::kMetricCtaMaintdRoutineDuration,
                                 cta::semconv::metrics::descrCtaMaintdRoutineDuration,
                                 cta::semconv::metrics::unitCtaMaintdRoutineDuration);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);

}  // namespace
