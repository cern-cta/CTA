#include "FrontendInstruments.hpp"

#include <opentelemetry/metrics/provider.h>

#include "version.h"
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaFrontendRequestDuration;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaFrontend, CTA_VERSION);

  cta::telemetry::metrics::ctaFrontendRequestDuration =
    meter->CreateUInt64Histogram(cta::semconv::metrics::kMetricCtaFrontendRequestDuration,
                                 cta::semconv::metrics::descrCtaFrontendRequestDuration,
                                 cta::semconv::metrics::unitCtaFrontendRequestDuration);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
