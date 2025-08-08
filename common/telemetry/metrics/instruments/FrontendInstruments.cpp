
#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> frontendRequestDurationHistogram;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.frontend");
  // Instrument initialisation

  cta::telemetry::metrics::frontendRequestDurationHistogram =
    meter->CreateUInt64Histogram("cta.frontend.request.duration",
                                 "Duration in milliseconds to serve a frontend request",
                                 "ms");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
