
#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> frontendRequestCounter;
std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> frontendRequestDurationHistogram;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.frontend");
  // Instrument initialisation

  cta::telemetry::metrics::frontendRequestCounter =
    meter->CreateUInt64Counter("frontend.request.count", "Total number of requests served by the frontend", "1");

  cta::telemetry::metrics::frontendRequestDurationHistogram =
    meter->CreateUInt64Histogram("frontend.request.duration",
                                 "Duration in milliseconds to serve a frontend request",
                                 "ms");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
