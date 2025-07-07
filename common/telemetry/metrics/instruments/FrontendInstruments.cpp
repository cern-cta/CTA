
#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MeterProvider.hpp"
#include "version.h"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> frontendRequestCounter;
std::unique_ptr<opentelemetry::metrics::Histogram<double>> frontendRequestDurationHistogram;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.frontend");
  // Instrument initialisation

  cta::telemetry::metrics::frontendRequestCounter =
    meter->CreateUInt64Counter("frontend.request.count", "Total number of requests served by the frontend");
  cta::telemetry::metrics::frontendRequestDurationHistogram =
    meter->CreateDoubleHistogram("frontend.request.duration", "Duration in seconds to serve a frontend request");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
