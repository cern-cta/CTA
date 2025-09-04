#include "FrontendInstruments.hpp"

#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"
#include "common/semconv/SemConv.hpp"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<double>> frontendRequestDuration;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.frontend", CTA_VERSION);
  // Instrument initialisation

  // This should probably be replaced/augmented by: https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/
  cta::telemetry::metrics::frontendRequestDuration =
    meter->CreateDoubleHistogram("cta.frontend.request.duration",
                                 "Duration to serve a frontend request",
                                 "s");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
