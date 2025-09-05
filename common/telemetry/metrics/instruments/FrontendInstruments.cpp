#include "FrontendInstruments.hpp"

#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"
#include "common/semconv/SemConv.hpp"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> frontendRequestDuration;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.frontend", CTA_VERSION);
  // Instrument initialisation

  // This should probably be replaced/augmented by: https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/
  // Also note, according to the specs this should technically be in seconds.
  // However, opentelemetry-cpp currently does not provide a nice way to set explicit bucket boundaries.
  // See: https://github.com/open-telemetry/opentelemetry-cpp/issues/2132
  cta::telemetry::metrics::frontendRequestDuration =
    meter->CreateUInt64Histogram("cta.frontend.request.duration",
                                 "Duration to serve a frontend request",
                                 "ms");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
