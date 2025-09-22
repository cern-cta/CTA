#include "FrontendInstruments.hpp"

#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"
#include "common/semconv/SemConv.hpp"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaFrontendRequestDuration;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.frontend", CTA_VERSION);
  // Instrument initialisation

  /**
   * Loosely based on https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/
   * We don't use the rpc metric because we only instrument the core method behind it; not the full RPC call.
   */
  cta::telemetry::metrics::ctaFrontendRequestDuration =
    meter->CreateUInt64Histogram("cta.frontend.request.duration", "Duration the frontend takes to process a request.", "ms");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
