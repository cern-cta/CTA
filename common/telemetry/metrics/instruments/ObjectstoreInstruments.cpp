#include "ObjectstoreInstruments.hpp"

#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"
#include "common/semconv/SemConv.hpp"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaObjectstoreLockAcquireDuration;

}  // namespace cta::telemetry::metrics

namespace {

void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.objectstore", CTA_VERSION);

  cta::telemetry::metrics::ctaObjectstoreLockAcquireDuration =
    meter->CreateUInt64Histogram("cta.objectstore.lock.acquire.duration",
                                 "Duration taken to acquire an objectstore lock",
                                 "ms");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
