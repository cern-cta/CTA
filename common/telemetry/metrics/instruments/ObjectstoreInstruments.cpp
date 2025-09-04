#include "ObjectstoreInstruments.hpp"

#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"
#include "common/semconv/SemConv.hpp"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<double>> objectstoreLockAcquireDuration;

}  // namespace cta::telemetry::metrics

namespace {

void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.objectstore", CTA_VERSION);

  // Instrument initialisation

  cta::telemetry::metrics::objectstoreLockAcquireDuration = meter->CreateDoubleHistogram(
    "cta.objectstore.lock.acquire.duration",
    "Duration taken to acquire a lock, measured from wait start to acquisition",
    "s");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
