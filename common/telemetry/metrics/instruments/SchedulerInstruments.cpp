#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> schedulerQueueingCounter;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> schedulerRepackCounter;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> schedulerTapeStateChangeCounter;
std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> objectstoreLockAcquireDurationHistogram;

}  // namespace cta::telemetry::metrics

namespace {

void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.scheduler");

  // Instrument initialisation

  cta::telemetry::metrics::schedulerQueueingCounter =
    meter->CreateUInt64Counter("cta.scheduler.queueing.count", "Total number of transfer events in the scheduler", "1");
  cta::telemetry::metrics::schedulerRepackCounter =
    meter->CreateUInt64Counter("cta.scheduler.repack.count", "Total number of repack events in the scheduler", "1");
  cta::telemetry::metrics::schedulerTapeStateChangeCounter =
    meter->CreateUInt64Counter("cta.scheduler.tape.state_change.count", "Total number of state changes for all tapes", "1");


  cta::telemetry::metrics::objectstoreLockAcquireDurationHistogram = meter->CreateUInt64Histogram(
    "cta.objectstore.lock.acquire.duration",
    "Duration in milliseconds taken to acquire a lock, measured from wait start to acquisition",
    "ms");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
