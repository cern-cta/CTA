#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> schedulerQueueingCounter;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> objectstoreLockAcquireCounter;
std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> objectstoreLockAcquireDurationHistogram;

}  // namespace cta::telemetry::metrics

namespace {

void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.scheduler");

  // Instrument initialisation

  cta::telemetry::metrics::schedulerQueueingCounter =
    meter->CreateUInt64Counter("scheduler.queueing.count", "Total number of files enqueued in the scheduler", "1");
  cta::telemetry::metrics::objectstoreLockAcquireCounter =
    meter->CreateUInt64Counter("objectstore.lock.acquire.count",
                               "Total number of locks acquired on the objectstore",
                               "1");

  cta::telemetry::metrics::objectstoreLockAcquireDurationHistogram = meter->CreateUInt64Histogram(
    "objectstore.lock.acquire.duration",
    "Duration in milliseconds taken to acquire a lock, measured from wait start to acquisition",
    "ms");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
