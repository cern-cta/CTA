#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MeterProvider.hpp"
#include "version.h"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> schedulerQueueingCounter;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> objectstoreLockAcquireCounter;
std::unique_ptr<opentelemetry::metrics::Histogram<double>> objectstoreLockAcquireDurationHistogram;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.scheduler");

  // Instrument initialisation

  cta::telemetry::metrics::schedulerQueueingCounter =
    meter->CreateUInt64Counter("scheduler.queueing.count", "Total number of files enqueued in the scheduler");
  cta::telemetry::metrics::objectstoreLockAcquireCounter =
    meter->CreateUInt64Counter("objectstore.lock.acquire.count", "Total number of locks acquired on the objectstore");
  cta::telemetry::metrics::objectstoreLockAcquireDurationHistogram =
    meter->CreateDoubleHistogram("objectstore.lock.acquire.duration",
                               "Duration in seconds taken to acquire a lock, measured from wait start to acquisition");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
