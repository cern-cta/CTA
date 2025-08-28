
#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> tapedTransferCounter;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> tapedMountCounter;
std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> tapedDiskThreadUpDownCounter;
std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> tapedTapeThreadUpDownCounter;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.taped");

  // Instrument initialisation

  cta::telemetry::metrics::tapedTransferCounter =
    meter->CreateUInt64Counter("cta.taped.transfer.count", "Total number of transfers to/from tape", "1");

  cta::telemetry::metrics::tapedMountCounter =
    meter->CreateUInt64Counter("cta.taped.mount.count", "Total number of tape mounts", "1");

  cta::telemetry::metrics::tapedDiskThreadUpDownCounter =
    meter->CreateInt64UpDownCounter("cta.taped.disk_thread.count", "Total number of active disk threads", "1");
  cta::telemetry::metrics::tapedTapeThreadUpDownCounter =
    meter->CreateInt64UpDownCounter("cta.taped.tape_thread.count", "Total number of active tape threads", "1");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
