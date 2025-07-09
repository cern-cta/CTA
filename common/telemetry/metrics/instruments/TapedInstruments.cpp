
#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> tapedTransferCounter;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> tapedMountCounter;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.taped");

  // Instrument initialisation

  cta::telemetry::metrics::tapedTransferCounter =
    meter->CreateUInt64Counter("taped.transfer.count", "Total number of transfers to/from tape", "1");

  cta::telemetry::metrics::tapedMountCounter =
    meter->CreateUInt64Counter("taped.mount.count", "Total number of tape mounts", "1");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
