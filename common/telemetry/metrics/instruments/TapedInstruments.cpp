#include "TapedInstruments.hpp"

#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> tapedTransferCount;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> tapedMountCount;
std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> tapedThreadCount;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.taped", CTA_VERSION);

  // Instrument initialisation

  cta::telemetry::metrics::tapedTransferCount =
    meter->CreateUInt64Counter("cta.taped.transfer.count", "Total number of transfers to/from tape", "1");

  cta::telemetry::metrics::tapedMountCount =
    meter->CreateUInt64Counter("cta.taped.mount.count", "Total number of tape mounts", "1");

  cta::telemetry::metrics::tapedThreadCount =
    meter->CreateInt64UpDownCounter("cta.taped.thread.count", "Total number of active threads", "1");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
