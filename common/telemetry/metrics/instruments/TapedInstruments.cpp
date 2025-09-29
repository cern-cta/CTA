#include "TapedInstruments.hpp"

#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransfers;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedMounts;
std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> ctaTapedThreadPoolSize;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.taped", CTA_VERSION);

  // It seems that there is a common .io suffix used, but this is only used for data flow in bytes.
  // Here we count files; not bytes.... Maybe we should be counting bytes? In that case: cta.taped.io
  cta::telemetry::metrics::ctaTapedTransfers =
    meter->CreateUInt64Counter("cta.taped.transfers", "Number of file transfers to/from tape", "1");

  cta::telemetry::metrics::ctaTapedMounts =
    meter->CreateUInt64Counter("cta.taped.mounts", "Number of tape mounts", "1");

  cta::telemetry::metrics::ctaTapedThreadPoolSize =
    meter->CreateInt64UpDownCounter("cta.taped.thread_pool.size",
                                    "Number of active threads in the cta taped tape pools",
                                    "1");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
