#include "TapedInstruments.hpp"

#include <opentelemetry/metrics/provider.h>

#include "version.h"
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferCount;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferIO;
std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaTapedMountDuration;
std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> ctaTapedThreadPoolSize;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaTaped, CTA_VERSION);

  cta::telemetry::metrics::ctaTapedTransferCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaTapedTransferCount,
                               cta::semconv::metrics::descrCtaTapedTransferCount,
                               cta::semconv::metrics::unitCtaTapedTransferCount);

  cta::telemetry::metrics::ctaTapedTransferIO =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaTapedTransferIO,
                               cta::semconv::metrics::descrCtaTapedTransferIO,
                               cta::semconv::metrics::unitCtaTapedTransferIO);

  cta::telemetry::metrics::ctaTapedMountDuration =
    meter->CreateUInt64Histogram(cta::semconv::metrics::kMetricCtaTapedMountDuration,
                                 cta::semconv::metrics::descrCtaTapedMountDuration,
                                 cta::semconv::metrics::unitCtaTapedMountDuration);

  cta::telemetry::metrics::ctaTapedThreadPoolSize =
    meter->CreateInt64UpDownCounter(cta::semconv::metrics::kMetricCtaTapedThreadPoolSize,
                                    cta::semconv::metrics::descrCtaTapedThreadPoolSize,
                                    cta::semconv::metrics::unitCtaTapedThreadPoolSize);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
