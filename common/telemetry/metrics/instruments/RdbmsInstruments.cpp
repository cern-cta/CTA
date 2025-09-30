#include "RdbmsInstruments.hpp"

#include <opentelemetry/metrics/provider.h>

#include "version.h"
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> dbClientOperationDuration;
std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> dbClientConnectionCount;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaRdbms, CTA_VERSION);

  cta::telemetry::metrics::dbClientOperationDuration =
    meter->CreateUInt64Histogram(cta::semconv::metrics::kMetricDbClientOperationDuration,
                                 cta::semconv::metrics::descrDbClientOperationDuration,
                                 cta::semconv::metrics::unitDbClientOperationDuration);

  cta::telemetry::metrics::dbClientConnectionCount =
    meter->CreateInt64UpDownCounter(cta::semconv::metrics::kMetricDbClientConnectionCount,
                                    cta::semconv::metrics::descrDbClientConnectionCount,
                                    cta::semconv::metrics::unitDbClientConnectionCount);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
