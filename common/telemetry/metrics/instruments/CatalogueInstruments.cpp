
#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> catalogueQueryCounter;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> catalogueQueryErrorCounter;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.catalogue");

  // Instrument initialisation

  cta::telemetry::metrics::catalogueQueryCounter =
    meter->CreateUInt64Counter("catalogue.query.count", "Total number of queries executed", "1");

  cta::telemetry::metrics::catalogueQueryErrorCounter =
    meter->CreateUInt64Counter("catalogue.query.error_count", "Total number of queries resulting in an error", "1");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
