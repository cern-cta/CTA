
#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> databaseQueryDurationHistogram;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> databaseQueryErrorCounter;
std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> databaseConnectionsUpDownCounter;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.database");

  // Instrument initialisation

  cta::telemetry::metrics::databaseQueryDurationHistogram =
    meter->CreateUInt64Histogram("cta.database.query.duration",
                                 "Duration in milliseconds to execute a database query",
                                 "ms");

  cta::telemetry::metrics::databaseQueryErrorCounter =
    meter->CreateUInt64Counter("cta.database.query_error.count", "Total number of queries resulting in an error", "1");


  cta::telemetry::metrics::databaseConnectionsUpDownCounter =
    meter->CreateInt64UpDownCounter("cta.database.connections.count", "Total number of active DB connections", "1");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
