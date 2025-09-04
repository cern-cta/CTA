#include "RdbmsInstruments.hpp"

#include <opentelemetry/metrics/provider.h>
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"
#include <opentelemetry/semconv/incubating/process_attributes.h>

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<double>> rdbmsOperationDuration;
std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> rdbmsConnectionCount;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter("cta.rdbms", CTA_VERSION);

  // Instrument initialisation


  // See https://opentelemetry.io/docs/specs/semconv/database/database-metrics/#metric-dbclientoperationduration
  // TODO: specify explicit bucket boundaries with seconds as units to follow the spec
  cta::telemetry::metrics::rdbmsOperationDuration =
    meter->CreateDoubleHistogram("db.client.operation.duration",
                                 "Duration of database client operations.",
                                 "s");

  // See https://opentelemetry.io/docs/specs/semconv/database/database-metrics/#metric-dbclientconnectioncount
  cta::telemetry::metrics::rdbmsConnectionCount =
    meter->CreateInt64UpDownCounter("db.client.connection.count", "The number of connections that are currently in state described by the state attribute.", "1");
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
