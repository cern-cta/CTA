#pragma once

#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/metrics/meter.h>
#include <string>

namespace cta::telemetry::metrics {

opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Meter> getMeter(const std::string& component_name);

}  // namespace cta::telemetry::metrics
