#pragma once

#include "config/TelemetryConfig.hpp"
#include <opentelemetry/metrics/meter.h>

namespace cta::telemetry {

void initTelemetry(const TelemetryConfig& config);
void reinitTelemetry();

}  // namespace cta::telemetry
