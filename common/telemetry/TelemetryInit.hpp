#pragma once

#include "config/TelemetryConfig.hpp"
#include <opentelemetry/metrics/meter.h>

namespace cta::telemetry {

/**
 * Initialises telemetry according to the provided config
 */
void initTelemetry(const TelemetryConfig& config);

/**
 * Provided that initTelemetry has been called at some point in the past,
 * this method will reuse the original telemetry config to reinitialise telemetry
 */
void reinitTelemetry();

/**
 * Sets telemetry to NOOP
 */
void resetTelemetry();

}  // namespace cta::telemetry
