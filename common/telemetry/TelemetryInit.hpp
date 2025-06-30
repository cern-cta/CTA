#pragma once

#include "config/TelemetryConfig.hpp"
#include <opentelemetry/metrics/meter.h>

namespace cta::telemetry {

/**
 * Initialises telemetry according to the provided config.
 * Note that this method is not "fork-safe". That is, before doing a fork, reset the MeterProvider by calling resetTelemetry
 * Immediately after the fork, call reinitTelemetry. Provided that there are no meters actively being instantiated at the time of forking,
 * this will ensure both parent and child process end up with correct state.
 *
 * If this function is not called, any telemetry-related functionality will be a NOOP
 */
void initTelemetry(const TelemetryConfig& config);

/**
 * Initialises the telemetry config, but not telemetry itself
 * After this has been used, reinitTelemetry() can be used.
 */
void initTelemetryConfig(const TelemetryConfig& config);

/**
 * This method will reuse the existing telemetry config to reinitialise telemetry.
 * Requires initTelemetry or initTelemetryConfig to have been called prior to this.
 *
 */
void reinitTelemetry();

/**
 * Sets telemetry to NOOP
 */
void resetTelemetry();

}  // namespace cta::telemetry
