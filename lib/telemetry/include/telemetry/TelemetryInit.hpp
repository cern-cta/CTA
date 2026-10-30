/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/log/LogContext.hpp"
#include "config/TelemetryConfig.hpp"

#include <opentelemetry/metrics/meter.h>

namespace cta::telemetry {

/**
 * Initialises telemetry according to the provided config.
 * Note that this method is not "fork-safe". That is, before doing a fork, reset the MeterProvider by calling shutdownTelemetry.
 * Immediately after the fork, call reinitTelemetry. Provided that there are no meters actively being instantiated at the time of forking,
 * this will ensure both parent and child process end up with correct state.
 *
 * If this function is not called, any telemetry-related functionality will be a NOOP
 */
void initTelemetry(const TelemetryConfig& config, cta::log::LogContext& lc);

/**
 * Initialises the telemetry config, but not telemetry itself. Useful if the locations where the config is initialised and telemetry itself is initialised are different.
 * After this has been used, reinitTelemetry() can be used.
 */
void initTelemetryConfig(const TelemetryConfig& config);

/**
 * This method will reuse the existing telemetry config to reinitialise telemetry.
 * Requires initTelemetry or initTelemetryConfig to have been called prior to this.
 *
 */
void reinitTelemetry(cta::log::LogContext& lc);

/**
 * Like reinitTelemetry, but first sleeps for a hash-derived fraction of the
 * configured export interval so that many instances (e.g. 100 drive pods) do
 * not all export simultaneously.  No-op when the backend is NOOP.
 *
 * @param instanceId  A string unique to this instance (e.g. drive name).
 *                    The sleep duration is hash(instanceId) % (exportInterval / 2).
 */
void reinitTelemetryWithJitter(const std::string& instanceId, cta::log::LogContext& lc);

/**
 * Sets telemetry to NOOP and flushes any remaining metrics still in the buffer.
 */
void shutdownTelemetry(cta::log::LogContext& lc);

}  // namespace cta::telemetry
