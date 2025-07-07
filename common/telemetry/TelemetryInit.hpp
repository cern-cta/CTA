/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */
#pragma once

#include <opentelemetry/metrics/meter.h>

#include "config/TelemetryConfig.hpp"
#include "common/log/LogContext.hpp"

namespace cta::telemetry {

/**
 * Initialises telemetry according to the provided config.
 * Note that this method is not "fork-safe". That is, before doing a fork, reset the MeterProvider by calling shutdownTelemetry
 * Immediately after the fork, call reinitTelemetry. Provided that there are no meters actively being instantiated at the time of forking,
 * this will ensure both parent and child process end up with correct state.
 *
 * If this function is not called, any telemetry-related functionality will be a NOOP
 */
void initTelemetry(const TelemetryConfig& config, cta::log::LogContext& lc);

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
void reinitTelemetry(cta::log::LogContext& lc, bool persistServiceInstanceId = false);

/**
 * Sets telemetry to NOOP and flushes any remaining metrics still in the buffer.
 */
void shutdownTelemetry(cta::log::LogContext& lc);

}  // namespace cta::telemetry
