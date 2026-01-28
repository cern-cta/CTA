/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/log/LogContext.hpp"

namespace cta::telemetry {

/**
 * Initialises telemetry according to the provided config. This config file uses the declarative configuration format from OpenTelemetry.
 *
 * If this function is not called, any telemetry-related functionality will be a NOOP.
 * This function is not safe to use in a forked environment.
 */
void initOpenTelemetry(const std::string& configFile,
                       const std::map<std::string, std::string>& ctaResourceAttributes,
                       cta::log::LogContext& lc);

/**
 * Sets telemetry to NOOP and flushes any remaining metrics still in the buffer.
 */
void cleanupOpenTelemetry(cta::log::LogContext& lc);

}  // namespace cta::telemetry
