/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <opentelemetry/metrics/meter.h>

namespace cta::telemetry::metrics {

/**
 * Convenience function to obtain a meter for a given library name and version.
 * @param libraryName The name of the library you are instrumenting
 * @param libraryVersion The version of the library you are instrumenting.
 */
std::shared_ptr<opentelemetry::metrics::Meter> getMeter(std::string_view libraryName, std::string_view libraryVersion);

}  // namespace cta::telemetry::metrics
