/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {

/**
 * Duration to execute a routine of the given type.
 */
extern std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaMaintdRoutineDuration;

}  // namespace cta::telemetry::metrics
