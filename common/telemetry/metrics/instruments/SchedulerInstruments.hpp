/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {

/**
 * Duration of a CTA scheduling operation.
 */
extern std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaSchedulerOperationDuration;

}  // namespace cta::telemetry::metrics
