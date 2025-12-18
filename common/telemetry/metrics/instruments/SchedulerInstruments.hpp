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

/**
 * Number of files of the given type reported to disk.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaSchedulerDiskReportCount;

/**
 * Number of files of the given type reported for repack.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaSchedulerRepackReportCount;

/**
 * Number of repack requests expanded.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaSchedulerRepackExpandCount;

}  // namespace cta::telemetry::metrics
