/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {

/**
 * Duration the frontend takes to process a request.
 */
extern std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaFrontendRequestDuration;
/**
 * Number of active requests in the CTA frontend.
 */
extern std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> ctaFrontendActiveRequests;

}  // namespace cta::telemetry::metrics
