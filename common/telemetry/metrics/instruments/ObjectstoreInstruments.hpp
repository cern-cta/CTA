/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {

/**
 * Duration taken to acquire an objectstore lock.
 */
extern std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaObjectstoreLockAcquireDuration;

/**
 * Number of garbage collected agents.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaObjectstoreGcAgentCount;

/**
 * Number of garbage collected objects as a result of agent cleanup.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaObjectstoreGcObjectCount;

/**
 * Number of queues cleaned up.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaObjectstoreCleanupQueueCount;

/**
 * Number of files moved as a result of queue cleanup.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaObjectstoreCleanupFileCount;

}  // namespace cta::telemetry::metrics
