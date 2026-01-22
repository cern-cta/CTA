/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {

/**
 * Duration of database client operations.
 */
extern std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> dbClientOperationDuration;

/**
 * The actual number of records returned by the database operation.
 */
extern std::unique_ptr<opentelemetry::metrics::Histogram<int64_t>> dbClientOperationReturnedRows;

/**
 * The number of connections that are currently in state described by the state attribute.
 */
extern std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> dbClientConnectionCount;

}  // namespace cta::telemetry::metrics
