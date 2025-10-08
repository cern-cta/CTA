/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {

/**
 * Number of files transferred to/from tape.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferCount;

/**
 * Bytes transferred to/from tape.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferIO;

/**
 * Duration to mount a tape.
 */
extern std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaTapedMountDuration;

}  // namespace cta::telemetry::metrics
