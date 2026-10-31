/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {

/**
 * Duration to execute an RMC request handler, in seconds.
 */
extern std::unique_ptr<opentelemetry::metrics::Histogram<double>> ctaRmcdRequestDuration;

/** Duration of one SCSI command attempt, in seconds. */
extern std::unique_ptr<opentelemetry::metrics::Histogram<double>> ctaRmcdScsiCommandDuration;

}  // namespace cta::telemetry::metrics
