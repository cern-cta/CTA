/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */
#include "SchedulerInstruments.hpp"

#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaSchedulerOperationDuration;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaSchedulerDiskReportCount;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaSchedulerRepackReportCount;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaSchedulerRepackExpandCount;

}  // namespace cta::telemetry::metrics

namespace {

void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaScheduler, CTA_VERSION);

  cta::telemetry::metrics::ctaSchedulerOperationDuration =
    meter->CreateUInt64Histogram(cta::semconv::metrics::kMetricCtaSchedulerOperationDuration,
                                 cta::semconv::metrics::descrCtaSchedulerOperationDuration,
                                 cta::semconv::metrics::unitCtaSchedulerOperationDuration);

  cta::telemetry::metrics::ctaSchedulerDiskReportCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaSchedulerDiskReportCount,
                               cta::semconv::metrics::descrCtaSchedulerDiskReportCount,
                               cta::semconv::metrics::unitCtaSchedulerDiskReportCount);

  cta::telemetry::metrics::ctaSchedulerRepackReportCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaSchedulerRepackReportCount,
                               cta::semconv::metrics::descrCtaSchedulerRepackReportCount,
                               cta::semconv::metrics::unitCtaSchedulerRepackReportCount);

  cta::telemetry::metrics::ctaSchedulerRepackExpandCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaSchedulerRepackExpandCount,
                               cta::semconv::metrics::descrCtaSchedulerRepackExpandCount,
                               cta::semconv::metrics::unitCtaSchedulerRepackExpandCount);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
