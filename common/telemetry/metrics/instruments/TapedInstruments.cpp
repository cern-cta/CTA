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
#include "TapedInstruments.hpp"

#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/telemetry/metrics/MetricsUtils.hpp"
#include "version.h"

#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferFileCount;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferFileSize;
std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> ctaTapedTransferActive;
std::shared_ptr<opentelemetry::metrics::ObservableInstrument> ctaTapedBufferUsage;
std::shared_ptr<opentelemetry::metrics::ObservableInstrument> ctaTapedBufferLimit;
std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaTapedMountDuration;
std::shared_ptr<opentelemetry::metrics::ObservableInstrument> ctaTapedMountType;
std::shared_ptr<opentelemetry::metrics::ObservableInstrument> CtaTapedDriveStatus;

}  // namespace cta::telemetry::metrics

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaTaped, CTA_VERSION);

  cta::telemetry::metrics::ctaTapedTransferFileCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaTapedTransferFileCount,
                               cta::semconv::metrics::descrCtaTapedTransferFileCount,
                               cta::semconv::metrics::unitCtaTapedTransferFileCount);

  cta::telemetry::metrics::ctaTapedTransferFileSize =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaTapedTransferFileSize,
                               cta::semconv::metrics::descrCtaTapedTransferFileSize,
                               cta::semconv::metrics::unitCtaTapedTransferFileSize);

  cta::telemetry::metrics::ctaTapedTransferActive =
    meter->CreateInt64UpDownCounter(cta::semconv::metrics::kMetricCtaTapedTransferActive,
                                    cta::semconv::metrics::descrCtaTapedTransferActive,
                                    cta::semconv::metrics::unitCtaTapedTransferActive);

  cta::telemetry::metrics::ctaTapedBufferUsage =
    meter->CreateInt64ObservableGauge(cta::semconv::metrics::kMetricCtaTapedBufferUsage,
                                      cta::semconv::metrics::descrCtaTapedBufferUsage,
                                      cta::semconv::metrics::unitCtaTapedBufferUsage);

  cta::telemetry::metrics::ctaTapedBufferLimit =
    meter->CreateInt64ObservableGauge(cta::semconv::metrics::kMetricCtaTapedBufferLimit,
                                      cta::semconv::metrics::descrCtaTapedBufferLimit,
                                      cta::semconv::metrics::unitCtaTapedBufferLimit);

  cta::telemetry::metrics::ctaTapedMountDuration =
    meter->CreateUInt64Histogram(cta::semconv::metrics::kMetricCtaTapedMountDuration,
                                 cta::semconv::metrics::descrCtaTapedMountDuration,
                                 cta::semconv::metrics::unitCtaTapedMountDuration);

  cta::telemetry::metrics::ctaTapedMountType =
    meter->CreateInt64ObservableUpDownCounter(cta::semconv::metrics::kMetricCtaTapedMountType,
                                              cta::semconv::metrics::descrCtaTapedMountType,
                                              cta::semconv::metrics::unitCtaTapedMountType);

  cta::telemetry::metrics::CtaTapedDriveStatus =
    meter->CreateInt64ObservableUpDownCounter(cta::semconv::metrics::kMetricCtaTapedDriveStatus,
                                              cta::semconv::metrics::descrCtaTapedDriveStatus,
                                              cta::semconv::metrics::unitCtaTapedDriveStatus);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
