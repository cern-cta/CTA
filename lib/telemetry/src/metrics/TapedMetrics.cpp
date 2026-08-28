/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "telemetry/metrics/TapedMetrics.hpp"

#include "common/semconv/Attributes.hpp"
#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"
#include "telemetry/metrics/InstrumentRegistry.hpp"
#include "telemetry/metrics/MetricsUtils.hpp"
#include "version.hpp"

#include <array>
#include <opentelemetry/metrics/provider.h>

namespace {
void observeDriveStatus(opentelemetry::metrics::ObserverResult result, void*) noexcept {
  const auto observer = std::get_if<std::shared_ptr<opentelemetry::metrics::ObserverResultT<int64_t>>>(&result);
  if (!observer) {
    return;
  }
  const auto current = cta::telemetry::metrics::getDriveStatus();
  // Emit every category so inactive values become zero rather than disappearing.
  for (const auto status : cta::common::dataStructures::AllDriveStatuses) {
    (*observer)->Observe(status == current ? 1 : 0,
                         {
                           {cta::semconv::attr::kCtaTapedDriveState, cta::common::dataStructures::toString(status)}
    });
  }
}

void observeMountType(opentelemetry::metrics::ObserverResult result, void*) noexcept {
  const auto observer = std::get_if<std::shared_ptr<opentelemetry::metrics::ObserverResultT<int64_t>>>(&result);
  if (!observer) {
    return;
  }
  const auto current = cta::telemetry::metrics::getMountType();
  using enum cta::common::dataStructures::MountType;
  // Retain the existing categories; Label and ArchiveAllTypes are not active mount types.
  constexpr std::array types {ArchiveForUser, ArchiveForRepack, Retrieve, NoMount};
  for (const auto type : types) {
    (*observer)->Observe(
      type == current ? 1 : 0,
      {
        {cta::semconv::attr::kCtaTapedMountType, cta::common::dataStructures::toCamelCaseString(type)}
    });
  }
}
}  // namespace

namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferFileCount;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferFileSize;
std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> ctaTapedTransferActive;
std::shared_ptr<opentelemetry::metrics::ObservableInstrument> ctaTapedBufferUsage;
std::shared_ptr<opentelemetry::metrics::ObservableInstrument> ctaTapedBufferLimit;
std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaTapedMountDuration;
std::shared_ptr<opentelemetry::metrics::ObservableInstrument> ctaTapedMountType;
std::shared_ptr<opentelemetry::metrics::ObservableInstrument> CtaTapedDriveStatus;

ScopedTapedStateMetrics::ScopedTapedStateMetrics()
    : m_mountType(ctaTapedMountType),
      m_driveStatus(CtaTapedDriveStatus) {
  m_mountType->AddCallback(observeMountType, nullptr);
  m_driveStatus->AddCallback(observeDriveStatus, nullptr);
}

ScopedTapedStateMetrics::~ScopedTapedStateMetrics() {
  m_mountType->RemoveCallback(observeMountType, nullptr);
  m_driveStatus->RemoveCallback(observeDriveStatus, nullptr);
}

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
