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
#include <atomic>
#include <opentelemetry/metrics/provider.h>

namespace {
// Each taped process owns one drive. Callbacks retain no session or catalogue pointers.
std::atomic<cta::common::dataStructures::MountType> mountType {cta::common::dataStructures::MountType::NoMount};

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
  const auto current = mountType.load(std::memory_order_relaxed);
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

void setMountType(common::dataStructures::MountType type) noexcept {
  mountType.store(type, std::memory_order_relaxed);
}

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
  // Reinitialization must not leave callbacks attached to the previous instruments.
  if (cta::telemetry::metrics::ctaTapedMountType) {
    cta::telemetry::metrics::ctaTapedMountType->RemoveCallback(observeMountType, nullptr);
  }
  if (cta::telemetry::metrics::CtaTapedDriveStatus) {
    cta::telemetry::metrics::CtaTapedDriveStatus->RemoveCallback(observeDriveStatus, nullptr);
  }
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

  cta::telemetry::metrics::ctaTapedMountType->AddCallback(observeMountType, nullptr);
  cta::telemetry::metrics::CtaTapedDriveStatus->AddCallback(observeDriveStatus, nullptr);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
