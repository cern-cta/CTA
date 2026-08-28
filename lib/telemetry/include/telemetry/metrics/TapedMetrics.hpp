/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "telemetry/metrics/DriveState.hpp"

#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {

// Observe taped state only while this scope is alive.
// Telemetry must be initialized first; only one scope may exist at a time.
class ScopedTapedStateMetrics final {
public:
  ScopedTapedStateMetrics();
  ~ScopedTapedStateMetrics();

  ScopedTapedStateMetrics(const ScopedTapedStateMetrics&) = delete;
  ScopedTapedStateMetrics& operator=(const ScopedTapedStateMetrics&) = delete;
  ScopedTapedStateMetrics(ScopedTapedStateMetrics&&) = delete;
  ScopedTapedStateMetrics& operator=(ScopedTapedStateMetrics&&) = delete;

private:
  // Retain the exact instruments used for callback registration until removal.
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument> m_mountType;
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument> m_driveStatus;
};

extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferFileCount;
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferFileSize;
extern std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> ctaTapedTransferActive;
extern std::shared_ptr<opentelemetry::metrics::ObservableInstrument> ctaTapedBufferUsage;
extern std::shared_ptr<opentelemetry::metrics::ObservableInstrument> ctaTapedBufferLimit;
extern std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaTapedMountDuration;
extern std::shared_ptr<opentelemetry::metrics::ObservableInstrument> ctaTapedMountType;
extern std::shared_ptr<opentelemetry::metrics::ObservableInstrument> CtaTapedDriveStatus;

}  // namespace cta::telemetry::metrics
