/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/dataStructures/MountType.hpp"
#include "telemetry/metrics/DriveStatus.hpp"

#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {

// Record local lifecycle decisions independently of backend publication.
void setMountType(common::dataStructures::MountType type) noexcept;

extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferFileCount;
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferFileSize;
extern std::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> ctaTapedTransferActive;
extern std::shared_ptr<opentelemetry::metrics::ObservableInstrument> ctaTapedBufferUsage;
extern std::shared_ptr<opentelemetry::metrics::ObservableInstrument> ctaTapedBufferLimit;
extern std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaTapedMountDuration;
extern std::shared_ptr<opentelemetry::metrics::ObservableInstrument> ctaTapedMountType;
extern std::shared_ptr<opentelemetry::metrics::ObservableInstrument> CtaTapedDriveStatus;

}  // namespace cta::telemetry::metrics
