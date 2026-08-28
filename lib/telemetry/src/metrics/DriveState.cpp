/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "telemetry/metrics/DriveState.hpp"

#include <atomic>

namespace cta::telemetry::metrics {
namespace {
using Status = common::dataStructures::DriveStatus;
using MountType = common::dataStructures::MountType;
// State updates must never wait for a telemetry reader or exporter.
static_assert(std::atomic<Status>::is_always_lock_free);
static_assert(std::atomic<MountType>::is_always_lock_free);
std::atomic<Status> currentStatus {Status::Unknown};
std::atomic<MountType> currentMountType {MountType::NoMount};
}  // namespace

void setDriveStatus(Status status) noexcept {
  currentStatus.store(status, std::memory_order_relaxed);
}

Status getDriveStatus() noexcept {
  return currentStatus.load(std::memory_order_relaxed);
}

void setMountType(MountType type) noexcept {
  currentMountType.store(type, std::memory_order_relaxed);
}

MountType getMountType() noexcept {
  return currentMountType.load(std::memory_order_relaxed);
}

}  // namespace cta::telemetry::metrics
