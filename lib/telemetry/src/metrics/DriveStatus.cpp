/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "telemetry/metrics/DriveStatus.hpp"

#include <atomic>

namespace cta::telemetry::metrics {
namespace {
using Status = common::dataStructures::DriveStatus;
// Catalogue reporting must never wait for a telemetry reader or exporter.
static_assert(std::atomic<Status>::is_always_lock_free);
std::atomic<Status> currentStatus {Status::Unknown};
}  // namespace

void setDriveStatus(Status status) noexcept {
  currentStatus.store(status, std::memory_order_relaxed);
}

Status getDriveStatus() noexcept {
  return currentStatus.load(std::memory_order_relaxed);
}

}  // namespace cta::telemetry::metrics
