/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/dataStructures/DriveStatus.hpp"

namespace cta::telemetry::metrics {

// A process-local snapshot: no allocation, locks, callbacks, or exporter calls.
void setDriveStatus(common::dataStructures::DriveStatus status) noexcept;
common::dataStructures::DriveStatus getDriveStatus() noexcept;

}  // namespace cta::telemetry::metrics
