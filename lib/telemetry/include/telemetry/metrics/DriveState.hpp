/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/dataStructures/DriveStatus.hpp"
#include "common/dataStructures/MountType.hpp"

namespace cta::telemetry::metrics {

// Process-local snapshots let telemetry observe drive state without querying the catalogue.
// Updates are independent of backend publication and do not activate metric observation.
void setDriveStatus(common::dataStructures::DriveStatus status) noexcept;
common::dataStructures::DriveStatus getDriveStatus() noexcept;

void setMountType(common::dataStructures::MountType type) noexcept;
common::dataStructures::MountType getMountType() noexcept;

}  // namespace cta::telemetry::metrics
