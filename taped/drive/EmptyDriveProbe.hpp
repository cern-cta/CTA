/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/DriveInfo.hpp"
#include "taped/system/Wrapper.hpp"

#include <string>

namespace cta::tape::daemon {

/// Result of inspecting cartridge presence without changing drive configuration.
struct EmptyDriveProbeResult {
  enum class Status { Empty, CartridgePresent, Failed };

  Status status;
  std::string errorMessage;
};

/// Discover and open the drive, check cartridge presence, and release the handle.
/// Device failures are returned as diagnostics; no cleanup or robotic fallback is attempted.
EmptyDriveProbeResult probeEmptyDrive(const common::dataStructures::DriveInfo& driveInfo,
                                      System::virtualWrapper& sysWrapper);

}  // namespace cta::tape::daemon
