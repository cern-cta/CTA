/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "EmptyDriveProbe.hpp"

#include "DriveInterface.hpp"
#include "common/exception/Exception.hpp"
#include "taped/scsi/Device.hpp"

namespace cta::tape::daemon {

EmptyDriveProbeResult probeEmptyDrive(const common::dataStructures::DriveInfo& driveInfo,
                                      System::virtualWrapper& sysWrapper) {
  using Status = EmptyDriveProbeResult::Status;
  try {
    // createDrive(DriveInfo, sw) dispatches on the devFilename URI: "virtual://<base_dir>"
    // yields a VtlDrive (no SCSI device scan), any other path yields a DriveGeneric.
    auto drive = drive::createDrive(driveInfo, sysWrapper);
    if (!drive) {
      return {Status::Failed, "Failed to instantiate drive object"};
    }

    // Drives that manage their own state (e.g. VtlDrive — no SCSI device, no robot arm,
    // no mechanical damage risk) opt out of the hardware probe. Any stale state left by
    // a crashed session is handled inside the drive implementation itself.
    if (!drive->requiresEmptyDriveProbe()) {
      return {Status::Empty, {}};
    }

    // Only inspect presence: even an empty drive must retain its current configuration.
    return {drive->hasTapeInPlace() ? Status::CartridgePresent : Status::Empty, {}};
  } catch (...) {
    return {Status::Failed, exception::currentExceptionMessage()};
  }
}

}  // namespace cta::tape::daemon
