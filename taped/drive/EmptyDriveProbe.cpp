/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "EmptyDriveProbe.hpp"

#include "DriveInterface.hpp"
#include "common/exception/Exception.hpp"
#include "taped/scsi/Device.hpp"

#include <exception>

namespace cta::tape::daemon {

EmptyDriveProbeResult probeEmptyDrive(const common::dataStructures::DriveInfo& driveInfo,
                                      System::virtualWrapper& sysWrapper) {
  using Status = EmptyDriveProbeResult::Status;
  try {
    SCSI::DeviceVector devices(sysWrapper);
    const auto device = devices.findBySymlink(driveInfo.devFilename);
    auto drive = drive::createDrive(device, sysWrapper);
    if (!drive) {
      return {Status::Failed, "Failed to instantiate drive object"};
    }

    // Only inspect presence: even an empty drive must retain its current configuration.
    return {drive->hasTapeInPlace() ? Status::CartridgePresent : Status::Empty, {}};
  } catch (const cta::exception::Exception& ex) {
    return {Status::Failed, ex.getMessageValue()};
  } catch (const std::exception& ex) {
    return {Status::Failed, ex.what()};
  } catch (...) {
    return {Status::Failed, "Unknown exception while probing drive"};
  }
}

}  // namespace cta::tape::daemon
