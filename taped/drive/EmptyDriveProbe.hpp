/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveInterface.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/log/Logger.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "taped/file/Structures.hpp"
#include "taped/scsi/Device.hpp"
#include "taped/system/Wrapper.hpp"

#include <memory>
#include <optional>

namespace cta::tape::daemon {

/// Probe whether a tape drive can be opened and has no cartridge present.
class EmptyDriveProbe {
public:
  /// @brief Copy the drive identity and borrow the logger and system wrapper for the probe lifetime.
  ///
  /// @param log Object representing the API to the CTA logging system.
  /// @param driveInfo Information of the tape drive to be probed.
  /// @param sysWrapper Object representing the operating system.
  EmptyDriveProbe(cta::log::Logger& log,
                  const cta::common::dataStructures::DriveInfo& driveInfo,
                  System::virtualWrapper& sysWrapper);

  /// @brief Open the drive and check for cartridge presence, recording and logging probe exceptions.
  ///
  /// @return True only if the probe succeeds and no cartridge is present.
  bool driveIsEmpty() noexcept;

  /// Return the most recently recorded probe failure, or no value if no failure has been recorded.
  std::optional<std::string> getProbeErrorMsg();

private:
  cta::log::Logger& m_log;

  const cta::common::dataStructures::DriveInfo m_driveInfo;

  System::virtualWrapper& m_sysWrapper;

  /// @brief Open the drive and check cartridge presence, allowing discovery and device errors to propagate.
  ///
  /// @return True only if the drive is accessible and no cartridge is present.
  bool exceptionThrowingDriveIsEmpty();

  /// Discover and open an owned drive object using the borrowed system wrapper; propagate discovery or open errors.
  std::unique_ptr<drive::DriveInterface> createDrive();

  /// Eventual error message if we could not check whether the drive is empty or not.
  std::optional<std::string> m_probeErrorMsg;

};  // class EmptyDriveProbe

}  // namespace cta::tape::daemon
