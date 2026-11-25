/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveInterface.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/log/LogContext.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "taped/scsi/Device.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace cta::catalogue {
class Catalogue;
}

namespace cta::tape::daemon {

class TapeSessionTracker;

/// @brief Reset drive configuration and return a possibly stranded cartridge to the library.
///
/// Call only after all other drive users have stopped; the caller publishes the final drive status.
class DriveCleaner {
public:
  /// @brief Create a cleaner; media changer, logger, catalogue and tracker must outlive it.
  ///
  /// Drive identity and VID are copied; an empty VID means the mounted tape is unknown.
  /// tapeUnloadTimeout limits readiness polling in seconds, including time spent opening the drive.
  DriveCleaner(cta::mediachanger::MediaChangerFacade& mc,
               cta::log::Logger& log,
               const cta::common::dataStructures::DriveInfo& driveInfo,
               const std::string& vid,
               const uint32_t tapeUnloadTimeout,
               cta::catalogue::Catalogue& catalogue,
               TapeSessionTracker& tracker);

  /// @brief Record cleanup failures that prevent drive reuse.
  ///
  /// An already-empty drive has no eject failure; configuration-reset failures still prevent reuse.
  struct CleanupResult {
    bool driveOpenFailed = false;
    bool driveStateFailed = false;  ///< Checking readiness or unloading left the drive state unknown or unusable.
    bool configurationResetFailed = false;
    bool ejectFailed = false;
    bool unexpectedFailure = false;  ///< Cleanup threw before returning a result.
    std::string errorMessage;

    /// Return true only if no cleanup failure flag is set.
    bool driveReusable() const {
      return !driveOpenFailed && !driveStateFailed && !configurationResetFailed && !ejectFailed && !unexpectedFailure;
    }
  };

  /// @brief Open and clean the drive, then read and clear its tape alerts.
  ///
  /// If opening fails, attempt robotic dismount but return a non-reusable result.
  CleanupResult cleanDrive(System::virtualWrapper& sysWrapper);

  /// @brief Clean a borrowed drive and report failures that prevent reuse.
  ///
  /// Attempt to disable a known tape after failed ejection.
  /// Publish progress synchronously through the tracker; callback failures are counted without stopping cleanup.
  CleanupResult cleanDrive(drive::DriveInterface& drive);

private:
  TapeSessionTracker& m_tracker;
  cta::mediachanger::MediaChangerFacade& m_mediachanger;
  cta::log::LogContext m_lc;
  const cta::common::dataStructures::DriveInfo m_driveInfo;
  const std::string m_vid;
  const std::chrono::seconds m_tapeUnloadTimeout;
  cta::catalogue::Catalogue& m_catalogue;

  /// Clean the drive using the supplied deadline for readiness polling.
  CleanupResult cleanDrive(drive::DriveInterface& drive, std::chrono::steady_clock::time_point readinessDeadline);

  /// Check media readiness, recording unexpected device or command failures.
  bool checkReadiness(drive::DriveInterface& drive,
                      std::chrono::steady_clock::time_point readinessDeadline,
                      CleanupResult& result);

  /// Attempt both encryption and logical block protection resets, recording failures independently.
  void resetConfiguration(drive::DriveInterface& drive, CleanupResult& result);

  /// Select the cartridge VID and attempt dismount, retrying without the VID on failure.
  void dismountCartridge(const std::optional<std::string>& volumeLabel, CleanupResult& result);

  /// Read, count and log tape alerts, clearing them on hardware; log and suppress query failures.
  void logAndClearTapeAlerts(drive::DriveInterface& drive) noexcept;

  /// @brief Rewind and read the label using the catalogue format for the known VID.
  ///
  /// Return std::nullopt if the VID is unknown or inspection fails; log and suppress inspection errors.
  std::optional<std::string> readVolumeLabel(drive::DriveInterface& drive);

  /// Discover and open the configured drive; return ownership or propagate an error.
  std::unique_ptr<drive::DriveInterface> createDrive(System::virtualWrapper& sysWrapper);

  /// Attempt unloading, counting failures and recording errors that prevent drive reuse.
  void unloadTape(drive::DriveInterface& drive, CleanupResult& result);

  /// @brief Time robotic dismount, counting failures before propagating them.
  ///
  /// An empty VID bypasses the robot cartridge-name check.
  void dismountTape(const std::string& vid);

  /// @brief Attempt to disable a known active or repacking tape, logging catalogue failures.
  ///
  /// Include errorMsg in the tape-state reason; leave other tape states unchanged.
  void disableTapeAfterFailedEject(const std::string& errorMsg) noexcept;

};  // class DriveCleaner

}  // namespace cta::tape::daemon
