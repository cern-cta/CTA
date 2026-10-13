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
  /// @param waitMediaInDrive Wait for readiness before checking presence, even if cached status says empty.
  /// Use true when a mount may have occurred.
  /// @param waitMediaInDriveTimeout Readiness timeout in seconds.
  DriveCleaner(cta::mediachanger::MediaChangerFacade& mc,
               cta::log::Logger& log,
               const cta::common::dataStructures::DriveInfo& driveInfo,
               const std::string& vid,
               const bool waitMediaInDrive,
               const uint32_t waitMediaInDriveTimeout,
               cta::catalogue::Catalogue& catalogue,
               TapeSessionTracker& tracker);

  /// @brief Record drive-open, configuration-reset and tape-ejection failures.
  ///
  /// An already-empty drive has no eject failure; configuration-reset failures still prevent reuse.
  struct CleanupResult {
    bool driveOpenFailed = false;
    bool configurationResetFailed = false;
    bool ejectFailed = false;
    bool unexpectedFailure = false;  ///< Cleanup threw before returning a result.
    std::string errorMessage;

    /// Return true only if no cleanup failure flag is set.
    bool driveReusable() const {
      return !driveOpenFailed && !configurationResetFailed && !ejectFailed && !unexpectedFailure;
    }
  };

  /// @brief Open and clean the drive, then read and clear its tape alerts.
  ///
  /// If opening fails, attempt robotic dismount but return a non-reusable result.
  CleanupResult cleanDrive(System::virtualWrapper& sysWrapper);

  /// @brief Clean a borrowed drive and return independent configuration-reset and eject failures.
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
  const bool m_waitMediaInDrive;
  const uint32_t m_tapeLoadTimeout;
  cta::catalogue::Catalogue& m_catalogue;

  /// Attempt to disable a known tape when the result reports failed ejection.
  void finishCleanup(const CleanupResult& result);

  /// @brief Reset configuration and eject the tape, retaining independent failures.
  ///
  /// Readiness waits and label reads are best effort; attempt robotic dismount despite reset or unload errors.
  CleanupResult cleanDriveImpl(drive::DriveInterface& drive);

  /// Read, count and log tape alerts, clearing them on hardware; log and suppress query failures.
  void logAndClearTapeAlerts(drive::DriveInterface& drive) noexcept;

  /// @brief Read the label using the catalogue format for the known VID.
  ///
  /// Return std::nullopt if the VID is unknown; blank tapes and label or catalogue errors throw.
  std::optional<std::string> readVolumeLabel(drive::DriveInterface& drive);

  /// Discover and open the configured drive; return ownership or propagate an error.
  std::unique_ptr<drive::DriveInterface> createDrive(System::virtualWrapper& sysWrapper);

  /// Wait for readiness within the configured timeout, logging and ignoring failures.
  void waitForMediaToBeReady(drive::DriveInterface& drive);

  /// Time and log the rewind operation; drive failures propagate.
  void rewindDrive(drive::DriveInterface& drive);

  /// Throw if the tape is blank, including when it has no volume label.
  void checkTapeContainsData(drive::DriveInterface& drive);

  /// Time and log unloading, counting drive failures before propagating them.
  void unloadTape(drive::DriveInterface& drive);

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
