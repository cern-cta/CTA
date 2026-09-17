/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveUsability.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/dataStructures/DriveStatus.hpp"
#include "common/log/LogContext.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "taped/drive/DriveInterface.hpp"
#include "taped/scsi/Device.hpp"

#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace cta::catalogue {
class Catalogue;
}

namespace cta::tape::daemon {

class TapeSessionTracker;

/**
 * @brief Class responsible for cleaning up a tape drive left in a (possibly) dirty state.
 */
class DriveCleaner {
public:
  /**
   * @brief Create a cleaner using borrowed media changer, catalogue and tracker objects.
   *
   * @param mc Object representing the media changer.
   * @param log Object representing the API to the CTA logging system.
   * @param driveInfo Identity and device paths of the tape drive to clean.
   * @param vid Mounted tape identifier when known; otherwise an empty string.
   * @param waitMediaInDrive Whether to wait for media readiness before cleaning.
   * @param waitMediaInDriveTimeout Maximum wait for media readiness, in seconds.
   * @param catalogue Borrowed catalogue used for drive and tape state updates.
   * @param tracker Borrowed tracker, which must outlive this cleaner.
   */
  DriveCleaner(cta::mediachanger::MediaChangerFacade& mc,
               cta::log::Logger& log,
               const cta::common::dataStructures::DriveInfo& driveInfo,
               const std::string& vid,
               const bool waitMediaInDrive,
               const uint32_t waitMediaInDriveTimeout,
               cta::catalogue::Catalogue& catalogue,
               TapeSessionTracker& tracker);

  /**
   * @brief Open and clean the drive, publishing down-state decisions for recorded failures.
   *
   * If ejection fails, attempt to disable the tape when its VID is known.
   *
   * @param sysWrapper System-call wrapper used to discover and open the drive.
   * @return Reusable when cleanup permits reuse; MustRemainDown when a recorded failure prevents it.
   */
  DriveUsability execute(System::virtualWrapper& sysWrapper);

  /**
   * @brief Record drive-configuration reset and tape-ejection failures.
   *
   * An already-empty drive has no eject failure; configuration-reset failures still prevent reuse.
   */
  struct CleanupResult {
    bool configurationResetFailed = false;
    bool ejectFailed = false;
    std::string errorMessage;

    // TODO: Revisit whether unload, encryption or LBP errors should force the drive down.
    /**
     * @brief Check whether the recorded cleanup failures permit drive reuse.
     *
     * An already-empty drive needs no eject operation and does not set ejectFailed.
     *
     * @return True if neither configurationResetFailed nor ejectFailed is set.
     */
    bool driveReusable() const { return !configurationResetFailed && !ejectFailed; }
  };

  using DriveStatusReporter = std::function<void(common::dataStructures::DriveStatus)>;

  /**
   * @brief Clean a drive, delegating progress publication to the caller.
   *
   * This method neither publishes a final down decision nor disables tapes.
   * The caller is responsible for handling the returned failure flags.
   * A successful eject does not imply that the drive configuration was reset successfully.
   * Progress is reported synchronously; callback failures are counted without interrupting cleanup.
   *
   * @param drive Borrowed drive on which to perform the operation.
   * @param reportStatus Synchronous callback for publishing cleanup progress; callback failures do not stop cleanup.
   * @return Configuration-reset and eject failure flags, with diagnostic text when available.
   */
  CleanupResult cleanDrive(drive::DriveInterface& drive, const DriveStatusReporter& reportStatus);

private:
  TapeSessionTracker& m_tracker;
  cta::mediachanger::MediaChangerFacade& m_mediachanger;
  cta::log::LogContext m_lc;
  const cta::common::dataStructures::DriveInfo m_driveInfo;
  const std::string m_vid;
  const bool m_waitMediaInDrive;
  const uint32_t m_tapeLoadTimeout;
  cta::catalogue::Catalogue& m_catalogue;

  /**
   * @brief Reset drive configuration and attempt tape ejection, retaining independent cleanup failures.
   *
   * Readiness waits and label reads are best effort; eject is attempted despite reset or unload errors.
   *
   * @param drive Borrowed drive on which to perform the operation.
   * @param reportStatus Synchronous callback for publishing cleanup progress; callback failures do not stop cleanup.
   * @return Configuration-reset and eject failure flags, with diagnostic text when available.
   */
  CleanupResult cleanDriveImpl(drive::DriveInterface& drive, const DriveStatusReporter& reportStatus);

  /**
   * @brief Update the tracker phase and publish cleanup progress.
   *
   * Count callback failures as reporting errors without interrupting hardware cleanup.
   *
   * @param status Reported drive status to publish.
   * @param reportStatus Synchronous callback for publishing cleanup progress; callback failures do not stop cleanup.
   */
  void reportProgress(common::dataStructures::DriveStatus status, const DriveStatusReporter& reportStatus);

  /**
   * @brief Logs and clears (just by reading them...) any outstanding tape alerts
   *
   * @param drive The tape drive.
   */
  void logAndClearTapeAlerts(drive::DriveInterface& drive) noexcept;

  /**
   * @brief Reads the volume label when its format can be determined.
   *
   * @param drive The tape drive.
   * @return The VSN from the label, or std::nullopt if no VID was provided to determine the label format.
   */
  std::optional<std::string> readVolumeLabel(drive::DriveInterface& drive);

  /**
   * @brief Creates and returns the object that represents the tape drive to be cleaned.
   *
   * @param sysWrapper System-call wrapper used to discover and open the drive.
   * @return The tape drive.
   */
  std::unique_ptr<drive::DriveInterface> createDrive(System::virtualWrapper& sysWrapper);

  /**
   * @brief Waits for the specified drive to be ready, logging and ignoring readiness failures.
   *
   * @param drive The tape drive.
   */
  void waitForMediaToBeReady(drive::DriveInterface& drive);

  /**
   * @brief Rewinds the specified tape drive.
   *
   * @param drive The tape drive.
   */
  void rewindDrive(drive::DriveInterface& drive);

  /**
   * @brief Checks the tape in the specified tape drive contains some data where no
   *
   * data means the tape does not even contain a volume label.
   *
   * @param drive The tape drive.
   */
  void checkTapeContainsData(drive::DriveInterface& drive);

  /**
   * @brief Unloads a tape from the drive.
   *
   * @param drive Drive on which to perform the operation.
   */
  void unloadTape(drive::DriveInterface& drive);

  /**
   * @brief Dismounts the specified tape.
   *
   * @param vid Volume identifier, or an empty string to bypass the robot cartridge-name check.
   */
  void dismountTape(const std::string& vid);

  /**
   * @brief Put the drive down if the cleaner failed.
   *
   * @param errorMsg Cleanup failure details to include in diagnostics and the down or disabled reason.
   */
  void setDriveDownAfterCleanerFailed(const std::string& errorMsg) noexcept;

  /**
   * @brief Prevent a stuck tape from being scheduled.
   *
   * @param errorMsg Cleanup failure details to include in diagnostics and the down or disabled reason.
   */
  void disableTapeAfterFailedEject(const std::string& errorMsg) noexcept;

};  // class DriveCleaner

}  // namespace cta::tape::daemon
