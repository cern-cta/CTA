/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveUsability.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/log/LogContext.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "scheduler/Scheduler.hpp"
#include "taped/drive/DriveInterface.hpp"
#include "taped/scsi/Device.hpp"

#include <memory>
#include <optional>
#include <string>

namespace cta::catalogue {
class Catalogue;
}

namespace cta::tape::daemon {

class TapeSessionTracker;

/**
  * Class responsible for cleaning up a tape drive left in a (possibly) dirty state.
  */
class CleanerSession {
public:
  /**
    * Constructor
    *
    * @param mc Object representing the media changer.
    * @param log Object representing the API to the CTA logging system.
    * @param driveInfo Info of the tape drive to be cleaned.
    * @param sysWrapper Object representing the operating system.
    * @param vid The volume identifier of the mounted tape if known,
    * else the empty string.
    * @param waitMediaInDrive true if we want to check the presence of the media in the drive before cleaning,
    * false otherwise.
    * @param waitMediaInDriveTimeout The maximum number of seconds to wait for
    * the media to be ready for operations inside the drive.
    * @param tracker Optional borrowed tracker, which must outlive this session.
    * @param catalogue the CTA catalogue
    */
  CleanerSession(cta::mediachanger::MediaChangerFacade& mc,
                 cta::log::Logger& log,
                 const cta::common::dataStructures::DriveInfo& driveInfo,
                 System::virtualWrapper& sysWrapper,
                 const std::string& vid,
                 const bool waitMediaInDrive,
                 const uint32_t waitMediaInDriveTimeout,
                 cta::catalogue::Catalogue& catalogue,
                 cta::Scheduler& scheduler,
                 TapeSessionTracker* tracker = nullptr);

  /** Clean the drive and return whether it can be reused. */
  DriveUsability execute();

  // An empty drive counts as successful eject. Reset failures still prevent reuse.
  struct CleanupResult {
    bool configurationResetFailed = false;
    bool ejectFailed = false;
    std::string errorMessage;

    bool driveReusable() const { return !configurationResetFailed && !ejectFailed; }
  };

  /**
   * Clean an already open drive without taking ownership or publishing scheduler/catalogue state.
   * The caller owns failure publication and tape disabling when ejectFailed is true.
   * A successful eject does not imply that the drive configuration was reset successfully.
   */
  CleanupResult cleanDrive(drive::DriveInterface& drive);

private:
  CleanupResult cleanDriveImpl(drive::DriveInterface& drive);

  TapeSessionTracker* const m_tracker;

  /**
    * The object representing the media changer.
    */
  cta::mediachanger::MediaChangerFacade& m_mediachanger;

  /** Log context containing parameters shared by every cleaner log entry. */
  cta::log::LogContext m_lc;

  /**
    * The information of the tape drive to be cleaned.
    */
  const cta::common::dataStructures::DriveInfo m_driveInfo;

  /**
    * The system wrapper used to find the device and instantiate the drive object
    */
  System::virtualWrapper& m_sysWrapper;

  /**
    * The volume identifier of the mounted tape if known, else the empty
    * string.
    */
  const std::string m_vid;

  /**
    * true if we want to check the presence of the media in the drive before cleaning,
    * false otherwise.
    */
  const bool m_waitMediaInDrive;

  /**
    * The maximum number of seconds to wait for
    * the media to be ready for operations inside the drive.
    */
  const uint32_t m_tapeLoadTimeout;

  /**
    * CTA catalogue
    */
  cta::catalogue::Catalogue& m_catalogue;

  /**
    * CTA scheduler
    */
  cta::Scheduler& m_scheduler;

  /**
    * Logs and clears (just by reading them...) any outstanding tape alerts
    *
    * @param drive The tape drive.
    */
  void logAndClearTapeAlerts(drive::DriveInterface& drive) noexcept;

  /**
   * Reads the volume label when its format can be determined.
   *
   * @param drive The tape drive.
   * @return The VSN from the label, or std::nullopt if no VID was provided to determine the label format.
   */
  std::optional<std::string> readVolumeLabel(drive::DriveInterface& drive);

  /**
    * Creates and returns the object that represents the tape drive to be
    * cleaned.
    *
    * @return The tape drive.
    */
  std::unique_ptr<drive::DriveInterface> createDrive();

  /**
    * Waits for the specified drive to be ready.
    *
    * @param drive The tape drive.
    */
  void waitForMediaToBeReady(drive::DriveInterface& drive);

  /**
    * Rewinds the specified tape drive.
    *
    * @param drive The tape drive.
    */
  void rewindDrive(drive::DriveInterface& drive);

  /**
    * Checks the tape in the specified tape drive contains some data where no
    * data means the tape does not even contain a volume label.
    *
    * @param drive The tape drive.
    */
  void checkTapeContainsData(drive::DriveInterface& drive);

  /** Unloads a tape from the drive. */
  void unloadTape(drive::DriveInterface& drive);

  /**
    * Dismounts the specified tape.
    *
    * @param vid The volume identifier of the tape to be dismounted.
    */
  void dismountTape(const std::string& vid);

  /** Put the drive down if the cleaner failed. */
  void setDriveDownAfterCleanerFailed(const std::string& errorMsg) noexcept;

  /** Prevent a tape with unresolved physical location from being scheduled. */
  void disableTapeAfterFailedEject(const std::string& errorMsg) noexcept;

};  // class CleanerSession

}  // namespace cta::tape::daemon
