/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <memory>
#include <string>

#include "common/log/Logger.hpp"
#include "common/processCap/ProcessCap.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "scheduler/Scheduler.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/EncryptionControl.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/Session.hpp"
#include "tapeserver/castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "tapeserver/castor/tape/tapeserver/file/Structures.hpp"
#include "tapeserver/castor/tape/tapeserver/SCSI/Device.hpp"
#include "tapeserver/daemon/DriveConfigEntry.hpp"

namespace cta::catalogue {
class Catalogue;
}

namespace castor::tape::tapeserver::daemon {

/**
  * Class responsible for cleaning up a tape drive left in a (possibly) dirty state.
  */
class CleanerSession : public Session {
public:
  /**
    * Constructor
    *
    * @param capUtils Object providing support for UNIX capabilities.
    * @param mc Object representing the media changer.
    * @param log Object representing the API to the CASTOR logging system.
    * @param driveConfig Configuration of the tape drive to be cleaned.
    * @param sysWrapper Object representing the operating system.
    * @param vid The volume identifier of the mounted tape if known,
    * else the empty string.
    * @param waitMediaInDrive true if we want to check the presence of the media in the drive before cleaning,
    * false otherwise.
    * @param waitMediaInDriveTimeout The maximum number of seconds to wait for
    * the media to be ready for operations inside the drive.
    * @param externalEncryptionKeyScript path to the operator provided script
    * for encryption control.
    * @param catalogue the CTA catalogue
    */
  CleanerSession(
    cta::server::ProcessCap &capUtils,
    cta::mediachanger::MediaChangerFacade &mc,
    cta::log::Logger &log,
    const cta::tape::daemon::DriveConfigEntry &driveConfig,
    System::virtualWrapper &sysWrapper,
    const std::string &vid,
    const bool waitMediaInDrive,
    const uint32_t waitMediaInDriveTimeout,
    const std::string & externalEncryptionKeyScript,
    cta::catalogue::Catalogue & catalogue,
    cta::Scheduler & scheduler);

  /**
    * Execute the session and return the type of action to be performed
    * immediately after the session has completed.
    *
    * @return Returns the type of action to be performed after the session has
    * completed.
    */
  EndOfSessionAction execute() noexcept;

private:
  /**
    * Object providing support for UNIX capabilities.
    */
  cta::server::ProcessCap &m_capUtils;

  /**
    * The object representing the media changer.
    */
  cta::mediachanger::MediaChangerFacade &m_mc;

  /**
    * The logging object
    */
  cta::log::Logger & m_log;

  /**
    * The configuration of the tape drive to be cleaned.
    */
  const cta::tape::daemon::DriveConfigEntry m_driveConfig;

  /**
    * The system wrapper used to find the device and instantiate the drive object
    */
  System::virtualWrapper & m_sysWrapper;

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
    * Encryption helper object
    */
  EncryptionControl m_encryptionControl;

  /**
    * CTA catalogue
    */
  cta::catalogue::Catalogue & m_catalogue;

  /**
    * CTA scheduler
    */
  cta::Scheduler & m_scheduler;

  /**
    * Variable used to log UPDATE_USER_NAME in the DB
    */
  const std::string c_defaultUserNameUpdate = "cta-taped";

  /**
    * Execute the session and return the type of action to be performed
    * immediately after the session has completed.
    *
    * @return Returns the type of action to be performed after the session has
    * completed.
    */
  EndOfSessionAction exceptionThrowingExecute();

  /**
    * Logs and clears (just by reading them...) any outstanding tape alerts
    *
    * @param drive The tape drive.
    */
  void logAndClearTapeAlerts(drive::DriveInterface &drive) noexcept;

  /**
    * Does the actual steps to clean the drive
    *
    * @param drive The tape drive.
    */
  void cleanDrive(drive::DriveInterface &drive);

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
  void waitUntilMediaIsReady(drive::DriveInterface &drive);

  /**
    * Rewinds the specified tape drive.
    *
    * @param drive The tape drive.
    */
  void rewindDrive(drive::DriveInterface &drive);

  /**
    * Checks the tape in the specified tape drive contains some data where no
    * data means the tape does not even contain a volume label.
    *
    * @param drive The tape drive.
    */
  void checkTapeContainsData(drive::DriveInterface &drive);

  /**
    * Checks that the tape in the specified drive contains a valid volume
    * label.
    *
    * @param drive The tape drive for which it is assumed the tape to be
    * tested is present and rewound to the beginning.
    * @return The VSN stored within the colue label.
    */
  std::string checkVolumeLabel(drive::DriveInterface &drive);

  /**
    * Unloads the specified tape from the specified tape drive.
    *
    * @param vid The volume identifier of the tape to be unloaded.  Please note
    * that the value of this field is only used for logging purposes.
    * @param drive The tape drive.
    */
  void unloadTape(const std::string &vid, drive::DriveInterface &drive);

  /**
    * Dismounts the specified tape.
    *
    * @param vid The volume identifier of the tape to be dismounted.
    */
  void dismountTape(const std::string &vid);

  /**
    * Put the drive down in case the Cleaner has failed
    */
  void setDriveDownAfterCleanerFailed(const std::string & errorMsg);
};  // class CleanerSession

} // namespace castor::tape::tapeserver::daemon
