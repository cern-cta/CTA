/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "common/log/Logger.hpp"
#include "mediachanger/MediaChangerFacade.hpp"

#include <memory>
#include <optional>

namespace castor::tape::tapeserver::daemon {

/**
   * Class responsible for probing a tape drive to see if it empty and
   * accessible.
   */
class EmptyDriveProbe {
public:
  /**
     * Constructor
     *
     * @param log Object representing the API to the CASTOR logging system.
     * @param driveConfig Configuration of the tape drive to be probed.
     * @param sysWrapper Object representing the operating system.
     */
  EmptyDriveProbe(cta::log::Logger& log,
                  const cta::tape::daemon::DriveConfigEntry& driveConfig,
                  System::virtualWrapper& sysWrapper);

  /**
     * Probes the tape drive to determine whether it is empty and accessible.
     *
     * @return True if the drive is empty and accessible.
     */
  bool driveIsEmpty() noexcept;

  /**
     * Returns the eventual probe error message
     */
  std::optional<std::string> getProbeErrorMsg();

private:
  /**
     * The logging object
     */
  cta::log::Logger& m_log;

  /**
     * The configuration of the tape drive to be probed.
     */
  const cta::tape::daemon::DriveConfigEntry m_driveConfig;

  /**
     * The system wrapper used to find the device and instantiate the drive object
     */
  System::virtualWrapper& m_sysWrapper;

  /**
     * Probes the tape drive to determine whether it is empty and accessible.
     *
     * @return True if the drive is empty and accessible.
     */
  bool exceptionThrowingDriveIsEmpty();

  /**
     * Creates and returns the object that represents the tape drive to be
     * probed.
     *
     * @return The tape drive.
     */
  std::unique_ptr<drive::DriveInterface> createDrive();

  /**
     * Eventual error message if we could not check whether the drive is empty or not
     */
  std::optional<std::string> m_probeErrorMsg;

};  // class EmptyDriveProbe

}  // namespace castor::tape::tapeserver::daemon
