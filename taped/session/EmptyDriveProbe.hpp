/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/DriveInfo.hpp"
#include "common/log/Logger.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "taped/drive/DriveInterface.hpp"
#include "taped/file/Structures.hpp"
#include "taped/scsi/Device.hpp"
#include "taped/system/Wrapper.hpp"

#include <memory>
#include <optional>

namespace cta::tape::daemon {

/**
   * Class responsible for probing a tape drive to see if it empty and
   * accessible.
   */
class EmptyDriveProbe {
public:
  /**
     * Constructor
     *
     * @param log Object representing the API to the CTA logging system.
     * @param driveInfo Information of the tape drive to be probed.
     * @param sysWrapper Object representing the operating system.
     */
  EmptyDriveProbe(cta::log::Logger& log,
                  const cta::common::dataStructures::DriveInfo& driveInfo,
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
     * The information of the tape drive to be probed.
     */
  const cta::common::dataStructures::DriveInfo m_driveInfo;

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

}  // namespace cta::tape::daemon
