/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <list>
#include <map>
#include <optional>
#include <string>

#include "common/dataStructures/DiskSpaceReservationRequest.hpp"

namespace cta {

namespace common::dataStructures {
class DesiredDriveState;
struct TapeDrive;
struct TapeDriveStatistics;
}

namespace log {
class LogContext;
}

namespace catalogue {

/**
 * Specifies the interface to a factory Catalogue objects.
 */
class DriveStateCatalogue {
public:
  virtual ~DriveStateCatalogue() = default;

  /**
   * Creates the specified Tape Drive
   * @param tapeDrive Parameters of the Tape Drive.
   */
  virtual void createTapeDrive(const common::dataStructures::TapeDrive &tapeDrive) = 0;

  /**
   * Gets the names of all stored Tape Drive
   * @return List of tape drive names
   */
  virtual std::list<std::string> getTapeDriveNames() const = 0;

  /**
   * Gets the information of all Tape Drives
   * @return Parameters of all Tape Drives.
   */
  virtual std::list<common::dataStructures::TapeDrive> getTapeDrives() const = 0;

  /**
   * Gets the information of the specified Tape Drive
   * @param tapeDriveName The name of the tape drive.
   * @return Parameters of the Tape Drive.
   */
  virtual std::optional<common::dataStructures::TapeDrive> getTapeDrive(const std::string &tapeDriveName) const = 0;

  /**
   * Modifies the desired state parameters off the specified Tape Drive
   * @param tapeDriveName Name of the Tape Drive.
   * @param desiredState Desired state parameters of the Tape Drive.
   */
  virtual void setDesiredTapeDriveState(const std::string& tapeDriveName,
      const common::dataStructures::DesiredDriveState &desiredState) = 0;

  virtual void setDesiredTapeDriveStateComment(const std::string& tapeDriveName,
    const std::string &comment) = 0;

  virtual void updateTapeDriveStatistics(const std::string& tapeDriveName,
    const std::string& host, const std::string& logicalLibrary,
    const common::dataStructures::TapeDriveStatistics& statistics) = 0;

  virtual void updateTapeDriveStatus(const common::dataStructures::TapeDrive &tapeDrive) = 0;

  /**
   * Deletes the entry of a Tape Drive
   * @param tapeDriveName The name of the tape drive.
   */
  virtual void deleteTapeDrive(const std::string &tapeDriveName) = 0;

  /**
   * Gets the disk space reservations for all disk systems
   */
  virtual std::map<std::string, uint64_t, std::less<>> getDiskSpaceReservations() const = 0;

  /**
   * Adds to the current disk space reservation
   */
  virtual void reserveDiskSpace(const std::string& driveName, const uint64_t mountId,
    const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) = 0;

  /**
   * Subtracts from the current disk space reservation.
   *
   * If the amount released exceeds the current reservation, the reservation will be reduced to zero.
   */
  virtual void releaseDiskSpace(const std::string& driveName, const uint64_t mountId,
    const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) = 0;
};  // class DriveStateCatalogue

}} // namespace cta::catalogue
