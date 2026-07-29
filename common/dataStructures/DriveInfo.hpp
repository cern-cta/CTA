/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::common::dataStructures {

/**
 * This struct holds minimal drive info.
 */
class DriveInfo {
public:
  /**
   * Trivial constructor (used in unit tests)
   */
  DriveInfo() = default;

  /**
   * Constructor.
   *
   * @param driveName The unit name of the tape drive.
   * @param hostname The hostname of the server the tape drive is connected to.
   * @param logicalLibrary The logical library of the tape drive.
   * @param devFilename The filename of the device file of the tape drive.
   * @param librarySlot The slot in the tape library that contains the tape drive.
   */
  DriveInfo(const std::string& driveName,
            const std::string& hostname,
            const std::string& logicalLibrary,
            const std::string& devFilename,
            const std::string& librarySlot);

  /**
   * The unit name of the tape drive.
   */
  std::string driveName;

  /**
   * The name of the host the tape drive is connected to.
   */
  std::string host;
  /**
   * The logical library of the tape drive.
   */
  std::string logicalLibrary;

  /**
   * The filename of the device file of the tape drive.
   */
  std::string devFilename;

  /**
   * The slot in the tape library that contains the tape drive (string encoded).
   */
  std::string rawLibrarySlot;

private:
  /**
   * Length check for the various string fields.
   * For now this limit is rather arbitrary.
   */
  static const size_t maxNameLen = 100;
};

}  // namespace cta::common::dataStructures
