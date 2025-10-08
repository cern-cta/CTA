/*
 * SPDX-FileCopyrightText: 2024 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>
#include "mediachanger/LibrarySlot.hpp"
#include <memory>

namespace cta::tape::daemon {

/**
 * The Drive configuration entry containing the unit name, logicalLibrary,
 * device filename, and library slot. Loaded from a drive configuration
 * file in /etc/cta/.
 */
class DriveConfigEntry {
public:
  /**
   * The unit name of the tape drive.
   */
  std::string unitName;

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

  /**
   * Accessor method to the library slot strcuture.
   * @return reference to the library slot.
   */
  const cta::mediachanger::LibrarySlot & librarySlot() const;

private:
  /**
   * The library slot structure.
   */
  std::unique_ptr <cta::mediachanger::LibrarySlot> m_librarySlot;

public:
  /**
   * Trivial constructor (used in unit tests)
   */
  DriveConfigEntry() = default;

  /**
   * Constructor.
   *
   * @param unitName The unit name of the tape drive.
   * @param dgn The Device Group Name (DGN) of the tape drive.
   * @param devFilename The filename of the device file of the tape drive.
   * @param librarySlot The slot in the tape library that contains the tape
   * drive.
   */
  DriveConfigEntry(
    const std::string &unitName,
    const std::string &logicalLibrary,
    const std::string &devFilename,
    const std::string &librarySlot);

  /**
   * Copy constructor
   * @param o the other DriveConfigEntry to be copied.
   */
  DriveConfigEntry(const DriveConfigEntry& o);

  /**
   * Copy operator
   * @param o the other DriveConfigEntry to copy.
   * @return a reference to the object
   */
  DriveConfigEntry& operator=(const DriveConfigEntry& o);

  static const size_t maxNameLen = 100;
}; // struct DriveConfigEntry

} // namespace cta::tape::daemon
