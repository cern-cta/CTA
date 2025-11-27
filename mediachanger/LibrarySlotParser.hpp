/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "mediachanger/DummyLibrarySlot.hpp"
#include "mediachanger/LibrarySlot.hpp"
#include "mediachanger/ScsiLibrarySlot.hpp"

namespace cta::mediachanger {

/**
 * Creates objects representing tape library slots by parsing their string
 * representations.
 */
class LibrarySlotParser {
public:
  /**
   * Parses the specified string representation of a tape library slot and
   * creates the corresponding object.
   *
   * The string representation of a tape library-slot must be in one of the
   * following two forms:
   * - dummy
   * - smcDRIVE_ORDINAL
   *
   * @param str The string representation of the library slot.
   */
  static std::unique_ptr<LibrarySlot> parse(const std::string &str);

private:
  /**
   * Gets the type of the specified string representation of a tape library
   * slot.
   *
   * This method purposely only parses the beginning of the specified string.
   * This permits a two step parsing strategy where the user can be given more
   * detailed syntax errors where necessary.
   *
   * This method throws a cta::exception::Exception if the type of the
   * library slot cannot be determined.
   *
   * @param str The string representation of the tape library slot.
   * @return The type of the library slot.
   */
  static cta::mediachanger::TapeLibraryType getLibrarySlotType(const std::string& str);

  /**
   * Returns true if the type of the specified tape library slot is SCSI.
   *
   * @param str The string representation of the tape library slot.
   * @return True if the type of the library slot is dummy.
   */
  static bool isDummy(const std::string& str);

  /**
   * Returns true if the type of the specified tape library slot is SCSI.
   *
   * @param str The string representation of the tape library slot.
   * @return True if the type of the library slot is SCSI.
   */
  static bool isScsi(const std::string& str);

  /**
   * Parses the specified string representation of a library slot taking into
   * account the specified library type.
   *
   * @param libraryType The library type of the slot.
   * @param str The string representation of the tape library slot.
   * @return The newly created library slot.
   */
  static std::unique_ptr<cta::mediachanger::LibrarySlot> parse(
    const cta::mediachanger::TapeLibraryType libraryType,
    const std::string &str);

  /**
   * Parses the specified string representation of a dummy library slot.
   *
   * @param str The string representation of the tape library slot.
   */
  static std::unique_ptr<cta::mediachanger::DummyLibrarySlot> parseDummyLibrarySlot(const std::string& str);

  /**
   * Parses the specified string representation of a SCSI library slot.
   *
   * @param str The string representation of the tape library slot.
   */
  static std::unique_ptr<cta::mediachanger::ScsiLibrarySlot> parseScsiLibrarySlot(
    const std::string &str);

};  // class LibrarySlot

}  // namespace cta::mediachanger
