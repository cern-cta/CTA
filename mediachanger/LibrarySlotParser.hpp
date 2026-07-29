/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "mediachanger/LibrarySlot.hpp"

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
  static LibrarySlot parse(const std::string& str);

private:
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
   * Parses the specified string representation of a dummy library slot.
   *
   * @param str The string representation of the tape library slot.
   */
  static cta::mediachanger::LibrarySlot parseDummyLibrarySlot(const std::string& str);

  /**
   * Parses the specified string representation of a SCSI library slot.
   *
   * @param str The string representation of the tape library slot.
   */
  static cta::mediachanger::LibrarySlot parseScsiLibrarySlot(const std::string& str);

};  // class LibrarySlot

}  // namespace cta::mediachanger
