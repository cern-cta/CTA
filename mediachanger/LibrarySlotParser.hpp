/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "mediachanger/LibrarySlot.hpp"
#include "mediachanger/AcsLibrarySlot.hpp"
#include "mediachanger/ManualLibrarySlot.hpp"
#include "mediachanger/ScsiLibrarySlot.hpp"

namespace cta {
namespace mediachanger {

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
   * following three forms:
   * - acsACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER
   * - manual
   * - smcDRIVE_ORDINAL
   *
   * @param str The string represetion of the library slot.
   */
  static LibrarySlot *parse(const std::string &str);

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
  static cta::mediachanger::TapeLibraryType getLibrarySlotType(
    const std::string &str);

  /**
   * Returns true if the type of the specified tape library slot is ACS.
   *
   * @param str The string representation of the tape library slot.
   * @return True if the type if the library slot is ACS.
   */
  static bool isAcs(const std::string &str);

  /**
   * Returns true if the type of the specified tape library slot is manual.
   *
   * @param str The string representation of the tape library slot.
   * @return True if the type of the library slot is a manual.
   */
  static bool isManual(const std::string &str); 

  /**
   * Returns true if the type of the specified tape library slot is SCSI.
   *
   * @param str The string representation of the tape library slot.
   * @return True if the type of the library slot is a manual.
   */
  static bool isScsi(const std::string &str); 

  /**
   * Parses the specified string representation of a library slot taking into
   * account the specified library type.
   *
   * @param libraryType The library type of the slot.
   * @param str The string representation of the tape library slot.
   * @return The newly created library slot.
   */
  static cta::mediachanger::LibrarySlot *parse(
    const cta::mediachanger::TapeLibraryType libraryType,
    const std::string &str);

  /**
   * Parses the specified string representation of an ACS library slot.
   *
   * @param str The string representation of the tape library slot.
   */
  static cta::mediachanger::AcsLibrarySlot *parseAcsLibrarySlot(
    const std::string &str);

  /**
   * Parses the specified string representation of a manual library slot.
   *
   * @param str The string representation of the tape library slot.
   */
  static cta::mediachanger::ManualLibrarySlot *parseManualLibrarySlot(
    const std::string &str);

  /**
   * Parses the specified string representation of a SCSI library slot.
   *
   * @param str The string representation of the tape library slot.
   */
  static cta::mediachanger::ScsiLibrarySlot *parseScsiLibrarySlot(
    const std::string &str);

}; // class LibrarySlot

} // namespace mediachanger
} // namespace cta
