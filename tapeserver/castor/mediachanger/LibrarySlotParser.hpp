/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "LibrarySlot.hpp"
#include "AcsLibrarySlot.hpp"
#include "ManualLibrarySlot.hpp"
#include "ScsiLibrarySlot.hpp"

namespace castor {
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
  static castor::mediachanger::TapeLibraryType getLibrarySlotType(
    const std::string &str);

  /**
   * Returns true if the type of the specified tape library slot is ACS.
   *
   * @param str The string representation of the tape library slot.
   * @return True if the type if the library slot is ACS.
   */
  static bool isAcs(const std::string &str) throw();

  /**
   * Returns true if the type of the specified tape library slot is manual.
   *
   * @param str The string representation of the tape library slot.
   * @return True if the type of the library slot is a manual.
   */
  static bool isManual(const std::string &str) throw(); 

  /**
   * Returns true if the type of the specified tape library slot is SCSI.
   *
   * @param str The string representation of the tape library slot.
   * @return True if the type of the library slot is a manual.
   */
  static bool isScsi(const std::string &str) throw(); 

  /**
   * Parses the specified string representation of a library slot taking into
   * account the specified library type.
   *
   * @param libraryType The library type of the slot.
   * @param str The string representation of the tape library slot.
   * @return The newly created library slot.
   */
  static castor::mediachanger::LibrarySlot *parse(
    const castor::mediachanger::TapeLibraryType libraryType,
    const std::string &str);

  /**
   * Parses the specified string representation of an ACS library slot.
   *
   * @param str The string representation of the tape library slot.
   */
  static castor::mediachanger::AcsLibrarySlot *parseAcsLibrarySlot(
    const std::string &str);

  /**
   * Parses the specified string representation of a manual library slot.
   *
   * @param str The string representation of the tape library slot.
   */
  static castor::mediachanger::ManualLibrarySlot *parseManualLibrarySlot(
    const std::string &str);

  /**
   * Parses the specified string representation of a SCSI library slot.
   *
   * @param str The string representation of the tape library slot.
   */
  static castor::mediachanger::ScsiLibrarySlot *parseScsiLibrarySlot(
    const std::string &str);

}; // class LibrarySlot

} // namespace mediachanger
} // namespace castor
