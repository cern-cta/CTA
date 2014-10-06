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

#include "castor/mediachanger/TapeLibraryType.hpp"

#include <string>

namespace castor {
namespace mediachanger {

/**
 * Class representing a slot in a tape library.
 */
class TapeLibrarySlot {
public:

  /**
   * Constructor.
   */
  TapeLibrarySlot();

  /**
   * Constructs a TapeLibrarySlot object from the specified string
   * representation.
   *
   * The string representation of a tape library-slot must be in one of the
   * following three forms:
   * - acsACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER
   * - manual
   * - smc@rmc_host,drive_ordinal
   *
   * This method will throw a castor::exception::Exception if the string
   * representation is invalid.
   *
   * @param str The string reprsentation of the library slot.
   */
  TapeLibrarySlot(const std::string &str);

  /**
   * Returns the string representation of this tape library slot.
   */
  const std::string &str() const throw();

  /**
   * Returns the type of the tape library to which this library slot refers.
   */
  TapeLibraryType getLibraryType() const throw();

private:

  /**
   * The string representation of this tape library slot.
   */
  std::string m_str;

  /**
   * The type of the tape library to which this library slot refers.
   */
  TapeLibraryType m_libraryType;

  /**
   * Thread safe method that returns the type of the tape-library to which the
   * specified library slot refers.
   *
   * This function throws a castor::exception::Exception if the type of the
   * tape-library cannot be determined.
   *
   * @param slot The tape-library slot.
   * @return The type of the tape library.
   */
  static TapeLibraryType getLibraryTypeOfSlot(const std::string &slot);

}; // class TapeLibrarySlot

} // namespace mediachanger
} // namespace castor
