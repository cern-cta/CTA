/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "mediachanger/TapeLibraryType.hpp"

#include <string>

namespace cta::mediachanger {

/**
 * Class representing a generic tape-library slot as found in the
 * /etc/cta/cta-taped-unitName.conf.
 */
class LibrarySlot {
protected:
  /**
   * Constructor
   *
   * @param libraryType The library type of the slot
   */
  explicit LibrarySlot(TapeLibraryType libraryType);

public:
  /**
   * Destructor
   */
  virtual ~LibrarySlot() = 0;

  /**
   * Creates a clone of this object.
   *
   * @return The clone.
   */
  virtual LibrarySlot *clone() = 0;

  /**
   * Gets the string representation of this tape library slot.
   *
   * @return The string representation of this tape library slot.
   */
  const std::string &str() const;

  /**
   * Returns the type of the tape library to which this library slot refers.
   */
  TapeLibraryType getLibraryType() const;

protected:

  /**
   * The string representation of this tape library slot.
   */
  std::string m_str;

private:

  /**
   * The library type of the slot.
   */
  TapeLibraryType m_libraryType;

  /**
   * Thread safe method that returns the type of the tape-library to which the
   * specified library slot refers.
   *
   * This function throws a cta::exception::Exception if the type of the
   * tape-library cannot be determined.
   *
   * @param slot The tape-library slot.
   * @return The type of the tape library.
   */
  static TapeLibraryType getLibraryTypeOfSlot(const std::string &slot);

}; // class LibrarySlot

} // namespace cta::mediachanger
