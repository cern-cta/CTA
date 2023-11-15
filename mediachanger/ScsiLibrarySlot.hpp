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

#include "mediachanger/LibrarySlot.hpp"

#include <stdint.h>

namespace cta::mediachanger {

/**
 * Class representing a slot in a SCSI tape-library.
 */
class ScsiLibrarySlot: public LibrarySlot {
public:

  /**
   * Constructor.
   *
   * Sets all integer member-variables to 0 and all strings to the empty string.
   */
  ScsiLibrarySlot();

  /**
   * Constructor.
   *
   * @param drvOrd The drive ordinal.
   */
  ScsiLibrarySlot(const uint16_t drvOrd);

  /**
   * Destructor.
   */
  ~ScsiLibrarySlot();

  /**
   * Creates a clone of this object.
   *
   * @return The clone.
   */
  LibrarySlot *clone();

  /**
   * Gets the drive ordinal.
   *
   * @return The drive ordinal.
   */
  uint16_t getDrvOrd() const;

private:

  /**
   * The drive ordinal.
   */
  uint16_t m_drvOrd;

  /**
   * Returns the string representation of the specified SCSI library slot.
   *
   * @param drvOrd The drive ordinal.
   * @return The string representation.
   */
  std::string librarySlotToString(const uint16_t drvOrd);

}; // class ScsiLibrarySlot

} // namespace cta::mediachanger

