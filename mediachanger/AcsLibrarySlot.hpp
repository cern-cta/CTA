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

#include "common/exception/InvalidArgument.hpp"
#include "mediachanger/LibrarySlot.hpp"

#include <stdint.h>

namespace cta {
namespace mediachanger {

/**
 * Class reprsenting a slot in an ACS tape-library.
 */
class AcsLibrarySlot: public LibrarySlot {
public:

  /**
   * Default constructor that sets all integer members to 0.
   */
  AcsLibrarySlot();

  /**
   * Constructor.
   *
   * @param acs The acs component of the library slot.
   * @param lsm The lsm component of the library slot.
   * @param panel The panel component of the library slot.
   * @param drive The drive component of the library slot.
   */
  AcsLibrarySlot(const uint32_t acs, const uint32_t lsm,
    const uint32_t panel, const uint32_t drive);

  /**
   * Destructor.
   */
  ~AcsLibrarySlot();

  /**
   * Creates a clone of this object.
   *
   * @return The clone.
   */
  LibrarySlot *clone();

  /**
   * Gets the acs component of the library slot.
   *
   * @return the acs component of the library slot.
   */
  uint32_t getAcs() const;

  /**
   * Gets the lsm component of the library slot.
   *
   * @return the lsm component of the library slot.
   */
  uint32_t getLsm() const;

  /**
   * Gets the panel component of the library slot.
   *
   * @return the panel component of the library slot.
   */
  uint32_t getPanel() const;

  /**
   * Gets the drive component of the library slot.
   *
   * @return the drive component of the library slot.
   */
  uint32_t getDrive() const;

private:

  /**
   * The acs component of the library slot.
   */
  uint32_t m_acs;

  /**
   * The lsm component of the library slot.
   */
  uint32_t m_lsm;

  /**
   * The panel component of the library slot.
   */
  uint32_t m_panel;

  /**
   * The drive component of the library slot.
   */
  uint32_t m_drive;

  /**
   * Returns the string representation of the specified ACS library slot.
   *
   * @param acs The acs component of the library slot.
   * @param lsm The lsm component of the library slot.
   * @param panel The panel component of the library slot.
   * @param drive The drive component of the library slot.
   * @return The string representation.
   */
  std::string librarySlotToString(const uint32_t acs, const uint32_t lsm,
    const uint32_t panel, const uint32_t drive) const;
  
}; // class AcsProxy

} // namespace mediachanger
} // namespace cta
