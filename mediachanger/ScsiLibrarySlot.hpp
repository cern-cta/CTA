/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "mediachanger/LibrarySlot.hpp"

#include <stdint.h>

namespace cta::mediachanger {

/**
 * Class representing a slot in a SCSI tape-library.
 */
class ScsiLibrarySlot : public LibrarySlot {
public:
  /**
   * Constructor
   *
   * Sets all integer member-variables to 0 and all strings to the empty string.
   */
  ScsiLibrarySlot();

  /**
   * Constructor
   *
   * @param drvOrd The drive ordinal.
   */
  explicit ScsiLibrarySlot(uint16_t drvOrd);

  /**
   * Destructor
   */
  ~ScsiLibrarySlot() final = default;

  /**
   * Creates a clone of this object.
   *
   * @return The clone.
   */
  LibrarySlot* clone() final;

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

};  // class ScsiLibrarySlot

}  // namespace cta::mediachanger
