/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::mediachanger {

/**
 * Class representing a generic tape-library slot as found in the
 * /etc/cta/cta-taped-unitName.conf.
 */
class LibrarySlot {
public:
  /**
   * Constructor
   *
   * @param drvOrd The drive ordinal.
   * @param dummy Whether this is a dummy slot or not
   */
  explicit LibrarySlot(uint16_t drvOrd, bool dummy = false);

  /**
   * Gets the string representation of this tape library slot.
   *
   * @return The string representation of this tape library slot.
   */
  const std::string& str() const;

  /**
   * Gets the drive ordinal.
   *
   * @return The drive ordinal.
   */
  uint16_t getDrvOrd() const;

  /**
   * Whether the library slot is a dummy or not.
   *
   * @return True if the slot is a dummy slot.
   */
  bool isDummy() const;

private:
  /**
   * Returns the string representation of the specified SCSI library slot.
   *
   * @param drvOrd The drive ordinal.
   * @return The string representation.
   */
  std::string librarySlotToString(const uint16_t drvOrd) const;

  /**
   * The drive ordinal. This is the logical sequence number of SCSI library slot identifying the tape drive.
   */
  uint16_t m_drvOrd = 0;

  /**
   * Whether the slot is a dummy or not.
   */
  bool m_dummy = false;

  /**
   * The string representation of this tape library slot.
   */
  std::string m_str;

};  // class LibrarySlot

}  // namespace cta::mediachanger
