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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/mediachanger/LibrarySlot.hpp"

#include <stdint.h>

namespace castor {
namespace mediachanger {

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
  ScsiLibrarySlot() throw();

  /**
   * Constructor.
   *
   * @param rmcHostName The name of the host on which the rmcd daemon is
   * running.
   * @param drvOrd The drive ordinal.
   */
  ScsiLibrarySlot(const std::string &rmcHostName, const uint16_t drvOrd);

  /**
   * Destructor.
   */
  ~ScsiLibrarySlot() throw();

  /**
   * Creates a clone of this object.
   *
   * @return The clone.
   */
  LibrarySlot *clone();

  /**
   * Gets the name of the host on which the rmcd daemon is running.
   *
   * @return The name of the host on which the rmcd daemon is running.
   */
  const std::string &getRmcHostName() const throw();

  /**
   * Gets the drive ordinal.
   *
   * @return The drive ordinal.
   */
  uint16_t getDrvOrd() const throw();

private:

  /**
   * The name of the host on which the rmcd daemon is running.
   */
  std::string m_rmcHostName;

  /**
   * The drive ordinal.
   */
  uint16_t m_drvOrd;

  /**
   * Returns the string representation of the specified SCSI library slot.
   *
   * @param rmcHostName The name of the host on which the rmcd daemon is
   * running.
   * @param drvOrd The drive ordinal.
   * @return The string representation.
   */
  std::string librarySlotToString(const std::string &rmcHostName,
    const uint16_t drvOrd);

}; // class ScsiLibrarySlot

} // namespace mediachanger
} // namespace castor

