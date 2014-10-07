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

#include <string>

namespace castor {
namespace mediachanger {

/**
 * Class representing a slot in a manually operated tape library.
 */
class ManualLibrarySlot {
public:

  /**
   * Constructor.
   *
   * Sets all string member-variables to the empty string.
   */
  ManualLibrarySlot() throw();

  /**
   * Constructor.
   *
   * Initialises the member variables based on the result of parsing the
   * specified string representation of the library slot.
   *
   * @param str The string representation of the library slot.
   */
  ManualLibrarySlot(const std::string &str);

  /**
   * Returns the string representation of the library slot.
   */
  const std::string &str() const throw();

private:

  /**
   * The string representation of the library slot.
   */
  std::string m_str;

}; // class ManualLibrarySlot

} // namespace mediachanger
} // namespace castor
