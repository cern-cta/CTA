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

#include "castor/exception/InvalidArgument.hpp"

#include <stdint.h>
#include <string>

namespace castor {
namespace mediachanger {

/**
 * Class reprsenting a slot in an ACS tape-library.
 */
class AcsLibrarySlot {
public:

  /**
   * Default constructor that sets all integer members to 0.
   */
  AcsLibrarySlot() throw();

  /**
   * Constructor.
   *
   * This method throws a castor::exception::InvalidArgument if the specified
   * string representation is invalid.
   *
   * @param The string representation of a slot in an ACS tape-library in format
   * acsACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER
   */
  AcsLibrarySlot(const std::string &str);
  
  /**
   * Returns true if the specified string only contains numerals else false.
   *
   * @return True if the specified string only contains numerals else false.
   */
  bool onlyContainsNumerals(const std::string &str) const throw();
  
  /**
   * Gets the acs component of the library slot.
   *
   * @return the acs component of the library slot.
   */
  uint32_t getAcs() const throw ();

  /**
   * Gets the lsm component of the library slot.
   *
   * @return the lsm component of the library slot.
   */
  uint32_t getLsm() const throw ();

  /**
   * Gets the panel component of the library slot.
   *
   * @return the panel component of the library slot.
   */
  uint32_t getPanel() const throw ();

  /**
   * Gets the drive component of the library slot.
   *
   * @return the drive component of the library slot.
   */
  uint32_t getDrive() const throw ();

  /**
   * Returns the representation of the slot.
   */
  std::string str() const;

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
  
}; // class AcsProxy

} // namespace mediachanger
} // namespace castor

