/******************************************************************************
 *         castor/legacymsg/ScsiLibrarySlot.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/exception/Exception.hpp"

#include <stdint.h>
#include <string>

namespace castor {
namespace legacymsg {

/**
 * Structure representing a SCSI library slot.
 */
class ScsiLibrarySlot {
public:

  /**
   * The name of the host on which the rmcd daemon is running.
   */
  std::string rmcHostName;

  /**
   * The drive ordinal.
   */
  uint16_t drvOrd;

  /**
   * Constructor.
   *
   * Sets all integer member-variables to 0.
   */
  ScsiLibrarySlot() throw();

  /**
   * Constructor.
   *
   * Initialises the member variables based on the result of parsing the
   * specified string representation of a SCSI library slot.
   *
   * @param str The string representation of a SCSI library slot.
   */
  ScsiLibrarySlot(const std::string &str) throw(castor::exception::Exception);
}; // class ScsiLibrarySlot

} // namespace legacymsg
} // namespace castor

