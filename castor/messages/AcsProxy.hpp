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

#include "castor/mediachanger/AcsLibrarySlot.hpp"

#include <stdint.h>
#include <string>

namespace castor {
namespace messages {

/**
 * Abstract class defining the interface to a proxy object representing the
 * CASTOR ACS daemon.
 */
class AcsProxy {
public:

  /**
   * Destructor.
   */
  virtual ~AcsProxy()  = 0;

  /**
   * Request the CASTOR ACS daemon to mount the specified tape for read-only
   * access into the tape drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   */
  virtual void mountTapeReadOnly(const std::string &vid,
    const mediachanger::AcsLibrarySlot &librarySlot) = 0;
  
  /**
   * Request the CASTOR ACS daemon to mount the specifed tape for read/write
   * access into the tape drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   */
  virtual void mountTapeReadWrite(const std::string &vid,
    const mediachanger::AcsLibrarySlot &librarySlot) = 0;
  
  /**
   * Request the CASTOR ACS daemon to dismount the specifed tape from the tape
   * drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   */
  virtual void dismountTape(const std::string &vid,
    const mediachanger::AcsLibrarySlot &librarySlot) = 0;

}; // class AcsProxy

} // namespace messages
} // namespace castor

