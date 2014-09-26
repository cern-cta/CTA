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
   * Request the CASTOR ACS daemon to mount the specifed tape for recall.
   *
   * @param vid The tape to be mounted.
   * @param acs The ACS identifier.
   * @param lsm The LSM identifier.
   * @param panel The panel identifier.
   * @param drive The drive identifier.
   */
  virtual void mountTapeForRecall(const std::string &vid, const uint32_t acs,
     const uint32_t lsm, const uint32_t panel, const uint32_t drive) = 0;

}; // class AcsProxy

} // namespace messages
} // namespace castor

