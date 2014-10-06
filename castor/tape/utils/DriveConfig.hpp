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
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/mediachanger/TapeLibrarySlot.hpp"

#include <list>
#include <string>

namespace castor {
namespace tape {
namespace utils {

/**
 * Structure used to store the configuration of a tape drive.
 */
struct DriveConfig {

  /**
   * The unit name of the tape drive as defined in /etc/castor/TPCONFIG and
   * used by the vdqmd daemon.
   */
  std::string unitName;

  /**
   * The device group name of the tape drive as defined in
   * /etc/castor/TPCONFIG and used by the vdqmd and vmgrd daemons.
   */
  std::string dgn;
  
  /**
   * The device file of the tape drive as defined in /etc/castor/TPCONFIG, for
   * example: /dev/nst0
   */
   std::string devFilename;

  /**
   * The list of tape densities supported by the tape drive as defined in
   * /etc/castor/TPCONFIG.
   */
  std::list<std::string> densities;

  /**
   * The library slot n which the tape drive is located, for example:
   * smc@localhost,0
   */
  mediachanger::TapeLibrarySlot librarySlot;

  /**
   * The device type.
   */
  std::string devType;

}; // class DriveConfig

} // namespace utils
} // namespace tape
} // namespace castor
