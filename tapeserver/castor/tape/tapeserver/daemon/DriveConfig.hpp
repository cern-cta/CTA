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

#include "castor/mediachanger/LibrarySlot.hpp"
#include "castor/mediachanger/LibrarySlotParser.hpp"

#include <list>
#include <string>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/**
 * Class representing the configuration of a tape drive as specified in the
 * TPCONFIG file.
 */
class DriveConfig {
public:
  /**
   * Constructor.
   */
  DriveConfig() throw();

  /**
   * Constructor.
   *
   * @param unitName The unit name of the tape drive.
   * @param logicalLibrary The logical Library of the tape drive as used by the vdqmd
   * and vmgrd daemons.
   * @param devFilename The device file of the tape drive, for example:
   * /dev/nst0.
   * @param librarySlot The library slot in which the tape drive is located,
   * for example: smc@localhost,0
   */
  DriveConfig(
    const std::string &unitName,
    const std::string &logicalLibrary,
    const std::string &devFilename,
    const std::string &librarySlot);

  /**
   * Copy constructor.
   *
   * @param obj The object to be copied.
   */
  DriveConfig(const DriveConfig &obj);

  /**
   * Destructor.
   */
  ~DriveConfig() throw();

  /**
   * Assignment operator.
   *
   * @param rhs The right-hand side of the operator.
   */
  DriveConfig &operator=(const DriveConfig &rhs);

  /**
   * Gets the unit name of the tape drive as used by the vdqmd daemon.
   *
   * @return The unit name of the tape drive as used by the vdqmd daemon.
   */
  const std::string &getUnitName() const throw();

  /**
   * Gets the logical Library name of the tape drive as used by the vdqmd and
   * vmgrd daemons.
   *
   * @return The logical Library name of the tape drive as used by the vdqmd and
   * vmgrd daemons.
   */
  const std::string &getLogicalLibrary() const throw();
  
  /**
   * Gets the device file of the tape drive, for example: /dev/nst0
   *
   * @return The device file of the tape drive, for example: /dev/nst0
   */
  const std::string &getDevFilename() const throw();

  /**
   * Gets the library slot in which the tape drive is located, for example:
   * smc@localhost,0
   *
   * @return The library slot in which the tape drive is located, for example:
   * smc@localhost,0
   */
  const mediachanger::LibrarySlot &getLibrarySlot() const;

private:

  /**
   * The unit name of the tape drive as used by the vdqmd daemon.
   */
  std::string m_unitName;

  /**
   * The logical Library of the tape drive as used by the vdqmd and vmgrd
   * daemons.
   */
  std::string m_logicalLibrary;
  
  /**
   * The device file of the tape drive, for example: /dev/nst0
   */
   std::string m_devFilename;

  /**
   * The library slot in which the tape drive is located, for example:
   * smc@localhost,0
   */
  mediachanger::LibrarySlot *m_librarySlot;

}; // class DriveConfig

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
