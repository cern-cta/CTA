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

extern "C" {
#include "acssys.h"
#include "acsapi.h"
}

#include <string>

namespace castor {
namespace acs {

/**
 * Data type used to store the results of parsing the command-line.
 */
struct AcsMountCmdLine {
  /**
   * Constructor.
   *
   * Initialises all BOOLEAN member-variables to FALSE, all integer
   * member-variables to 0 and the volume identifier to an empty string.
   */
  AcsMountCmdLine() throw();

  /**
   * True if the debug option has been set.
   */
  bool debug;

  /**
   * True if the help option has been set.
   */
  bool help;

  /**
   * Time in seconds to wait between queries to ACS for responses.
   */
  int queryInterval;

  /**
   * True if the tape is to be mount for read-only access.
   */
  BOOLEAN readOnly;

  /**
   * Time in seconds to wait for the mount to conclude.
   */
  int timeout;

  /**
   * The volume identifier of the tape to be mounted.
   */
  VOLID volId;

  /**
   * The identifier of the drive into which the tape is to be mounted.
   */
  DRIVEID driveId;

}; // class AcsMountCmdLine

} // namespace acs
} // namespace castor
