/******************************************************************************
 *         castor/tape/tapeserver/daemon/ScsiLibraryDriveName.hpp
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

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Structure reprsenting a SCSI-library drive-name.
 */
class ScsiLibraryDriveName {
public:

  /**
   * The name of teh host on which the rmcd daemon is running.
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
  ScsiLibraryDriveName() throw();

  /**
   * Constructor.
   *
   * Initialises the member variables based on the result of parsing the
   * specified SCSI-library drive-name.
   *
   * @param drive The SCSI-library drive-name.
   */
  ScsiLibraryDriveName(const std::string &drive) throw(castor::exception::Exception);
}; // class ScsiLibraryDriveName

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

