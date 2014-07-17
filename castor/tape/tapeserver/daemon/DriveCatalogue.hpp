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

#include "castor/exception/Exception.hpp"
#include "castor/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/legacymsg/TapeLabelRqstMsgBody.hpp"
#include "castor/tape/utils/DriveConfigMap.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueEntry.hpp"
#include "castor/legacymsg/TapeUpdateDriveRqstMsgBody.hpp"

#include <map>
#include <string>
#include <string.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Class responsible for keeping track of the tape drive being controlled by
 * the tapeserverd daemon.
 */
class DriveCatalogue {
public:

  /**
   * Destructor.
   *
   * Closes the connection with the label command if the drive catalogue owns
   * the connection at the time of destruction.
   */
  ~DriveCatalogue() throw();

  /**
   * Poplates the catalogue using the specified tape-drive configurations.
   *
   * @param driveConfigs Tape-drive configurations.
   */
  void populateCatalogue(const utils::DriveConfigMap &driveConfigs);

  /**
   * Returns a const reference to the tape-drive entry corresponding to the
   * tape drive with the specified unit name.
   *
   * This method throws an exception if the tape-drive entry cannot be found.
   *
   * @param unitName The unit name of the tape drive.
   */
  const DriveCatalogueEntry *findDrive(const std::string &unitName)
    const;

  /**
   * Returns a const reference to the tape-drive entry associated with the
   * session with the specified process ID.
   *
   * This method throws an exception if the tape-drive entry cannot be found.
   *
   * @param sessionPid The process ID of the session.
   */
  const DriveCatalogueEntry *findDrive(const pid_t sessionPid) const;

  /**
   * Returns a reference to the tape-drive entry corresponding to the tape
   * drive with the specified unit name.
   *
   * This method throws an exception if the tape-drive entry cannot be found.
   *
   * @param unitName The unit name of the tape drive.
   */
  DriveCatalogueEntry *findDrive(const std::string &unitName);

  /**
   * Returns a reference to the tape-drive entry associated with the
   * session with the specified process ID.
   *
   * This method throws an exception if the tape-drive entry cannot be found.
   *
   * @param sessionPid The process ID of the session.
   */
  DriveCatalogueEntry *findDrive(const pid_t sessionPid);

  /**
   * Returns an unordered list of the unit names of all of the tape drives
   * stored within the tape drive catalogue.
   *
   * @return Unordered list of the unit names.
   */
  std::list<std::string> getUnitNames() const;

  /**
   * Returns an unordered list of the unit names of the tape drives in the
   * specified state.
   *
   * @return Unordered list of the unit names.
   */
  std::list<std::string> getUnitNames(const DriveCatalogueEntry::DriveState state) const;
  
  /**
   * Returns an unordered list of the unit names of the tape drives waiting for
   * forking a transfer session.
   *
   * @return Unordered list of the unit names.
   */
  std::list<std::string> getUnitNamesWaitingForTransferFork() const;
  
  /**
   * Returns an unordered list of the unit names of the tape drives waiting for
   * forking a label session.
   *
   * @return Unordered list of the unit names.
   */
  std::list<std::string> getUnitNamesWaitingForLabelFork() const;
  
  /**
   * Returns an unordered list of the unit names of the tape drives waiting for
   * forking a cleaner session.
   *
   * @return Unordered list of the unit names.
   */
  std::list<std::string> getUnitNamesWaitingForCleanerFork() const;

private:

  /**
   * Type that maps the unit name of a tape drive to the catalogue entry of
   * that drive.
   */
  typedef std::map<std::string, DriveCatalogueEntry *> DriveMap;

  /**
   * Map from the unit name of a tape drive to the catalogue entry of that
   * drive.
   */
  DriveMap m_drives;

  /** 
   * Enters the specified tape-drive configuration into the catalogue.
   *
   * @param driveConfig The tape-drive configuration.
   */
  void enterDriveConfig(const utils::DriveConfig &driveConfig);

}; // class DriveCatalogue

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
