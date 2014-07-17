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

#include "castor/tape/tapeserver/daemon/DriveCatalogue.hpp"
#include "castor/utils/utils.hpp"
#include "castor/legacymsg/TapeUpdateDriveRqstMsgBody.hpp"

#include <string.h>
#include <time.h>

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogue::~DriveCatalogue() throw() {
  // Close any label-command connections that are still owned by the
  // tape-drive catalogue
  for(DriveMap::const_iterator itor = m_drives.begin(); itor != m_drives.end();
     itor++) {
    const DriveCatalogueEntry *drive = itor->second;

    if(DriveCatalogueEntry::SESSION_TYPE_LABEL == drive->getSessionType() && 
      DriveCatalogueSession::SESSION_STATE_WAITFORK == drive->getSessionState()
      && -1 != drive->getLabelCmdConnection()) {
      close(drive->getLabelCmdConnection());
    }
    delete drive;
  }
}

//-----------------------------------------------------------------------------
// populateCatalogue
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::populateCatalogue(
  const utils::DriveConfigMap &driveConfigs)  {
  try {
    for(utils::DriveConfigMap::const_iterator itor = driveConfigs.begin();
      itor != driveConfigs.end(); itor++) {
      const std::string &unitName = itor->first;
      const utils::DriveConfig &driveConfig = itor->second;

      // Sanity check
      if(unitName != driveConfig.unitName) {
        // This should never happen
        castor::exception::Exception ex;
        ex.getMessage() << "Unit name mismatch: expected=" << unitName <<
          " actual=" << driveConfig.unitName;
        throw ex;
      }
      enterDriveConfig(driveConfig);
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to populate tape-drive catalogue: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// enterDriveConfig
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::enterDriveConfig(
  const utils::DriveConfig &driveConfig)  {

  DriveMap::iterator itor = m_drives.find(driveConfig.unitName);

  // If the drive is not in the catalogue
  if(m_drives.end() == itor) {
    // Insert it
    m_drives[driveConfig.unitName] = new DriveCatalogueEntry(driveConfig,
      DriveCatalogueEntry::DRIVE_STATE_DOWN);
  // Else the drive is already in the catalogue
  } else {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to enter tape-drive configuration into tape-drive catalogue"
      ": Duplicate drive-entry: unitName=" << driveConfig.unitName;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// getUnitNames
//-----------------------------------------------------------------------------
std::list<std::string>
  castor::tape::tapeserver::daemon::DriveCatalogue::getUnitNames() const  {
  std::list<std::string> unitNames;

  for(DriveMap::const_iterator itor = m_drives.begin();
    itor != m_drives.end(); itor++) {
    unitNames.push_back(itor->first);
  }

  return unitNames;
}

//-----------------------------------------------------------------------------
// getUnitNames
//-----------------------------------------------------------------------------
std::list<std::string>
  castor::tape::tapeserver::daemon::DriveCatalogue::getUnitNames(
    const DriveCatalogueEntry::DriveState state) const {
  std::list<std::string> unitNames;

  for(DriveMap::const_iterator itor = m_drives.begin();
    itor != m_drives.end(); itor++) {
    const std::string &unitName = itor->first;
    const DriveCatalogueEntry &drive = *(itor->second);
    const utils::DriveConfig &driveConfig = drive.getConfig();

    // Sanity check
    if(unitName != driveConfig.unitName) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to get unit names of drives in state " <<
        DriveCatalogueEntry::drvState2Str(state) <<
        ": unit name mismatch: unitName=" << unitName <<
        " driveConfig.unitName=" <<  driveConfig.unitName;
      throw ex;
    }

    if(state == drive.getState()) {
      unitNames.push_back(itor->first);
    }
  }

  return unitNames;
}

//-----------------------------------------------------------------------------
// getUnitNamesWaitingForTransferFork
//-----------------------------------------------------------------------------
std::list<std::string> castor::tape::tapeserver::daemon::DriveCatalogue::getUnitNamesWaitingForTransferFork() const {
  std::list<std::string> unitNames;

  for(DriveMap::const_iterator itor = m_drives.begin();
    itor != m_drives.end(); itor++) {
    const std::string &unitName = itor->first;
    const DriveCatalogueEntry &drive = *(itor->second);
    const utils::DriveConfig &driveConfig = drive.getConfig();

    // Sanity check
    if(unitName != driveConfig.unitName) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to get unit names of drives waiting for forking a transfer session" <<
        ": unit name mismatch: unitName=" << unitName <<
        " driveConfig.unitName=" <<  driveConfig.unitName;
      throw ex;
    }

    if(DriveCatalogueEntry::SESSION_TYPE_DATATRANSFER==drive.getSessionType()
       && DriveCatalogueSession::SESSION_STATE_WAITFORK == drive.getSessionState()) {
      unitNames.push_back(itor->first);
    }
  }

  return unitNames;
}

//-----------------------------------------------------------------------------
// getUnitNamesWaitingForLabelFork
//-----------------------------------------------------------------------------
std::list<std::string> castor::tape::tapeserver::daemon::DriveCatalogue::
  getUnitNamesWaitingForLabelFork() const {
  std::list<std::string> unitNames;

  for(DriveMap::const_iterator itor = m_drives.begin();
    itor != m_drives.end(); itor++) {
    const std::string &unitName = itor->first;
    const DriveCatalogueEntry &drive = *(itor->second);
    const utils::DriveConfig &driveConfig = drive.getConfig();

    // Sanity check
    if(unitName != driveConfig.unitName) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to get unit names of drives waiting for forking a label session" <<
        ": unit name mismatch: unitName=" << unitName <<
        " driveConfig.unitName=" <<  driveConfig.unitName;
      throw ex;
    }

    if(DriveCatalogueEntry::SESSION_TYPE_LABEL==drive.getSessionType() &&
      DriveCatalogueSession::SESSION_STATE_WAITFORK == drive.getSessionState()) {
      unitNames.push_back(itor->first);
    }
  }

  return unitNames;
}

//-----------------------------------------------------------------------------
// getUnitNamesWaitingForCleanerFork
//-----------------------------------------------------------------------------
std::list<std::string> castor::tape::tapeserver::daemon::DriveCatalogue::
  getUnitNamesWaitingForCleanerFork() const {
  std::list<std::string> unitNames;

  for(DriveMap::const_iterator itor = m_drives.begin();
    itor != m_drives.end(); itor++) {
    const std::string &unitName = itor->first;
    const DriveCatalogueEntry &drive = *(itor->second);
    const utils::DriveConfig &driveConfig = drive.getConfig();

    // Sanity check
    if(unitName != driveConfig.unitName) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to get unit names of drives waiting for forking a cleaner session" <<
        ": unit name mismatch: unitName=" << unitName <<
        " driveConfig.unitName=" <<  driveConfig.unitName;
      throw ex;
    }

    if(DriveCatalogueEntry::SESSION_TYPE_CLEANER==drive.getSessionType() &&
      DriveCatalogueSession::SESSION_STATE_WAITFORK == drive.getSessionState()) {
      unitNames.push_back(itor->first);
    }
  }

  return unitNames;
}

//-----------------------------------------------------------------------------
// findConstDrive
//-----------------------------------------------------------------------------
const castor::tape::tapeserver::daemon::DriveCatalogueEntry
  *castor::tape::tapeserver::daemon::DriveCatalogue::findDrive(
    const std::string &unitName) const {

  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to find tape-drive in catalogue: " <<
      ": No entry for tape-drive " << unitName;
    throw ex;
  }
  const DriveCatalogueEntry * const drive = itor->second;
  const utils::DriveConfig &driveConfig = drive->getConfig();

  // Sanity check
  if(unitName != driveConfig.unitName) {
    // This should never happen
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to find tape-drive in catalogue: " <<
      ": Found inconsistent entry in tape-drive catalogue"
      ": Unit name mismatch: expected=" << unitName <<
      " actual=" << driveConfig.unitName;
    throw ex;
  }

  return drive;
}

//-----------------------------------------------------------------------------
// findConstDrive
//-----------------------------------------------------------------------------
const castor::tape::tapeserver::daemon::DriveCatalogueEntry
  *castor::tape::tapeserver::daemon::DriveCatalogue::findDrive(
    const pid_t sessionPid) const {

  for(DriveMap::const_iterator i = m_drives.begin(); i!=m_drives.end(); i++) {
    const DriveCatalogueEntry * const drive = i->second;
    try {
      if(sessionPid == drive->getSessionPid()) {
        return drive;
      }
    } catch(...) {
      // Ignore any exceptions thrown by getSessionPid()
    }
  }

  castor::exception::Exception ex;
  ex.getMessage() << "Failed to find tape-drive in catalogue: " <<
    ": No drive associated with session process-ID " << sessionPid;
  throw ex;
}

//-----------------------------------------------------------------------------
// findDrive
//-----------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueEntry
  *castor::tape::tapeserver::daemon::DriveCatalogue::findDrive(
    const std::string &unitName) {

  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to find tape-drive in catalogue: " <<
      "No entry for tape-drive " << unitName;
    throw ex;
  }
  DriveCatalogueEntry * drive = itor->second;
  const utils::DriveConfig &driveConfig = drive->getConfig();

  // Sanity check
  if(unitName != driveConfig.unitName) {
    // This should never happen
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to find tape-drive in catalogue" <<
      ": Found inconsistent entry in tape-drive catalogue"
      ": Unit name mismatch: expected=" << unitName <<
      " actual=" << driveConfig.unitName;
    throw ex;
  }

  return drive;
}

//-----------------------------------------------------------------------------
// findDrive
//-----------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueEntry
  *castor::tape::tapeserver::daemon::DriveCatalogue::findDrive(
    const pid_t sessionPid) {

  for(DriveMap::iterator i = m_drives.begin(); i!=m_drives.end(); i++) {
    DriveCatalogueEntry * drive = i->second;
    try {
      if(sessionPid == drive->getSessionPid()) {
        return drive;
      }
    } catch(...) {
      // Ignore any exceptions thrown by getSessionPid()
    }
  }

  castor::exception::Exception ex;
  ex.getMessage() << "Failed to find tape-drive in catalogue: " <<
    ": No drive associated with session process-ID " << sessionPid;
  throw ex;
}
