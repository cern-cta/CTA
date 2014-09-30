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

#include "castor/tape/tapeserver/daemon/Catalogue.hpp"
#include "castor/utils/utils.hpp"

#include <string.h>
#include <time.h>

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Catalogue::Catalogue(
  const int netTimeout,
  log::Logger &log,
  const DataTransferSession::CastorConf &dataTransferConfig,
  ProcessForkerProxy &processForker,
  legacymsg::CupvProxy &cupv,
  legacymsg::VdqmProxy &vdqm,
  legacymsg::VmgrProxy &vmgr,
  const std::string &hostName):
  m_netTimeout(netTimeout),
  m_log(log),
  m_dataTransferConfig(dataTransferConfig),
  m_processForker(processForker),
  m_cupv(cupv),
  m_vdqm(vdqm),
  m_vmgr(vmgr),
  m_hostName(hostName) {
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Catalogue::~Catalogue() throw() {
  // Close any label-command connections that are still owned by the
  // tape-drive catalogue
  for(DriveMap::const_iterator itor = m_drives.begin(); itor != m_drives.end();
    itor++) {
    const CatalogueDrive *const drive = itor->second;

    delete drive;
  }
}

//-----------------------------------------------------------------------------
// tick
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::Catalogue::tick() {
  for(DriveMap::const_iterator itor = m_drives.begin(); itor != m_drives.end();
    itor++) {
    CatalogueDrive *const drive = itor->second;
    drive->tick();
  }
}

//-----------------------------------------------------------------------------
// populate
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::Catalogue::populate(
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
void castor::tape::tapeserver::daemon::Catalogue::enterDriveConfig(
  const utils::DriveConfig &driveConfig)  {

  DriveMap::iterator itor = m_drives.find(driveConfig.unitName);

  // If the drive is not in the catalogue
  if(m_drives.end() == itor) {
    // Insert it
    m_drives[driveConfig.unitName] = new CatalogueDrive(m_netTimeout,
      m_log, m_processForker, m_cupv, m_vdqm, m_vmgr, m_hostName, driveConfig,
      m_dataTransferConfig, CatalogueDrive::DRIVE_STATE_DOWN);
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
  castor::tape::tapeserver::daemon::Catalogue::getUnitNames() const  {
  std::list<std::string> unitNames;

  for(DriveMap::const_iterator itor = m_drives.begin();
    itor != m_drives.end(); itor++) {
    unitNames.push_back(itor->first);
  }

  return unitNames;
}

//-----------------------------------------------------------------------------
// findDrive
//-----------------------------------------------------------------------------
const castor::tape::tapeserver::daemon::CatalogueDrive
  &castor::tape::tapeserver::daemon::Catalogue::findDrive(
    const std::string &unitName) const {
  std::ostringstream task;
  task << "find tape drive in catalogue by unit name: unitName=" << unitName;

  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": Entry does not exist";
    throw ex;
  }

  if(NULL == itor->second) {
    // Should never get here
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Pointer to drive entry is unexpectedly NULL";
    throw ex;
  }

  const CatalogueDrive &drive = *(itor->second);
  const utils::DriveConfig &driveConfig = drive.getConfig();

  // Sanity check
  if(unitName != driveConfig.unitName) {
    // Should never get here
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Found inconsistent entry in tape-drive catalogue"
      ": Unit name mismatch: actual=" << driveConfig.unitName;
    throw ex;
  }

  return drive;
}

//-----------------------------------------------------------------------------
// findDrive
//-----------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueDrive
  &castor::tape::tapeserver::daemon::Catalogue::findDrive(
  const std::string &unitName) {
  std::ostringstream task;
  task << "find tape drive in catalogue by unit name: unitName=" << unitName;

  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": Entry does not exist";
    throw ex;
  }

  if(NULL == itor->second) {
    // Should never get here
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Pointer to drive entry is unexpectedly NULL";
    throw ex;
  }

  CatalogueDrive &drive = *(itor->second);
  const utils::DriveConfig &driveConfig = drive.getConfig();

  // Sanity check
  if(unitName != driveConfig.unitName) {
    // This should never happen
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
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
const castor::tape::tapeserver::daemon::CatalogueDrive
  &castor::tape::tapeserver::daemon::Catalogue::findDrive(
    const pid_t sessionPid) const {
  std::ostringstream task;
  task << "find tape drive in catalogue by session pid: sessionPid=" <<
    sessionPid;

  for(DriveMap::const_iterator itor = m_drives.begin(); itor != m_drives.end();
    itor++) {

    if(NULL == itor->second) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Encountered NULL drive-entry pointer: unitName=" <<  itor->first;
      throw ex;
    }

    const CatalogueDrive &drive = *(itor->second);
    try {
      const CatalogueSession &session = drive.getSession();
      if(sessionPid == session.getPid()) {
        return drive;
      }
    } catch(...) {
      // Ignore any exceptions thrown by getSession()
    }
  }

  castor::exception::Exception ex;
  ex.getMessage() << "Failed to " << task.str() << ": Entry does not exist";
  throw ex;
}

//-----------------------------------------------------------------------------
// findDrive
//-----------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueDrive
  &castor::tape::tapeserver::daemon::Catalogue::findDrive(
    const pid_t sessionPid) {
  std::ostringstream task;
  task << "find tape drive in catalogue by session pid: sessionPid=" <<
    sessionPid;

  for(DriveMap::iterator itor = m_drives.begin(); itor != m_drives.end();
    itor++) {

    if(NULL == itor->second) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Encountered NULL drive-entry pointer: unitName=" <<  itor->first;
      throw ex;
    }

    CatalogueDrive &drive = *(itor->second);
    try {
      const CatalogueSession &session = drive.getSession();
      if(sessionPid == session.getPid()) {
        return drive;
      }
    } catch(...) {
      // Ignore any exceptions thrown by getSessionPid()
    }
  }

  castor::exception::Exception ex;
  ex.getMessage() << "Failed to " << task.str() << ": Entry does not exist";
  throw ex;
}
