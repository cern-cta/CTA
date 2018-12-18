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

#include "castor/tape/tapeserver/daemon/EmptyDriveProbe.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::EmptyDriveProbe::EmptyDriveProbe(
  cta::log::Logger &log,
  const cta::tape::daemon::TpconfigLine &driveConfig,
  System::virtualWrapper &sysWrapper):
  m_log(log),
  m_driveConfig(driveConfig),
  m_sysWrapper(sysWrapper) {
}

//------------------------------------------------------------------------------
// driveIsEmpty()
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::EmptyDriveProbe::driveIsEmpty() throw() {
  std::string errorMessage;

  try {
    return exceptionThrowingDriveIsEmpty();
  } catch(cta::exception::Exception &ex) {
    errorMessage = ex.getMessage().str();
  } catch(std::exception &se) {
    errorMessage = se.what();
  } catch(...) {
    errorMessage = "Caught an unknown exception";
  }

  // Reaching this point means the probe failed and an exception was thrown
  std::list<cta::log::Param> params = {
    cta::log::Param("tapeDrive", m_driveConfig.unitName),
    cta::log::Param("message", errorMessage)};
  m_log(cta::log::ERR, "Probe failed", params);
  return false;
}

//------------------------------------------------------------------------------
// exceptionThrowingDriveIsEmpty
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::EmptyDriveProbe::
  exceptionThrowingDriveIsEmpty() {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("tapeDrive", m_driveConfig.unitName));

  std::unique_ptr<drive::DriveInterface> drivePtr = createDrive();
  drive::DriveInterface &drive = *drivePtr.get();

  if(drive.hasTapeInPlace()) {
    m_log(cta::log::INFO, "Probe found tape drive with a tape inside", params);
    return false;
  } else {
    m_log(cta::log::INFO, "Probe found tape drive is empty", params);
    return true;
  }
}

//------------------------------------------------------------------------------
// createDrive
//------------------------------------------------------------------------------
std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface>
  castor::tape::tapeserver::daemon::EmptyDriveProbe::createDrive() {
  SCSI::DeviceVector dv(m_sysWrapper);
  SCSI::DeviceInfo driveInfo = dv.findBySymlink(m_driveConfig.devFilename);
  
  // Instantiate the drive object
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface>
    drive(drive::createDrive(driveInfo, m_sysWrapper));

  if(NULL == drive.get()) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to instantiate drive object";
    throw ex;
  } 
    
  return drive;
}
