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

#include "castor/tape/tapeserver/daemon/ProbeSession.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProbeSession::ProbeSession(
  castor::log::Logger &log,
  const DriveConfig &driveConfig,
  System::virtualWrapper &sysWrapper):
  m_log(log),
  m_driveConfig(driveConfig),
  m_sysWrapper(sysWrapper) {
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
  castor::tape::tapeserver::daemon::ProbeSession::execute() throw() {
  std::string errorMessage;

  try {
    return exceptionThrowingExecute();
  } catch(castor::exception::Exception &ex) {
    errorMessage = ex.getMessage().str();
  } catch(std::exception &se) {
    errorMessage = se.what();
  } catch(...) {
    errorMessage = "Caught an unknown exception";
  }

  // Reaching this point means the probe failed and an exception was thrown
  log::Param params[] = {
    log::Param("unitName", m_driveConfig.getUnitName()),
    log::Param("message", errorMessage)};
  m_log(LOG_ERR, "Probe failed", params);
  return MARK_DRIVE_AS_DOWN;
}

//------------------------------------------------------------------------------
// exceptionThrowingExecute
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
  castor::tape::tapeserver::daemon::ProbeSession::exceptionThrowingExecute() {
  std::list<log::Param> params;
  params.push_back(log::Param("unitName", m_driveConfig.getUnitName()));

  std::auto_ptr<drive::DriveInterface> drivePtr = createDrive();
  drive::DriveInterface &drive = *drivePtr.get();

  if(drive.hasTapeInPlace()) {
    m_log(LOG_INFO, "Probe found tape drive with a tape inside", params);
    return MARK_DRIVE_AS_DOWN;
  } else {
    m_log(LOG_INFO, "Probe found tape drive is empty", params);
    return MARK_DRIVE_AS_UP;
  }
}

//------------------------------------------------------------------------------
// createDrive
//------------------------------------------------------------------------------
std::auto_ptr<castor::tape::tapeserver::drive::DriveInterface>
  castor::tape::tapeserver::daemon::ProbeSession::createDrive() {
  SCSI::DeviceVector dv(m_sysWrapper);
  SCSI::DeviceInfo driveInfo = dv.findBySymlink(m_driveConfig.getDevFilename());
  
  // Instantiate the drive object
  std::auto_ptr<castor::tape::tapeserver::drive::DriveInterface>
    drive(drive::createDrive(driveInfo, m_sysWrapper));

  if(NULL == drive.get()) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to instantiate drive object";
    throw ex;
  } 
    
  return drive;
}
