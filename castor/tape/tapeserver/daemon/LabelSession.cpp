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

#include "castor/io/io.hpp"
#include "castor/legacymsg/legacymsg.hpp"
#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/tape/tapeserver/daemon/LabelSession.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/System.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "h/Cns.h"
#include "h/getconfent.h"
#include "h/serrno.h"
#include "h/log.h"

#include <memory>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::LabelSession::LabelSession(
  messages::TapeserverProxy &tapeserver,
  legacymsg::RmcProxy &rmc, 
  const legacymsg::TapeLabelRqstMsgBody &clientRequest,
  castor::log::Logger &log,
  System::virtualWrapper &sysWrapper,
  const utils::DriveConfig &driveConfig,
  const bool force):
  m_tapeserver(tapeserver),
  m_rmc(rmc),
  m_request(clientRequest),
  m_log(log),
  m_sysWrapper(sysWrapper),
  m_driveConfig(driveConfig),
  m_force(force) {
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
  castor::tape::tapeserver::daemon::LabelSession::execute() {
  try {
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn)};
  
    mountTape();
    std::auto_ptr<castor::tape::tapeserver::drive::DriveInterface> drive =
      getDriveObject();
    waitUntilTapeLoaded(drive.get(), 60); // 60 = 60 seconds
    checkTapeIsWritable(drive.get());
    labelTheTape(drive.get());
    m_log(LOG_INFO, "The tape has been successfully labeled", params);
    drive->unloadTape();
    m_log(LOG_INFO, "The tape has been successfully unloaded after labeling",
      params);
    m_rmc.unmountTape(m_request.vid, m_driveConfig.librarySlot);
    m_log(LOG_INFO, "The tape has been successfully unmounted after labeling",
      params);

    return MARK_DRIVE_AS_UP;
  } catch(castor::exception::Exception &ex) {

    // Send details of exception to tapeserverd and then rethrow
    m_tapeserver.labelError(m_request.drive, ex);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getDriveObject
//------------------------------------------------------------------------------
std::auto_ptr<castor::tape::tapeserver::drive::DriveInterface>
  castor::tape::tapeserver::daemon::LabelSession::getDriveObject() {
  castor::tape::SCSI::DeviceVector dv(m_sysWrapper);    
  castor::tape::SCSI::DeviceInfo driveInfo =
    dv.findBySymlink(m_driveConfig.devFilename);
  
  // Instantiate the drive object
  std::auto_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(
    castor::tape::tapeserver::drive::createDrive(driveInfo, m_sysWrapper));

  if(NULL == drive.get()) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "End session with error. Failed to instantiate drive object";
    throw ex;
  }
  
  // check that drive is not write protected
  if(drive->isWriteProtected()) {   
    castor::exception::Exception ex;
    ex.getMessage() <<
      "End session with error. Drive is write protected. Aborting labelling...";
    throw ex;
  }

  return drive;
}

//------------------------------------------------------------------------------
// mountTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::mountTape() {
  try {
    m_rmc.mountTape(m_request.vid, m_driveConfig.librarySlot,
    castor::legacymsg::RmcProxy::MOUNT_MODE_READWRITE);
    const log::Param params[] = {
      log::Param("vid", m_request.vid),
      log::Param("unitName", m_request.drive),
      log::Param("librarySlot", m_driveConfig.librarySlot)};
    m_log(LOG_INFO, "Tape successfully mounted for labeling", params);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to mount tape for labeling: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// waitUntilTapeLoaded
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::waitUntilTapeLoaded(
  drive::DriveInterface *const drive, const int timeoutSecond) { 
  try {
    drive->waitUntilReady(timeoutSecond);
    const log::Param params[] = {
      log::Param("vid", m_request.vid),
      log::Param("unitName", m_request.drive),
      log::Param("librarySlot", m_driveConfig.librarySlot)};
    m_log(LOG_INFO, "Tape to be labelled has been mounted", params);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to wait for tape to be loaded: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// checkTapeIsWritable
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::checkTapeIsWritable(
  drive::DriveInterface *const drive) {
  if(drive->isWriteProtected()) {
    castor::exception::Exception ex;
    ex.getMessage() << "Tape to be labeled in write protected";
    throw ex;
  }
  const log::Param params[] = {
    log::Param("vid", m_request.vid),
    log::Param("unitName", m_request.drive),
    log::Param("librarySlot", m_driveConfig.librarySlot)};
  m_log(LOG_INFO, "Tape to be labelled is writable", params);
}

//------------------------------------------------------------------------------
// labelTheTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::labelTheTape(
  drive::DriveInterface *const drive) {
  tapeFile::LabelSession ls(*drive, m_request.vid, m_force);
}
