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

#include "castor/exception/Exception.hpp"
#include "castor/io/io.hpp"
#include "castor/legacymsg/legacymsg.hpp"
#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/System.hpp"
#include "castor/tape/tapeserver/daemon/LabelSession.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "h/Cns.h"
#include "h/getconfent.h"
#include "h/log.h"
#include "h/serrno.h"

#include <memory>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::LabelSession::LabelSession(
  server::ProcessCap &capUtils,
  messages::TapeserverProxy &tapeserver,
  mediachanger::MediaChangerFacade &mc, 
  const legacymsg::TapeLabelRqstMsgBody &clientRequest,
  castor::log::Logger &log,
  System::virtualWrapper &sysWrapper,
  const DriveConfig &driveConfig,
  const bool force):
  m_capUtils(capUtils),
  m_tapeserver(tapeserver),
  m_mc(mc),
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
  castor::tape::tapeserver::daemon::LabelSession::execute() throw() {
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

  // Reaching this point means the label session failed and an exception was
  // thrown
  std::list<log::Param> params;
  params.push_back(log::Param("uid", m_request.uid));
  params.push_back(log::Param("gid", m_request.gid));
  params.push_back(log::Param("TPVID", m_request.vid));
  params.push_back(log::Param("unitName", m_request.drive));
  params.push_back(log::Param("dgn", m_request.dgn));
  params.push_back(log::Param("force", boolToStr(m_force)));
  params.push_back(log::Param("message", errorMessage));
  m_log(LOG_ERR, "Label session failed", params);

  // Send details of exception to tapeserverd and then re-throw
  m_tapeserver.labelError(m_request.drive, errorMessage);

  return MARK_DRIVE_AS_DOWN;
}

//------------------------------------------------------------------------------
// exceptionThrowingExecute
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
  castor::tape::tapeserver::daemon::LabelSession::exceptionThrowingExecute() {

  setProcessCapabilities("cap_sys_rawio+ep");

  mountTape();

  std::auto_ptr<drive::DriveInterface> drivePtr = createDrive();
  drive::DriveInterface &drive = *drivePtr.get();

  waitUntilTapeLoaded(drive, 60); // 60 = 60 seconds

  checkTapeIsWritable(drive);

  rewindDrive(drive);

  // If the user is trying to label a non-empty tape without the force option
  if(!m_force && !drive.isTapeBlank()) {
    const std::string message = "Cannot label a non-empty tape without the"
      " force option";
    notifyTapeserverOfUserError(message);

  // Else the labeling can go ahead
  } else {
    writeLabelToTape(drive);
  }

  unloadTape(m_request.vid, drive);
  dismountTape(m_request.vid);

  return MARK_DRIVE_AS_UP;
}

//------------------------------------------------------------------------------
// setProcessCapabilities
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::setProcessCapabilities(
  const std::string &capabilities) {
  std::list<log::Param> params;
  params.push_back(log::Param("uid", m_request.uid));
  params.push_back(log::Param("gid", m_request.gid));
  params.push_back(log::Param("TPVID", m_request.vid));
  params.push_back(log::Param("unitName", m_request.drive));
  params.push_back(log::Param("dgn", m_request.dgn));
  params.push_back(log::Param("force", boolToStr(m_force)));

  m_capUtils.setProcText(capabilities);
  params.push_back(log::Param("capabilities", m_capUtils.getProcText()));
  m_log(LOG_INFO, "Label session set process capabilities", params);
}

//------------------------------------------------------------------------------
// createDrive
//------------------------------------------------------------------------------
std::auto_ptr<castor::tape::tapeserver::drive::DriveInterface>
  castor::tape::tapeserver::daemon::LabelSession::createDrive() {
  SCSI::DeviceVector dv(m_sysWrapper);    
  SCSI::DeviceInfo driveInfo = dv.findBySymlink(m_driveConfig.getDevFilename());
  
  // Instantiate the drive object
  std::auto_ptr<drive::DriveInterface>
    drive(drive::createDrive(driveInfo, m_sysWrapper));

  if(NULL == drive.get()) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to instantiate drive object";
    throw ex;
  }
  
  return drive;
}

//------------------------------------------------------------------------------
// mountTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::mountTape() {
  const mediachanger::LibrarySlot &librarySlot = m_driveConfig.getLibrarySlot();

  std::list<log::Param> params;
  params.push_back(log::Param("uid", m_request.uid));
  params.push_back(log::Param("gid", m_request.gid));
  params.push_back(log::Param("TPVID", m_request.vid));
  params.push_back(log::Param("unitName", m_request.drive));
  params.push_back(log::Param("dgn", m_request.dgn));
  params.push_back(log::Param("force", boolToStr(m_force)));
  params.push_back(log::Param("librarySlot", librarySlot.str()));

  m_log(LOG_INFO, "Label session mounting tape", params);
  m_mc.mountTapeReadWrite(m_request.vid, librarySlot);
  if(mediachanger::TAPE_LIBRARY_TYPE_MANUAL == librarySlot.getLibraryType()) {
    m_log(LOG_INFO, "Label session did not mounted tape because the media"
      " changer is manual", params);
  } else {
   m_log(LOG_INFO, "Label session mounted tape", params);
  }
}

//------------------------------------------------------------------------------
// waitUntilTapeLoaded
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::waitUntilTapeLoaded(
  drive::DriveInterface &drive, const int timeoutSecond) { 
  std::list<log::Param> params;
  params.push_back(log::Param("uid", m_request.uid));
  params.push_back(log::Param("gid", m_request.gid));
  params.push_back(log::Param("TPVID", m_request.vid));
  params.push_back(log::Param("unitName", m_request.drive));
  params.push_back(log::Param("dgn", m_request.dgn));
  params.push_back(log::Param("force", boolToStr(m_force)));

  try {
    drive.waitUntilReady(timeoutSecond);
    m_log(LOG_INFO, "Label session loaded tape", params);
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
  drive::DriveInterface &drive) {
  std::list<log::Param> params;
  params.push_back(log::Param("uid", m_request.uid));
  params.push_back(log::Param("gid", m_request.gid));
  params.push_back(log::Param("TPVID", m_request.vid));
  params.push_back(log::Param("unitName", m_request.drive));
  params.push_back(log::Param("dgn", m_request.dgn));
  params.push_back(log::Param("force", boolToStr(m_force)));

  if(drive.isWriteProtected()) {
    castor::exception::Exception ex;
    ex.getMessage() << "Tape to be labeled in write protected";
    throw ex;
  }
  m_log(LOG_INFO, "Label session detected tape is writable", params);
}

//------------------------------------------------------------------------------
// rewindDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::rewindDrive(
  drive::DriveInterface &drive) {
  std::list<log::Param> params;
  params.push_back(log::Param("uid", m_request.uid));
  params.push_back(log::Param("gid", m_request.gid));
  params.push_back(log::Param("TPVID", m_request.vid));
  params.push_back(log::Param("unitName", m_request.drive));
  params.push_back(log::Param("dgn", m_request.dgn));
  params.push_back(log::Param("force", boolToStr(m_force)));

  m_log(LOG_INFO, "Label session rewinding tape", params);
  drive.rewind();
  m_log(LOG_INFO, "Label session successfully rewound tape", params);
}

//------------------------------------------------------------------------------
// notifyTapeserverOfUserError
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::
  notifyTapeserverOfUserError(const std::string message) {
  std::list<log::Param> params;
  params.push_back(log::Param("uid", m_request.uid));
  params.push_back(log::Param("gid", m_request.gid));
  params.push_back(log::Param("TPVID", m_request.vid));
  params.push_back(log::Param("unitName", m_request.drive));
  params.push_back(log::Param("dgn", m_request.dgn));
  params.push_back(log::Param("force", boolToStr(m_force)));
  params.push_back(log::Param("message", message));

  m_log(LOG_ERR, "Label session encountered user error", params);
  m_tapeserver.labelError(m_request.drive, message);
}

//------------------------------------------------------------------------------
// writeLabelToTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::writeLabelToTape(
  drive::DriveInterface &drive) {
  std::list<log::Param> params;
  params.push_back(log::Param("uid", m_request.uid));
  params.push_back(log::Param("gid", m_request.gid));
  params.push_back(log::Param("TPVID", m_request.vid));
  params.push_back(log::Param("unitName", m_request.drive));
  params.push_back(log::Param("dgn", m_request.dgn));
  params.push_back(log::Param("force", boolToStr(m_force)));

  m_log(LOG_INFO, "Label session is writing label to tape", params);
  tapeFile::LabelSession ls(drive, m_request.vid);
  m_log(LOG_INFO, "Label session has written label to tape", params);
}

//------------------------------------------------------------------------------
// unloadTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::unloadTape(
  const std::string &vid, drive::DriveInterface &drive) {
  std::list<log::Param> params;
  params.push_back(log::Param("uid", m_request.uid));
  params.push_back(log::Param("gid", m_request.gid));
  params.push_back(log::Param("TPVID", m_request.vid));
  params.push_back(log::Param("unitName", m_request.drive));
  params.push_back(log::Param("dgn", m_request.dgn));
  params.push_back(log::Param("force", boolToStr(m_force)));

  // We implement the same policy as with the tape sessions: 
  // if the librarySlot parameter is "manual", do nothing.
  if(mediachanger::TAPE_LIBRARY_TYPE_MANUAL ==
    m_driveConfig.getLibrarySlot().getLibraryType()) {
    m_log(LOG_INFO, "Label session not unloading tape because media changer is"
      " manual", params);
    return;
  }

  try {
    m_log(LOG_INFO, "Label session unloading tape", params);
    drive.unloadTape();
    m_log(LOG_INFO, "Label session unloaded tape", params);
  } catch (castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Label session failed to unload tape: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::dismountTape(
  const std::string &vid) {
  const mediachanger::LibrarySlot &librarySlot = m_driveConfig.getLibrarySlot();
  std::list<log::Param> params;
  params.push_back(log::Param("uid", m_request.uid));
  params.push_back(log::Param("gid", m_request.gid));
  params.push_back(log::Param("TPVID", m_request.vid));
  params.push_back(log::Param("unitName", m_request.drive));
  params.push_back(log::Param("dgn", m_request.dgn));
  params.push_back(log::Param("force", boolToStr(m_force)));
  params.push_back(log::Param("librarySlot", librarySlot.str()));

  try {
    m_log(LOG_INFO, "Label session dismounting tape", params);
    m_mc.dismountTape(vid, librarySlot);
    const bool dismountWasManual = mediachanger::TAPE_LIBRARY_TYPE_MANUAL ==
      librarySlot.getLibraryType();
    if(dismountWasManual) {
      m_log(LOG_INFO, "Label session did not dismount tape because media"
        " changer is manual", params);
    } else {
      m_log(LOG_INFO, "Label session dismounted tape", params);
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Label session failed to dismount tape: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// boolToStr
//------------------------------------------------------------------------------
const char *castor::tape::tapeserver::daemon::LabelSession::boolToStr(
  const bool value) {
  return value ? "true" : "false";
}
