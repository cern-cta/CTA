/******************************************************************************
 *                      LabelSession.cpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#include "castor/io/io.hpp"
#include "castor/legacymsg/legacymsg.hpp"
#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/tape/tapeserver/daemon/LabelSession.hpp"
#include "castor/tape/tapeserver/exception/Exception.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "stager_client_commandline.h"
#include "castor/tape/utils/utils.hpp"
#include "castor/System.hpp"
#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/drive/Drive.hpp"
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
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
  const int labelCmdConnection,
  legacymsg::RmcProxy &rmc, 
  legacymsg::NsProxy &ns,
  const legacymsg::TapeLabelRqstMsgBody &clientRequest,
  castor::log::Logger &log,
  System::virtualWrapper &sysWrapper,
  const utils::TpconfigLines &tpConfig,
  const bool force):     
  m_labelCmdConnection(labelCmdConnection),
  m_rmc(rmc),
  m_ns(ns),
  m_request(clientRequest),
  m_log(log),
  m_sysWrapper(sysWrapper),
  m_tpConfig(tpConfig),
  m_force(force) {
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::execute()  {
  try {
    executeLabel();
    legacymsg::writeTapeRcReplyMsg(m_labelCmdConnection, 0);
  } catch(castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "Label session caught an exception", params);
    legacymsg::writeTapeRcReplyMsg(m_labelCmdConnection, 1);
    exit(1);
  }
}

//------------------------------------------------------------------------------
// checkIfVidStillHasSegments
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::LabelSession::checkIfVidStillHasSegments(utils::TpconfigLines::const_iterator &configLine) {
  // check if the volume still contains files (in that case no labeling allowed, even with the force flag enabled)
  castor::legacymsg::NsProxy::TapeNsStatus rc;
  try {
    rc = m_ns.doesTapeHaveNsFiles(m_request.vid);
  } catch (castor::exception::Exception & e) { // ns request failed
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn),
      log::Param("m_request.vid", m_request.vid),
      log::Param("errorMessage", e.getMessageValue())};
    m_log(LOG_ERR, "End session with error. Error while querying the name server", params);
    return LABEL_SESSION_STEP_FAILED;
  }
  
  if(rc==castor::legacymsg::NsProxy::NSPROXY_TAPE_EMPTY) {
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn),
      log::Param("m_request.vid", m_request.vid)};
    m_log(LOG_INFO, "Tape has no NS files. We are allowed to proceed with the labeling", params);
  }
  else if(rc==castor::legacymsg::NsProxy::NSPROXY_TAPE_HAS_AT_LEAST_ONE_DISABLED_SEGMENT) {
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn),
      log::Param("m_request.vid", m_request.vid)};
    m_log(LOG_ERR, "End session with error. Tape has at least one disabled segment. Aborting labeling...", params);
    return LABEL_SESSION_STEP_FAILED;
  }
  else { // rc==castor::legacymsg::NsProxy::NSPROXY_TAPE_HAS_AT_LEAST_ONE_ACTIVE_SEGMENT
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn),
      log::Param("m_request.vid", m_request.vid)};
    m_log(LOG_ERR, "End session with error. Tape has at least one active segment. Aborting labeling...", params);
    return LABEL_SESSION_STEP_FAILED;
  }
  return LABEL_SESSION_STEP_SUCCEEDED;
}

//------------------------------------------------------------------------------
// identifyDrive
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::LabelSession::identifyDrive(utils::TpconfigLines::const_iterator &configLine) {
  // Get hold of the drive and check it.
  for (configLine = m_tpConfig.begin(); configLine != m_tpConfig.end(); configLine++) {
    if (configLine->unitName == m_request.drive) {
      break;
    }
  }
  
  // If we did not find the drive in the tpConfig, we have a problem
  if (configLine == m_tpConfig.end()) {
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn)};
    m_log(LOG_ERR, "End session with error. Drive unit not found in TPCONFIG", params);
    return LABEL_SESSION_STEP_FAILED;
  }
  return LABEL_SESSION_STEP_SUCCEEDED;
}

//------------------------------------------------------------------------------
// mountTape
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::LabelSession::mountTape(utils::TpconfigLines::const_iterator &configLine) {
  // Let's mount the tape now
  try {
    m_rmc.mountTape(m_request.vid, configLine->librarySlot,
      legacymsg::RmcProxy::MOUNT_MODE_READWRITE);
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn),
      log::Param("m_request.vid", m_request.vid),
      log::Param("configLine->librarySlot", configLine->librarySlot)};
    m_log(LOG_INFO, "Tape mounted for labeling", params);
  } catch (castor::exception::Exception & e) { // mount failed
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn),
      log::Param("m_request.vid", m_request.vid),
      log::Param("configLine->librarySlot", configLine->librarySlot),
      log::Param("errorMessage", e.getMessageValue())};
    m_log(LOG_ERR, "End session with error. Error mounting tape", params);
    return LABEL_SESSION_STEP_FAILED;
  }
  return LABEL_SESSION_STEP_SUCCEEDED;
}

//------------------------------------------------------------------------------
// getDeviceInfo
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::LabelSession::getDeviceInfo(utils::TpconfigLines::const_iterator &configLine, castor::tape::SCSI::DeviceInfo &driveInfo) {
  // Actually find the drive.
  castor::tape::SCSI::DeviceVector dv(m_sysWrapper);
  try {
    driveInfo = dv.findBySymlink(configLine->devFilename);
  } catch (castor::tape::SCSI::DeviceVector::NotFound & e) {
    // We could not find this drive in the system's SCSI devices
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn),
      log::Param("devFilename", configLine->devFilename)};
    m_log(LOG_ERR, "End session with error. Drive not found on this path", params);
    return LABEL_SESSION_STEP_FAILED;
  } catch (castor::exception::Exception & e) {
    // We could not find this drive in the system's SCSI devices
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn),
      log::Param("devFilename", configLine->devFilename),
      log::Param("errorMessage", e.getMessageValue())};
    m_log(LOG_ERR, "End session with error. Error looking to path to tape drive", params);
    return LABEL_SESSION_STEP_FAILED;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn),
      log::Param("devFilename", configLine->devFilename)};
    m_log(LOG_ERR, "End session with error. Unexpected exception while looking for drive", params);
    return LABEL_SESSION_STEP_FAILED;
  }
  return LABEL_SESSION_STEP_SUCCEEDED;
}

//------------------------------------------------------------------------------
// getDriveObject
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::LabelSession::getDriveObject(utils::TpconfigLines::const_iterator &configLine, castor::tape::SCSI::DeviceInfo &driveInfo, std::auto_ptr<castor::tape::drives::DriveInterface> &drive) {
  // Instantiate the drive object
  
  try {
    drive.reset(castor::tape::drives::DriveFactory(driveInfo, m_sysWrapper));
  } catch (castor::exception::Exception & e) {
    // We could not find this drive in the system's SCSI devices
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn),
      log::Param("devFilename", configLine->devFilename),
      log::Param("errorMessage", e.getMessageValue())};
    m_log(LOG_ERR, "End session with error. Error opening tape drive", params);
    return LABEL_SESSION_STEP_FAILED;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn),
      log::Param("devFilename", configLine->devFilename)};
    m_log(LOG_ERR, "End session with error. Unexpected exception while opening drive", params);
    return LABEL_SESSION_STEP_FAILED;
  }
  return LABEL_SESSION_STEP_SUCCEEDED;
}

//------------------------------------------------------------------------------
// checkDriveObject
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::LabelSession::checkDriveObject(castor::tape::drives::DriveInterface *drive) {
  
    // check that drive is not write protected
  if(drive->isWriteProtected()) {
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn)};
    m_log(LOG_ERR, "End session with error. Failed to label the tape: drive is write protected", params);
    return LABEL_SESSION_STEP_FAILED;
  }
  
      log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn)};
      
    try{
      drive->waitUntilReady(600);
    }catch(const castor::tape::Exception& e){
      m_log(LOG_INFO, "Drive not ready after 600 seconds waiting", params);
    }
      m_log(LOG_INFO, "Drive  ready for labeling", params);
  return LABEL_SESSION_STEP_SUCCEEDED;
}

//------------------------------------------------------------------------------
// labelTheTape
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::LabelSession::labelTheTape(castor::tape::drives::DriveInterface *drive) {
  // We can now start labeling
  std::auto_ptr<castor::tape::tapeFile::LabelSession> ls;
  try {
    ls.reset(new castor::tape::tapeFile::LabelSession(*drive, m_request.vid, m_force));
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn),
      log::Param("m_force", m_force)};
    m_log(LOG_INFO, "Tape label session session successfully completed", params);
  } catch (castor::exception::Exception & ex) {// labeling failed
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn),
      log::Param("m_force", m_force),
      log::Param("errorMessage", ex.getMessageValue())};
    m_log(LOG_ERR, "End session with error. Failed tape label session", params);
    return LABEL_SESSION_STEP_FAILED;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn)};
    m_log(LOG_ERR, "End session with error. Unexpected exception while labeling tape", params);
    return LABEL_SESSION_STEP_FAILED;
  }
  return LABEL_SESSION_STEP_SUCCEEDED;
}

//------------------------------------------------------------------------------
// unmountTape
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::LabelSession::unmountTape(utils::TpconfigLines::const_iterator &configLine) {
  // We are done: unmount the tape now
  try {
    m_rmc.unmountTape(m_request.vid, configLine->librarySlot);
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn),
      log::Param("configLine->librarySlot", configLine->librarySlot)};
    m_log(LOG_INFO, "Tape unmounted after labeling", params);
  } catch (castor::exception::Exception & e) { // unmount failed
    log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn),
      log::Param("configLine->librarySlot", configLine->librarySlot),
      log::Param("errorMessage", e.getMessageValue())};
    m_log(LOG_ERR, "End session with error. Error unmounting tape", params);
    return LABEL_SESSION_STEP_FAILED;
  }

  return LABEL_SESSION_STEP_SUCCEEDED;
}

//------------------------------------------------------------------------------
// executeLabel
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::executeLabel() {
  std::ostringstream task;
  task << "label tape " << m_request.vid << " for gid=" << m_request.gid <<
    " uid=" << m_request.uid << " in drive " << m_request.drive;
  
  utils::TpconfigLines::const_iterator configLine;  
  castor::tape::SCSI::DeviceInfo driveInfo;
  std::auto_ptr<castor::tape::drives::DriveInterface> drive;
  
  if(checkIfVidStillHasSegments(configLine)             == LABEL_SESSION_STEP_FAILED) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Tape still contains CASTOR files";
    throw ex;
  }
  if(identifyDrive(configLine)                          == LABEL_SESSION_STEP_FAILED) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Unidentified drive";
    throw ex;
  }
  if(mountTape(configLine)                              == LABEL_SESSION_STEP_FAILED) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Failed to mount tape";
    throw ex;
  }
  if(getDeviceInfo(configLine, driveInfo)               == LABEL_SESSION_STEP_FAILED) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Failed to get device information";
    throw ex;
  }
  if(getDriveObject(configLine, driveInfo, drive)       == LABEL_SESSION_STEP_FAILED) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Failed to get drive object";
    throw ex;
  }
  if(checkDriveObject(drive.get())                      == LABEL_SESSION_STEP_FAILED) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Invalid drive object";
    throw ex;
  }
  if(labelTheTape(drive.get())                          == LABEL_SESSION_STEP_FAILED) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Failed to write pre-label to tape";
    throw ex;
  }
  if(unmountTape(configLine)                            == LABEL_SESSION_STEP_FAILED) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Failed to unmount tape";
    throw ex;
  }
}
