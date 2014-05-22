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
  m_timeout(1), // 1 second of timeout for the network in the label session. This is not going to be a parameter of the constructor
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
  executeLabel();
}

//------------------------------------------------------------------------------
// checkIfVidStillHasSegments
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::checkIfVidStillHasSegments() {
  
  // check if the volume still contains active or inactive segments. If yes, no labeling allowed, even with the force flag enabled.
  castor::legacymsg::NsProxy::TapeNsStatus rc = m_ns.doesTapeHaveNsFiles(m_request.vid);
  
  if(rc==castor::legacymsg::NsProxy::NSPROXY_TAPE_HAS_AT_LEAST_ONE_DISABLED_SEGMENT) {
    castor::exception::Exception ex;
    ex.getMessage() << "End session with error. Tape has at least one disabled segment. Aborting labeling...";
    throw ex;
  }
  else if (rc==castor::legacymsg::NsProxy::NSPROXY_TAPE_HAS_AT_LEAST_ONE_ACTIVE_SEGMENT) {
    castor::exception::Exception ex;
    ex.getMessage() << "End session with error. Tape has at least one active segment. Aborting labeling...";
    throw ex;
  }
  else {} // rc==castor::legacymsg::NsProxy::NSPROXY_TAPE_EMPTY. Tape is empty, we are good to go..
}

//------------------------------------------------------------------------------
// getDriveObject
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::getDriveObject(utils::TpconfigLines::const_iterator &configLine, std::auto_ptr<castor::tape::drives::DriveInterface> &drive) {
  
  // Get hold of the drive and check it.
  for (configLine = m_tpConfig.begin(); configLine != m_tpConfig.end(); configLine++) {
    if (configLine->unitName == m_request.drive) {
      break;
    }
  }
  
  // If we did not find the drive in the tpConfig, we have a problem
  if (configLine == m_tpConfig.end()) {
    castor::exception::Exception ex;
    ex.getMessage() << "End session with error. Drive unit not found in TPCONFIG. Aborting labeling...";
    throw ex;
  }
  
  // Actually find the drive.
  castor::tape::SCSI::DeviceVector dv(m_sysWrapper);    
  castor::tape::SCSI::DeviceInfo driveInfo = dv.findBySymlink(configLine->devFilename);
  
  // Instantiate the drive object
  drive.reset(castor::tape::drives::DriveFactory(driveInfo, m_sysWrapper));
  
  // check that drive is not write protected
  if(drive->isWriteProtected()) {   
    castor::exception::Exception ex;
    ex.getMessage() << "End session with error. Drive is write protected. Aborting labeling...";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// mountTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::mountTape(utils::TpconfigLines::const_iterator &configLine) {
  
  // Let's mount the tape now
  m_rmc.mountTape(m_request.vid, configLine->librarySlot, castor::legacymsg::RmcProxy::MOUNT_MODE_READWRITE);
}

//------------------------------------------------------------------------------
// waitUntilDriveReady
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::waitUntilDriveReady(castor::tape::drives::DriveInterface *drive) { 
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
}

//------------------------------------------------------------------------------
// labelTheTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::labelTheTape(castor::tape::drives::DriveInterface *drive) {
  
  // We can now start labeling
  std::auto_ptr<castor::tape::tapeFile::LabelSession> ls;
  ls.reset(new castor::tape::tapeFile::LabelSession(*drive, m_request.vid, m_force));
}

//------------------------------------------------------------------------------
// unmountTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::unmountTape(utils::TpconfigLines::const_iterator &configLine) {
  
  // We are done: unmount the tape now
  m_rmc.unmountTape(m_request.vid, configLine->librarySlot);
}

//------------------------------------------------------------------------------
// executeLabel
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::executeLabel() {
  std::ostringstream task;
  task << "label tape " << m_request.vid << " for gid=" << m_request.gid <<
    " uid=" << m_request.uid << " in drive " << m_request.drive;
  
  utils::TpconfigLines::const_iterator configLine;
  std::auto_ptr<castor::tape::drives::DriveInterface> drive;
  
  log::Param params[] = {
      log::Param("uid", m_request.uid),
      log::Param("gid", m_request.gid),
      log::Param("vid", m_request.vid),
      log::Param("drive", m_request.drive),
      log::Param("dgn", m_request.dgn)};
  
  bool tapeMounted=false;
  bool configlineSet=false;
  
  try {
    checkIfVidStillHasSegments();
    m_log(LOG_INFO, "The tape to be labeled has no files registered in the nameserver", params);
    getDriveObject(configLine, drive);
    m_log(LOG_INFO, "The tape drive object has been instantiated", params);
    configlineSet=true;
    mountTape(configLine);
    m_log(LOG_INFO, "The tape has been successfully mounted for labeling", params);
    tapeMounted=true;
    waitUntilDriveReady(drive.get());
    m_log(LOG_INFO, "The drive is ready for labeling", params);
    labelTheTape(drive.get());
    m_log(LOG_INFO, "The tape has been successfully labeled", params);
    unmountTape(configLine);
    m_log(LOG_INFO, "The tape has been successfully unmounted after labeling", params);
    tapeMounted=false;
    legacymsg::writeTapeReplyMsg(m_timeout, m_labelCmdConnection, 0, "");
  } catch(castor::exception::Exception &ex) {
    if(tapeMounted && configlineSet) unmountTape(configLine); // trying to clean up here
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "Label session caught an exception", params);
    legacymsg::writeTapeReplyMsg(m_timeout, m_labelCmdConnection, 1, ex.getMessage().str());
    exit(1);
  }
  
}
