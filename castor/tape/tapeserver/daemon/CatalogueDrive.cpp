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
#include "castor/tape/tapeserver/daemon/CatalogueDrive.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/utils/utils.hpp"
#include "h/Ctape_constants.h"
#include "h/rmc_constants.h"

#include <errno.h>
#include <signal.h>
#include <string.h>
#include <sys/types.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueDrive::CatalogueDrive(
  const int netTimeout,
  log::Logger &log,
  ProcessForkerProxy &processForker,
  legacymsg::CupvProxy &cupv,
  legacymsg::VdqmProxy &vdqm,
  legacymsg::VmgrProxy &vmgr,
  const std::string &hostName,
  const DriveConfig &config,
  const DriveState state,
  const time_t waitJobTimeoutInSecs,
  const time_t mountTimeoutInSecs,
  const time_t blockMoveTimeoutInSecs)
  throw():
  m_netTimeout(netTimeout),
  m_log(log),
  m_processForker(processForker),
  m_cupv(cupv),
  m_vdqm(vdqm),
  m_vmgr(vmgr),
  m_hostName(hostName),
  m_config(config),
  m_state(state),
  m_waitJobTimeoutInSecs(waitJobTimeoutInSecs),
  m_mountTimeoutInSecs(mountTimeoutInSecs),
  m_blockMoveTimeoutInSecs(blockMoveTimeoutInSecs),
  m_session(NULL) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueDrive::~CatalogueDrive()
  throw() {
  deleteSession();
}

//------------------------------------------------------------------------------
// handleTick
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::CatalogueDrive::handleTick() {
  try {
    checkForSession();
  } catch(...) {
    return true; // Continue the main event loop
  }

  return m_session->handleTick();
}

//------------------------------------------------------------------------------
// deleteSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::deleteSession() {
  delete m_session;
  m_session = NULL;
}

//-----------------------------------------------------------------------------
// driveStateToStr
//-----------------------------------------------------------------------------
const char
  *castor::tape::tapeserver::daemon::CatalogueDrive::driveStateToStr(
  const DriveState state) throw() {
  switch(state) {
  case DRIVE_STATE_INIT                : return "INIT";
  case DRIVE_STATE_DOWN                : return "DOWN";
  case DRIVE_STATE_UP                  : return "UP";
  case DRIVE_STATE_RUNNING             : return "RUNNING";
  case DRIVE_STATE_WAITDOWN            : return "WAITDOWN";
  case DRIVE_STATE_WAITSHUTDOWNKILL    : return "WAITSHUTDOWNKILL";
  case DRIVE_STATE_WAITSHUTDOWNCLEANER : return "WAITSHUTDOWNCLEANER";
  case DRIVE_STATE_SHUTDOWN            : return "SHUTDOWN";
  default                              : return "UNKNOWN";
  }
}

//------------------------------------------------------------------------------
// getConfig
//------------------------------------------------------------------------------
const castor::tape::tapeserver::daemon::DriveConfig
  &castor::tape::tapeserver::daemon::CatalogueDrive::getConfig() const {
  return m_config;
}

//------------------------------------------------------------------------------
// getState
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueDrive::DriveState
  castor::tape::tapeserver::daemon::CatalogueDrive::getState()
  const throw() {
  return m_state;
} 

//------------------------------------------------------------------------------
// getSession
//------------------------------------------------------------------------------
const castor::tape::tapeserver::daemon::CatalogueSession &
  castor::tape::tapeserver::daemon::CatalogueDrive::getSession() const {
  try {
    checkForSession();
    return *m_session;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get tape session for drive " <<
      m_config.unitName << " from drive catalogue: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// checkForSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::checkForSession()
  const {
  if(NULL == m_session) {
    castor::exception::Exception ex;
    ex.getMessage() << "Drive is currently not running a session: state=" <<
      driveStateToStr(m_state);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getCleanerSession
//------------------------------------------------------------------------------
const castor::tape::tapeserver::daemon::CatalogueCleanerSession &
  castor::tape::tapeserver::daemon::CatalogueDrive::getCleanerSession()
  const {
  try {
    checkForCleanerSession();
    const CatalogueCleanerSession *const cleanerSession =
      dynamic_cast<const CatalogueCleanerSession*>(m_session);
    if(NULL == cleanerSession) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to cast session to CatalogueCleanerSession";
      throw ex;
    }

    return *cleanerSession;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get cleaner session for drive " <<
      m_config.unitName << " from drive catalogue: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getCleanerSession
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueCleanerSession &
  castor::tape::tapeserver::daemon::CatalogueDrive::getCleanerSession() {
  try {
    checkForCleanerSession();
    CatalogueCleanerSession *const cleanerSession =
      dynamic_cast<CatalogueCleanerSession*>(m_session);
    if(NULL == cleanerSession) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << 
        "Failed to cast session to CatalogueCleanerSession";
      throw ex;
    }

    return *cleanerSession;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get cleaner session for drive " <<
      m_config.unitName << " from drive catalogue: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// checkForCleanerSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
  checkForCleanerSession() const {
  checkForSession();
  const CatalogueSession::Type sessionType = m_session->getType();
  if(CatalogueSession::SESSION_TYPE_CLEANER != sessionType) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Session associated with drive is not a cleaner session"
      ": actual=" << CatalogueSession::sessionTypeToStr(sessionType);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getLabelSession
//------------------------------------------------------------------------------
const castor::tape::tapeserver::daemon::CatalogueLabelSession &
  castor::tape::tapeserver::daemon::CatalogueDrive::getLabelSession()
  const {
  try {
    checkForLabelSession();
    const CatalogueLabelSession *const labelSession =
      dynamic_cast<const CatalogueLabelSession*>(m_session);
    if(NULL == labelSession) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to cast session to CatalogueLabelSession";
      throw ex;
    }

    return *labelSession;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get label session for drive " <<
      m_config.unitName << " from drive catalogue: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getLabelSession
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueLabelSession &
  castor::tape::tapeserver::daemon::CatalogueDrive::getLabelSession() {
  try {
    checkForLabelSession();
    CatalogueLabelSession *const labelSession =
      dynamic_cast<CatalogueLabelSession*>(m_session);
    if(NULL == labelSession) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to cast session to CatalogueLabelSession";
      throw ex;
    }

    return *labelSession;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get label session for drive " <<
      m_config.unitName << " from drive catalogue: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// checkForLabelSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
  checkForLabelSession() const {
  checkForSession();
  const CatalogueSession::Type sessionType = m_session->getType();
  if(CatalogueSession::SESSION_TYPE_LABEL != sessionType) {
    castor::exception::Exception ex;
    ex.getMessage() << "Session associated with drive is not a label session"
      ": actual=" << CatalogueSession::sessionTypeToStr(sessionType);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getTransferSession
//------------------------------------------------------------------------------
const castor::tape::tapeserver::daemon::CatalogueTransferSession &
  castor::tape::tapeserver::daemon::CatalogueDrive::getTransferSession()
  const {
  try {
    checkForTransferSession();
    CatalogueTransferSession *const transferSession =
      dynamic_cast<CatalogueTransferSession*>(m_session);
    if(NULL == transferSession) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to cast session to CatalogueTransferSession";
      throw ex;
    }

    return *transferSession;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get transfer session for drive " <<
      m_config.unitName << " from drive catalogue: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getTransferSession
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueTransferSession &
  castor::tape::tapeserver::daemon::CatalogueDrive::getTransferSession() {
  try {
    checkForTransferSession();
    CatalogueTransferSession *const transferSession =
      dynamic_cast<CatalogueTransferSession*>(m_session);
    if(NULL == transferSession) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to cast session to CatalogueTransferSession";
      throw ex;
    }

    return *transferSession;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get transfer session for drive " <<
      m_config.unitName << " from drive catalogue: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// checkForTransferSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
  checkForTransferSession() const {
  checkForSession();
  const CatalogueSession::Type sessionType = m_session->getType();
  if(CatalogueSession::SESSION_TYPE_TRANSFER != sessionType) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Session associated with drive is not a transfer session"
      ": actual=" << CatalogueSession::sessionTypeToStr(sessionType);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// configureUp
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::configureUp() {
  switch(m_state) {
  case DRIVE_STATE_UP:
  case DRIVE_STATE_RUNNING:
    break;
  case DRIVE_STATE_DOWN:
    changeState(DRIVE_STATE_UP);
    break;
  case DRIVE_STATE_WAITDOWN:
    changeState(DRIVE_STATE_RUNNING);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to configure tape-drive " << m_config.unitName
        << " up : Incompatible drive state: state=" << driveStateToStr(m_state);
      throw ex;
    }
  }
  m_vdqm.setDriveUp(m_hostName, m_config.unitName, m_config.dgn);
}

//-----------------------------------------------------------------------------
// configureDown
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::configureDown() {
  switch(m_state) {
  case DRIVE_STATE_DOWN:
  case DRIVE_STATE_WAITDOWN:
    break;
  case DRIVE_STATE_UP:
    changeState(DRIVE_STATE_DOWN);
    m_vdqm.setDriveDown(m_hostName, m_config.unitName, m_config.dgn);
    break;
  case CatalogueDrive::DRIVE_STATE_RUNNING:
    changeState(DRIVE_STATE_WAITDOWN);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to configure tape drive " << m_config.unitName
        << " down: Incompatible drive state: state=" <<
        driveStateToStr(m_state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// receivedVdqmJob
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::receivedVdqmJob(
  const legacymsg::RtcpJobRqstMsgBody &job)  {

  std::ostringstream task;
  task << "handle vdqm job for tape drive " << m_config.unitName;

  // Sanity check
  if(job.driveUnit != m_config.unitName) {
    // Should never happen
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": unit name mismatch: job.driveUnit=" << job.driveUnit <<
      " m_config.unitName=" << m_config.unitName;
    throw ex;
  }
  
  switch(m_state) {
  case DRIVE_STATE_UP:
    if(std::string(job.dgn) != m_config.dgn) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": DGN mismatch: catalogueDgn=" << m_config.dgn << " vdqmJobDgn="
          << job.dgn;
      throw ex;
    }
    changeState(DRIVE_STATE_RUNNING);
    {
      CatalogueTransferSession *const transferSession =
        CatalogueTransferSession::create(
          m_log,
          m_netTimeout,
          m_config,
          job,
          m_vmgr,
          m_cupv,
          m_hostName,
          m_waitJobTimeoutInSecs,
          m_mountTimeoutInSecs,
          m_blockMoveTimeoutInSecs,
          m_processForker);
      m_session = dynamic_cast<CatalogueSession *>(transferSession);
      m_vdqm.assignDrive(m_hostName, m_config.unitName, job.dgn,
        job.volReqId, m_session->getPid());
    }
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << driveStateToStr(m_state);
      throw ex;
    }
  } 
} 

//-----------------------------------------------------------------------------
// receivedLabelJob
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::receivedLabelJob(
  const legacymsg::TapeLabelRqstMsgBody &job, const int labelCmdConnection) {
  const std::string unitName(job.drive);

  std::ostringstream task;
  task << "handle label job for tape drive " << m_config.unitName;

  // Sanity check
  if(job.drive != m_config.unitName) {
    // Should never happen
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": unit name mismatch: job.drive=" << job.drive << 
      " m_config.unitName=" << m_config.unitName;
    throw ex;
  }

  switch(m_state) {
  case DRIVE_STATE_UP:
    if(std::string(job.dgn) != m_config.dgn) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": DGN mismatch: catalogueDgn=" << m_config.dgn << " labelJobDgn="
        << job.dgn;
      throw ex;
    }
    m_session = CatalogueLabelSession::create(
      m_log,
      m_netTimeout,
      m_config,
      job,
      labelCmdConnection,
      m_cupv,
      m_processForker);
    changeState(DRIVE_STATE_RUNNING);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << driveStateToStr(m_state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// createCleaner
//-----------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueCleanerSession
  *castor::tape::tapeserver::daemon::CatalogueDrive::createCleaner(
  const std::string &vid, const time_t assignmentTime,
  const uint32_t driveReadyDelayInSeconds) const {
  try {
    return CatalogueCleanerSession::create(
      m_log,
      m_netTimeout,
      m_config,
      m_processForker,
      vid,
      assignmentTime,
      driveReadyDelayInSeconds);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create cleaner session: " <<
      ne.getMessage().str();
    throw ex;
  } 
}

//-----------------------------------------------------------------------------
// sessionSucceeded
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
  sessionSucceeded() {
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
    changeState(DRIVE_STATE_UP);
    m_vdqm.setDriveUp(m_hostName, m_config.unitName, m_config.dgn);
    break;
  case DRIVE_STATE_WAITDOWN:
    changeState(CatalogueDrive::DRIVE_STATE_DOWN);
    m_vdqm.setDriveDown(m_hostName, m_config.unitName, m_config.dgn);
    break;
  case DRIVE_STATE_WAITSHUTDOWNCLEANER:
    changeState(DRIVE_STATE_SHUTDOWN);
    m_vdqm.setDriveDown(m_hostName, m_config.unitName, m_config.dgn);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to record tape session succeeded for session with pid " <<
        getSession().getPid() << ": Incompatible drive state: state=" <<
        driveStateToStr(m_state);
      delete m_session;
      m_session = NULL;
      throw ex;
    }
  }

  std::auto_ptr<CatalogueSession> session(m_session);
  m_session = NULL;
  session->sessionSucceeded();
}

//-----------------------------------------------------------------------------
// sessionFailed
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::sessionFailed() {
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return runningSessionFailed();
  case DRIVE_STATE_WAITSHUTDOWNKILL:
    return sessionKilledByShutdown();
  case DRIVE_STATE_WAITSHUTDOWNCLEANER:
    return cleanerOfShutdownFailed();
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to record tape session failed for session with pid " <<
        getSession().getPid() << ": Incompatible drive state: state=" <<
        driveStateToStr(m_state);
      delete m_session;
      m_session = NULL;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// runningSessionFailed
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::runningSessionFailed() {
  std::auto_ptr<CatalogueSession> session(m_session);
  m_session = NULL;
  session->sessionFailed();

  if(CatalogueSession::SESSION_TYPE_CLEANER != session->getType()) {
    const uint32_t driveReadyDelayInSeconds = 60;
    m_session = createCleaner(session->getVid(), session->getAssignmentTime(),
      driveReadyDelayInSeconds);
  } else {
    changeState(DRIVE_STATE_DOWN);
    m_vdqm.setDriveDown(m_hostName, m_config.unitName, m_config.dgn);
  }
}

//------------------------------------------------------------------------------
// sessionKilledByShutdown
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
  sessionKilledByShutdown() {
  std::auto_ptr<CatalogueSession> session(m_session);
  m_session = NULL;
  session->sessionFailed();

  changeState(DRIVE_STATE_WAITSHUTDOWNCLEANER);
  const uint32_t driveReadyDelayInSeconds = 60;
  m_session = createCleaner(session->getVid(), session->getAssignmentTime(),
    driveReadyDelayInSeconds);
}

//------------------------------------------------------------------------------
// cleanerOfShutdownFailed
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
  cleanerOfShutdownFailed() {
  std::auto_ptr<CatalogueSession> session(m_session);
  m_session = NULL;
  session->sessionFailed();

  // Cleaner failed, no more can be done, mark drive as shutdown
  changeState(DRIVE_STATE_SHUTDOWN);
}

//------------------------------------------------------------------------------
// sessionFailedAndRequestsCleaner
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
  sessionFailedAndRequestsCleaner() {
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return runningSessionFailedAndRequestedCleaner();
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to record tape session failed for session with pid " <<
        getSession().getPid() << ": Incompatible drive state: state=" <<
        driveStateToStr(m_state);
      delete m_session;
      m_session = NULL;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// runningSessionFailedAndRequestedCleaner
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
  runningSessionFailedAndRequestedCleaner() {
  {
    const pid_t sessionPid = m_session->getPid();
    const CatalogueSession::Type sessionType = m_session->getType();
    const char *sessionTypeStr =
      CatalogueSession::sessionTypeToStr(sessionType);

    std::list<log::Param> params;
    params.push_back(log::Param("unitName", m_config.unitName));
    params.push_back(log::Param("sessionType", sessionTypeStr));
    params.push_back(log::Param("sessionPid", sessionPid));

    m_log(LOG_INFO, "Failed session has requested cleaner", params);
  }

  std::auto_ptr<CatalogueSession> session(m_session);
  m_session = NULL;
  session->sessionFailed();

  if(CatalogueSession::SESSION_TYPE_CLEANER != session->getType()) {
    const uint32_t driveReadyDelayInSeconds = 60;
    m_session = createCleaner(session->getVid(), session->getAssignmentTime(),
      driveReadyDelayInSeconds);
  } else {
    changeState(DRIVE_STATE_DOWN);
    m_vdqm.setDriveDown(m_hostName, m_config.unitName, m_config.dgn);
  }
}

//------------------------------------------------------------------------------
// getTapeStatDriveEntry
//------------------------------------------------------------------------------
castor::legacymsg::TapeStatDriveEntry
  castor::tape::tapeserver::daemon::CatalogueDrive::getTapeStatDriveEntry()
  const {
  legacymsg::TapeStatDriveEntry entry;

  try {
    entry.uid = getUidForTapeStatDriveEntry();
    entry.jid = getJidForTapeStatDriveEntry();
    castor::utils::copyString(entry.dgn, m_config.dgn);
    entry.up = getUpForTapeStatDriveEntry();
    entry.asn = getAsnForTapeStatDriveEntry();
    entry.asn_time = getAsnTimeForTapeStatDriveEntry();
    castor::utils::copyString(entry.drive, m_config.unitName);
    entry.mode = getModeForTapeStatDriveEntry();
    castor::utils::copyString(entry.lblcode,
      getLblCodeForTapeStatDriveEntry().c_str());
    entry.tobemounted = getToBeMountedForTapeStatDriveEntry();
    castor::utils::copyString(entry.vid, getVidForTapeStatDriveEntry());
    castor::utils::copyString(entry.vsn, getVsnForTapeStatDriveEntry());
    entry.cfseq = 0; // the fseq is ignored by tpstat, so we leave it set to 0
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get TapeStatDriveEntry: " <<
      ne.getMessage().str();
    throw ex;
  }

  return entry;
}

//------------------------------------------------------------------------------
// getUidForTapeStatDriveEntry
//------------------------------------------------------------------------------
uint32_t castor::tape::tapeserver::daemon::CatalogueDrive::
  getUidForTapeStatDriveEntry() const throw() {
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return geteuid();
  default:
    return 0;
  }
}

//------------------------------------------------------------------------------
// getJidForTapeStatDriveEntry
//------------------------------------------------------------------------------
uint32_t castor::tape::tapeserver::daemon::CatalogueDrive::
  getJidForTapeStatDriveEntry() const throw() {
  try {
    return getSession().getPid();
  } catch(...) {
    return 0;
  }
}

//------------------------------------------------------------------------------
// getUpForTapeStatDriveEntry
//------------------------------------------------------------------------------
uint16_t castor::tape::tapeserver::daemon::CatalogueDrive::
  getUpForTapeStatDriveEntry() const throw() {
  switch(m_state) {
  case DRIVE_STATE_UP:
  case DRIVE_STATE_RUNNING:
    return 1;
  default:
    return 0;
  }
}

//------------------------------------------------------------------------------
// getAsnForTapeStatDriveEntry
//------------------------------------------------------------------------------
uint16_t castor::tape::tapeserver::daemon::CatalogueDrive::
  getAsnForTapeStatDriveEntry() const throw() {
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return 1;
  default:
    return 0;
  }
}

//------------------------------------------------------------------------------
// getAsnTimeForTapeStatDriveEntry
//------------------------------------------------------------------------------
uint32_t castor::tape::tapeserver::daemon::CatalogueDrive::
  getAsnTimeForTapeStatDriveEntry() const throw() {
  try {
    return getSession().getAssignmentTime();
  } catch(...) {
    return 0;
  }
}

//------------------------------------------------------------------------------
// getModeForTapeStatDriveEntry
//------------------------------------------------------------------------------
uint16_t castor::tape::tapeserver::daemon::CatalogueDrive::
  getModeForTapeStatDriveEntry() const throw() {
  try {
    return getSession().getMode();
  } catch(...) {
    return WRITE_DISABLE;
  }
}

//------------------------------------------------------------------------------
// getLblCodeForTapeStatDriveEntry
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::CatalogueDrive::
  getLblCodeForTapeStatDriveEntry() const throw() {
  return "aul";
}

//------------------------------------------------------------------------------
// getToBeMountedForTapeStatDriveEntry
//------------------------------------------------------------------------------
uint16_t castor::tape::tapeserver::daemon::CatalogueDrive::
  getToBeMountedForTapeStatDriveEntry() const throw() {
  try {
    return getSession().tapeIsBeingMounted();
  } catch(...) {
    return 0;
  }
}

//------------------------------------------------------------------------------
// getVidForTapeStatDriveEntry
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::CatalogueDrive::
  getVidForTapeStatDriveEntry() const throw() {
  try {
    return getSession().getVid();
  } catch(...) {
    return "";
  }
}

//------------------------------------------------------------------------------
// getVidForCleaner
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::CatalogueDrive::
  getVidForCleaner() const throw() {
  try {
    return getSession().getVid();
  } catch(...) {
    return "";
  }
}

//------------------------------------------------------------------------------
// getAssignmentTimeForCleaner
//------------------------------------------------------------------------------
time_t castor::tape::tapeserver::daemon::CatalogueDrive::
  getAssignmentTimeForCleaner() const throw() {
  try {
    return getSession().getAssignmentTime();
  } catch(...) {
    return 0;
  }
}

//------------------------------------------------------------------------------
// shutdown
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::shutdown() {
  // If there is no running session
  if(NULL == m_session) {

    changeState(DRIVE_STATE_WAITSHUTDOWNCLEANER);

    // Create a cleaner process to make 100% sure the tape drive is empty
    const std::string vid = ""; // Empty string means VID is not known
    const time_t assignmentTime = time(NULL);
    const uint32_t driveReadyDelayInSeconds = 0;
    m_session = createCleaner(vid, assignmentTime, driveReadyDelayInSeconds);

  // Else there is a running session
  } else {

    // If the running session is a cleaner
    if(CatalogueSession::SESSION_TYPE_CLEANER == m_session->getType()) {

      changeState(DRIVE_STATE_WAITSHUTDOWNCLEANER);

    // Else the running session is not a cleaner
    } else {

      changeState(DRIVE_STATE_WAITSHUTDOWNKILL);

      const pid_t sessionPid = m_session->getPid();
      const CatalogueSession::Type sessionType = m_session->getType();
      const char *sessionTypeStr =
        CatalogueSession::sessionTypeToStr(sessionType);

      std::list<log::Param> params;
      params.push_back(log::Param("unitName", m_config.unitName));
      params.push_back(log::Param("sessionType", sessionTypeStr));
      params.push_back(log::Param("sessionPid", sessionPid));

      // Kill the non-cleaner session
      if(kill(sessionPid, SIGKILL)) {
        const std::string message = castor::utils::errnoToString(errno);
        params.push_back(log::Param("message", message));
        m_log(LOG_ERR, "Failed to kill non-cleaner session whilst shutting"
          " down", params);

        // Nothing else can be done, mark drive as shutdown
        changeState(DRIVE_STATE_SHUTDOWN);

      } else {
        m_log(LOG_WARNING, "Sent SIGKILL to tape-session child-process",
          params);
      }

    } // Else the running session is not a cleaner

  } // Else there is a running session
}

//------------------------------------------------------------------------------
// killSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::killSession() {
  // Do nothing if there is no running session
  if(NULL == m_session) {
    return;
  }

  const pid_t sessionPid = m_session->getPid();
  const CatalogueSession::Type sessionType = m_session->getType();
  const char *sessionTypeStr =
    CatalogueSession::sessionTypeToStr(sessionType);

  std::list<log::Param> params;
  params.push_back(log::Param("unitName", m_config.unitName));
  params.push_back(log::Param("sessionType", sessionTypeStr));
  params.push_back(log::Param("sessionPid", sessionPid));

  if(kill(sessionPid, SIGKILL)) {
    const std::string errorStr = castor::utils::errnoToString(errno);
    castor::exception::Exception ex;
    ex.getMessage() << "CatalogueDrive failed to kill session: sessionPid=" <<
     sessionPid << " sessionType=" << sessionTypeStr << ": " << errorStr;
    throw ex;
  }
  m_log(LOG_WARNING, "Killed tape-session child-process", params);
  delete m_session;
  m_session = NULL;
  changeState(DRIVE_STATE_DOWN);
  m_vdqm.setDriveDown(m_hostName, m_config.unitName, m_config.dgn);
}

//------------------------------------------------------------------------------
// getVsnForTapeStatDriveEntry
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::CatalogueDrive::
  getVsnForTapeStatDriveEntry() const throw() {
  return getVidForTapeStatDriveEntry();
}

//------------------------------------------------------------------------------
// changeState
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::changeState(
  const DriveState newState) throw() {
  std::list<log::Param> params;
  params.push_back(log::Param("unitName", m_config.unitName));
  params.push_back(log::Param("previousState", driveStateToStr(m_state)));
  params.push_back(log::Param("newState", driveStateToStr(newState)));
  if(NULL != m_session) {
    params.push_back(log::Param("sessionPid", m_session->getPid()));
  }

  m_state = newState;
  m_log(LOG_DEBUG, "CatalogueDrive changed state", params);
}
