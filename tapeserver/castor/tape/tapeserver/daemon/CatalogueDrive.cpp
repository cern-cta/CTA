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
#include "castor/tape/tapeserver/daemon/EmptyDriveProbe.hpp"
#include "castor/utils/utils.hpp"
#include "Ctape_constants.h"
#include "rmc_constants.h"
#include "serrno.h"

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
  const std::string &hostName,
  const DriveConfig &config,
  const CatalogueDriveState state,
  const CatalogueConfig &catalogueConfig,
  System::virtualWrapper &sysWrapper)
  throw():
  m_netTimeout(netTimeout),
  m_log(log),
  m_processForker(processForker),
  m_cupv(cupv),
  m_hostName(hostName),
  m_config(config),
  m_sysWrapper(sysWrapper),
  m_state(state),
  m_waitJobTimeoutSecs(catalogueConfig.waitJobTimeoutSecs),
  m_mountTimeoutSecs(catalogueConfig.mountTimeoutSecs),
  m_blockMoveTimeoutSecs(catalogueConfig.blockMoveTimeoutSecs),
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
  // If there is a tape session and it does not want to continue the main event
  // loop
  if(NULL != m_session && !m_session->handleTick()) {
    return false; // Do no continue the main event loop
  }

  return true; // Continue the main event loop
}

//------------------------------------------------------------------------------
// deleteSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::deleteSession() {
  delete m_session;
  m_session = NULL;
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
castor::tape::tapeserver::daemon::CatalogueDriveState castor::tape::tapeserver::
  daemon::CatalogueDrive::getState() const throw() {
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
      m_config.getUnitName() << " from drive catalogue: " <<
      ne.getMessage().str();
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
      catalogueDriveStateToStr(m_state);
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
      m_config.getUnitName() << " from drive catalogue: " <<
      ne.getMessage().str();
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
      m_config.getUnitName() << " from drive catalogue: " <<
      ne.getMessage().str();
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
      m_config.getUnitName() << " from drive catalogue: " <<
      ne.getMessage().str();
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
      m_config.getUnitName() << " from drive catalogue: " <<
      ne.getMessage().str();
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
      m_config.getUnitName() << " from drive catalogue: " <<
      ne.getMessage().str();
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
      m_config.getUnitName() << " from drive catalogue: " <<
      ne.getMessage().str();
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
    // This state transition is idempotent
    break;
  case DRIVE_STATE_DOWN:
    transitionFromDownToUp();
    break;
  case DRIVE_STATE_WAITDOWN:
    // Leave state in vdqm as RUNNING, just update internally state to reflect
    // the  future intention of transitioning to DOWN
    changeState(DRIVE_STATE_RUNNING);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to configure tape-drive " <<
        m_config.getUnitName() << " up: Incompatible drive state: state=" <<
        catalogueDriveStateToStr(m_state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// transitionFromDownToUp
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
  transitionFromDownToUp() {
  checkDriveIsEmpty();
  changeState(DRIVE_STATE_UP);
}

//-----------------------------------------------------------------------------
// checkDriveIsEmpty
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::checkDriveIsEmpty() {
  EmptyDriveProbe probe(m_log, m_config, m_sysWrapper);

  if(!probe.driveIsEmpty()) {
    castor::exception::Exception ex(ETDRVNOTREADYFORMNT);
    ex.getMessage() << "Drive " << m_config.getUnitName() << " is not empty";
    throw ex;
  }
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
    break;
  case DRIVE_STATE_RUNNING:
    changeState(DRIVE_STATE_WAITDOWN);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to configure tape drive " <<
        m_config.getUnitName() << " down: Incompatible drive state: state=" <<
        catalogueDriveStateToStr(m_state);
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
  task << "handle label job for tape drive " << m_config.getUnitName();

  // Sanity check
  if(job.drive != m_config.getUnitName()) {
    // Should never happen
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": unit name mismatch: job.drive=" << job.drive << 
      " m_config.getUnitName()=" << m_config.getUnitName();
    throw ex;
  }

  switch(m_state) {
  case DRIVE_STATE_UP:
    if(std::string(job.dgn) != m_config.getDgn()) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": DGN mismatch: catalogueDgn=" << m_config.getDgn() << " labelJobDgn="
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
        ": Incompatible drive state: state=" <<
        catalogueDriveStateToStr(m_state);
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
  const bool waitMediaInDrive,
  const uint32_t waitMediaInDriveTimeout) const {
  try {
    return CatalogueCleanerSession::create(
      m_log,
      m_netTimeout,
      m_config,
      m_processForker,
      vid,
      assignmentTime,
      waitMediaInDrive,
      waitMediaInDriveTimeout);
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
  std::unique_ptr<CatalogueSession> session(m_session);
  m_session = NULL;

  switch(m_state) {
  case DRIVE_STATE_RUNNING:
    changeState(DRIVE_STATE_UP);
    session->sessionSucceeded();
    break;
  case DRIVE_STATE_WAITDOWN:
    changeState(DRIVE_STATE_DOWN);
    session->sessionSucceeded();
    break;
  case DRIVE_STATE_WAITSHUTDOWNKILL:
    changeState(DRIVE_STATE_SHUTDOWN);
    session->sessionSucceeded();
    break;
  case DRIVE_STATE_WAITSHUTDOWNCLEANER:
    changeState(DRIVE_STATE_SHUTDOWN);
    session->sessionSucceeded();
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to record tape session succeeded for session with pid " <<
        getSession().getPid() << ": Incompatible drive state: state=" <<
        catalogueDriveStateToStr(m_state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// sessionFailedAndRequestedDriveDown
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
  sessionFailedAndRequestedDriveDown() {
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
  case DRIVE_STATE_WAITSHUTDOWNKILL:
    return runningSessionFailedAndRequestedDriveDown();
  case DRIVE_STATE_WAITSHUTDOWNCLEANER:
    return cleanerOfShutdownFailed();
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to record tape session failed for session with pid " <<
        getSession().getPid() << ": Incompatible drive state: state=" <<
        catalogueDriveStateToStr(m_state);
      delete m_session;
      m_session = NULL;
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// sessionKilled
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
sessionKilled(uint32_t signal) {
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return runningSessionKilled(signal);
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
        catalogueDriveStateToStr(m_state);
      delete m_session;
      m_session = NULL;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// runningSessionFailedAndRequestedDriveDown
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
  runningSessionFailedAndRequestedDriveDown() {
  std::unique_ptr<CatalogueSession> session(m_session);
  m_session = NULL;
  session->sessionFailed();
  changeState(DRIVE_STATE_DOWN);
}

//------------------------------------------------------------------------------
// runningSessionKilled
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
runningSessionKilled(uint32_t signal) {
  std::unique_ptr<CatalogueSession> session(m_session);
  m_session = NULL;
  session->sessionKilled(signal);

  if(CatalogueSession::SESSION_TYPE_CLEANER != session->getType()) {
    const uint32_t waitMediaInDriveTimeout = 60;
    const bool waitMediaInDrive = true;
    m_session = createCleaner(session->getVid(), session->getAssignmentTime(),
      waitMediaInDrive, waitMediaInDriveTimeout);
  } else {
    changeState(DRIVE_STATE_DOWN);
  }
}


//------------------------------------------------------------------------------
// sessionKilledByShutdown
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
  sessionKilledByShutdown() {
  std::unique_ptr<CatalogueSession> session(m_session);
  m_session = NULL;
  session->sessionFailed();

  changeState(DRIVE_STATE_WAITSHUTDOWNCLEANER);
  const uint32_t waitMediaInDriveTimeout = 60;
  const bool waitMediaInDrive = true;
  m_session = createCleaner(session->getVid(), session->getAssignmentTime(),
    waitMediaInDrive, waitMediaInDriveTimeout);
}

//------------------------------------------------------------------------------
// cleanerOfShutdownFailed
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
  cleanerOfShutdownFailed() {
  std::unique_ptr<CatalogueSession> session(m_session);
  m_session = NULL;
  session->sessionFailed();

  // Cleaner failed, no more can be done, mark drive as shutdown
  changeState(DRIVE_STATE_SHUTDOWN);
}

//------------------------------------------------------------------------------
// sessionFailedAndRequestedCleaner
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueDrive::
  sessionFailedAndRequestedCleaner() {
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
        catalogueDriveStateToStr(m_state);
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
    params.push_back(log::Param("unitName", m_config.getUnitName()));
    params.push_back(log::Param("sessionType", sessionTypeStr));
    params.push_back(log::Param("sessionPid", sessionPid));

    m_log(LOG_INFO, "Failed session has requested cleaner", params);
  }

  std::unique_ptr<CatalogueSession> session(m_session);
  m_session = NULL;
  session->sessionFailed();

  if(CatalogueSession::SESSION_TYPE_CLEANER != session->getType()) {
    const uint32_t waitMediaInDriveTimeout = 60;
    const bool waitMediaInDrive = true;
    m_session = createCleaner(session->getVid(), session->getAssignmentTime(),
      waitMediaInDrive, waitMediaInDriveTimeout);
  } else {
    changeState(DRIVE_STATE_DOWN);
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
    castor::utils::copyString(entry.dgn, m_config.getDgn());
    entry.up = getUpForTapeStatDriveEntry();
    entry.asn = getAsnForTapeStatDriveEntry();
    entry.asn_time = getAsnTimeForTapeStatDriveEntry();
    castor::utils::copyString(entry.drive, m_config.getUnitName());
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
    
    const uint32_t waitMediaInDriveTimeout = 0;
    const bool waitMediaInDrive = false;
    m_session = createCleaner(vid, assignmentTime,
      waitMediaInDrive, waitMediaInDriveTimeout);

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
      params.push_back(log::Param("unitName", m_config.getUnitName()));
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
  params.push_back(log::Param("unitName", m_config.getUnitName()));
  params.push_back(log::Param("sessionType", sessionTypeStr));
  params.push_back(log::Param("sessionPid", sessionPid));
  params.push_back(log::Param("TPVID", m_session->getVid()));

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
  const CatalogueDriveState newState) throw() {
  std::list<log::Param> params;
  params.push_back(log::Param("unitName", m_config.getUnitName()));
  params.push_back(log::Param("previousState",
    catalogueDriveStateToStr(m_state)));
  params.push_back(log::Param("newState", catalogueDriveStateToStr(newState)));
  if(NULL != m_session) {
    params.push_back(log::Param("sessionPid", m_session->getPid()));
  }

  m_state = newState;
  m_log(LOG_DEBUG, "CatalogueDrive changed state", params);
}
