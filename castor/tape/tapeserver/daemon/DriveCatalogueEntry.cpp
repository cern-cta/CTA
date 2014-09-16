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

#include "castor/common/CastorConfiguration.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueEntry.hpp"
#include "castor/utils/utils.hpp"
#include "h/Ctape_constants.h"
#include "h/rmc_constants.h"

#include <string.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueEntry::DriveCatalogueEntry(
  const int netTimeout,
  log::Logger &log,
  ProcessForkerProxy &processForker,
  legacymsg::VdqmProxy &vdqm,
  const std::string &hostName,
  const utils::DriveConfig &config,
  const DataTransferSession::CastorConf &dataTransferConfig,
  const DriveState state)
  throw():
  m_netTimeout(netTimeout),
  m_log(log),
  m_processForker(processForker),
  m_vdqm(vdqm),
  m_hostName(hostName),
  m_config(config),
  m_dataTransferConfig(dataTransferConfig),
  m_state(state),
  m_sessionType(SESSION_TYPE_NONE),
  m_session(NULL) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueEntry::~DriveCatalogueEntry()
  throw() {
  deleteSession();
}

//------------------------------------------------------------------------------
// deleteSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::deleteSession() {
  delete m_session;
  m_session = NULL;
}

//-----------------------------------------------------------------------------
// drvState2Str
//-----------------------------------------------------------------------------
const char
  *castor::tape::tapeserver::daemon::DriveCatalogueEntry::drvState2Str(
  const DriveState state) throw() {
  switch(state) {
  case DRIVE_STATE_INIT    : return "INIT";
  case DRIVE_STATE_DOWN    : return "DOWN";
  case DRIVE_STATE_UP      : return "UP";
  case DRIVE_STATE_RUNNING : return "RUNNING";
  case DRIVE_STATE_WAITDOWN: return "WAITDOWN";
  default                  : return "UNKNOWN";
  }
}

//-----------------------------------------------------------------------------
// sessionType2Str
//-----------------------------------------------------------------------------
const char
   *castor::tape::tapeserver::daemon::DriveCatalogueEntry::sessionType2Str(
  const SessionType sessionType) throw() {
  switch(sessionType) {
  case SESSION_TYPE_NONE        : return "NONE";
  case SESSION_TYPE_CLEANER     : return "CLEANER";
  case SESSION_TYPE_DATATRANSFER: return "DATATRANSFER";
  case SESSION_TYPE_LABEL       : return "LABEL";
  default                       : return "UNKNOWN";
  }
}

//------------------------------------------------------------------------------
// getConfig
//------------------------------------------------------------------------------
const castor::tape::utils::DriveConfig
  &castor::tape::tapeserver::daemon::DriveCatalogueEntry::getConfig() const {
  return m_config;
}

//------------------------------------------------------------------------------
// getState
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueEntry::DriveState
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getState()
  const throw() {
  return m_state;
} 

//------------------------------------------------------------------------------
// getSession
//------------------------------------------------------------------------------
const castor::tape::tapeserver::daemon::DriveCatalogueSession &
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getSession()
  const {
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
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::checkForSession()
  const {
  if(DRIVE_STATE_RUNNING != m_state && DRIVE_STATE_WAITDOWN != m_state) {
    castor::exception::Exception ex;
    ex.getMessage() << "Drive is currently not running a session";
    throw ex;
  }
  if(NULL == m_session) {
    // Should never get here
    castor::exception::Exception ex;
    ex.getMessage() << "Pointer to tape session is unexpectedly NULL";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getCleanerSession
//------------------------------------------------------------------------------
const castor::tape::tapeserver::daemon::DriveCatalogueCleanerSession &
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getCleanerSession()
  const {
  try {
    checkForCleanerSession();
    const DriveCatalogueCleanerSession *const cleanerSession =
      dynamic_cast<const DriveCatalogueCleanerSession*>(m_session);
    if(NULL == cleanerSession) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to cast session to DriveCatalogueCleanerSession";
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
castor::tape::tapeserver::daemon::DriveCatalogueCleanerSession &
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getCleanerSession() {
  try {
    checkForCleanerSession();
    DriveCatalogueCleanerSession *const cleanerSession =
      dynamic_cast<DriveCatalogueCleanerSession*>(m_session);
    if(NULL == cleanerSession) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << 
        "Failed to cast session to DriveCatalogueCleanerSession";
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
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  checkForCleanerSession() const {
  checkForSession();
  if(SESSION_TYPE_CLEANER != m_sessionType) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Session associated with drive is not a cleaner session"
      ": actual=" << sessionType2Str(m_sessionType);
    throw ex;
  }
  if(NULL == m_session) {
    // Should never get here
    castor::exception::Exception ex;
    ex.getMessage() << "Pointer to cleaner session is unexpectedly NULL";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getLabelSession
//------------------------------------------------------------------------------
const castor::tape::tapeserver::daemon::DriveCatalogueLabelSession &
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getLabelSession()
  const {
  try {
    checkForLabelSession();
    const DriveCatalogueLabelSession *const labelSession =
      dynamic_cast<const DriveCatalogueLabelSession*>(m_session);
    if(NULL == labelSession) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to cast session to DriveCatalogueLabelSession";
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
castor::tape::tapeserver::daemon::DriveCatalogueLabelSession &
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getLabelSession() {
  try {
    checkForLabelSession();
    DriveCatalogueLabelSession *const labelSession =
      dynamic_cast<DriveCatalogueLabelSession*>(m_session);
    if(NULL == labelSession) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to cast session to DriveCatalogueLabelSession";
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
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  checkForLabelSession() const {
  checkForSession();
  if(SESSION_TYPE_LABEL != m_sessionType) {
    castor::exception::Exception ex;
    ex.getMessage() << "Session associated with drive is not a label session"
      ": actual=" << sessionType2Str(m_sessionType);
    throw ex;
  }
  if(NULL == m_session) {
    // Should never get here
    castor::exception::Exception ex;
    ex.getMessage() << "Pointer to label session is unexpectedly NULL";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getTransferSession
//------------------------------------------------------------------------------
const castor::tape::tapeserver::daemon::DriveCatalogueTransferSession &
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getTransferSession()
  const {
  try {
    checkForTransferSession();
    DriveCatalogueTransferSession *const transferSession =
      dynamic_cast<DriveCatalogueTransferSession*>(m_session);
    if(NULL == transferSession) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to cast session to DriveCatalogueTransferSession";
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
castor::tape::tapeserver::daemon::DriveCatalogueTransferSession &
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getTransferSession() {
  try {
    checkForTransferSession();
    DriveCatalogueTransferSession *const transferSession =
      dynamic_cast<DriveCatalogueTransferSession*>(m_session);
    if(NULL == transferSession) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to cast session to DriveCatalogueTransferSession";
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
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  checkForTransferSession() const {
  checkForSession();
  if(SESSION_TYPE_DATATRANSFER != m_sessionType) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Session associated with drive is not a transfer session"
      ": actual=" << sessionType2Str(m_sessionType);
    throw ex;
  }
  if(NULL == m_session) {
    // Should never get here
    castor::exception::Exception ex;
    ex.getMessage() << "Pointer to transfer session is unexpectedly NULL";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getSessionType
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueEntry::SessionType
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getSessionType()
  const throw() {
  return m_sessionType;
}

//------------------------------------------------------------------------------
// configureUp
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::configureUp() {
  switch(m_state) {
  case DRIVE_STATE_UP:
  case DRIVE_STATE_RUNNING:
    break;
  case DRIVE_STATE_DOWN:
    m_state = DRIVE_STATE_UP;
    break;
  case DRIVE_STATE_WAITDOWN:
    m_state = DRIVE_STATE_RUNNING;
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to configure tape-drive " << m_config.unitName
        << " up : Incompatible drive state: state=" << drvState2Str(m_state);
      throw ex;
    }
  }
  m_vdqm.setDriveUp(m_hostName, m_config.unitName, m_config.dgn);
}

//-----------------------------------------------------------------------------
// configureDown
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::configureDown() {
  switch(m_state) {
  case DRIVE_STATE_DOWN:
    break;
  case DRIVE_STATE_UP:
    m_state = DRIVE_STATE_DOWN;
    break;
  case DriveCatalogueEntry::DRIVE_STATE_RUNNING:
    m_state = DRIVE_STATE_WAITDOWN;
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to configure tape drive " << m_config.unitName
        << " down: " << ": Incompatible drive state: state=" <<
        drvState2Str(m_state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// receivedVdqmJob
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::receivedVdqmJob(
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
    m_state = DRIVE_STATE_RUNNING;
    m_sessionType = SESSION_TYPE_DATATRANSFER;
    {
      const unsigned short rmcPort =
        common::CastorConfiguration::getConfig().getConfEntInt("RMC", "PORT",
          (unsigned short)RMC_PORT, &m_log);
      DriveCatalogueTransferSession *const transferSession =
        DriveCatalogueTransferSession::create(
          m_log,
          m_netTimeout,
          m_config,
          job,
          m_dataTransferConfig,
          rmcPort,
          m_processForker);
      m_session = dynamic_cast<DriveCatalogueSession *>(transferSession);
      m_vdqm.assignDrive(m_hostName, m_config.unitName, job.dgn,
        job.volReqId, m_session->getPid());
    }
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << drvState2Str(m_state);
      throw ex;
    }
  } 
} 

//-----------------------------------------------------------------------------
// receivedLabelJob
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::receivedLabelJob(
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
    m_state = DRIVE_STATE_RUNNING;
    m_sessionType = SESSION_TYPE_LABEL;
    {
      const unsigned short rmcPort =
        common::CastorConfiguration::getConfig().getConfEntInt("RMC", "PORT",
          (unsigned short)RMC_PORT, &m_log);
      m_session = DriveCatalogueLabelSession::create(
        m_log,
        m_netTimeout,
        m_config,
        m_processForker,
        job,
        rmcPort,
        labelCmdConnection);
    }
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << drvState2Str(m_state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// createCleaner
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::createCleaner(
  const std::string &vid, const time_t assignmentTime) {
  try {
    // Create a cleaner session in the catalogue
    m_state = DRIVE_STATE_RUNNING;
    m_sessionType = SESSION_TYPE_CLEANER;
    const unsigned short rmcPort =
      common::CastorConfiguration::getConfig().getConfEntInt("RMC", "PORT",
        (unsigned short)RMC_PORT, &m_log);
    m_session = DriveCatalogueCleanerSession::create(
      m_log,
      m_netTimeout,
      m_config,
      m_processForker,
      vid,
      rmcPort,
      assignmentTime);
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
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  sessionSucceeded() {
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
    m_state = DRIVE_STATE_UP;
    m_vdqm.setDriveUp(m_hostName, m_config.unitName, m_config.dgn);
    break;
  case DRIVE_STATE_WAITDOWN:
    m_state = DriveCatalogueEntry::DRIVE_STATE_DOWN;
    m_vdqm.setDriveDown(m_hostName, m_config.unitName, m_config.dgn);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to record tape session succeeded for session with pid " <<
        getSession().getPid() << ": Incompatible drive state: state=" <<
        drvState2Str(m_state);
      delete m_session;
      m_session = NULL;
      m_sessionType = SESSION_TYPE_NONE;
      throw ex;
    }
  }

  // Wrap the session in an auto pointer to gaurantee it is deleted even in the
  // event of an exception
  std::auto_ptr<DriveCatalogueSession> session(m_session);

  // Set the member variable m_session to NULL to prevent the desstruxtor from
  // trying a double delete
  m_session = NULL;
  m_sessionType = SESSION_TYPE_NONE;

  session->sessionSucceeded();
}

//-----------------------------------------------------------------------------
// sessionFailed
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::sessionFailed() {
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    m_state = DRIVE_STATE_DOWN;
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to record tape session failed for session with pid " <<
        getSession().getPid() << ": Incompatible drive state: state=" <<
        drvState2Str(m_state);
      delete m_session;
      m_session = NULL;
      m_sessionType = SESSION_TYPE_NONE;
      throw ex;
    }
  }

  m_vdqm.setDriveDown(m_hostName, m_config.unitName, m_config.dgn);

  // Wrap the session in an auto pointer to gaurantee it is deleted even in the
  // event of an exception
  std::auto_ptr<DriveCatalogueSession> session(m_session);

  // Set the member variable m_session to NULL to prevent the desstruxtor from
  // trying a double delete
  m_session = NULL;
  m_sessionType = SESSION_TYPE_NONE;

  session->sessionFailed();
}

//------------------------------------------------------------------------------
// getTapeStatDriveEntry
//------------------------------------------------------------------------------
castor::legacymsg::TapeStatDriveEntry
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getTapeStatDriveEntry()
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
uint32_t castor::tape::tapeserver::daemon::DriveCatalogueEntry::
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
uint32_t castor::tape::tapeserver::daemon::DriveCatalogueEntry::
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
uint16_t castor::tape::tapeserver::daemon::DriveCatalogueEntry::
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
uint16_t castor::tape::tapeserver::daemon::DriveCatalogueEntry::
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
uint32_t castor::tape::tapeserver::daemon::DriveCatalogueEntry::
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
uint16_t castor::tape::tapeserver::daemon::DriveCatalogueEntry::
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
std::string castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  getLblCodeForTapeStatDriveEntry() const throw() {
  return "aul";
}

//------------------------------------------------------------------------------
// getToBeMountedForTapeStatDriveEntry
//------------------------------------------------------------------------------
uint16_t castor::tape::tapeserver::daemon::DriveCatalogueEntry::
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
std::string castor::tape::tapeserver::daemon::DriveCatalogueEntry::
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
std::string castor::tape::tapeserver::daemon::DriveCatalogueEntry::
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
time_t castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  getAssignmentTimeForCleaner() const throw() {
  try {
    return getSession().getAssignmentTime();
  } catch(...) {
    return 0;
  }
}

//------------------------------------------------------------------------------
// getVsnForTapeStatDriveEntry
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  getVsnForTapeStatDriveEntry() const throw() {
  return getVidForTapeStatDriveEntry();
}
