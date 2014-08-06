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
#include "castor/tape/tapeserver/daemon/DriveCatalogueEntry.hpp"
#include "castor/utils/utils.hpp"
#include "h/Ctape_constants.h"
#include "castor/tape/tapeserver/daemon/DriveCatalogueCleanerSession.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueLabelSession.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueTransferSession.hpp"

#include <string.h>

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueEntry::~DriveCatalogueEntry()
  throw() {
  delete m_session;
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueEntry::DriveCatalogueEntry()
  throw():
  m_mode(WRITE_DISABLE),
  m_getToBeMountedForTapeStatDriveEntry(false),
  m_state(DRIVE_STATE_INIT),
  m_sessionType(SESSION_TYPE_NONE),
  m_labelCmdConnection(-1),
  m_session(NULL) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueEntry::DriveCatalogueEntry(
  const utils::DriveConfig &config, const DriveState state) throw():
  m_config(config),
  m_mode(WRITE_DISABLE),
  m_getToBeMountedForTapeStatDriveEntry(false),
  m_state(state),
  m_sessionType(SESSION_TYPE_NONE),
  m_labelCmdConnection(-1),
  m_session(NULL) {
}

//-----------------------------------------------------------------------------
// drvState2Str
//-----------------------------------------------------------------------------
const char
  *castor::tape::tapeserver::daemon::DriveCatalogueEntry::drvState2Str(
  const DriveState state) throw() {
  switch(state) {
  case DRIVE_STATE_INIT             : return "INIT";
  case DRIVE_STATE_DOWN             : return "DOWN";
  case DRIVE_STATE_UP               : return "UP";
  case DRIVE_STATE_SESSIONRUNNING   : return "SESSIONRUNNING";
  case DRIVE_STATE_WAITDOWN         : return "WAITDOWN";
  default                           : return "UNKNOWN";
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
  case SESSION_TYPE_DATATRANSFER: return "DATATRANSFER";
  case SESSION_TYPE_LABEL       : return "LABEL";
  case SESSION_TYPE_CLEANER     : return "CLEANER";
  default                       : return "UNKNOWN";
  }
}

//------------------------------------------------------------------------------
// setConfig
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::setConfig(
  const tape::utils::DriveConfig &config) throw() {
}

//------------------------------------------------------------------------------
// getConfig
//------------------------------------------------------------------------------
const castor::tape::utils::DriveConfig
  &castor::tape::tapeserver::daemon::DriveCatalogueEntry::getConfig() const {
  return m_config;
}

//------------------------------------------------------------------------------
// setVid
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::setVid(
  const std::string &vid) {
  m_vid = vid;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::DriveCatalogueEntry::getVid()
  const {
  return m_vid;
}

//------------------------------------------------------------------------------
// setAssignmentTime
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::setAssignmentTime(
 const time_t assignmentTime) {
 m_assignmentTime = assignmentTime;
}

//------------------------------------------------------------------------------
// getAssignmentTime
//------------------------------------------------------------------------------
time_t
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getAssignmentTime()
  const {
  return m_assignmentTime;
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
// getSessionState
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueSession::SessionState
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getSessionState()
  const throw() {
  return getSession()->getState();
}

//------------------------------------------------------------------------------
// getSessionState
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueSession * castor::tape::tapeserver::daemon::DriveCatalogueEntry::getSession()
  const {
  if(m_session) return m_session;
  else {
    castor::exception::Exception ex;
    ex.getMessage() << "The session pointer is null";
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
// getVdqmJob
//------------------------------------------------------------------------------
const castor::legacymsg::RtcpJobRqstMsgBody
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getVdqmJob() const {
  switch(m_state) {
  case DRIVE_STATE_SESSIONRUNNING:
  case DRIVE_STATE_WAITDOWN:
  {
    DriveCatalogueTransferSession *theTransferSession = dynamic_cast<DriveCatalogueTransferSession *>(getSession());
    return theTransferSession->getVdqmJob();
  }
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to get VDQM job of tape-drive " <<
        m_config.unitName << ": Incompatible drive state: state=" <<
        drvState2Str(m_state);
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// getLabelJob
//------------------------------------------------------------------------------
const castor::legacymsg::TapeLabelRqstMsgBody
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getLabelJob() const {
  switch(m_state) {
  case DRIVE_STATE_SESSIONRUNNING:
  case DRIVE_STATE_WAITDOWN:
  {
    DriveCatalogueLabelSession *theLabelSession = dynamic_cast<DriveCatalogueLabelSession *>(getSession());
    return theLabelSession->getLabelJob();
  }
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to get label job of tape-drive " <<
        m_config.unitName << ": Incompatible drive state: state=" <<
        drvState2Str(m_state);
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// getSessionPid
//------------------------------------------------------------------------------
pid_t castor::tape::tapeserver::daemon::DriveCatalogueEntry::getSessionPid()
  const {
  switch(m_state) {
  case DRIVE_STATE_SESSIONRUNNING:
  case DRIVE_STATE_WAITDOWN:
    return getSession()->getPid();
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to get the process ID of tape-drive " <<
        m_config.unitName << ": Incompatible drive state: state=" <<
        drvState2Str(m_state);
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// getLabelCmdConnection
//------------------------------------------------------------------------------
int
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getLabelCmdConnection()
  const {
  switch(m_state) {
  case DRIVE_STATE_SESSIONRUNNING:
  case DRIVE_STATE_WAITDOWN:
    return m_labelCmdConnection;;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to get the file descriptor of the TCP/IP"
        " connection with the castor-tape-label command-line tool for"
        " tape-drive" << m_config.unitName << ": Incompatible drive state"
        ": state=" << drvState2Str(m_state);
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// releaseLabelCmdConnection
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  releaseLabelCmdConnection() {
    switch(m_state) {
  case DRIVE_STATE_SESSIONRUNNING:
  case DRIVE_STATE_WAITDOWN:
    if(0 < m_labelCmdConnection) {
      const int tmpConnection = m_labelCmdConnection;
      m_labelCmdConnection = -1;
      return tmpConnection;
    } else {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to release the file descriptor of the TCP/IP"
        " connection with the castor-tape-label command-line tool for"
        " tape-drive" << m_config.unitName << ": File descriptor is not owned"
        " by the drive entry in the tape-drive catalogue";
      throw ex;
    }
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to release the file descriptor of the TCP/IP"
        " connection with the castor-tape-label command-line tool for"
        " tape-drive" << m_config.unitName << ": Incompatible drive state"
        ": state=" << drvState2Str(m_state);
      throw ex;
    }
  }

}

//-----------------------------------------------------------------------------
// receivedRecallJob
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::receivedRecallJob(
  const std::string &vid) {
  m_assignmentTime = time(0);
  m_vid = vid;
  m_getToBeMountedForTapeStatDriveEntry = true;
  m_mode = WRITE_DISABLE;
}

//-----------------------------------------------------------------------------
// receivedMigrationJob
//-----------------------------------------------------------------------------
void
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::receivedMigrationJob(
  const std::string &vid) {
  m_assignmentTime = time(0);
  m_vid = vid;
  m_getToBeMountedForTapeStatDriveEntry = true;
  m_mode = WRITE_ENABLE;
}

//-----------------------------------------------------------------------------
// tapeMountedForMigration
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  tapeMountedForMigration(const std::string &vid) {
  m_vid = vid;
  m_getToBeMountedForTapeStatDriveEntry = false;
  m_mode = WRITE_ENABLE;
}

//-----------------------------------------------------------------------------
// tapeMountedForRecall
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  tapeMountedForRecall(const std::string &vid) {
  m_vid = vid;
  m_getToBeMountedForTapeStatDriveEntry = false;
  m_mode = WRITE_DISABLE;
}

//------------------------------------------------------------------------------
// configureUp
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::configureUp() {
  switch(m_state) {
  case DRIVE_STATE_UP:
  case DRIVE_STATE_SESSIONRUNNING:
    break;
  case DRIVE_STATE_DOWN:
    m_state = DRIVE_STATE_UP;
    break;
  case DRIVE_STATE_WAITDOWN:
    m_state = DRIVE_STATE_SESSIONRUNNING;
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to configure tape-drive " << m_config.unitName
        << " up : Incompatible drive state: state=" << drvState2Str(m_state);
      throw ex;
    }
  }
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
  case DriveCatalogueEntry::DRIVE_STATE_SESSIONRUNNING:
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
    m_state = DRIVE_STATE_SESSIONRUNNING;
    m_sessionType = SESSION_TYPE_DATATRANSFER;
    m_session = new DriveCatalogueTransferSession(castor::tape::tapeserver::daemon::DriveCatalogueSession::SESSION_STATE_WAITFORK, job);
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
    if(-1 != m_labelCmdConnection) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Drive catalogue already owns a label-command connection";
      throw ex;
    }
    m_labelCmdConnection = labelCmdConnection;
    m_state = DRIVE_STATE_SESSIONRUNNING;
    m_sessionType = SESSION_TYPE_LABEL;
    m_session = new DriveCatalogueLabelSession(
      DriveCatalogueSession::SESSION_STATE_WAITFORK, job, labelCmdConnection);
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
// toBeCleaned
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::toBeCleaned()  {

  std::ostringstream task;
  task << "handle cleaner job for tape drive " << m_config.unitName;
  
  switch(m_state) {
    case DRIVE_STATE_INIT:
    case DRIVE_STATE_DOWN:
    case DRIVE_STATE_UP:
    case DRIVE_STATE_SESSIONRUNNING:
    case DRIVE_STATE_WAITDOWN:
      m_state = DRIVE_STATE_SESSIONRUNNING;
      m_sessionType = SESSION_TYPE_CLEANER;
      m_session = new DriveCatalogueCleanerSession(castor::tape::tapeserver::daemon::DriveCatalogueSession::SESSION_STATE_WAITFORK);
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
// forkedDataTransferSession
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::forkedDataTransferSession(
  const pid_t sessionPid)  {
  std::ostringstream task;
  task << "handle fork of data-transfer session for tape drive " <<
    m_config.unitName;

  switch(getSession()->getState()) {
  case DriveCatalogueSession::SESSION_STATE_WAITFORK:
    getSession()->setState(DriveCatalogueSession::SESSION_STATE_RUNNING);
    getSession()->setPid(sessionPid);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible session state: state=" << getSession()->getState();
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// forkedLabelSession
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::forkedLabelSession(
  const pid_t sessionPid) {
  std::ostringstream task;
  task << "handle fork of label session for tape drive " << m_config.unitName;
    
  switch(getSession()->getState()) {
  case castor::tape::tapeserver::daemon::DriveCatalogueSession::SESSION_STATE_WAITFORK:
    getSession()->setState(castor::tape::tapeserver::daemon::DriveCatalogueSession::SESSION_STATE_RUNNING);
    getSession()->setPid(sessionPid);
    break;
  default:
    {   
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << getSession()->getState();
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// forkedCleanerSession
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::forkedCleanerSession(
  const pid_t sessionPid) {
  std::ostringstream task;
  task << "handle fork of cleaner session for tape drive " << m_config.unitName;
    
  switch(getSession()->getState()) {
  case castor::tape::tapeserver::daemon::DriveCatalogueSession::SESSION_STATE_WAITFORK:
    getSession()->setState(castor::tape::tapeserver::daemon::DriveCatalogueSession::SESSION_STATE_RUNNING);
    getSession()->setPid(sessionPid);
    break;
  default:
    {   
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << getSession()->getState();
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// sessionSucceeded
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::sessionSucceeded() {
  switch(m_state) {
  case DRIVE_STATE_SESSIONRUNNING:
    m_state = DRIVE_STATE_UP;
    break;
  case DRIVE_STATE_WAITDOWN:
    m_state = DriveCatalogueEntry::DRIVE_STATE_DOWN;
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to record tape session succeeded for session with pid " <<
        getSession()->getPid() << ": Incompatible drive state: state=" <<
        drvState2Str(m_state);
      delete m_session;
      m_session = NULL;
      m_sessionType = SESSION_TYPE_NONE;
      throw ex;
    }
  }
  delete m_session;
  m_session = NULL;
  m_sessionType = SESSION_TYPE_NONE;
}

//-----------------------------------------------------------------------------
// sessionFailed
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::sessionFailed() {
  switch(m_state) {
  case DRIVE_STATE_SESSIONRUNNING:
  case DRIVE_STATE_WAITDOWN:
    m_state = DRIVE_STATE_DOWN;
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to record tape session failed for session with pid " <<
        getSession()->getPid() << ": Incompatible drive state: state=" <<
        drvState2Str(m_state);
      delete m_session;
      m_session = NULL;
      m_sessionType = SESSION_TYPE_NONE;
      throw ex;
    }
  }
  delete m_session;
  m_session = NULL;
  m_sessionType = SESSION_TYPE_NONE;
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
  case DRIVE_STATE_SESSIONRUNNING:
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
  switch(m_state) {
  case DRIVE_STATE_SESSIONRUNNING:
  case DRIVE_STATE_WAITDOWN:
    return getSession()->getPid();
  default:
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
  case DRIVE_STATE_SESSIONRUNNING:
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
  case DRIVE_STATE_SESSIONRUNNING:
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
  return m_assignmentTime;
}

//------------------------------------------------------------------------------
// getModeForTapeStatDriveEntry
//------------------------------------------------------------------------------
uint16_t castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  getModeForTapeStatDriveEntry() const throw() {
  switch(m_state) {
  case DRIVE_STATE_SESSIONRUNNING:
  case DRIVE_STATE_WAITDOWN:
    return m_mode;
  default:
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
  return m_getToBeMountedForTapeStatDriveEntry;
}

//------------------------------------------------------------------------------
// getVidForTapeStatDriveEntry
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  getVidForTapeStatDriveEntry() const throw() {
  return m_vid;
}

//------------------------------------------------------------------------------
// getVsnForTapeStatDriveEntry
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  getVsnForTapeStatDriveEntry() const throw() {
  return getVidForTapeStatDriveEntry();
}
