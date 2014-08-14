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

#include <string.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueEntry::DriveCatalogueEntry(
  log::Logger &log,
  ProcessForkerProxy &processForker,
  legacymsg::VdqmProxy &vdqm,
  const std::string &hostName,
  const utils::DriveConfig &config,
  const DataTransferSession::CastorConf &dataTransferConfig,
  const DriveState state)
  throw():
  m_log(log),
  m_processForker(processForker),
  m_vdqm(vdqm),
  m_config(config),
  m_dataTransferConfig(dataTransferConfig),
  m_state(state),
  m_sessionType(SESSION_TYPE_NONE),
  m_labelCmdConnection(-1),
  m_session(NULL) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueEntry::~DriveCatalogueEntry()
  throw() {
  delete m_session;
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
  if(DRIVE_STATE_RUNNING != m_state) {
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
  if(DRIVE_STATE_RUNNING != m_state) {
    castor::exception::Exception ex;
    ex.getMessage() << "Drive is currently not running a session";
    throw ex;
  }
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
  if(DRIVE_STATE_RUNNING != m_state) {
    castor::exception::Exception ex;
    ex.getMessage() << "Drive is currently not running a session";
    throw ex;
  }
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
  if(DRIVE_STATE_RUNNING != m_state) {
    castor::exception::Exception ex;
    ex.getMessage() << "Drive is currently not running a session";
    throw ex;
  }
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
// getVdqmJob
//------------------------------------------------------------------------------
const castor::legacymsg::RtcpJobRqstMsgBody
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getVdqmJob() const {
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return getTransferSession().getVdqmJob();
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
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return getLabelSession().getLabelJob();
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
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return getSession().getPid();
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
  case DRIVE_STATE_RUNNING:
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
  case DRIVE_STATE_RUNNING:
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
    m_session = new DriveCatalogueTransferSession(m_log, m_config,
      m_dataTransferConfig, job, m_processForker, m_vdqm, m_hostName);
    getTransferSession().forkDataTransferSession();
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
    m_state = DRIVE_STATE_RUNNING;
    m_sessionType = SESSION_TYPE_LABEL;
    m_session = new DriveCatalogueLabelSession(m_log, m_processForker, m_config,
      job, labelCmdConnection);
    getLabelSession().forkLabelSession();
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
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::toBeCleaned() {
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    {
      // Get the vid and assignment time of the crashed tape session
      std::string vid;
      try {
        vid = m_session->getVid();
      } catch(...) {
        vid = "";
      }
      const time_t assignmentTime = m_session->getAssignmentTime();

      // Free the catalogue representation of the crashed tape session setting
      // the sesison pointer to NULL to prevent the destructor from perfroming
      // a doyble delete
      delete(m_session);
      m_session = NULL;

      // Create a cleaner session in the catalogue
      m_state = DRIVE_STATE_RUNNING;
      m_sessionType = SESSION_TYPE_CLEANER;
      m_session = new DriveCatalogueCleanerSession(vid, assignmentTime);

      // TO BE DONE - fork porcess
    }
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to clean drive"
        ": Drive is not running a tape session: state=" <<
        drvState2Str(m_state);
      throw ex;
    }
  } 
}

//-----------------------------------------------------------------------------
// sessionSucceeded
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::sessionSucceeded() {
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
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
        getSession().getPid() << ": Incompatible drive state: state=" <<
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
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return getSession().getPid();
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
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    try {
      return m_session->getAssignmentTime();
    } catch(...) {
      return 0;
    }
  default:
    return 0;
  }
}

//------------------------------------------------------------------------------
// getModeForTapeStatDriveEntry
//------------------------------------------------------------------------------
uint16_t castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  getModeForTapeStatDriveEntry() const throw() {
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    try {
      return m_session->getMode();
    } catch(...) {
      return WRITE_DISABLE;
    }
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
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return m_session->tapeIsBeingMounted();
  default:
    return 0;
  }
}

//------------------------------------------------------------------------------
// getVidForTapeStatDriveEntry
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  getVidForTapeStatDriveEntry() const throw() {
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    try {
      return m_session->getVid();
    } catch(...) {
      return "";
    }
  default:
    return "";
  }
}

//------------------------------------------------------------------------------
// getVsnForTapeStatDriveEntry
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  getVsnForTapeStatDriveEntry() const throw() {
  return getVidForTapeStatDriveEntry();
}
