/******************************************************************************
 *         castor/tape/tapeserver/daemon/DriveCatalogueEntry.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Exception.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueEntry.hpp"
#include "castor/utils/utils.hpp"
#include "h/Ctape_constants.h"

#include <string.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueEntry::DriveCatalogueEntry()
  throw():
  m_mode(castor::messages::TAPE_MODE_NONE),
  m_getToBeMountedForTapeStatDriveEntry(false),
  m_state(DRIVE_STATE_INIT),
  m_sessionType(SESSION_TYPE_NONE),
  m_labelCmdConnection(-1) {
  m_labelJob.gid = 0;
  m_labelJob.uid = 0;
  memset(m_labelJob.vid, '\0', sizeof(m_labelJob.vid));
  m_vdqmJob.volReqId = 0;
  m_vdqmJob.clientPort = 0;
  m_vdqmJob.clientEuid = 0;
  m_vdqmJob.clientEgid = 0;
  memset(m_vdqmJob.clientHost, '\0', sizeof(m_vdqmJob.clientHost));
  memset(m_vdqmJob.dgn, '\0', sizeof(m_vdqmJob.dgn));
  memset(m_vdqmJob.driveUnit, '\0', sizeof(m_vdqmJob.driveUnit));
  memset(m_vdqmJob.clientUserName, '\0', sizeof(m_vdqmJob.clientUserName));
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueEntry::DriveCatalogueEntry(
  const utils::DriveConfig &config, const DriveState state) throw():
  m_config(config),
  m_mode(castor::messages::TAPE_MODE_NONE),
  m_getToBeMountedForTapeStatDriveEntry(false),
  m_state(state),
  m_sessionType(SESSION_TYPE_NONE),
  m_labelCmdConnection(-1) {
  m_labelJob.gid = 0;
  m_labelJob.uid = 0;
  memset(m_labelJob.vid, '\0', sizeof(m_labelJob.vid));
  m_vdqmJob.volReqId = 0;
  m_vdqmJob.clientPort = 0;
  m_vdqmJob.clientEuid = 0;
  m_vdqmJob.clientEgid = 0;
  memset(m_vdqmJob.clientHost, '\0', sizeof(m_vdqmJob.clientHost));
  memset(m_vdqmJob.dgn, '\0', sizeof(m_vdqmJob.dgn));
  memset(m_vdqmJob.driveUnit, '\0', sizeof(m_vdqmJob.driveUnit));
  memset(m_vdqmJob.clientUserName, '\0', sizeof(m_vdqmJob.clientUserName));
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
  case DRIVE_STATE_WAITFORKTRANSFER : return "WAITFORKTRANSFER";
  case DRIVE_STATE_WAITFORKLABEL    : return "WAITFORKLABEL";
  case DRIVE_STATE_RUNNING          : return "RUNNING";
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
  &castor::tape::tapeserver::daemon::DriveCatalogueEntry::getVdqmJob() const {
  switch(m_state) {
  case DRIVE_STATE_WAITFORKTRANSFER:
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return m_vdqmJob;
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
  &castor::tape::tapeserver::daemon::DriveCatalogueEntry::getLabelJob() const {
  switch(m_state) {
  case DRIVE_STATE_WAITFORKLABEL:
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return m_labelJob;
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
    return m_sessionPid;
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
  case DRIVE_STATE_WAITFORKLABEL:
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
  case DRIVE_STATE_WAITFORKLABEL:
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

//-----------------------------------------------------------------------------
// updateDriveVolumeInfo
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// updateDriveVolumeInfo
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::updateVolumeInfo(
  const castor::messages::NotifyDriveBeforeMountStarted &body) {
  m_assignmentTime = time(0);
  m_vid = body.vid();
  m_getToBeMountedForTapeStatDriveEntry = true;
  m_mode = body.mode();
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
    m_vdqmJob = job;
    m_state = DRIVE_STATE_WAITFORKTRANSFER;
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
    m_labelJob = job;
    m_labelCmdConnection = labelCmdConnection;
    m_state = DRIVE_STATE_WAITFORKLABEL;
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
// forkedMountSession
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::forkedMountSession(
  const pid_t sessionPid)  {
  std::ostringstream task;
  task << "handle fork of mount session for tape drive " << m_config.unitName;

  switch(m_state) {
  case DRIVE_STATE_WAITFORKTRANSFER:
    m_sessionPid = sessionPid;
    m_sessionType = SESSION_TYPE_DATATRANSFER;
    m_state = DRIVE_STATE_RUNNING;
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
// forkedLabelSession
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueEntry::forkedLabelSession(
  const pid_t sessionPid) {
  std::ostringstream task;
  task << "handle fork of label session for tape drive " << m_config.unitName;
    
  switch(m_state) {
  case DRIVE_STATE_WAITFORKLABEL:
    m_sessionPid = sessionPid;
    m_sessionType = SESSION_TYPE_LABEL;
    m_state = DRIVE_STATE_RUNNING;
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
// sessionSucceeded
//-----------------------------------------------------------------------------
void
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::sessionSucceeded() {
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
        m_sessionPid << ": Incompatible drive state: state=" <<
        drvState2Str(m_state);
      throw ex;
    }
  }
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
        m_sessionPid << ": Incompatible drive state: state=" <<
        drvState2Str(m_state);
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// getTapeStatDriveEntry
//------------------------------------------------------------------------------
castor::legacymsg::TapeStatDriveEntry
  castor::tape::tapeserver::daemon::DriveCatalogueEntry::getTapeStatDriveEntry()
  const {
  legacymsg::TapeStatDriveEntry entry;

  try {
    entry.jid = getUidForTapeStatDriveEntry();
    entry.uid = getJidForTapeStatDriveEntry();
    castor::utils::copyString(entry.dgn, m_config.dgn.c_str());
    entry.up = getUpForTapeStatDriveEntry();
    entry.asn = getAsnForTapeStatDriveEntry();
    entry.asn_time = getAsnTimeForTapeStatDriveEntry();
    castor::utils::copyString(entry.drive, m_config.unitName.c_str());
    entry.mode = getModeForTapeStatDriveEntry();
    castor::utils::copyString(entry.lblcode,
      getLblCodeForTapeStatDriveEntry().c_str());
    entry.tobemounted = getToBeMountedForTapeStatDriveEntry();
    castor::utils::copyString(entry.vid, getVidForTapeStatDriveEntry().c_str());
    castor::utils::copyString(entry.vsn, getVsnForTapeStatDriveEntry().c_str());
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
/*
  switch(m_state) {
    DRIVE_STATE_INIT,
    DRIVE_STATE_DOWN,
    DRIVE_STATE_UP,
    DRIVE_STATE_WAITFORKTRANSFER,
    DRIVE_STATE_WAITFORKLABEL,
    DRIVE_STATE_RUNNING,
    DRIVE_STATE_WAITDOWN
  }
*/
}

//------------------------------------------------------------------------------
// getJidForTapeStatDriveEntry
//------------------------------------------------------------------------------
uint32_t castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  getJidForTapeStatDriveEntry() const throw() {
  switch(m_state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return m_sessionPid;
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
  case DRIVE_STATE_WAITFORKTRANSFER:
  case DRIVE_STATE_WAITFORKLABEL:
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
  case DRIVE_STATE_WAITFORKTRANSFER:
  case DRIVE_STATE_WAITFORKLABEL:
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
  return 0;
}

//------------------------------------------------------------------------------
// getModeForTapeStatDriveEntry
//------------------------------------------------------------------------------
uint16_t castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  getModeForTapeStatDriveEntry() const throw() {
  switch(m_state) {
  case DRIVE_STATE_WAITFORKTRANSFER:
  case DRIVE_STATE_WAITFORKLABEL:
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    switch(m_mode) {
    case castor::messages::TAPE_MODE_READ:
      return WRITE_DISABLE;
    case castor::messages::TAPE_MODE_READWRITE:
      return WRITE_ENABLE;
    case castor::messages::TAPE_MODE_DUMP:
      return WRITE_DISABLE;
      case castor::messages::TAPE_MODE_NONE:
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
  return m_getToBeMountedForTapeStatDriveEntry;
}

//------------------------------------------------------------------------------
// getVidForTapeStatDriveEntry
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  getVidForTapeStatDriveEntry() const throw() {
  return "";
}

//------------------------------------------------------------------------------
// getVsnForTapeStatDriveEntry
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::DriveCatalogueEntry::
  getVsnForTapeStatDriveEntry() const throw() {
  return getVidForTapeStatDriveEntry();
}
