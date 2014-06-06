/******************************************************************************
 *                castor/tape/tapeserver/daemon/DriveCatalogue.cpp
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

#include "castor/tape/tapeserver/daemon/DriveCatalogue.hpp"
#include "castor/utils/utils.hpp"
#include "castor/legacymsg/TapeUpdateDriveRqstMsgBody.hpp"

#include <string.h>
#include <time.h>

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogue::~DriveCatalogue() throw() {
  // Close any label-command connections that are still owned by the
  // tape-drive catalogue
  for(DriveMap::const_iterator itor = m_drives.begin(); itor != m_drives.end();
     itor++) {
    const DriveState driveState = itor->second.state;
    const SessionType sessionType = itor->second.sessionType;
    const int labelCmdConnection = itor->second.labelCmdConnection;

    if(DRIVE_STATE_WAITLABEL == driveState &&
      SESSION_TYPE_LABEL == sessionType &&
      -1 != labelCmdConnection) {
      close(labelCmdConnection);
    }
  }
}

//-----------------------------------------------------------------------------
// drvState2Str
//-----------------------------------------------------------------------------
const char *castor::tape::tapeserver::daemon::DriveCatalogue::drvState2Str(
  const DriveState state) throw() {
  switch(state) {
  case DRIVE_STATE_INIT     : return "INIT";
  case DRIVE_STATE_DOWN     : return "DOWN";
  case DRIVE_STATE_UP       : return "UP";
  case DRIVE_STATE_WAITFORK : return "WAITFORK";
  case DRIVE_STATE_WAITLABEL: return "WAITLABEL";
  case DRIVE_STATE_RUNNING  : return "RUNNING";
  case DRIVE_STATE_WAITDOWN : return "WAITDOWN";
  default                   : return "UNKNOWN";
  }
}

//-----------------------------------------------------------------------------
// mountSessionType2Str
//-----------------------------------------------------------------------------
const char *castor::tape::tapeserver::daemon::DriveCatalogue::sessionType2Str(
  const SessionType sessionType) throw() {
  switch(sessionType) {
  case SESSION_TYPE_NONE        : return "NONE";
  case SESSION_TYPE_DATATRANSFER: return "DATATRANSFER";
  case SESSION_TYPE_LABEL       : return "LABEL";
  default                       : return "UNKNOWN";
  }
}

//-----------------------------------------------------------------------------
// populateCatalogue
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::populateCatalogue(
  const utils::DriveConfigMap &driveConfigs)  {

  try {
    for(utils::DriveConfigMap::const_iterator itor = driveConfigs.begin();
      itor != driveConfigs.end(); itor++) {
      const std::string &unitName = itor->first;
      const utils::DriveConfig &driveConfig = itor->second;

      // Sanity check
      if(unitName != driveConfig.unitName) {
        // This should never happen
        castor::exception::Exception ex;
        ex.getMessage() << "Unit name mismatch: expected=" << unitName <<
          " actual=" << driveConfig.unitName;
        throw ex;
      }

      enterDriveConfig(driveConfig);
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to populate tape-drive catalogue: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// enterDriveConfig
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::enterDriveConfig(
  const utils::DriveConfig &driveConfig)  {

  DriveMap::iterator itor = m_drives.find(driveConfig.unitName);

  // If the drive is not in the catalogue
  if(m_drives.end() == itor) {
    // Insert it
    DriveEntry entry;
    entry.config = driveConfig;
    entry.state = DRIVE_STATE_DOWN;
    m_drives[driveConfig.unitName] = entry;
  // Else the drive is already in the catalogue
  } else {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to enter tape-drive configuration into tape-drive catalogue"
      ": Duplicate entry: unitName=" << driveConfig.unitName;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// getUnitName
//-----------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::DriveCatalogue::getUnitName(
  const pid_t sessionPid) const  {

  for(DriveMap::const_iterator i = m_drives.begin(); i!=m_drives.end(); i++) {
    if(sessionPid == i->second.sessionPid) return i->first;
  }
  castor::exception::Exception ex;
  ex.getMessage() << "Failed to find the unit name of the tape drive on which "
    << "the session with process ID " << sessionPid << " is running.";
  throw ex;
}

//-----------------------------------------------------------------------------
// getUnitNames
//-----------------------------------------------------------------------------
std::list<std::string> castor::tape::tapeserver::daemon::DriveCatalogue::getUnitNames() const  {
  std::list<std::string> unitNames;

  for(DriveMap::const_iterator itor = m_drives.begin();
    itor != m_drives.end(); itor++) {
    unitNames.push_back(itor->first);
  }

  return unitNames;
}

//-----------------------------------------------------------------------------
// getUnitNames
//-----------------------------------------------------------------------------
std::list<std::string> castor::tape::tapeserver::daemon::DriveCatalogue::getUnitNames(const DriveState state) const  {
  std::list<std::string> unitNames;

  for(DriveMap::const_iterator itor = m_drives.begin();
    itor != m_drives.end(); itor++) {
    if(state == itor->second.state) {
      unitNames.push_back(itor->first);
    }
  }

  return unitNames;
}

//-----------------------------------------------------------------------------
// getDgn
//-----------------------------------------------------------------------------
const std::string &castor::tape::tapeserver::daemon::DriveCatalogue::getDgn(
  const std::string &unitName) const  {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get DGN of tape drive " <<
      unitName << ": Unknown drive";
    throw ex;
  }

  const DriveEntry &drive = itor->second;
  return drive.config.dgn;
}

//-----------------------------------------------------------------------------
// getVid
//-----------------------------------------------------------------------------
const std::string &castor::tape::tapeserver::daemon::DriveCatalogue::getVid(
  const std::string &unitName) const  {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get VID of tape drive " <<
      unitName << ": Unknown drive";
    throw ex;
  }

  const DriveEntry &drive = itor->second;
  return drive.vid;
}

//-----------------------------------------------------------------------------
// getAssignmentTime
//-----------------------------------------------------------------------------
time_t castor::tape::tapeserver::daemon::DriveCatalogue::getAssignmentTime(
  const std::string &unitName) const  {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get the assignment time of tape drive " <<
      unitName << ": Unknown drive";
    throw ex;
  }

  const DriveEntry &drive = itor->second;
  return drive.assignment_time;
}

//-----------------------------------------------------------------------------
// getDevFilename
//-----------------------------------------------------------------------------
const std::string
  &castor::tape::tapeserver::daemon::DriveCatalogue::getDevFilename(
    const std::string &unitName) const  {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get the device filename of tape drive " <<
      unitName << ": Unknown drive";
    throw ex;
  }

  const DriveEntry &drive = itor->second;
  return drive.config.devFilename;
}

//-----------------------------------------------------------------------------
// getDensities
//-----------------------------------------------------------------------------
const std::list<std::string>
  &castor::tape::tapeserver::daemon::DriveCatalogue::getDensities(
    const std::string &unitName) const  {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get the supported taep densities of tape"
      " drive " << unitName << ": Unknown drive";
    throw ex;
  }

  const DriveEntry &drive = itor->second;
  return drive.config.densities;
}

//-----------------------------------------------------------------------------
// getSessionType
//-----------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogue::SessionType
  castor::tape::tapeserver::daemon::DriveCatalogue::getSessionType(
  const pid_t sessionPid) const  {
  std::ostringstream task;
  task << "get the type of the session with pid " << sessionPid;

  std::string unitName;
  try {
    unitName = getUnitName(sessionPid);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": " << ne.getMessage();
    throw ex;
  }

  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": unit name " <<
      unitName << " is not known";
    throw ex;
  }

  const DriveEntry &drive = itor->second;
  return drive.sessionType;
}

//-----------------------------------------------------------------------------
// getState
//-----------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogue::DriveState
  castor::tape::tapeserver::daemon::DriveCatalogue::getState(
  const std::string &unitName) const  {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get current state of tape drive " <<
      unitName << ": Unknown drive";
    throw ex;
  }

  const DriveEntry &drive = itor->second;
  return drive.state;
}

//-----------------------------------------------------------------------------
// getLibrarySlot
//-----------------------------------------------------------------------------
const std::string &
  castor::tape::tapeserver::daemon::DriveCatalogue::getLibrarySlot(
    const std::string &unitName) const  {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get library slot of tape drive " << unitName
      << ": Unknown drive";
    throw ex;
  }

  const DriveEntry &drive = itor->second;
  return drive.config.librarySlot;
}

//-----------------------------------------------------------------------------
// getDevType
//-----------------------------------------------------------------------------
const std::string &
  castor::tape::tapeserver::daemon::DriveCatalogue::getDevType(
    const std::string &unitName) const  {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get device type of tape drive " << unitName <<
      ": Unknown drive";
    throw ex;
  }

  const DriveEntry &drive = itor->second;
  return drive.config.devType;
}

//-----------------------------------------------------------------------------
// getTapeMode
//-----------------------------------------------------------------------------
castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeMode castor::tape::tapeserver::daemon::DriveCatalogue::getTapeMode(
    const std::string &unitName) const  {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get device type of tape drive " << unitName <<
      ": Unknown drive";
    throw ex;
  }

  const DriveEntry &drive = itor->second;
  return drive.mode;
}

//-----------------------------------------------------------------------------
// getTapeEvent
//-----------------------------------------------------------------------------
castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeEvent castor::tape::tapeserver::daemon::DriveCatalogue::getTapeEvent(
    const std::string &unitName) const  {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get tape event of tape drive " << unitName <<
      ": Unknown drive";
    throw ex;
  }

  const DriveEntry &drive = itor->second;
  return drive.event;
}

//-----------------------------------------------------------------------------
// updateVidAssignment
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::updateDriveVolumeInfo(
  const legacymsg::TapeUpdateDriveRqstMsgBody &body)
   {
  std::ostringstream task;
  task << "update the VID of tape drive " << body.drive;

  DriveMap::iterator itor = m_drives.find(body.drive);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": Unknown drive";
    throw ex;
  }
  
  itor->second.vid = body.vid;
  if(castor::legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_STATUS_BEFORE_MOUNT_STARTED == body.event) {    
    itor->second.assignment_time = time(0); // set to "now"
  }
  itor->second.event = (castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeEvent)body.event;
  itor->second.mode = (castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeMode)body.mode;
}

//-----------------------------------------------------------------------------
// releaseLabelCmdConnection
//-----------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::DriveCatalogue::releaseLabelCmdConnection(
  const std::string &unitName)  {
  std::ostringstream task;
  task << "get the file-descriptor of the connection with the label command"
    " associated with tape drive " << unitName;

  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": unit name " <<
      unitName << " is not known";
    throw ex;
  }

  const DriveState driveState = itor->second.state;
  const SessionType sessionType = itor->second.sessionType;
  if(DRIVE_STATE_WAITLABEL != driveState &&
    SESSION_TYPE_LABEL != sessionType) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Invalid drive state and session type: driveState=" <<
      drvState2Str(driveState) << " sessionType=" <<
      sessionType2Str(sessionType);
    throw ex;
  }

  if(-1 == itor->second.labelCmdConnection) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Connection with label command already released";
    throw ex;
  }

  const int labelCmdConnection = itor->second.labelCmdConnection;
  itor->second.labelCmdConnection = -1;
  
  return labelCmdConnection;
}

//-----------------------------------------------------------------------------
// configureUp
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::configureUp(const std::string &unitName)  {
  std::ostringstream task;
  task << "configure tape drive " << unitName << " up";

  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": Unknown drive";
    throw ex;
  }

  switch(itor->second.state) {
  case DRIVE_STATE_UP:
  case DRIVE_STATE_RUNNING:
    break;
  case DRIVE_STATE_DOWN:
    itor->second.state = DRIVE_STATE_UP;
    break;
  case DRIVE_STATE_WAITDOWN:
    itor->second.state = DRIVE_STATE_RUNNING;
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << drvState2Str(itor->second.state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// configureDown
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::configureDown(
  const std::string &unitName)  {
  std::ostringstream task;
  task << "configure tape drive " << unitName << " down";


  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": Unknown drive";
    throw ex;
  }

  DriveEntry &drive = itor->second;

  switch(drive.state) {
  case DRIVE_STATE_DOWN:
    break;
  case DRIVE_STATE_UP:
    drive.state = DRIVE_STATE_DOWN;
    break;
  case DRIVE_STATE_RUNNING:
    drive.state = DRIVE_STATE_WAITDOWN;
    break;
  default:
    {
      castor::exception::Exception ex;  
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << drvState2Str(drive.state);
      throw ex;
    }
  } 
}

//-----------------------------------------------------------------------------
// receivedVdqmJob
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::receivedVdqmJob(const legacymsg::RtcpJobRqstMsgBody &job)  {
  const std::string unitName(job.driveUnit);

  std::ostringstream task;
  task << "handle vdqm job for tape drive " << unitName;

  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": Unknown drive";
    throw ex;
  }

  DriveEntry &drive = itor->second;

  switch(drive.state) {
  case DRIVE_STATE_UP:
    if(std::string(job.dgn) != drive.config.dgn) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": DGN mismatch: catalogueDgn=" << drive.config.dgn << " vdqmJobDgn=" << job.dgn;
      throw ex;
    }
    drive.vdqmJob = job;
    drive.state = DRIVE_STATE_WAITFORK;
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << drvState2Str(drive.state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// receivedLabelJob
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::receivedLabelJob(
  const legacymsg::TapeLabelRqstMsgBody &job, const int labelCmdConnection)
   {
  const std::string unitName(job.drive);

  std::ostringstream task;
  task << "handle label job for tape drive " << unitName;

  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": Unknown drive";
    throw ex;
  }

  DriveEntry &drive = itor->second;

  switch(drive.state) {
  case DRIVE_STATE_UP:
    if(std::string(job.dgn) != drive.config.dgn) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": DGN mismatch: catalogueDgn=" << drive.config.dgn << " labelJobDgn="
        << job.dgn;
      throw ex;
    }
    if(-1 != drive.labelCmdConnection) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Drive catalogue already owns a label-command connection";
      throw ex;
    }
    drive.labelJob = job;
    drive.labelCmdConnection = labelCmdConnection;
    drive.state = DRIVE_STATE_WAITLABEL;
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << drvState2Str(drive.state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// getVdqmJob
//-----------------------------------------------------------------------------
const castor::legacymsg::RtcpJobRqstMsgBody &castor::tape::tapeserver::daemon::DriveCatalogue::getVdqmJob(const std::string &unitName) const  {
  std::ostringstream task;
  task << "get vdqm job for tape drive " << unitName;

  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": Unknown drive";
    throw ex;
  }

  const DriveEntry &drive = itor->second;

  switch(drive.state) {
  case DRIVE_STATE_WAITFORK:
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return drive.vdqmJob;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << drvState2Str(drive.state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// getLabelJob
//-----------------------------------------------------------------------------
const castor::legacymsg::TapeLabelRqstMsgBody &castor::tape::tapeserver::daemon::DriveCatalogue::getLabelJob(const std::string &unitName) const  {
  std::ostringstream task;
  task << "get label job for tape drive " << unitName;

  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": Unknown drive";
    throw ex;
  }

  const DriveEntry &drive = itor->second;

  switch(drive.state) {
  case DRIVE_STATE_WAITLABEL:
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return drive.labelJob;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << drvState2Str(drive.state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// forkedMountSession
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::forkedMountSession(const std::string &unitName, const pid_t sessionPid)  {
  std::ostringstream task;
  task << "handle fork of mount session for tape drive " << unitName;

  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": Unknown drive";
    throw ex;
  }

  DriveEntry &drive = itor->second;

  switch(drive.state) {
  case DRIVE_STATE_WAITFORK:
    drive.sessionPid = sessionPid;
    drive.sessionType = SESSION_TYPE_DATATRANSFER;
    drive.state = DRIVE_STATE_RUNNING;
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << drvState2Str(drive.state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// forkedLabelSession
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::forkedLabelSession(
  const std::string &unitName, const pid_t sessionPid)
   {
  std::ostringstream task;
  task << "handle fork of label session for tape drive " << unitName;

  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": Unknown drive";
    throw ex;
  }

  DriveEntry &drive = itor->second;

  switch(drive.state) {
  case DRIVE_STATE_WAITLABEL:
    drive.sessionPid = sessionPid;
    drive.sessionType = SESSION_TYPE_LABEL;
    drive.state = DRIVE_STATE_RUNNING;
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << drvState2Str(drive.state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// getSessionPid
//-----------------------------------------------------------------------------
pid_t castor::tape::tapeserver::daemon::DriveCatalogue::getSessionPid(
  const std::string &unitName) const  {
  std::ostringstream task;
  task << "get process ID of mount session for tape drive " << unitName;

  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": Unknown drive";
    throw ex;
  }

  const DriveEntry &drive = itor->second;

  switch(drive.state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    return drive.sessionPid;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << drvState2Str(drive.state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// sessionSucceeded
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::sessionSucceeded(const pid_t sessionPid)  {
  std::string unitName;
  try {
    unitName = getUnitName(sessionPid);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to record tape session succeeded for session with pid " <<
      sessionPid << ": " << ne.getMessage();
    throw ex;
  }

  sessionSucceeded(unitName);  
}

//-----------------------------------------------------------------------------
// sessionSucceeded
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::sessionSucceeded(const std::string &unitName)  {
  std::ostringstream task;
  task << "record tape session succeeded for tape drive " << unitName;

  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": Unknown drive";
    throw ex;
  }

  switch(itor->second.state) {
  case DRIVE_STATE_RUNNING:
    itor->second.state = DRIVE_STATE_UP;
    break;
  case DRIVE_STATE_WAITDOWN:
    itor->second.state = DRIVE_STATE_DOWN;
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << drvState2Str(itor->second.state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// sessionFailed
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::sessionFailed(const pid_t sessionPid)  {
  std::string unitName;
  try {
    unitName = getUnitName(sessionPid);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to record tape session failed for session with pid " <<
      sessionPid << ": " << ne.getMessage();
    throw ex;
  }

  sessionFailed(unitName);
}

//-----------------------------------------------------------------------------
// sessionFailed
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::sessionFailed(const std::string &unitName)  {
  std::ostringstream task;
  task << "record tape session failed for tape drive " << unitName;

  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": Unknown drive";
    throw ex;
  }

  switch(itor->second.state) {
  case DRIVE_STATE_RUNNING:
  case DRIVE_STATE_WAITDOWN:
    itor->second.state = DRIVE_STATE_DOWN;
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task.str() <<
        ": Incompatible drive state: state=" << drvState2Str(itor->second.state);
      throw ex;
    }
  }
}
