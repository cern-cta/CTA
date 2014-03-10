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

#include "castor/exception/Internal.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogue.hpp"

#include <string.h>

//-----------------------------------------------------------------------------
// enterDrive
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::enterDrive(
  const std::string &unitName, const std::string &dgn,
  const DriveState initialState) throw(castor::exception::Exception) {
  DriveEntry entry;

  entry.state = initialState;
  entry.dgn = dgn;

  std::pair<DriveMap::iterator, bool> insertResult =
    m_drives.insert(DriveMap::value_type(unitName, entry));
  if(!insertResult.second) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to enter tape drive " << unitName <<
      " into drive catalogue: Tape drive already entered";
    throw ex;
  }

  if(DRIVE_STATE_UP != initialState && DRIVE_STATE_DOWN != initialState) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to enter tape drive " << unitName <<
      " into drive catalogue: Initial state is neither up nor down";
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// getState
//-----------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogue::DriveState
  castor::tape::tapeserver::daemon::DriveCatalogue::getState(
  const std::string &unitName) const throw(castor::exception::Exception) {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to get current state of tape drive " <<
      unitName << ": Tape drive not in catalogue";
    throw ex;
  }

  return itor->second.state;
}

//-----------------------------------------------------------------------------
// configureUp
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::configureUp(
  const std::string &unitName) throw(castor::exception::Exception) {
  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to configure tape drive " <<
      unitName << " to be in the up state: Tape drive not in catalogue";
    throw ex;
  }

  if(DRIVE_STATE_DOWN != itor->second.state) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to configure tape drive " <<
      unitName << " to be in the up state"
      ": Current state of tape drive is not down";
    throw ex;
  }

  itor->second.state = DRIVE_STATE_UP;
}

//-----------------------------------------------------------------------------
// configureDown
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::configureDown(
  const std::string &unitName) throw(castor::exception::Exception) {
  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to configure tape drive " <<
      unitName << " to be in the down state: Tape drive not in catalogue";
    throw ex;
  }

  if(DRIVE_STATE_UP != itor->second.state) {
    castor::exception::Internal ex;  
    ex.getMessage() << "Failed to configure tape drive " <<
      unitName << " to be in the down state"
      ": Current state of tape drive is not up";
    throw ex;
  } 

  itor->second.state = DRIVE_STATE_DOWN;
}

//-----------------------------------------------------------------------------
// tapeSessionStarted
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::tapeSessionStarted(
  const std::string &unitName, const legacymsg::RtcpJobRqstMsgBody &job)
  throw(castor::exception::Exception) {
  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to record start of tape session for tape drive "
      << unitName << ": Tape drive not in catalogue";
    throw ex;
  }

  if(DRIVE_STATE_UP != itor->second.state) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to record start of tape session for tape drive "
      << unitName << ": Current state of tape drive is not up";
    throw ex;
  }

  if(std::string(job.dgn) != itor->second.dgn) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to record start of tape session for tape drive "
      << unitName << ": DGN mistatch: catalogueDgn=" << itor->second.dgn <<
      " vdqmJobDgn=" << job.dgn;
    throw ex;
  }

  itor->second.state = DRIVE_STATE_RUNNING;
  itor->second.job = job;
}

//-----------------------------------------------------------------------------
// getJob
//-----------------------------------------------------------------------------
const castor::tape::legacymsg::RtcpJobRqstMsgBody
  &castor::tape::tapeserver::daemon::DriveCatalogue::getJob(
    const std::string &unitName) const throw(castor::exception::Exception) {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to get vdqm job for tape drive " <<
      unitName << ": Tape drive not in catalogue";
    throw ex;
  }

  if(DRIVE_STATE_RUNNING != itor->second.state) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to get vdqm job for tape drive " <<
      unitName << ": Current state of tape drive is not running";
    throw ex;
  }

  return itor->second.job;
}

//-----------------------------------------------------------------------------
// tapeSessionSuceeeded
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::tapeSessionSuceeeded(
  const std::string &unitName) throw(castor::exception::Exception) {
  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to record tape session succeeded for tape drive "
      << unitName << ": Tape drive not in catalogue";
    throw ex;
  }

  if(DRIVE_STATE_RUNNING != itor->second.state) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to record tape session succeeded for tape drive "
      << unitName << ": Current state of tape drive is not running";
    throw ex;
  }

  itor->second.state = DRIVE_STATE_UP;
}

//-----------------------------------------------------------------------------
// tapeSessionFailed
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::tapeSessionFailed(
  const std::string &unitName) throw(castor::exception::Exception) {
  DriveMap::iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to record tape session failed for tape drive "
      << unitName << ": Tape drive not in catalogue";
    throw ex;
  }

  if(DRIVE_STATE_RUNNING != itor->second.state) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to record tape session failed for tape drive "
      << unitName << ": Current state of tape drive is not running";
    throw ex;
  }

  itor->second.state = DRIVE_STATE_DOWN;
}
