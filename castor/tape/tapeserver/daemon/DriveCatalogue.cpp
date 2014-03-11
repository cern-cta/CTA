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
#include "castor/utils/utils.hpp"

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
// populateCatalogue
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::populateCatalogue(
  const utils::TpconfigLines &lines) throw(castor::exception::Exception) {

  // enter each TPCONFIG line into the catalogue
  for(utils::TpconfigLines::const_iterator itor = lines.begin();
    itor != lines.end(); itor++) {
    enterTpconfigLine(*itor);
  }
}

//-----------------------------------------------------------------------------
// enterTpconfigLine
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::enterTpconfigLine(
  const utils::TpconfigLine &line) throw(castor::exception::Exception) {

  DriveMap::iterator itor = m_drives.find(line.unitName);

  // If the drive is not in the catalogue
  if(m_drives.end() == itor) {
    // Insert it
    DriveEntry entry;
    entry.dgn = line.dgn;
    entry.devFilename = line.devFilename;
    entry.densities.push_back(line.density);
    entry.state = str2InitialState(line.initialState);
    entry.positionInLibrary = line.positionInLibrary;
    entry.devType = line.devType;
    m_drives[line.unitName] = entry;
  // Else the drive is already in the catalogue
  } else {
    checkTpconfigLine(itor->second, line);

    // Each TPCONFIG line for a given drive specifies a new tape density
    //
    // Add the new density to the list of supported densities already stored
    // within the tape-drive catalogue
    itor->second.densities.push_back(line.density);
  }
}

//-----------------------------------------------------------------------------
// str2InitialState
//-----------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogue::DriveState
  castor::tape::tapeserver::daemon::DriveCatalogue::str2InitialState(
  const std::string &initialState) const throw(castor::exception::Exception) {
  std::string upperCaseInitialState = initialState;
  castor::utils::toUpper(upperCaseInitialState);

  if(upperCaseInitialState == "UP") {
    return DRIVE_STATE_UP;
  } else if(upperCaseInitialState == "DOWN") {
    return DRIVE_STATE_DOWN;
  } else {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to convert initial tape-drive state:"
      " Invalid string value: value=" << initialState;
    throw ex;
  }

  return DRIVE_STATE_INIT;
}

//-----------------------------------------------------------------------------
// checkTpconfigLine
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::checkTpconfigLine(
  const DriveEntry &catalogueEntry, const utils::TpconfigLine &line)
  throw(castor::exception::Exception) {
  checkTpconfigLineDgn(catalogueEntry.dgn, line);
  checkTpconfigLineDevFilename(catalogueEntry.devFilename, line);
  checkTpconfigLineDensity(catalogueEntry.densities, line);
  checkTpconfigLineInitialState(catalogueEntry.state, line);
  checkTpconfigLinePositionInLibrary(catalogueEntry.positionInLibrary, line);
  checkTpconfigLineDevType(catalogueEntry.devType, line);
}

//-----------------------------------------------------------------------------
// checkTpconfigLineDgn
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::checkTpconfigLineDgn(
  const std::string &catalogueDgn, const utils::TpconfigLine &line)
  throw(castor::exception::Exception) {
  if(catalogueDgn != line.dgn) {
    castor::exception::Internal ex;
    ex.getMessage() << "Invalid TPCONFIG line"
      ": A tape drive can only be asscoiated with one DGN"
      ": catalogueDgn=" << catalogueDgn << " lineDgn=" << line.dgn;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// checkTpconfigLineDevFilename
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::
  checkTpconfigLineDevFilename(const std::string &catalogueDevFilename,
  const utils::TpconfigLine &line) throw(castor::exception::Exception) {
  if(catalogueDevFilename != line.devFilename) {
    castor::exception::Internal ex;
    ex.getMessage() << "Invalid TPCONFIG line"
      ": A tape drive can only have one device filename"
      ": catalogueDevFilename=" << catalogueDevFilename <<
      " lineDevFilename=" << line.devFilename;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// checkTpconfigLineDensity
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::checkTpconfigLineDensity(
  const std::list<std::string> &catalogueDensities,
  const utils::TpconfigLine &line) throw(castor::exception::Exception) {
  for(std::list<std::string>::const_iterator itor = catalogueDensities.begin();
    itor != catalogueDensities.end(); itor++) {
    if((*itor) == line.density) {
      castor::exception::Internal ex;
      ex.getMessage() << "Invalid TPCONFIG line"
        ": A tape drive can only be associated with a tape density once"
        ": repeatedDensity=" << line.density;
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// checkTpconfigLineInitialState
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::
  checkTpconfigLineInitialState(const DriveState catalogueInitialState,
  const utils::TpconfigLine &line) throw(castor::exception::Exception) {
  if(catalogueInitialState != str2InitialState(line.initialState)) {
    castor::exception::Internal ex;
    ex.getMessage() << "Invalid TPCONFIG line"
      ": A tape drive can only have one initial state"
      ": catalogueInitialState=" << catalogueInitialState <<
      " lineInitialState=" << line.initialState;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// checkTpconfigLinePositionInLibrary
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::
  checkTpconfigLinePositionInLibrary(
  const std::string &cataloguePositionInLibrary,
  const utils::TpconfigLine &line) throw(castor::exception::Exception) {
  if(cataloguePositionInLibrary != line.positionInLibrary) {
    castor::exception::Internal ex;
    ex.getMessage() << "Invalid TPCONFIG line"
      ": A tape drive can only have one position within its library"
      ": cataloguePositionInLibrary=" << cataloguePositionInLibrary <<
      " linePositionInLibrary=" << line.positionInLibrary;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// checkTpconfigLineDevType
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogue::
  checkTpconfigLineDevType(const std::string &catalogueDevType,
  const utils::TpconfigLine &line) throw(castor::exception::Exception) {
  if(catalogueDevType != line.devType) {
    castor::exception::Internal ex;
    ex.getMessage() << "Invalid TPCONFIG line"
      ": A tape drive can only have one device type"
      ": catalogueDevType=" << catalogueDevType <<
      " lineDevType=" << line.devType;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// getDgn
//-----------------------------------------------------------------------------
const std::string &castor::tape::tapeserver::daemon::DriveCatalogue::getDgn(
  const std::string &unitName) const throw(castor::exception::Exception) {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to get DGN of tape drive " <<
      unitName << ": Tape drive not in catalogue";
    throw ex;
  }

  return itor->second.dgn;
}

//-----------------------------------------------------------------------------
// getDevFilename
//-----------------------------------------------------------------------------
const std::string
  &castor::tape::tapeserver::daemon::DriveCatalogue::getDevFilename(
    const std::string &unitName) const throw(castor::exception::Exception) {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to get the device filename of tape drive " <<
      unitName << ": Tape drive not in catalogue";
    throw ex;
  }

  return itor->second.devFilename;
}

//-----------------------------------------------------------------------------
// getDensities
//-----------------------------------------------------------------------------
const std::list<std::string>
  &castor::tape::tapeserver::daemon::DriveCatalogue::getDensities(
    const std::string &unitName) const throw(castor::exception::Exception) {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to get the supported taep densities of tape"
      " drive " << unitName << ": Tape drive not in catalogue";
    throw ex;
  }

  return itor->second.densities;
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
// getPositionInLibrary
//-----------------------------------------------------------------------------
const std::string &
  castor::tape::tapeserver::daemon::DriveCatalogue::getPositionInLibrary(
    const std::string &unitName) const throw(castor::exception::Exception) {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to get position of tape drive " << unitName <<
      " in library: Tape drive not in catalogue";
    throw ex;
  }

  return itor->second.positionInLibrary;
}

//-----------------------------------------------------------------------------
// getDevType
//-----------------------------------------------------------------------------
const std::string &
  castor::tape::tapeserver::daemon::DriveCatalogue::getDevType(
    const std::string &unitName) const throw(castor::exception::Exception) {
  DriveMap::const_iterator itor = m_drives.find(unitName);
  if(m_drives.end() == itor) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to get devide type of tape drive " << unitName <<
      ": Tape drive not in catalogue";
    throw ex;
  }

  return itor->second.devType;
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
