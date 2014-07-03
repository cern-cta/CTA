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
#include "castor/tape/utils/DriveConfigMap.hpp"

//------------------------------------------------------------------------------
// enterTpconfigLines
//------------------------------------------------------------------------------
void castor::tape::utils::DriveConfigMap::enterTpconfigLines(
  const TpconfigLines &lines) {
  for(TpconfigLines::const_iterator itor = lines.begin();
    itor != lines.end(); itor++) {
    enterTpconfigLine(*itor);
  }
}

//------------------------------------------------------------------------------
// enterTpconfigLine
//------------------------------------------------------------------------------
void castor::tape::utils::DriveConfigMap::enterTpconfigLine(
  const TpconfigLine &line) {
  // Try to find the drive within the map
  DriveConfigMap::iterator itor = find(line.unitName);

  // If the drive is not in the map
  if(end() == itor) {
    // Insert it
    DriveConfig d;
    d.unitName = line.unitName;
    d.dgn = line.dgn;
    d.devFilename = line.devFilename;
    d.densities.push_back(line.density);
    d.librarySlot = line.librarySlot;
    d.devType = line.devType;
    (*this)[line.unitName] = d;

  // Else the drive is already in the catalogue and therefore the TPCONFIG line
  // is simply appending a new supported density
  } else {
    DriveConfig &drive = itor->second;

    // Check that the TPCONFIG line does not conflict with the current
    // configuration of the tape drive
    checkTpconfigLine(drive, line);

    // Add the new density to the list of supported densities
    drive.densities.push_back(line.density);
  }
}

//-----------------------------------------------------------------------------
// checkTpconfigLine
//-----------------------------------------------------------------------------
void castor::tape::utils::DriveConfigMap::checkTpconfigLine(
  const DriveConfig &drive, const TpconfigLine &line) {
  checkTpconfigLineDgn(drive, line);
  checkTpconfigLineDevFilename(drive, line);
  checkTpconfigLineDensity(drive, line);
  checkTpconfigLineLibrarySlot(drive, line);
  checkTpconfigLineDevType(drive, line);
}

//-----------------------------------------------------------------------------
// checkTpconfigLineDgn
//-----------------------------------------------------------------------------
void castor::tape::utils::DriveConfigMap::checkTpconfigLineDgn(
  const DriveConfig &drive, const TpconfigLine &line) {
  if(drive.dgn != line.dgn) {
    castor::exception::Exception ex;
    ex.getMessage() << "Invalid TPCONFIG line"
      ": A tape drive can only be asscoiated with one DGN"
      ": first=" << drive.dgn << " second=" << line.dgn;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// checkTpconfigLineDevFilename
//-----------------------------------------------------------------------------
void castor::tape::utils::DriveConfigMap::checkTpconfigLineDevFilename(
  const DriveConfig &drive, const TpconfigLine &line) {
  if(drive.devFilename != line.devFilename) {
    castor::exception::Exception ex;
    ex.getMessage() << "Invalid TPCONFIG line"
      ": A tape drive can only have one device filename"
      ": first=" << drive.devFilename <<
      " second=" << line.devFilename;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// checkTpconfigLineDensity
//-----------------------------------------------------------------------------
void castor::tape::utils::DriveConfigMap::checkTpconfigLineDensity(
  const DriveConfig &drive, const TpconfigLine &line) {
  const std::list<std::string> &densities = drive.densities;

  for(std::list<std::string>::const_iterator itor = densities.begin();
    itor != densities.end(); itor++) {
    if((*itor) == line.density) {
      castor::exception::Exception ex;
      ex.getMessage() << "Invalid TPCONFIG line"
        ": A tape drive can only be associated with a specific tape density"
        " once: repeatedDensity=" << line.density;
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// checkTpconfigLineLibrarySlot
//-----------------------------------------------------------------------------
void castor::tape::utils::DriveConfigMap::checkTpconfigLineLibrarySlot(
  const DriveConfig &drive, const  TpconfigLine &line) {
  if(drive.librarySlot != line.librarySlot) {
    castor::exception::Exception ex;
    ex.getMessage() << "Invalid TPCONFIG line"
      ": A tape drive can only be in one slot within its library"
      ": first=" << drive.librarySlot <<
      " second=" << line.librarySlot;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// checkTpconfigLineDevType
//-----------------------------------------------------------------------------
void castor::tape::utils::DriveConfigMap::checkTpconfigLineDevType(
  const DriveConfig &drive, const TpconfigLine &line) {
  if(drive.devType != line.devType) {
    castor::exception::Exception ex;
    ex.getMessage() << "Invalid TPCONFIG line"
      ": A tape drive can only have one device type"
      ": first=" << drive.devType <<
      " second=" << line.devType;
    throw ex;
  }
}
