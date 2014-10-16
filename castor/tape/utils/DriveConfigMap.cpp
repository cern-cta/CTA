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
  try {
    // Try to find the drive within the map
    DriveConfigMap::iterator itor = find(line.unitName);

    // Enforce one TPCONFIG line per drive
    if(end() != itor) {
      castor::exception::Exception ex;
      ex.getMessage() << "Invalid TPCONFIG line"
        ": There should only be one TPCONFIG line per tape drive: unitName=" <<
        line.unitName;
      throw ex;
    }

    // Insert the drive
    {
      DriveConfig d;
      d.unitName = line.unitName;
      d.dgn = line.dgn;
      d.devFilename = line.devFilename;
      d.librarySlot = line.librarySlot;
      (*this)[line.unitName] = d;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to enter TPCONFIG line into drive map: " <<
      ne.getMessage().str();
    throw ex;
  }
}
