/******************************************************************************
 *         castor/tape/tapeserver/daemon/ScsiLibraryDriveName.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Internal.hpp"
#include "castor/tape/tapeserver/daemon/ScsiLibraryDriveName.hpp"
#include "castor/utils/utils.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ScsiLibraryDriveName::ScsiLibraryDriveName()
  throw(): drvOrd(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ScsiLibraryDriveName::ScsiLibraryDriveName(
  const std::string &drive) throw(castor::exception::Exception): drvOrd(0) {
  if(drive.find("smc@")) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to construct ScsiLibraryDriveName"
      ": Drive name must start with smc@: drive=" << drive;
    throw ex;
  }

  const std::string::size_type indexOfComma = drive.find(',');
  if(std::string::npos == indexOfComma) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to construct ScsiLibraryDriveName"
      ": Failed to find comma: Drive name must start with smc@host,"
      ": drive=" << drive;
    throw ex;
  }

  const std::string::size_type indexOfAtSign = 3;  // smc@
  const std::string::size_type hostLen = indexOfComma - indexOfAtSign - 1;
  if(0 == hostLen) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to construct ScsiLibraryDriveName"
      ": Missing rmc host-name: drive=" << drive;
    throw ex;
  }

  rmcHostName = drive.substr(indexOfAtSign + 1, hostLen);

  const std::string::size_type drvOrdLen = drive.length() - indexOfComma;
  if(0 == drvOrdLen) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to construct ScsiLibraryDriveName"
      ": Missing drive ordinal: drive=" << drive;
    throw ex;
  }

  const std::string drvOrdStr = drive.substr(indexOfComma + 1, drvOrdLen);
  if(!castor::utils::isValidUInt(drvOrdStr.c_str())) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to construct ScsiLibraryDriveName"
      ": Drive ordinal is not a valid unsigned integer: drive=" << drive;
    throw ex;
  }

  drvOrd = atoi(drvOrdStr.c_str());
}
