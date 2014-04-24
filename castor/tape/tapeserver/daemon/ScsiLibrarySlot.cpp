/******************************************************************************
 *         castor/tape/tapeserver/daemon/ScsiLibrarySlot.cpp
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
#include "castor/tape/tapeserver/daemon/ScsiLibrarySlot.hpp"
#include "castor/utils/utils.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ScsiLibrarySlot::ScsiLibrarySlot()
  throw(): drvOrd(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ScsiLibrarySlot::ScsiLibrarySlot(
  const std::string &str) throw(castor::exception::Exception): drvOrd(0) {
  if(str.find("smc@")) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to construct ScsiLibrarySlot"
      ": SCSI library slot must start with smc@: str=" << str;
    throw ex;
  }

  const std::string::size_type indexOfComma = str.find(',');
  if(std::string::npos == indexOfComma) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to construct ScsiLibrarySlot"
      ": Failed to find comma: SCSI library slot must start with smc@host,"
      ": str=" << str;
    throw ex;
  }

  const std::string::size_type indexOfAtSign = 3;  // smc@
  const std::string::size_type hostLen = indexOfComma - indexOfAtSign - 1;
  if(0 == hostLen) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to construct ScsiLibrarySlot"
      ": Missing rmc host-name: str=" << str;
    throw ex;
  }

  rmcHostName = str.substr(indexOfAtSign + 1, hostLen);

  const std::string::size_type drvOrdLen = str.length() - indexOfComma;
  if(0 == drvOrdLen) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to construct ScsiLibrarySlot"
      ": Missing str ordinal: str=" << str;
    throw ex;
  }

  const std::string drvOrdStr = str.substr(indexOfComma + 1, drvOrdLen);
  if(!castor::utils::isValidUInt(drvOrdStr.c_str())) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to construct ScsiLibrarySlot"
      ": Drive ordinal is not a valid unsigned integer: str=" << str;
    throw ex;
  }

  drvOrd = atoi(drvOrdStr.c_str());
}
