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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/exception/Exception.hpp"
#include "castor/mediachanger/ScsiLibrarySlot.hpp"
#include "castor/utils/utils.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::mediachanger::ScsiLibrarySlot::ScsiLibrarySlot()
  throw(): m_drvOrd(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::mediachanger::ScsiLibrarySlot::ScsiLibrarySlot(
  const std::string &str): m_str(str) {
  if(str.find("smc@")) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to construct ScsiLibrarySlot"
      ": Library slot must start with smc@: str=" << str;
    throw ex;
  }

  const std::string::size_type indexOfComma = str.find(',');
  if(std::string::npos == indexOfComma) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to construct ScsiLibrarySlot"
      ": Failed to find comma: Library slot must start with smc@host,"
      ": str=" << str;
    throw ex;
  }

  const std::string::size_type indexOfAtSign = 3;  // smc@
  const std::string::size_type hostLen = indexOfComma - indexOfAtSign - 1;
  if(0 == hostLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to construct ScsiLibrarySlot"
      ": Missing rmc host-name: str=" << str;
    throw ex;
  }

  m_rmcHostName = str.substr(indexOfAtSign + 1, hostLen);

  const std::string::size_type drvOrdLen = str.length() - indexOfComma;
  if(0 == drvOrdLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to construct ScsiLibrarySlot"
      ": Missing str ordinal: str=" << str;
    throw ex;
  }

  const std::string drvOrdStr = str.substr(indexOfComma + 1, drvOrdLen);
  if(!castor::utils::isValidUInt(drvOrdStr.c_str())) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to construct ScsiLibrarySlot"
      ": Drive ordinal is not a valid unsigned integer: str=" << str;
    throw ex;
  }

  m_drvOrd = atoi(drvOrdStr.c_str());
}

//------------------------------------------------------------------------------
// str
//------------------------------------------------------------------------------
const std::string &castor::mediachanger::ScsiLibrarySlot::str() const throw() {
  return m_str;
}

//------------------------------------------------------------------------------
// getRmcHostName
//------------------------------------------------------------------------------
const std::string &castor::mediachanger::ScsiLibrarySlot::getRmcHostName() const
  throw() {
  return m_rmcHostName;
}

//------------------------------------------------------------------------------
// getDrvOrd
//------------------------------------------------------------------------------
uint16_t castor::mediachanger::ScsiLibrarySlot::getDrvOrd() const throw() {
  return m_drvOrd;
}
