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

#include "castor/mediachanger/AcsLibrarySlot.hpp"
#include "castor/utils/utils.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::mediachanger::AcsLibrarySlot::AcsLibrarySlot() throw():
  m_acs(0),
  m_lsm(0),
  m_panel(0),
  m_drive(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::mediachanger::AcsLibrarySlot::AcsLibrarySlot(const std::string &str) {
  const std::string errMsg("Failed to construct AcsLibrarySlot");
  std::vector<std::string> components;
  castor::utils::splitString(str, ',', components);
  if(4 != components.size()) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << errMsg << ": Invalid number of components"
      ": expected=4, actual=" << components.size();
    throw ex;
  }
  
  // check for acs in the beginning 
  const std::string &acsWithACS_NUMBERStr = components[0];
  if(0 != acsWithACS_NUMBERStr.find("acs")) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << errMsg << ": Invalid tape library-slot format"
      ": expected=acsACS_NUMBER, actual=" << acsWithACS_NUMBERStr;
    throw ex;   
  }
  
  const std::string::size_type indexOfACS_NUMBER = 3;  // skip acs
  const std::string &acsStr = acsWithACS_NUMBERStr.substr(indexOfACS_NUMBER);
  const std::string &lsmStr = components[1];
  const std::string &panStr = components[2];
  const std::string &drvStr = components[3];

  // Each of the 4 components must be between 1 and than 3 characters long
  if(1 > acsStr.length() ||  3 < acsStr.length()) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << errMsg << ": Invalid ACS_NUMBER string length"
      ": expected=1..3, actual=" << acsStr.length();
    throw ex;
  }
  if(1 > lsmStr.length() || 3 < lsmStr.length()) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << errMsg << ": Invalid LSM_NUMBER string length"
      ": expected=1..3, actual=" << lsmStr.length();
    throw ex;
  }
  if(1 > panStr.length() || 3 < panStr.length()) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << errMsg << ": Invalid PANEL_NUMBER string length"
      ": expected=1..3, actual=" << panStr.length();
    throw ex;
  }
  if(1 > drvStr.length() || 3 < drvStr.length()) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << errMsg << ": Invalid TRANSPORT_NUMBER string length"
      ": expected=1..3, actual=" << drvStr.length();
    throw ex;
  }

  // Each of the 4 components must only contain numerals
  if(!onlyContainsNumerals(acsStr)) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << errMsg << ": ACS_NUMBER must only contain numerals:"
      " value=" << acsStr;
    throw ex;
  }
  if(!onlyContainsNumerals(lsmStr)) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << errMsg << ": LSM_NUMBER must only contain numerals:"
      " value=" << acsStr;
    throw ex;
  }
  if(!onlyContainsNumerals(panStr)) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << errMsg << ": PANEL_NUMBER must only contain numerals:"
      " value=" << acsStr;
    throw ex;
  }
  if(!onlyContainsNumerals(drvStr)) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << errMsg << ": TRANSPORT__NUMBER must only contain "
      "numerals: value=" << acsStr;
    throw ex;
  }

  m_acs = atoi(acsStr.c_str());
  m_lsm = atoi(lsmStr.c_str());
  m_panel = atoi(panStr.c_str());
  m_drive = atoi(drvStr.c_str());
}

//------------------------------------------------------------------------------
// onlyContainsNumerals
//------------------------------------------------------------------------------
bool castor::mediachanger::AcsLibrarySlot::onlyContainsNumerals(
  const std::string &str) const throw() {
  for(std::string::const_iterator itor = str.begin(); itor != str.end();
    itor++) {
    if(*itor < '0' || *itor  > '9') {
      return false;
    }
  }
  return true;
}

//------------------------------------------------------------------------------
// getAcs
//------------------------------------------------------------------------------
uint32_t castor::mediachanger::AcsLibrarySlot::getAcs() const throw () {
  return m_acs;
}

//------------------------------------------------------------------------------
// getLsm
//------------------------------------------------------------------------------
uint32_t castor::mediachanger::AcsLibrarySlot::getLsm() const throw () {
  return m_lsm;
}

//------------------------------------------------------------------------------
// getPanel
//------------------------------------------------------------------------------
uint32_t castor::mediachanger::AcsLibrarySlot::getPanel() const throw () {
  return m_panel;
}

//------------------------------------------------------------------------------
// getDrive
//------------------------------------------------------------------------------
uint32_t castor::mediachanger::AcsLibrarySlot::getDrive() const throw () {
  return m_drive;
}

//------------------------------------------------------------------------------
// str
//------------------------------------------------------------------------------
std::string castor::mediachanger::AcsLibrarySlot::str() const {
  std::ostringstream oss;

  oss << "acs=" << m_acs << " lsm=" << m_lsm << " panel=" << m_panel <<
    " drive=" << m_drive;
  return oss.str();
}
