/******************************************************************************
 *                 castor/tape/mediachanger/Acs.cpp
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

#include "castor/tape/mediachanger/Acs.hpp"
#include "castor/tape/utils/utils.hpp"

#include <iomanip>
#include <sstream>
#include <stdint.h>

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::mediachanger::Acs::~Acs() throw() {
}

//------------------------------------------------------------------------------
// str2DriveId
//------------------------------------------------------------------------------
DRIVEID castor::tape::mediachanger::Acs::str2DriveId(const std::string &str)
  const throw(castor::exception::InvalidArgument) {
  std::vector<std::string> components;
  utils::splitString(str, ':', components);

  // The drive ID should consist of 4 components: ACS, LSM, Panel and Transport
  if(4 != components.size()) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Invalid number of components in drive ID"
      ": expected=4, actual=" << components.size();
    throw(ex);
  }

  const std::string &acsStr = components[0];
  const std::string &lsmStr = components[1];
  const std::string &panStr = components[2];
  const std::string &drvStr = components[3];

  // Each of the 4 components must be between 1 and than 3 characters long
  if(1 > acsStr.length() ||  3 < acsStr.length()) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Invalid ACS string length"
      ": expected=1..3, actual=" << acsStr.length();
    throw(ex);
  }
  if(1 > lsmStr.length() || 3 < lsmStr.length()) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Invalid LSM string length"
      ": expected=1..3, actual=" << lsmStr.length();
    throw(ex);
  }
  if(1 > panStr.length() || 3 < panStr.length()) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Invalid panel string length"
      ": expected=1..3, actual=" << panStr.length();
    throw(ex);
  }
  if(1 > drvStr.length() || 3 < drvStr.length()) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Invalid drive string length"
      ": expected=1..3, actual=" << drvStr.length();
    throw(ex);
  }

  // Each of the 4 components must only contain numerals
  if(!onlyContainsNumerals(acsStr)) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "ACS must only contain numerals: value=" << acsStr;
    throw(ex);
  }
  if(!onlyContainsNumerals(lsmStr)) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "LSM must only contain numerals: value=" << acsStr;
    throw(ex);
  }
  if(!onlyContainsNumerals(panStr)) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Panel must only contain numerals: value=" << acsStr;
    throw(ex);
  }
  if(!onlyContainsNumerals(drvStr)) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Drive/Transport must only contain numerals: value=" <<
      acsStr;
    throw(ex);
  }

  DRIVEID driveId;
  driveId.panel_id.lsm_id.acs = (ACS)atoi(acsStr.c_str());
  driveId.panel_id.lsm_id.lsm = (LSM)atoi(lsmStr.c_str());
  driveId.panel_id.panel = (PANEL)atoi(panStr.c_str());
  driveId.drive = (DRIVE)atoi(drvStr.c_str());

  return driveId;
}

//------------------------------------------------------------------------------
// str2Volid
//------------------------------------------------------------------------------
VOLID castor::tape::mediachanger::Acs::str2Volid(const std::string &str) const
  throw(castor::exception::InvalidArgument) {
  if(EXTERNAL_LABEL_SIZE < str.length()) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Failed to convert string to volume identifier"
      ": String is longer than the " << EXTERNAL_LABEL_SIZE <<
      " character maximum";
    throw ex;
  }

  VOLID v;
  strncpy(v.external_label, str.c_str(), sizeof(v.external_label));
  v.external_label[sizeof(v.external_label) - 1] = '\0';
  return v;
}

//------------------------------------------------------------------------------
// onlyContainsNumerals
//------------------------------------------------------------------------------
bool castor::tape::mediachanger::Acs::onlyContainsNumerals(
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
// driveId2Str
//------------------------------------------------------------------------------
std::string castor::tape::mediachanger::Acs::driveId2Str(const DRIVEID &driveId)
  const throw() {
  std::ostringstream oss;
  oss << std::setfill('0') <<
    std::setw(3) << (int32_t)driveId.panel_id.lsm_id.acs << ":" <<
    std::setw(3) << (int32_t)driveId.panel_id.lsm_id.lsm << ":" <<
    std::setw(3) << (int32_t)driveId.panel_id.panel << ":" <<
    std::setw(3) << (int32_t)driveId.drive;
  return oss.str();
}
