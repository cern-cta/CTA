/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "AcsCmdLine.hpp"
#include "common/exception/InvalidArgument.hpp"
#include "common/exception/MissingOperand.hpp"

#include <stdlib.h>
#include <unistd.h>
#include <vector>

//------------------------------------------------------------------------------
// str2DriveId
//------------------------------------------------------------------------------
DRIVEID cta::mediachanger::acs::AcsCmdLine::str2DriveId(const std::string &str) {
  std::vector<std::string> components;

  if(!str.empty()) {
    const char separator = ':';
    std::string::size_type beginIndex = 0;
    std::string::size_type endIndex   = str.find(separator);

    while(endIndex != std::string::npos) {
      components.push_back(str.substr(beginIndex, endIndex - beginIndex));
      beginIndex = ++endIndex;
      endIndex = str.find(separator, endIndex);
    }

    components.push_back(str.substr(beginIndex));
  }

  // The drive ID should consist of 4 components: ACS, LSM, Panel and Transport
  if(4 != components.size()) {
    InvalidDriveId ex;
    ex.getMessage() << "Invalid number of components in drive ID"
      ": expected=4, actual=" << components.size();
    throw ex;
  }

  const std::string &acsStr = components[0];
  const std::string &lsmStr = components[1];
  const std::string &panStr = components[2];
  const std::string &drvStr = components[3];

  // Each of the 4 components must be between 1 and than 3 characters long
  if(1 > acsStr.length() ||  3 < acsStr.length()) {
    InvalidDriveId ex;
    ex.getMessage() << "Invalid ACS string length"
      ": expected=1..3, actual=" << acsStr.length();
    throw ex;
  }
  if(1 > lsmStr.length() || 3 < lsmStr.length()) {
    InvalidDriveId ex;
    ex.getMessage() << "Invalid LSM string length"
      ": expected=1..3, actual=" << lsmStr.length();
    throw ex;
  }
  if(1 > panStr.length() || 3 < panStr.length()) {
    InvalidDriveId ex;
    ex.getMessage() << "Invalid panel string length"
      ": expected=1..3, actual=" << panStr.length();
    throw ex;
  }
  if(1 > drvStr.length() || 3 < drvStr.length()) {
    InvalidDriveId ex;
    ex.getMessage() << "Invalid drive string length"
      ": expected=1..3, actual=" << drvStr.length();
    throw ex;
  }

  // Each of the 4 components must only contain numerals
  if(!onlyContainsNumerals(acsStr)) {
    InvalidDriveId ex;
    ex.getMessage() << "ACS must only contain numerals: value=" << acsStr;
    throw ex;
  }
  if(!onlyContainsNumerals(lsmStr)) {
    InvalidDriveId ex;
    ex.getMessage() << "LSM must only contain numerals: value=" << acsStr;
    throw ex;
  }
  if(!onlyContainsNumerals(panStr)) {
    InvalidDriveId ex;
    ex.getMessage() << "Panel must only contain numerals: value=" << acsStr;
    throw ex;
  }
  if(!onlyContainsNumerals(drvStr)) {
    InvalidDriveId ex;
    ex.getMessage() << "Drive/Transport must only contain numerals: value=" <<
      acsStr;
    throw ex;
  }

  DRIVEID driveId;
  driveId.panel_id.lsm_id.acs = (ACS)atoi(acsStr.c_str());
  driveId.panel_id.lsm_id.lsm = (LSM)atoi(lsmStr.c_str());
  driveId.panel_id.panel = (PANEL)atoi(panStr.c_str());
  driveId.drive = (DRIVE)atoi(drvStr.c_str());

  return driveId;
}

//------------------------------------------------------------------------------
// onlyContainsNumerals
//------------------------------------------------------------------------------
bool cta::mediachanger::acs::AcsCmdLine::onlyContainsNumerals(const std::string &str) {
  for(std::string::const_iterator itor = str.begin(); itor != str.end();
    itor++) {
    if(*itor < '0' || *itor  > '9') {
      return false;
    }
  }
  return true;
}

//------------------------------------------------------------------------------
// parseQueryInterval
//------------------------------------------------------------------------------
int cta::mediachanger::acs::AcsCmdLine::parseQueryInterval(const std::string &s) {
  const int queryInterval = atoi(s.c_str());
  if(0 >= queryInterval) {
    InvalidQueryInterval ex;
    ex.getMessage() << "Query value must be an integer greater than 0"
      ": value=" << queryInterval;
    throw ex;
  }

  return queryInterval;
}

//------------------------------------------------------------------------------
// parseTimeout
//------------------------------------------------------------------------------
int cta::mediachanger::acs::AcsCmdLine::parseTimeout(const std::string &s) {
  const int timeout = atoi(s.c_str());
  if(0 >= timeout) {
    InvalidTimeout ex;
    ex.getMessage() << "Timeout value must be an integer greater than 0"
      ": value=" << timeout;
    throw ex;
  }
  return timeout;
}

//------------------------------------------------------------------------------
// handleMissingParameter
//------------------------------------------------------------------------------
void cta::mediachanger::acs::AcsCmdLine::handleMissingParameter(const int opt) {
  cta::exception::MissingOperand ex;
  ex.getMessage() << "The -" << (char)opt << " option requires a parameter";
 throw ex;
}

//------------------------------------------------------------------------------
// handleUnknownOption
//------------------------------------------------------------------------------
void cta::mediachanger::acs::AcsCmdLine::handleUnknownOption(const int opt) {
  cta::exception::InvalidArgument ex;
  if(0 == optopt) {
    ex.getMessage() << "Unknown command-line option";
  } else {
    ex.getMessage() << "Unknown command-line option: -" << (char)opt;
  }
  throw ex;
}
