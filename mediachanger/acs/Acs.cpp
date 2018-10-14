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

#include "Acs.hpp"

#include <iomanip>
#include <sstream>
#include <stdint.h>
#include <string.h>
#include <vector>

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::Acs::~Acs() {
}

//------------------------------------------------------------------------------
// alpd2DriveId
//------------------------------------------------------------------------------
DRIVEID cta::mediachanger::acs::Acs::alpd2DriveId(const uint32_t acs,
  const uint32_t lsm, const uint32_t panel, const uint32_t drive) {
  
  DRIVEID driveId;
  driveId.panel_id.lsm_id.acs = (ACS)acs;
  driveId.panel_id.lsm_id.lsm = (LSM)lsm;
  driveId.panel_id.panel = (PANEL)panel;
  driveId.drive = (DRIVE)drive;

  return driveId;
}

//------------------------------------------------------------------------------
// str2Volid
//------------------------------------------------------------------------------
VOLID cta::mediachanger::acs::Acs::str2Volid(const std::string &str) {
  if(EXTERNAL_LABEL_SIZE < str.length()) {
    cta::exception::InvalidArgument ex;
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
// driveId2Str
//------------------------------------------------------------------------------
std::string cta::mediachanger::acs::Acs::driveId2Str(const DRIVEID &driveId) {
  std::ostringstream oss;
  oss << std::setfill('0') <<
    std::setw(3) << (int32_t)driveId.panel_id.lsm_id.acs << ":" <<
    std::setw(3) << (int32_t)driveId.panel_id.lsm_id.lsm << ":" <<
    std::setw(3) << (int32_t)driveId.panel_id.panel << ":" <<
    std::setw(3) << (int32_t)driveId.drive;
  return oss.str();
}
