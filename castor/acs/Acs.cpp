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

#include "castor/acs/Acs.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/utils.hpp"

#include <iomanip>
#include <sstream>
#include <stdint.h>

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::acs::Acs::~Acs() throw() {
}

//------------------------------------------------------------------------------
// alpd2DriveId
//------------------------------------------------------------------------------
DRIVEID castor::acs::Acs::alpd2DriveId(const uint32_t acs,
  const uint32_t lsm, const uint32_t panel, const uint32_t drive)
  const throw () {
  
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
VOLID castor::acs::Acs::str2Volid(const std::string &str) const
   {
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
// driveId2Str
//------------------------------------------------------------------------------
std::string castor::acs::Acs::driveId2Str(const DRIVEID &driveId)
  const throw() {
  std::ostringstream oss;
  oss << std::setfill('0') <<
    std::setw(3) << (int32_t)driveId.panel_id.lsm_id.acs << ":" <<
    std::setw(3) << (int32_t)driveId.panel_id.lsm_id.lsm << ":" <<
    std::setw(3) << (int32_t)driveId.panel_id.panel << ":" <<
    std::setw(3) << (int32_t)driveId.drive;
  return oss.str();
}