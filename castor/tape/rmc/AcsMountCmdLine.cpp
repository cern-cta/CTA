/******************************************************************************
 *                 castor/tape/rmc/MountAcsCmdLine.hpp
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

#include "castor/tape/rmc/MountAcsCmdLine.hpp"

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::rmc::MountAcsCmdLine::MountAcsCmdLine() throw():
  debug(false),
  help(false),
  queryInterval(0),
  readOnly(FALSE),
  timeout(0) {
  driveId.panel_id.lsm_id.acs = (ACS)0;
  driveId.panel_id.lsm_id.lsm = (LSM)0;
  driveId.panel_id.panel = (PANEL)0;
  driveId.drive = (DRIVE)0;
  memset(volId.external_label, '\0', sizeof(volId.external_label));
}
