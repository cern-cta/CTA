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

#include "castor/tape/tapeserver/daemon/CatalogueDriveState.hpp"

//-----------------------------------------------------------------------------
// catalogueDriveStateToStr
//-----------------------------------------------------------------------------
const char *castor::tape::tapeserver::daemon::catalogueDriveStateToStr(
  const CatalogueDriveState state) throw() {
  switch(state) {
  case DRIVE_STATE_INIT               : return "INIT";
  case DRIVE_STATE_DOWN               : return "DOWN";
  case DRIVE_STATE_UP                 : return "UP";
  case DRIVE_STATE_RUNNING            : return "RUNNING";
  case DRIVE_STATE_WAITDOWN           : return "WAITDOWN";
  case DRIVE_STATE_WAITSHUTDOWNKILL   : return "WAITSHUTDOWNKILL";
  case DRIVE_STATE_WAITSHUTDOWNCLEANER: return "WAITSHUTDOWNCLEANER";
  case DRIVE_STATE_SHUTDOWN           : return "SHUTDOWN";
  default                             : return "UNKNOWN";
  }
}
