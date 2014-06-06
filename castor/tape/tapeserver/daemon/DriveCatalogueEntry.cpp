/******************************************************************************
 *         castor/tape/tapeserver/daemon/DriveCatalogueEntry.cpp
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

#include "castor/tape/tapeserver/daemon/DriveCatalogueEntry.hpp"

#include <string.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueEntry::DriveCatalogueEntry()
  throw():
  mode(castor::legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_MODE_NONE),
  event(castor::legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_STATUS_NONE),
  state(DRIVE_STATE_INIT),
  sessionType(SESSION_TYPE_NONE),
  labelCmdConnection(-1) {
  labelJob.gid = 0;
  labelJob.uid = 0;
  memset(labelJob.vid, '\0', sizeof(labelJob.vid));
  vdqmJob.volReqId = 0;
  vdqmJob.clientPort = 0;
  vdqmJob.clientEuid = 0;
  vdqmJob.clientEgid = 0;
  memset(vdqmJob.clientHost, '\0', sizeof(vdqmJob.clientHost));
  memset(vdqmJob.dgn, '\0', sizeof(vdqmJob.dgn));
  memset(vdqmJob.driveUnit, '\0', sizeof(vdqmJob.driveUnit));
  memset(vdqmJob.clientUserName, '\0', sizeof(vdqmJob.clientUserName));
}

//-----------------------------------------------------------------------------
// drvState2Str
//-----------------------------------------------------------------------------
const char
  *castor::tape::tapeserver::daemon::DriveCatalogueEntry::drvState2Str(
  const DriveState state) throw() {
  switch(state) {
  case DRIVE_STATE_INIT     : return "INIT";
  case DRIVE_STATE_DOWN     : return "DOWN";
  case DRIVE_STATE_UP       : return "UP";
  case DRIVE_STATE_WAITFORK : return "WAITFORK";
  case DRIVE_STATE_WAITLABEL: return "WAITLABEL";
  case DRIVE_STATE_RUNNING  : return "RUNNING";
  case DRIVE_STATE_WAITDOWN : return "WAITDOWN";
  default                   : return "UNKNOWN";
  }
}

//-----------------------------------------------------------------------------
// sessionType2Str
//-----------------------------------------------------------------------------
const char
   *castor::tape::tapeserver::daemon::DriveCatalogueEntry::sessionType2Str(
  const SessionType sessionType) throw() {
  switch(sessionType) {
  case SESSION_TYPE_NONE        : return "NONE";
  case SESSION_TYPE_DATATRANSFER: return "DATATRANSFER";
  case SESSION_TYPE_LABEL       : return "LABEL";
  default                       : return "UNKNOWN";
  }
}
