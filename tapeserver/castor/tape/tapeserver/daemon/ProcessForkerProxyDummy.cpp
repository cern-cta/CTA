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

#include "castor/tape/tapeserver/daemon/ProcessForkerProxyDummy.hpp"

//------------------------------------------------------------------------------
// stopProcessForker
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxyDummy::
  stopProcessForker(const std::string &reason) {
  // This is a dummy method and so intentionally does nothing
}

//------------------------------------------------------------------------------
// forkDataTransfer
//------------------------------------------------------------------------------
pid_t castor::tape::tapeserver::daemon::ProcessForkerProxyDummy::
  forkDataTransfer(const DriveConfig &driveConfig) {
  // This is a dummy method and so intentionally does nothing
  return 0;
}

//------------------------------------------------------------------------------
// forkLabel
//------------------------------------------------------------------------------
pid_t castor::tape::tapeserver::daemon::ProcessForkerProxyDummy::
  forkLabel(const DriveConfig &driveConfig,
  const legacymsg::TapeLabelRqstMsgBody &labelJob) {
  // This is a dummy method and so intentionally does nothing
  return 0;
}

//------------------------------------------------------------------------------
// forkCleaner
//------------------------------------------------------------------------------
pid_t castor::tape::tapeserver::daemon::ProcessForkerProxyDummy::
  forkCleaner(const DriveConfig &driveConfig, const std::string &vid,
  const bool waitMediaInDrive, const uint32_t waitMediaInDriveTimeout) {
  // This is a dummy method and so intentionally does nothing
  return 0;
}
