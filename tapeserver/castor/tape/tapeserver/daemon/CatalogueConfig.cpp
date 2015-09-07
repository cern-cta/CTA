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

#include "castor/common/CastorConfiguration.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/tapeserver/daemon/CatalogueConfig.hpp"
#include "castor/tape/tapeserver/TapeBridgeConstants.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueConfig::CatalogueConfig() throw():
  waitJobTimeoutSecs(0),
  mountTimeoutSecs(0),
  blockMoveTimeoutSecs(0),
  vdqmDriveSyncIntervalSecs(0),
  transferSessionTimerSecs(0) {
}

//------------------------------------------------------------------------------
// createFromCastorConf
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueConfig
  castor::tape::tapeserver::daemon::CatalogueConfig::createFromCastorConf(
    log::Logger *const log) {
  common::CastorConfiguration &castorConf =
    common::CastorConfiguration::getConfig();

  CatalogueConfig config;
  config.waitJobTimeoutSecs = castorConf.getConfEntInt("TapeServer",
    "WaitJobTimeout", TAPESERVER_WAITJOBTIMEOUT, log);
  config.mountTimeoutSecs = castorConf.getConfEntInt("TapeServer",
    "MountTimeout", TAPESERVER_MOUNTTIMEOUT, log);
  config.blockMoveTimeoutSecs = castorConf.getConfEntInt("TapeServer",
    "BlkMoveTimeout", TAPESERVER_BLKMOVETIMEOUT, log);
  config.vdqmDriveSyncIntervalSecs = castorConf.getConfEntInt("TapeServer",
    "VdqmDriveSyncInterval", TAPESERVER_VDQMDRIVESYNCINTERVAL, log);
  config.transferSessionTimerSecs = castorConf.getConfEntInt("TapeServer",
    "TransferSessionTimer", TAPESERVER_TRANSFERSESSION_TIMER, log);

  return config;
}
