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
#include "castor/tape/tapeserver/Constants.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/tapeserver/daemon/TapeDaemonConfig.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeDaemonConfig::TapeDaemonConfig() throw():
  waitJobTimeoutInSecs(0),
  mountTimeoutInSecs(0),
  blockMoveTimeoutInSec(0) {
}

//------------------------------------------------------------------------------
// createFromCastorConf
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeDaemonConfig
  castor::tape::tapeserver::daemon::TapeDaemonConfig::createFromCastorConf(
    log::Logger *const log) {
  common::CastorConfiguration &castorConf =
    common::CastorConfiguration::getConfig();

  TapeDaemonConfig config;
  
  config.processForkerConfig = ProcessForkerConfig::createFromCastorConf(log);
  config.cupvHost = castorConf.getConfEntString("UPV" , "HOST", log);
  config.vdqmHost = castorConf.getConfEntString("VDQM", "HOST", log);
  config.vmgrHost = castorConf.getConfEntString("VMGR", "HOST", log);
  config.waitJobTimeoutInSecs = castorConf.getConfEntInt("TapeServer",
    "WaitJobTimeout", (time_t)TAPESERVER_WAITJOBTIMEOUT_DEFAULT, log);
  config.mountTimeoutInSecs = castorConf.getConfEntInt("TapeServer",
    "MountTimeout", (time_t)TAPESERVER_MOUNTTIMEOUT_DEFAULT, log);
  config.blockMoveTimeoutInSec = castorConf.getConfEntInt("TapeServer",
    "BlkMoveTimeout", (time_t)TAPESERVER_BLKMOVETIMEOUT_DEFAULT, log);

  return config;
}
