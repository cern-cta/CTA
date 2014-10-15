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
#include "castor/tape/tapeserver/daemon/ProcessForkerOneTimeConfig.hpp"
#include "h/rmc_constants.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForkerOneTimeConfig::
  ProcessForkerOneTimeConfig():
  rmcPort(0) {
}

//------------------------------------------------------------------------------
// createFromCastorConfig
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForkerOneTimeConfig
  castor::tape::tapeserver::daemon::ProcessForkerOneTimeConfig::
  createFromCastorConf(log::Logger *const log) {
  common::CastorConfiguration &castorConf =
    common::CastorConfiguration::getConfig();
  ProcessForkerOneTimeConfig config;

  config.rmcPort = castorConf.getConfEntInt("RMC", "PORT",
    (unsigned short)RMC_PORT, log);

  config.dataTransfer = DataTransferConfig::createFromCastorConf(log);

  return config;
}
