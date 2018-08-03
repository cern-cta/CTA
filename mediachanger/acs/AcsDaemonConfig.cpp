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

#include "AcsDaemonConfig.hpp"
//#include "castor/acs/Constants.hpp"
#include "common/log/Constants.hpp"
#include "castor/server/LoggedCastorConfiguration.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::acs::AcsDaemonConfig::AcsDaemonConfig():
  port(0),
  queryInterval(0),
  cmdTimeout(0) {
}

//------------------------------------------------------------------------------
// createFromCastorConfig
//------------------------------------------------------------------------------
castor::acs::AcsDaemonConfig castor::acs::AcsDaemonConfig::
  createFromCastorConf(log::Logger *const log) {
  server::LoggedCastorConfiguration castorConf(
    common::CastorConfiguration::getConfig());
  AcsDaemonConfig config;

  config.port = castorConf.getConfEntInt("AcsDaemon", "Port",
    (unsigned short)ACS_PORT);

  config.queryInterval = castorConf.getConfEntInt("AcsDaemon", "QueryInterval",
    (time_t)ACS_QUERY_INTERVAL);

  config.cmdTimeout = castorConf.getConfEntInt("AcsDaemon", "CmdTimeout",
    (time_t)ACS_CMD_TIMEOUT);

  return config;
}
