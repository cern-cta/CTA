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
#include "castor/tape/tapeserver/daemon/LabelSessionConfig.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::LabelSessionConfig::LabelSessionConfig()
  throw():
  useLbp(false) {
}

//------------------------------------------------------------------------------
// createFromCastorConf
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::LabelSessionConfig
  castor::tape::tapeserver::daemon::LabelSessionConfig::createFromCastorConf(
    log::Logger *const log) {
  common::CastorConfiguration &castorConf =
    common::CastorConfiguration::getConfig();
  LabelSessionConfig config;

  const std::string useLBP = castorConf.getConfEntString(
    "TapeServer", "UseLogicalBlockProtection", "no", log);

  if (!strcasecmp(useLBP.c_str(), "yes") || !strcmp(useLBP.c_str(), "1")) {
    config.useLbp = true;
  } else {
    config.useLbp = false;
  }

  return config;
}
