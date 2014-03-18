/******************************************************************************
 *                 castor/tape/tapeserver/TapeDaemonMain.cpp
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

#include "castor/common/CastorConfiguration.hpp"
#include "castor/log/LoggerImplementation.hpp"
#include "castor/io/PollReactorImpl.hpp"
#include "castor/tape/tapeserver/daemon/TapeDaemon.hpp"
#include "castor/tape/tapeserver/daemon/VdqmImpl.hpp"

#include "h/vdqm_constants.h"

#include <string>

//------------------------------------------------------------------------------
// getVdqmHostName
//
// Tries to get the name of the host on which the vdqmd daemon is running by
// parsing /etc/castor/castor.conf.
//
// This function logs the host name if it is successful, else it logs an
// error message and returns an empty string.
//------------------------------------------------------------------------------
static std::string getVdqmHostName(castor::log::Logger &log) throw();

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char **const argv) {
  using namespace castor::tape::tapeserver::daemon;
  castor::log::LoggerImplementation log("tapeserverd");
  const std::string vdqmHostName = getVdqmHostName(log);
  if(vdqmHostName.empty()) {
    log(LOG_ERR, "Aborting: Cannot continue without vdqm host-name");
    return 1;
  }
  const int newTimeout = 1;
  VdqmImpl vdqm(vdqmHostName, VDQM_PORT, newTimeout);
  castor::io::PollReactorImpl reactor(log);
  TapeDaemon daemon(std::cout, std::cerr, log, vdqm, reactor);

  return daemon.main(argc, argv);
}

//------------------------------------------------------------------------------
// getVdqmHostName
//------------------------------------------------------------------------------
static std::string getVdqmHostName(castor::log::Logger &log) throw() {
  using namespace castor;

  common::CastorConfiguration config;
  std::string vdqmHostName;

  try {
    config = common::CastorConfiguration::getConfig();
  } catch(castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("Message", ex.getMessage().str())};
    log(LOG_ERR, "Failed to get vdqm host-name"
      ": Failed to get castor configuration", params);
  }

  try {
    vdqmHostName = config.getConfEnt("VDQM", "HOST");
  } catch(castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("Message", ex.getMessage().str())};
    log(LOG_ERR, "Failed to get vdqm host-name"
      ": Failed to get castor configuration entry VDQM:HOST", params);
  }

  if(!vdqmHostName.empty()) {
    log::Param params[] = {log::Param("vdqmHostName", vdqmHostName)};
    log(LOG_INFO, "Got vdqm host-name", params);
  } else {
    log(LOG_INFO, "Failed to get vdqm host-name");
  }

  return vdqmHostName;
}
