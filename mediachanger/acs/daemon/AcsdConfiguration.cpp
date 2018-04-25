/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "AcsdConfiguration.hpp"
#include "common/ConfigurationFile.hpp"
#include "Tpconfig.hpp"
#include <time.h>


//------------------------------------------------------------------------------
// GlobalConfiguration::createFromCtaConf w path
//------------------------------------------------------------------------------
cta::mediachanger::acs::daemon::AcsdConfiguration cta::mediachanger::acs::daemon::AcsdConfiguration::createFromCtaConf(
  const std::string& generalConfigPath, cta::log::Logger& log) {
  AcsdConfiguration ret;
  // Parse config file
  ConfigurationFile cf(generalConfigPath);
  // Extract configuration from parsed config file TpConfig
  ret.port.setFromConfigurationFile(cf, generalConfigPath);
  ret.QueryInterval.setFromConfigurationFile(cf, generalConfigPath);
  ret.CmdTimeout.setFromConfigurationFile(cf, generalConfigPath);
  ret.daemonUserName.setFromConfigurationFile(cf, generalConfigPath);
  ret.daemonGroupName.setFromConfigurationFile(cf, generalConfigPath);

  // If we get here, the configuration file is good enough to be logged.
  ret.port.log(log);
  ret.QueryInterval.log(log);
  ret.CmdTimeout.log(log);
  ret.daemonUserName.log(log);
  ret.daemonGroupName.log(log);
  
  return ret;
}

//------------------------------------------------------------------------------
// GlobalConfiguration::gDummyLogger (static member)
//------------------------------------------------------------------------------
cta::log::DummyLogger cta::mediachanger::acs::daemon::AcsdConfiguration::gDummyLogger("");

