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

#include "TapedConfiguration.hpp"
#include "ConfigurationFile.hpp"

namespace cta { namespace tape { namespace daemon {
//------------------------------------------------------------------------------
// GlobalConfiguration::createFromCtaConf w/o path
//------------------------------------------------------------------------------
TapedConfiguration TapedConfiguration::createFromCtaConf(cta::log::Logger& log) {
  return createFromCtaConf("/etc/cta/cta.conf", log);
}

//------------------------------------------------------------------------------
// GlobalConfiguration::createFromCtaConf w path
//------------------------------------------------------------------------------
TapedConfiguration TapedConfiguration::createFromCtaConf(
  const std::string& generalConfigPath, cta::log::Logger& log) {
  TapedConfiguration ret;
  // Parse config file
  ConfigurationFile cf(generalConfigPath);
  // Extract configuration from parsed config file
  ret.tpConfigPath.setFromConfigurationFile(cf, generalConfigPath);
  ret.wdIdleSessionTimer.setFromConfigurationFile(cf, generalConfigPath);
  ret.wdMountMaxSecs.setFromConfigurationFile(cf, generalConfigPath);
  ret.wdNoBlockMoveMaxSecs.setFromConfigurationFile(cf, generalConfigPath);
  ret.wdScheduleMaxSecs.setFromConfigurationFile(cf, generalConfigPath);
  ret.objectStoreURL.setFromConfigurationFile(cf, generalConfigPath);
  ret.fileCatalogURL.setFromConfigurationFile(cf, generalConfigPath);
  return ret;
}

//------------------------------------------------------------------------------
// GlobalConfiguration::gDummyLogger (static member)
//------------------------------------------------------------------------------
cta::log::DummyLogger TapedConfiguration::gDummyLogger("");

}}} // namespace cta::tape::daemon
