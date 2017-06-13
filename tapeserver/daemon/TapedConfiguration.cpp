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
#include "Tpconfig.hpp"

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
  // TpConfig
  ret.logMask.setFromConfigurationFile(cf, generalConfigPath);
  ret.tpConfigPath.setFromConfigurationFile(cf, generalConfigPath);
  // Memory management
  ret.bufferSizeBytes.setFromConfigurationFile(cf, generalConfigPath);
  ret.bufferCount.setFromConfigurationFile(cf, generalConfigPath);
  // Batched metadata access and tape write flush parameters
  ret.archiveFetchBytesFiles.setFromConfigurationFile(cf, generalConfigPath);
  ret.archiveFlushBytesFiles.setFromConfigurationFile(cf, generalConfigPath);
  ret.retrieveFetchBytesFiles.setFromConfigurationFile(cf, generalConfigPath);
  // Mount criteria
  ret.mountCriteria.setFromConfigurationFile(cf, generalConfigPath);
  // Disk file access parameters
  ret.nbDiskThreads.setFromConfigurationFile(cf, generalConfigPath);
  ret.useRAO.setFromConfigurationFile(cf, generalConfigPath);
  // Watchdog: parameters for timeouts in various situations.
  ret.wdIdleSessionTimer.setFromConfigurationFile(cf, generalConfigPath);
  ret.wdMountMaxSecs.setFromConfigurationFile(cf, generalConfigPath);
  ret.wdNoBlockMoveMaxSecs.setFromConfigurationFile(cf, generalConfigPath);
  ret.wdScheduleMaxSecs.setFromConfigurationFile(cf, generalConfigPath);
  // The central storage access configuration
  ret.objectStoreURL.setFromConfigurationFile(cf, generalConfigPath);
  ret.fileCatalogConfigFile.setFromConfigurationFile(cf, generalConfigPath);
  // Extract drive list from tpconfig + parsed config file
  ret.driveConfigs = Tpconfig::parseFile(ret.tpConfigPath.value());
  
  // If we get here, the configuration file is good enough to be logged.
  ret.logMask.log(log);
  ret.tpConfigPath.log(log);
  
  ret.bufferSizeBytes.log(log);
  ret.bufferCount.log(log);
  
  ret.archiveFetchBytesFiles.log(log);
  ret.archiveFlushBytesFiles.log(log);
  ret.retrieveFetchBytesFiles.log(log);
  
  ret.mountCriteria.log(log);
  
  ret.nbDiskThreads.log(log);
  ret.useRAO.log(log);
  
  ret.wdIdleSessionTimer.log(log);
  ret.wdMountMaxSecs.log(log);
  ret.wdNoBlockMoveMaxSecs.log(log);
  ret.wdScheduleMaxSecs.log(log);
  
  ret.objectStoreURL.log(log);
  ret.fileCatalogConfigFile.log(log);
  
  for (auto & i:ret.driveConfigs) {
    i.second.log(log);
  }
  return ret;
}

//------------------------------------------------------------------------------
// GlobalConfiguration::gDummyLogger (static member)
//------------------------------------------------------------------------------
cta::log::DummyLogger TapedConfiguration::gDummyLogger("");

}}} // namespace cta::tape::daemon
