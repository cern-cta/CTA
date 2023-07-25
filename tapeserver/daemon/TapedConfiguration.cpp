/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "TapedConfiguration.hpp"
#include "common/ConfigurationFile.hpp"
#include "Tpconfig.hpp"

#include <algorithm>

namespace cta { 
//------------------------------------------------------------------------------
// addLogParamForValue
//------------------------------------------------------------------------------
template<>
void SourcedParameter<tape::daemon::TpconfigLine>::addLogParamForValue(log::LogContext & lc) {
  lc.pushOrReplace({"category", "TPCONFIG Entry"});
  lc.pushOrReplace({"tapeDrive", m_value.unitName});
  lc.pushOrReplace({"logicalLibrary", m_value.logicalLibrary});
  lc.pushOrReplace({"devFilename", m_value.devFilename});
  lc.pushOrReplace({"librarySlot", m_value.rawLibrarySlot});
}

//------------------------------------------------------------------------------
// addLogParamForValue
//------------------------------------------------------------------------------
template<>
void SourcedParameter<tape::daemon::FetchReportOrFlushLimits>::addLogParamForValue(log::LogContext & lc) {
  lc.pushOrReplace({"maxBytes", m_value.maxBytes});
  lc.pushOrReplace({"maxFiles", m_value.maxFiles});
}

//------------------------------------------------------------------------------
// set
//------------------------------------------------------------------------------
template<>
void SourcedParameter<tape::daemon::FetchReportOrFlushLimits>::set(const std::string & value,
  const std::string & source) {
  // We expect an entry in the form "<size limit>, <file limit>"
  // There should be one and only one comma in the parameter.
  if (1 != std::count(value.begin(), value.end(), ',')) {
    BadlyFormattedSizeFileLimit ex;
    ex.getMessage() << "In SourcedParameter<FetchReportOrFlushLimits>::set() : badly formatted entry: one (and only one) comma expected"
      << " for category=" << m_category << " key=" << m_key
      << " value=\'" << value << "' at:" << source;
    throw ex;
  }
  // We can now split the entry
  std::string bytes, files;
  size_t commaPos=value.find(',');
  bytes=value.substr(0, commaPos);
  files=value.substr(commaPos+1);
  bytes=utils::trimString(bytes);
  files=utils::trimString(files);
  if (!(utils::isValidUInt(bytes)&&utils::isValidUInt(files))) {
    BadlyFormattedInteger ex;
    ex.getMessage() << "In SourcedParameter<FetchReportOrFlushLimits>::set() : badly formatted integer"
      << " for category=" << m_category << " key=" << m_key
      << " value=\'" << value << "' at:" << source;
    throw ex;
  }
  std::istringstream(bytes) >> m_value.maxBytes;
  std::istringstream(files) >> m_value.maxFiles;
  m_source = source;
  m_set = true;
}
}

namespace cta { namespace tape { namespace daemon {

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
  ret.daemonUserName.setFromConfigurationFile(cf, generalConfigPath);
  ret.daemonGroupName.setFromConfigurationFile(cf, generalConfigPath);
  ret.logMask.setFromConfigurationFile(cf, generalConfigPath);
  ret.tpConfigPath.setFromConfigurationFile(cf, generalConfigPath);
  ret.externalEncryptionKeyScript.setFromConfigurationFile(cf, generalConfigPath);
  ret.useEncryption.setFromConfigurationFile(cf, generalConfigPath);
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
  //RAO
  ret.useRAO.setFromConfigurationFile(cf, generalConfigPath);
  ret.raoLtoAlgorithm.setFromConfigurationFile(cf,generalConfigPath);
  ret.raoLtoOptions.setFromConfigurationFile(cf,generalConfigPath);
  // Watchdog: parameters for timeouts in various situations.
  ret.wdIdleSessionTimer.setFromConfigurationFile(cf, generalConfigPath);
  ret.wdMountMaxSecs.setFromConfigurationFile(cf, generalConfigPath);
  ret.wdNoBlockMoveMaxSecs.setFromConfigurationFile(cf, generalConfigPath);
  ret.wdScheduleMaxSecs.setFromConfigurationFile(cf, generalConfigPath);
  ret.wdGlobalLockAcqMaxSecs.setFromConfigurationFile(cf, generalConfigPath);
  // The central storage access configuration
  ret.backendPath.setFromConfigurationFile(cf, generalConfigPath);
  ret.fileCatalogConfigFile.setFromConfigurationFile(cf, generalConfigPath);
  // Repack management configuration
  ret.useRepackManagement.setFromConfigurationFile(cf,generalConfigPath);
  // Maintenance process configuration
  ret.useMaintenanceProcess.setFromConfigurationFile(cf,generalConfigPath);
  ret.repackMaxToToExpand.setFromConfigurationFile(cf, generalConfigPath);
  // External free disk space script configuration
  ret.externalFreeDiskSpaceScript.setFromConfigurationFile(cf,generalConfigPath);
  // Timeout for tape load action
  ret.tapeLoadTimeout.setFromConfigurationFile(cf,generalConfigPath);
  // Extract drive list from tpconfig + parsed config file
  ret.driveConfigs = Tpconfig::parseFile(ret.tpConfigPath.value());
  
  ret.authenticationProtocol.set(cta::utils::getEnv("XrdSecPROTOCOL"),"Environment variable");
  ret.authenticationSSSKeytab.set(cta::utils::getEnv("XrdSecSSSKT"),"Environment variable");
  // rmcd connection options
  ret.rmcPort.setFromConfigurationFile(cf, generalConfigPath);
  ret.rmcNetTimeout.setFromConfigurationFile(cf, generalConfigPath);
  ret.rmcRequestAttempts.setFromConfigurationFile(cf, generalConfigPath);
  
  // If we get here, the configuration file is good enough to be logged.
  ret.daemonUserName.log(log);
  ret.daemonGroupName.log(log);
  ret.logMask.log(log);
  ret.tpConfigPath.log(log);
  ret.externalEncryptionKeyScript.log(log);
  ret.useEncryption.log(log);
  
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
  ret.wdGlobalLockAcqMaxSecs.log(log);
  
  ret.backendPath.log(log);
  ret.fileCatalogConfigFile.log(log);
  
  ret.useRepackManagement.log(log);
  ret.useMaintenanceProcess.log(log);
  ret.repackMaxToToExpand.log(log);
  ret.externalFreeDiskSpaceScript.log(log);
  
  ret.tapeLoadTimeout.log(log);

  ret.rmcPort.log(log);
  ret.rmcNetTimeout.log(log);
  ret.rmcRequestAttempts.log(log);

  for (auto & i:ret.driveConfigs) {
    i.second.log(log);
  }
  return ret;
}

//------------------------------------------------------------------------------
// GlobalConfiguration::gDummyLogger (static member)
//------------------------------------------------------------------------------
cta::log::DummyLogger TapedConfiguration::gDummyLogger("", "");

}}} // namespace cta::tape::daemon
