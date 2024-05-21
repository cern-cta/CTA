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

#include <algorithm>
#include <regex>
#include <filesystem>

namespace cta {

//------------------------------------------------------------------------------
// addLogParamForValue
//------------------------------------------------------------------------------
template<>
void SourcedParameter<tape::daemon::common::FetchReportOrFlushLimits>::addLogParamForValue(log::LogContext & lc) {
  lc.pushOrReplace({"maxBytes", m_value.maxBytes});
  lc.pushOrReplace({"maxFiles", m_value.maxFiles});
} // namespace cta

//------------------------------------------------------------------------------
// set
//------------------------------------------------------------------------------
template<>
void SourcedParameter<tape::daemon::common::FetchReportOrFlushLimits>::set(const std::string & value,
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
  std::string bytes;
  std::string files;

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
} // namespace cta
} // namespace cta

namespace cta::tape::daemon::common {

//------------------------------------------------------------------------------
// TapedConfiguration::getDriveConfigFile
//------------------------------------------------------------------------------
std::string TapedConfiguration::getDriveConfigFile(const std::optional<std::string> &unitName, cta::log::Logger &log) {
  const std::regex CTA_CONF_REGEX("cta-taped.*\\.conf");

  if (unitName){
    // Try exact match.
    std::string tapeDriveConfigFile = "/etc/cta/cta-taped-" + unitName.value() + ".conf";
    if(std::filesystem::exists(tapeDriveConfigFile)){
      return tapeDriveConfigFile;
    }
  } else {
      log(cta::log::INFO, "Unit name not specified, choosing first config file found.");
      for(auto const& entry : std::filesystem::directory_iterator("/etc/cta/")){
        if (std::regex_match( entry.path().filename().string(), CTA_CONF_REGEX)){
          return entry.path().string();
        }
      }
  }
  cta::exception::Exception ex;
  ex.getMessage() << "Failed to find a drive configuration file for the server.";
  throw ex;
}

//------------------------------------------------------------------------------
// TapedConfiguration::getFirstDriveName
//------------------------------------------------------------------------------
std::string TapedConfiguration::getFirstDriveName() {
  // Get first config file path
  const std::string driveTapedConfigPath = getDriveConfigFile(std::nullopt, gDummyLogger);

  // Read config file
  ConfigurationFile cf(driveTapedConfigPath);

  return cf.entries.at("taped").at("DriveName").value;
}

//------------------------------------------------------------------------------
// TapedConfiguration::createFromConfigPath
//------------------------------------------------------------------------------
TapedConfiguration TapedConfiguration::createFromConfigPath(
  const std::string &driveTapedConfigPath, cta::log::Logger &log) {
  TapedConfiguration ret;

  // Parse config file
  ConfigurationFile cf(driveTapedConfigPath);

  // Extract configuration from parsed config file
  ret.daemonUserName.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.daemonGroupName.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.logMask.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.logFormat.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.externalEncryptionKeyScript.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.useEncryption.setFromConfigurationFile(cf, driveTapedConfigPath);
  // Memory management
  ret.bufferSizeBytes.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.bufferCount.setFromConfigurationFile(cf, driveTapedConfigPath);
  // Batched metadata access and tape write flush parameters
  ret.archiveFetchBytesFiles.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.archiveFlushBytesFiles.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.retrieveFetchBytesFiles.setFromConfigurationFile(cf, driveTapedConfigPath);
  // Mount criteria
  ret.mountCriteria.setFromConfigurationFile(cf, driveTapedConfigPath);
  // Disk file access parameters
  ret.nbDiskThreads.setFromConfigurationFile(cf, driveTapedConfigPath);
  //RAO
  ret.useRAO.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.raoLtoAlgorithm.setFromConfigurationFile(cf,driveTapedConfigPath);
  ret.raoLtoOptions.setFromConfigurationFile(cf,driveTapedConfigPath);
  // Watchdog: parameters for timeouts in various situations.
  ret.wdIdleSessionTimer.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.wdMountMaxSecs.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.wdNoBlockMoveMaxSecs.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.wdScheduleMaxSecs.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.wdGetNextMountMaxSecs.setFromConfigurationFile(cf, driveTapedConfigPath);
  // The central storage access configuration
  ret.backendPath.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.fileCatalogConfigFile.setFromConfigurationFile(cf, driveTapedConfigPath);
  // Repack management configuration
  ret.useRepackManagement.setFromConfigurationFile(cf,driveTapedConfigPath);
  // Maintenance process configuration
  ret.useMaintenanceProcess.setFromConfigurationFile(cf,driveTapedConfigPath);
  ret.repackMaxRequestsToExpand.setFromConfigurationFile(cf, driveTapedConfigPath);
  // External free disk space script configuration
  ret.externalFreeDiskSpaceScript.setFromConfigurationFile(cf,driveTapedConfigPath);
  // Timeout for tape load action
  ret.tapeLoadTimeout.setFromConfigurationFile(cf,driveTapedConfigPath);

  ret.authenticationProtocol.set(cta::utils::getEnv("XrdSecPROTOCOL"),"Environment variable");
  ret.authenticationSSSKeytab.set(cta::utils::getEnv("XrdSecSSSKT"),"Environment variable");
  // rmcd connection options
  ret.rmcPort.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.rmcNetTimeout.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.rmcRequestAttempts.setFromConfigurationFile(cf, driveTapedConfigPath);

  // Drive options
  ret.driveName.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.driveLogicalLibrary.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.driveDevice.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.driveControlPath.setFromConfigurationFile(cf, driveTapedConfigPath);

  // General options
  ret.instanceName.setFromConfigurationFile(cf, driveTapedConfigPath);
  ret.schedulerBackendName.setFromConfigurationFile(cf, driveTapedConfigPath);

  // If we get here, the configuration file is good enough to be logged.
  ret.daemonUserName.log(log);
  ret.daemonGroupName.log(log);
  ret.logMask.log(log);
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
  ret.wdGetNextMountMaxSecs.log(log);

  ret.backendPath.log(log);
  ret.fileCatalogConfigFile.log(log);

  ret.useRepackManagement.log(log);
  ret.useMaintenanceProcess.log(log);
  ret.repackMaxRequestsToExpand.log(log);
  ret.externalFreeDiskSpaceScript.log(log);

  ret.tapeLoadTimeout.log(log);

  ret.rmcPort.log(log);
  ret.rmcNetTimeout.log(log);
  ret.rmcRequestAttempts.log(log);

  ret.driveName.log(log);
  ret.driveLogicalLibrary.log(log);
  ret.driveDevice.log(log);
  ret.driveControlPath.log(log);

  ret.instanceName.log(log);
  ret.schedulerBackendName.log(log);

  return ret;
}

TapedConfiguration TapedConfiguration::createFromOptionalDriveName(
  const std::optional<std::string> &unitName, cta::log::Logger &log) {
  // Get the config file path
  const std::string driveTapedConfigPath = getDriveConfigFile(unitName, log);

  return createFromConfigPath(driveTapedConfigPath, log);
}

//------------------------------------------------------------------------------
// GlobalConfiguration::gDummyLogger (static member)
//------------------------------------------------------------------------------
cta::log::DummyLogger TapedConfiguration::gDummyLogger("", "");

} // namespace cta::tape::daemon
