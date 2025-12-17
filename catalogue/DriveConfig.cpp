/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/DriveConfig.hpp"

#include "catalogue/Catalogue.hpp"
#include "common/SourcedParameter.hpp"
#include "tapeserver/daemon/common/FetchReportOrFlushLimits.hpp"

#include <algorithm>
#include <string>
#include <utility>

namespace cta {

void DriveConfig::setTapedConfiguration(const tape::daemon::common::TapedConfiguration& tapedConfiguration,
                                        catalogue::Catalogue* catalogue,
                                        const std::string& tapeDriveName) {
  setConfigToDB(tapedConfiguration.driveName, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.driveLogicalLibrary, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.driveDevice, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.driveControlPath, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.daemonUserName, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.daemonGroupName, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.logMask, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.bufferSizeBytes, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.bufferCount, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.archiveFetchBytesFiles, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.archiveFlushBytesFiles, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.retrieveFetchBytesFiles, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.mountCriteria, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.nbDiskThreads, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.useRAO, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.raoLtoAlgorithm, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.useEncryption, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.externalEncryptionKeyScript, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.raoLtoOptions, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.wdScheduleMaxSecs, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.wdMountMaxSecs, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.wdNoBlockMoveMaxSecs, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.wdIdleSessionTimer, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.backendPath, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.fileCatalogConfigFile, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.authenticationProtocol, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.authenticationSSSKeytab, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.externalFreeDiskSpaceScript, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.tapeLoadTimeout, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.wdGetNextMountMaxSecs, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.schedulerBackendName, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.instanceName, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.tapeCacheMaxAgeSecs, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.retrieveQueueCacheMaxAgeSecs, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.telemetryEnabled, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.metricsBackend, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.metricsFileEndpoint, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.metricsExportInterval, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.metricsExportTimeout, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.metricsOtlpAuthBasicUsername, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.metricsOtlpAuthBasicPasswordFile, catalogue, tapeDriveName);
  setConfigToDB(tapedConfiguration.metricsOtlpEndpoint, catalogue, tapeDriveName);
}

void DriveConfig::checkConfigInDB(catalogue::Catalogue* catalogue,
                                  const std::string& tapeDriveName,
                                  const std::string& key) {
  auto namesAndKeys = catalogue->DriveConfig()->getTapeDriveConfigNamesAndKeys();
  auto it = std::find_if(namesAndKeys.begin(),
                         namesAndKeys.end(),
                         [&tapeDriveName, &key](const std::pair<std::string, std::string>& element) {
                           return element.first == tapeDriveName && element.second == key;
                         });
  if (it != namesAndKeys.end()) {
    catalogue->DriveConfig()->deleteTapeDriveConfig(tapeDriveName, key);
  }
}

void DriveConfig::setConfigToDB(const SourcedParameter<std::string>& sourcedParameter,
                                catalogue::Catalogue* catalogue,
                                const std::string& tapeDriveName) {
  checkConfigInDB(catalogue, tapeDriveName, sourcedParameter.key());
  catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName,
                                                  sourcedParameter.category(),
                                                  sourcedParameter.key(),
                                                  sourcedParameter.value(),
                                                  sourcedParameter.source());
}

void DriveConfig::setConfigToDB(
  const SourcedParameter<tape::daemon::common::FetchReportOrFlushLimits>& sourcedParameter,
  catalogue::Catalogue* catalogue,
  const std::string& tapeDriveName) {
  std::string key = sourcedParameter.key();
  utils::searchAndReplace(key, "Bytes", "");
  utils::searchAndReplace(key, "Files", "");
  checkConfigInDB(catalogue, tapeDriveName, key.append("Files"));
  catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName,
                                                  sourcedParameter.category(),
                                                  key,
                                                  std::to_string(sourcedParameter.value().maxFiles),
                                                  sourcedParameter.source());
  utils::searchAndReplace(key, "Files", "");
  checkConfigInDB(catalogue, tapeDriveName, key.append("Bytes"));
  catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName,
                                                  sourcedParameter.category(),
                                                  key,
                                                  std::to_string(sourcedParameter.value().maxBytes),
                                                  sourcedParameter.source());
}

void DriveConfig::setConfigToDB(const SourcedParameter<uint32_t>& sourcedParameter,
                                catalogue::Catalogue* catalogue,
                                const std::string& tapeDriveName) {
  checkConfigInDB(catalogue, tapeDriveName, sourcedParameter.key());
  catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName,
                                                  sourcedParameter.category(),
                                                  sourcedParameter.key(),
                                                  std::to_string(sourcedParameter.value()),
                                                  sourcedParameter.source());
}

void DriveConfig::setConfigToDB(const SourcedParameter<uint64_t>& sourcedParameter,
                                catalogue::Catalogue* catalogue,
                                const std::string& tapeDriveName) {
  checkConfigInDB(catalogue, tapeDriveName, sourcedParameter.key());
  catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName,
                                                  sourcedParameter.category(),
                                                  sourcedParameter.key(),
                                                  std::to_string(sourcedParameter.value()),
                                                  sourcedParameter.source());
}

void DriveConfig::setConfigToDB(const SourcedParameter<time_t>& sourcedParameter,
                                catalogue::Catalogue* catalogue,
                                const std::string& tapeDriveName) {
  checkConfigInDB(catalogue, tapeDriveName, sourcedParameter.key());
  catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName,
                                                  sourcedParameter.category(),
                                                  sourcedParameter.key(),
                                                  std::to_string(sourcedParameter.value()),
                                                  sourcedParameter.source());
}

void DriveConfig::setConfigToDB(const SourcedParameter<bool>& sourcedParameter,
                                catalogue::Catalogue* catalogue,
                                const std::string& tapeDriveName) {
  checkConfigInDB(catalogue, tapeDriveName, sourcedParameter.key());
  catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName,
                                                  sourcedParameter.category(),
                                                  sourcedParameter.key(),
                                                  std::to_string(sourcedParameter.value()),
                                                  sourcedParameter.source());
}

}  // namespace cta
