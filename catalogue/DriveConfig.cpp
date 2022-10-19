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

#include <algorithm>
#include <string>
#include <utility>

#include "catalogue/Catalogue.hpp"
#include "catalogue/DriveConfig.hpp"
#include "common/SourcedParameter.hpp"
#include "tapeserver/daemon/FetchReportOrFlushLimits.hpp"

namespace cta {

void DriveConfig::setTapedConfiguration(const cta::tape::daemon::TapedConfiguration &tapedConfiguration,
  catalogue::Catalogue* catalogue, const std::string& tapeDriveName) {
  auto *config = const_cast<cta::tape::daemon::TapedConfiguration *>(&tapedConfiguration);

  setConfigToDB(&config->daemonUserName, catalogue, tapeDriveName);
  setConfigToDB(&config->daemonGroupName, catalogue, tapeDriveName);
  setConfigToDB(&config->logMask, catalogue, tapeDriveName);
  setConfigToDB(&config->tpConfigPath, catalogue, tapeDriveName);
  setConfigToDB(&config->bufferSizeBytes, catalogue, tapeDriveName);
  setConfigToDB(&config->bufferCount, catalogue, tapeDriveName);
  setConfigToDB(&config->archiveFetchBytesFiles, catalogue, tapeDriveName);
  setConfigToDB(&config->archiveFlushBytesFiles, catalogue, tapeDriveName);
  setConfigToDB(&config->retrieveFetchBytesFiles, catalogue, tapeDriveName);
  setConfigToDB(&config->mountCriteria, catalogue, tapeDriveName);
  setConfigToDB(&config->nbDiskThreads, catalogue, tapeDriveName);
  setConfigToDB(&config->useRAO, catalogue, tapeDriveName);
  setConfigToDB(&config->raoLtoAlgorithm, catalogue, tapeDriveName);
  setConfigToDB(&config->useEncryption, catalogue, tapeDriveName);
  setConfigToDB(&config->externalEncryptionKeyScript, catalogue, tapeDriveName);
  setConfigToDB(&config->raoLtoOptions, catalogue, tapeDriveName);
  setConfigToDB(&config->wdScheduleMaxSecs, catalogue, tapeDriveName);
  setConfigToDB(&config->wdMountMaxSecs, catalogue, tapeDriveName);
  setConfigToDB(&config->wdNoBlockMoveMaxSecs, catalogue, tapeDriveName);
  setConfigToDB(&config->wdIdleSessionTimer, catalogue, tapeDriveName);
  setConfigToDB(&config->backendPath, catalogue, tapeDriveName);
  setConfigToDB(&config->fileCatalogConfigFile, catalogue, tapeDriveName);
  setConfigToDB(&config->authenticationProtocol, catalogue, tapeDriveName);
  setConfigToDB(&config->authenticationSSSKeytab, catalogue, tapeDriveName);
  setConfigToDB(&config->useRepackManagement, catalogue, tapeDriveName);
  setConfigToDB(&config->useMaintenanceProcess, catalogue, tapeDriveName);
  setConfigToDB(&config->externalFreeDiskSpaceScript, catalogue, tapeDriveName);
  setConfigToDB(&config->tapeLoadTimeout, catalogue, tapeDriveName);
}

void DriveConfig::checkConfigInDB(catalogue::Catalogue* catalogue, const std::string& tapeDriveName,
  const std::string& key) {
  auto namesAndKeys = catalogue->getTapeDriveConfigNamesAndKeys();
  auto it = std::find_if(namesAndKeys.begin(), namesAndKeys.end(),
    [&tapeDriveName, &key](const std::pair<std::string, std::string>& element) {
      return element.first == tapeDriveName && element.second == key;
    });
  if (it != namesAndKeys.end()) {
    catalogue->deleteTapeDriveConfig(tapeDriveName, key);
  }
}

void DriveConfig::setConfigToDB(cta::SourcedParameter<std::string>* sourcedParameter,
  catalogue::Catalogue* catalogue, const std::string& tapeDriveName) {
  checkConfigInDB(catalogue, tapeDriveName, sourcedParameter->key());
  catalogue->createTapeDriveConfig(tapeDriveName, sourcedParameter->category(), sourcedParameter->key(),
    sourcedParameter->value(), sourcedParameter->source());
}

void DriveConfig::setConfigToDB(cta::SourcedParameter<cta::tape::daemon::FetchReportOrFlushLimits>* sourcedParameter,
  catalogue::Catalogue* catalogue, const std::string& tapeDriveName) {
  std::string key = sourcedParameter->key();
  cta::utils::searchAndReplace(key, "Bytes", "");
  cta::utils::searchAndReplace(key, "Files", "");
  checkConfigInDB(catalogue, tapeDriveName, key.append("Files"));
  catalogue->createTapeDriveConfig(tapeDriveName, sourcedParameter->category(), key,
    std::to_string(sourcedParameter->value().maxFiles), sourcedParameter->source());
  cta::utils::searchAndReplace(key, "Files", "");
  checkConfigInDB(catalogue, tapeDriveName, key.append("Bytes"));
  catalogue->createTapeDriveConfig(tapeDriveName, sourcedParameter->category(), key,
    std::to_string(sourcedParameter->value().maxBytes), sourcedParameter->source());
}

void DriveConfig::setConfigToDB(cta::SourcedParameter<uint32_t>* sourcedParameter,
  catalogue::Catalogue* catalogue, const std::string& tapeDriveName) {
  checkConfigInDB(catalogue, tapeDriveName, sourcedParameter->key());
  catalogue->createTapeDriveConfig(tapeDriveName, sourcedParameter->category(), sourcedParameter->key(),
    std::to_string(sourcedParameter->value()), sourcedParameter->source());
}

void DriveConfig::setConfigToDB(cta::SourcedParameter<uint64_t>* sourcedParameter,
  catalogue::Catalogue* catalogue, const std::string& tapeDriveName) {
  checkConfigInDB(catalogue, tapeDriveName, sourcedParameter->key());
  catalogue->createTapeDriveConfig(tapeDriveName, sourcedParameter->category(), sourcedParameter->key(),
    std::to_string(sourcedParameter->value()), sourcedParameter->source());
}

void DriveConfig::setConfigToDB(cta::SourcedParameter<time_t>* sourcedParameter,
  catalogue::Catalogue* catalogue, const std::string& tapeDriveName) {
  checkConfigInDB(catalogue, tapeDriveName, sourcedParameter->key());
  catalogue->createTapeDriveConfig(tapeDriveName, sourcedParameter->category(), sourcedParameter->key(),
    std::to_string(sourcedParameter->value()), sourcedParameter->source());
}

}  // namespace cta
