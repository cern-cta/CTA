/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <string>

#include "DriveConfig.hpp"
#include "tapeserver/daemon/FetchReportOrFlushLimits.hpp"
#include "common/SourcedParameter.hpp"

namespace cta {
namespace tape {
namespace daemon {

void DriveConfig::setTapedConfiguration(const cta::tape::daemon::TapedConfiguration &tapedConfiguration,
  const std::unique_ptr<catalogue::Catalogue>& catalogue, const std::string& tapeDriveName) {
  cta::tape::daemon::TapedConfiguration * config = const_cast<cta::tape::daemon::TapedConfiguration*>(&tapedConfiguration);

  setConfigToDB(config->daemonUserName, catalogue, tapeDriveName);
  setConfigToDB(config->daemonGroupName, catalogue, tapeDriveName);
  setConfigToDB(config->logMask, catalogue, tapeDriveName);
  setConfigToDB(config->tpConfigPath, catalogue, tapeDriveName);
  setConfigToDB(config->bufferSizeBytes, catalogue, tapeDriveName);
  setConfigToDB(config->bufferCount, catalogue, tapeDriveName);
  setConfigToDB(config->archiveFetchBytesFiles, catalogue, tapeDriveName);
  setConfigToDB(config->archiveFlushBytesFiles, catalogue, tapeDriveName);
  setConfigToDB(config->retrieveFetchBytesFiles, catalogue, tapeDriveName);
  setConfigToDB(config->mountCriteria, catalogue, tapeDriveName);
  setConfigToDB(config->nbDiskThreads, catalogue, tapeDriveName);
  setConfigToDB(config->useRAO, catalogue, tapeDriveName);
  setConfigToDB(config->raoLtoAlgorithm, catalogue, tapeDriveName);
  setConfigToDB(config->raoLtoOptions, catalogue, tapeDriveName);
  setConfigToDB(config->wdScheduleMaxSecs, catalogue, tapeDriveName);
  setConfigToDB(config->wdMountMaxSecs, catalogue, tapeDriveName);
  setConfigToDB(config->wdNoBlockMoveMaxSecs, catalogue, tapeDriveName);
  setConfigToDB(config->wdIdleSessionTimer, catalogue, tapeDriveName);
  setConfigToDB(config->backendPath, catalogue, tapeDriveName);
  setConfigToDB(config->fileCatalogConfigFile, catalogue, tapeDriveName);
  setConfigToDB(config->authenticationProtocol, catalogue, tapeDriveName);
  setConfigToDB(config->authenticationSSSKeytab, catalogue, tapeDriveName);
  setConfigToDB(config->disableRepackManagement, catalogue, tapeDriveName);
  setConfigToDB(config->disableMaintenanceProcess, catalogue, tapeDriveName);
  setConfigToDB(config->fetchEosFreeSpaceScript, catalogue, tapeDriveName);
  setConfigToDB(config->tapeLoadTimeout, catalogue, tapeDriveName);
}

void DriveConfig::setConfigToDB(cta::SourcedParameter<std::string>& sourcedParameter,
  const std::unique_ptr<catalogue::Catalogue>& catalogue, const std::string& tapeDriveName) {
  cta::SourcedParameter<std::string> * param = const_cast<cta::SourcedParameter<std::string>*>(&sourcedParameter);
  catalogue->createDriveConfig(tapeDriveName, param->category(), param->key(),
    param->value(), param->source());
}

void DriveConfig::setConfigToDB(cta::SourcedParameter<cta::tape::daemon::FetchReportOrFlushLimits>& sourcedParameter,
  const std::unique_ptr<catalogue::Catalogue>& catalogue, const std::string& tapeDriveName) {
  cta::SourcedParameter<cta::tape::daemon::FetchReportOrFlushLimits> * param = const_cast<cta::SourcedParameter<
    cta::tape::daemon::FetchReportOrFlushLimits>*>(&sourcedParameter);

  std::string key = param->key();
  cta::utils::searchAndReplace(key,"Bytes","");
  cta::utils::searchAndReplace(key,"Files","");
  catalogue->createDriveConfig(tapeDriveName, param->category(), key.append("Files"),
    std::to_string(param->value().maxFiles), param->source());
  cta::utils::searchAndReplace(key,"Files","");
  catalogue->createDriveConfig(tapeDriveName, param->category(), key.append("Bytes"),
    std::to_string(param->value().maxBytes), param->source());
}

void DriveConfig::setConfigToDB(cta::SourcedParameter<uint32_t>& sourcedParameter,
  const std::unique_ptr<catalogue::Catalogue>& catalogue, const std::string& tapeDriveName) {
  cta::SourcedParameter<uint32_t> * param = const_cast<cta::SourcedParameter<uint32_t>*>(&sourcedParameter);
  catalogue->createDriveConfig(tapeDriveName, param->category(), param->key(),
    std::to_string(param->value()), param->source());
}

void DriveConfig::setConfigToDB(cta::SourcedParameter<uint64_t>& sourcedParameter,
  const std::unique_ptr<catalogue::Catalogue>& catalogue, const std::string& tapeDriveName) {
  cta::SourcedParameter<uint64_t> * param = const_cast<cta::SourcedParameter<uint64_t>*>(&sourcedParameter);
  catalogue->createDriveConfig(tapeDriveName, param->category(), param->key(),
    std::to_string(param->value()), param->source());
}

void DriveConfig::setConfigToDB(cta::SourcedParameter<time_t>& sourcedParameter,
  const std::unique_ptr<catalogue::Catalogue>& catalogue, const std::string& tapeDriveName) {
  cta::SourcedParameter<time_t> * param = const_cast<cta::SourcedParameter<time_t>*>(&sourcedParameter);
  catalogue->createDriveConfig(tapeDriveName, param->category(), param->key(),
    std::to_string(param->value()), param->source());
}

}  // namespace daemon
}  // namespace tape
}  // namespace cta
