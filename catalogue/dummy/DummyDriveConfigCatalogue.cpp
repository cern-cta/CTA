/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include "catalogue/dummy/DummyDriveConfigCatalogue.hpp"
#include "common/exception/Exception.hpp"

#include <algorithm>

namespace cta::catalogue {

void DummyDriveConfigCatalogue::createTapeDriveConfig(const std::string &tapeDriveName, const std::string &category,
  const std::string &keyName, const std::string &value, const std::string &source) {
  m_driveConfigs.emplace_back(DriveConfig{tapeDriveName, category, keyName, value, source});
}

std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig> DummyDriveConfigCatalogue::getTapeDriveConfigs() const {
  return m_driveConfigs;
}

std::list<std::string> DummyDriveConfigCatalogue::getTapeDriveNamesForSchedulerBackend(const std::string &schedulerBackendName) const {
  std::list<std::string> validTapeDrives;
  for (const auto& config : m_driveConfigs) {
    if (config.keyName == "SchedulerBackendName" && config.value == schedulerBackendName) {
      validTapeDrives.emplace_back(config.tapeDriveName);
    }
  }
  return validTapeDrives;
}

std::list<std::pair<std::string, std::string>> DummyDriveConfigCatalogue::getTapeDriveConfigNamesAndKeys() const {
  std::list<std::pair<std::string, std::string>> result;
  for (auto& config: m_driveConfigs) {
    result.emplace_back(config.tapeDriveName, config.keyName);
  }
  return result;
}

void DummyDriveConfigCatalogue::modifyTapeDriveConfig(const std::string &tapeDriveName, const std::string &category,
  const std::string &keyName, const std::string &value, const std::string &source) {
  const auto it = std::find_if(
    m_driveConfigs.begin(), m_driveConfigs.end(),
    [&tapeDriveName, &keyName](const DriveConfig &driveConfig) {
      return driveConfig.tapeDriveName == tapeDriveName && driveConfig.keyName == keyName;
    }
  );
  if (it == m_driveConfigs.end()) {
    throw exception::Exception(std::string("Cannot modify Config Drive with name: ") + tapeDriveName + " and key" + keyName + " because it doesn't exist");
  }
  it->category = category;
  it->value = value;
  it->source = source;
}

std::optional<std::tuple<std::string, std::string, std::string>> DummyDriveConfigCatalogue::getTapeDriveConfig(
  const std::string &tapeDriveName, const std::string &keyName) const {
  const auto it = std::find_if(
    m_driveConfigs.begin(), m_driveConfigs.end(),
    [&tapeDriveName, &keyName](const DriveConfig &driveConfig) {
      return driveConfig.tapeDriveName == tapeDriveName && driveConfig.keyName == keyName;
    }
  );
  if (it == m_driveConfigs.end()) {
    return std::nullopt;
  } else {
    return std::make_tuple(it->category, it->value, it->source);
  }
}

void DummyDriveConfigCatalogue::deleteTapeDriveConfig(const std::string &tapeDriveName, const std::string &keyName) {
  m_driveConfigs.erase(
    std::remove_if(
        m_driveConfigs.begin(),
        m_driveConfigs.end(),
        [&tapeDriveName, &keyName](const DriveConfig &driveConfig) {
          return driveConfig.tapeDriveName == tapeDriveName && driveConfig.keyName == keyName;
        }),
    m_driveConfigs.end());
}

} // namespace cta::catalogue
