/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyDriveConfigCatalogue.hpp"

#include "common/Constants.hpp"
#include "common/exception/Exception.hpp"

#include <algorithm>

namespace cta::catalogue {

void DummyDriveConfigCatalogue::createTapeDriveConfig(const std::string& tapeDriveName,
                                                      const std::string& category,
                                                      const std::string& keyName,
                                                      const std::string& value,
                                                      const std::string& source) {
  m_driveConfigs.emplace_back(DriveConfig {tapeDriveName, category, keyName, value, source});
}

std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig> DummyDriveConfigCatalogue::getTapeDriveConfigs() const {
  return m_driveConfigs;
}

std::list<std::string>
DummyDriveConfigCatalogue::getTapeDriveNamesForSchedulerBackend(const std::string& schedulerBackendName) const {
  std::list<std::string> validTapeDrives;
  for (const auto& config : m_driveConfigs) {
    if (config.keyName == SCHEDULER_NAME_CONFIG_KEY && config.value == schedulerBackendName) {
      validTapeDrives.emplace_back(config.tapeDriveName);
    }
  }
  return validTapeDrives;
}

std::list<std::pair<std::string, std::string>> DummyDriveConfigCatalogue::getTapeDriveConfigNamesAndKeys() const {
  std::list<std::pair<std::string, std::string>> result;
  for (auto& config : m_driveConfigs) {
    result.emplace_back(config.tapeDriveName, config.keyName);
  }
  return result;
}

void DummyDriveConfigCatalogue::modifyTapeDriveConfig(const std::string& tapeDriveName,
                                                      const std::string& category,
                                                      const std::string& keyName,
                                                      const std::string& value,
                                                      const std::string& source) {
  const auto it = std::find_if(m_driveConfigs.begin(),
                               m_driveConfigs.end(),
                               [&tapeDriveName, &keyName](const DriveConfig& driveConfig) {
                                 return driveConfig.tapeDriveName == tapeDriveName && driveConfig.keyName == keyName;
                               });
  if (it == m_driveConfigs.end()) {
    throw exception::Exception(std::string("Cannot modify Config Drive with name: ") + tapeDriveName + " and key"
                               + keyName + " because it doesn't exist");
  }
  it->category = category;
  it->value = value;
  it->source = source;
}

std::optional<std::tuple<std::string, std::string, std::string>>
DummyDriveConfigCatalogue::getTapeDriveConfig(const std::string& tapeDriveName, const std::string& keyName) const {
  const auto it = std::find_if(m_driveConfigs.begin(),
                               m_driveConfigs.end(),
                               [&tapeDriveName, &keyName](const DriveConfig& driveConfig) {
                                 return driveConfig.tapeDriveName == tapeDriveName && driveConfig.keyName == keyName;
                               });
  if (it == m_driveConfigs.end()) {
    return std::nullopt;
  } else {
    return std::make_tuple(it->category, it->value, it->source);
  }
}

void DummyDriveConfigCatalogue::deleteTapeDriveConfig(const std::string& tapeDriveName, const std::string& keyName) {
  m_driveConfigs.erase(std::remove_if(m_driveConfigs.begin(),
                                      m_driveConfigs.end(),
                                      [&tapeDriveName, &keyName](const DriveConfig& driveConfig) {
                                        return driveConfig.tapeDriveName == tapeDriveName
                                               && driveConfig.keyName == keyName;
                                      }),
                       m_driveConfigs.end());
}

}  // namespace cta::catalogue
