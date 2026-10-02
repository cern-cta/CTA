/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapedUtils.hpp"

#include "TapedConfig.hpp"
#include "common/exception/Exception.hpp"
#include "runtime/config/ConfigLoader.hpp"

#include <algorithm>
#include <filesystem>
#include <regex>

namespace cta::taped::utils {

std::vector<std::string> getTapedConfigPaths() {
  const std::regex configPattern("cta-taped.*\\.toml");
  std::vector<std::string> configPaths;

  for (const auto& entry : std::filesystem::directory_iterator("/etc/cta/")) {
    const auto filename = entry.path().filename().string();
    if (filename != "cta-taped.example.toml" && std::regex_match(filename, configPattern)) {
      configPaths.emplace_back(entry.path().string());
    }
  }

  std::sort(configPaths.begin(), configPaths.end());
  return configPaths;
}

std::string getFirstTapedConfigPath(const std::optional<std::string>& driveName) {
  if (driveName) {
    const std::string tapedConfigFile = "/etc/cta/cta-taped-" + driveName.value() + ".toml";
    if (!std::filesystem::exists(tapedConfigFile)) {
      throw cta::exception::Exception("Failed to find a drive configuration file for drive: " + driveName.value()
                                      + ". Expected file: " + tapedConfigFile);
    }
    return tapedConfigFile;
  }

  const auto configPaths = getTapedConfigPaths();
  if (!configPaths.empty()) {
    return configPaths.front();
  }

  throw cta::exception::Exception("Failed to find a drive configuration file in /etc/cta/");
}

std::string getFirstDriveName() {
  const auto configPaths = getTapedConfigPaths();
  if (configPaths.empty()) {
    throw cta::exception::Exception("Failed to find a drive configuration file in /etc/cta/");
  }

  const auto config = runtime::loadFromToml<cta::tape::daemon::TapedConfig>(configPaths.front(), false);
  return config.drive.name;
}

}  // namespace cta::taped::utils
