/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/Catalogue.hpp"
#include "catalogue/interfaces/DriveConfigCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <list>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>

namespace cta::catalogue {

class DriveConfigCatalogueRetryWrapper : public DriveConfigCatalogue {
public:
  DriveConfigCatalogueRetryWrapper(Catalogue& catalogue, log::Logger& m_log, const uint32_t maxTriesToConnect);

  ~DriveConfigCatalogueRetryWrapper() override = default;

  void createTapeDriveConfig(const std::string& tapeDriveName,
                             const std::string& category,
                             const std::string& keyName,
                             const std::string& value,
                             const std::string& source) override;

  std::vector<DriveConfig> getTapeDriveConfigs() const override;

  std::vector<std::pair<std::string, std::string>> getTapeDriveConfigNamesAndKeys() const override;

  void modifyTapeDriveConfig(const std::string& tapeDriveName,
                             const std::string& category,
                             const std::string& keyName,
                             const std::string& value,
                             const std::string& source) override;

  std::optional<std::tuple<std::string, std::string, std::string>>
  getTapeDriveConfig(const std::string& tapeDriveName, const std::string& keyName) const override;

  std::vector<std::string> getTapeDriveNamesForSchedulerBackend(const std::string& schedulerBackendName) const override;

  void deleteTapeDriveConfig(const std::string& tapeDriveName, const std::string& keyName) override;

private:
  Catalogue& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};

}  // namespace cta::catalogue
