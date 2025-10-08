/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <list>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>

#include "catalogue/interfaces/DriveConfigCatalogue.hpp"
#include "common/log/LogContext.hpp"

namespace cta {

namespace rdbms {
class ConnPool;
}

namespace catalogue {

class RdbmsDriveConfigCatalogue : public DriveConfigCatalogue {
public:
  RdbmsDriveConfigCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool);
  ~RdbmsDriveConfigCatalogue() override = default;

  void createTapeDriveConfig(const std::string &tapeDriveName, const std::string &category,
    const std::string &keyName, const std::string &value, const std::string &source) override;

  std::list<DriveConfig> getTapeDriveConfigs() const override;

  std::list<std::pair<std::string, std::string>> getTapeDriveConfigNamesAndKeys() const override;

  void modifyTapeDriveConfig(const std::string &tapeDriveName, const std::string &category,
    const std::string &keyName, const std::string &value, const std::string &source) override;

  std::optional<std::tuple<std::string, std::string, std::string>> getTapeDriveConfig(const std::string &tapeDriveName,
    const std::string &keyName) const override;

  std::list<std::string> getTapeDriveNamesForSchedulerBackend(const std::string &schedulerBackendName) const override;

  void deleteTapeDriveConfig(const std::string &tapeDriveName, const std::string &keyName) override;

private:
  log::Logger &m_log;

  std::shared_ptr<rdbms::ConnPool> m_connPool;
};

}} // namespace cta::catalogue
