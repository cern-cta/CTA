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
