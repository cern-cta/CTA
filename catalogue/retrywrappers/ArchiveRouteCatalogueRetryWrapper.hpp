/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>

#include "catalogue/interfaces/ArchiveRouteCatalogue.hpp"

namespace cta {

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class ArchiveRouteCatalogueRetryWrapper : public ArchiveRouteCatalogue {
public:
  ArchiveRouteCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue, log::Logger &m_log,
    const uint32_t maxTriesToConnect);
  ~ArchiveRouteCatalogueRetryWrapper() override = default;

  void createArchiveRoute(const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName,
    const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType,
    const std::string &tapePoolName, const std::string &comment) override;

  void deleteArchiveRoute(const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType) override;

  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes() const override;

  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes(const std::string &storageClassName,
    const std::string &tapePoolName) const override;

  void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType,
    const std::string &tapePoolName) override;

  void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType,
    const std::string &comment) override;

private:
  const std::unique_ptr<Catalogue>& m_catalogue;
  log::Logger &m_log;
  uint32_t m_maxTriesToConnect;
};  // class SchemaCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta
