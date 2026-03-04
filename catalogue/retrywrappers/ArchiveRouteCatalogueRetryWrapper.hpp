/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/ArchiveRouteCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta {

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class ArchiveRouteCatalogueRetryWrapper : public ArchiveRouteCatalogue {
public:
  ArchiveRouteCatalogueRetryWrapper(Catalogue& catalogue, log::Logger& m_log, const uint32_t maxTriesToConnect);
  ~ArchiveRouteCatalogueRetryWrapper() override = default;

  void createArchiveRoute(const common::dataStructures::SecurityIdentity& admin,
                          const std::string& storageClassName,
                          const uint32_t copyNb,
                          const common::dataStructures::ArchiveRouteType& archiveRouteType,
                          const std::string& tapePoolName,
                          const std::string& comment) override;

  void deleteArchiveRoute(const std::string& storageClassName,
                          const uint32_t copyNb,
                          const common::dataStructures::ArchiveRouteType& archiveRouteType) override;

  std::vector<common::dataStructures::ArchiveRoute> getArchiveRoutes() const override;

  std::vector<common::dataStructures::ArchiveRoute> getArchiveRoutes(const std::string& storageClassName,
                                                                     const std::string& tapePoolName) const override;

  void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity& admin,
                                      const std::string& storageClassName,
                                      const uint32_t copyNb,
                                      const common::dataStructures::ArchiveRouteType& archiveRouteType,
                                      const std::string& tapePoolName) override;

  void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity& admin,
                                 const std::string& storageClassName,
                                 const uint32_t copyNb,
                                 const common::dataStructures::ArchiveRouteType& archiveRouteType,
                                 const std::string& comment) override;

private:
  const Catalogue& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};  // class ArchiveRouteCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta
