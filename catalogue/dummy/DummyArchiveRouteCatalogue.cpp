/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyArchiveRouteCatalogue.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta::catalogue {

void DummyArchiveRouteCatalogue::createArchiveRoute(const common::dataStructures::SecurityIdentity& admin,
                                                    const std::string& storageClassName,
                                                    const uint32_t copyNb,
                                                    const common::dataStructures::ArchiveRouteType& archiveRouteType,
                                                    const std::string& tapePoolName,
                                                    const std::string& comment) {
  throw exception::NotImplementedException();
}

void DummyArchiveRouteCatalogue::deleteArchiveRoute(const std::string& storageClassName,
                                                    const uint32_t copyNb,
                                                    const common::dataStructures::ArchiveRouteType& archiveRouteType) {
  throw exception::NotImplementedException();
}

std::vector<common::dataStructures::ArchiveRoute> DummyArchiveRouteCatalogue::getArchiveRoutes() const {
  throw exception::NotImplementedException();
}

std::vector<common::dataStructures::ArchiveRoute>
DummyArchiveRouteCatalogue::getArchiveRoutes(const std::string& storageClassName,
                                             const std::string& tapePoolName) const {
  throw exception::NotImplementedException();
}

void DummyArchiveRouteCatalogue::modifyArchiveRouteTapePoolName(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& storageClassName,
  const uint32_t copyNb,
  const common::dataStructures::ArchiveRouteType& archiveRouteType,
  const std::string& tapePoolName) {
  throw exception::NotImplementedException();
}

void DummyArchiveRouteCatalogue::modifyArchiveRouteComment(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& storageClassName,
  const uint32_t copyNb,
  const common::dataStructures::ArchiveRouteType& archiveRouteType,
  const std::string& comment) {
  throw exception::NotImplementedException();
}

}  // namespace cta::catalogue
