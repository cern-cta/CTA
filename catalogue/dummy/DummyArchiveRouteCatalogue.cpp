/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyArchiveRouteCatalogue.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

void DummyArchiveRouteCatalogue::createArchiveRoute(const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName,
  const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType, const std::string &tapePoolName, const std::string &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyArchiveRouteCatalogue::deleteArchiveRoute(const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

std::list<common::dataStructures::ArchiveRoute> DummyArchiveRouteCatalogue::getArchiveRoutes() const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

std::list<common::dataStructures::ArchiveRoute> DummyArchiveRouteCatalogue::getArchiveRoutes(
  const std::string &storageClassName, const std::string &tapePoolName) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyArchiveRouteCatalogue::modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType,
  const std::string &tapePoolName) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyArchiveRouteCatalogue::modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType,
  const std::string &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

} // namespace cta::catalogue
