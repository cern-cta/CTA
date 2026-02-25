/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/retrywrappers/ArchiveRouteCatalogueRetryWrapper.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"

#include <memory>

namespace cta::catalogue {

ArchiveRouteCatalogueRetryWrapper::ArchiveRouteCatalogueRetryWrapper(Catalogue& catalogue,
                                                                     log::Logger& log,
                                                                     const uint32_t maxTriesToConnect)
    : m_catalogue(catalogue),
      m_log(log),
      m_maxTriesToConnect(maxTriesToConnect) {}

void ArchiveRouteCatalogueRetryWrapper::createArchiveRoute(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& storageClassName,
  const uint32_t copyNb,
  const common::dataStructures::ArchiveRouteType& archiveRouteType,
  const std::string& tapePoolName,
  const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &storageClassName, &copyNb, &archiveRouteType, &tapePoolName, &comment] {
      return m_catalogue.ArchiveRoute()
        ->createArchiveRoute(admin, storageClassName, copyNb, archiveRouteType, tapePoolName, comment);
    },
    m_maxTriesToConnect);
}

void ArchiveRouteCatalogueRetryWrapper::deleteArchiveRoute(
  const std::string& storageClassName,
  const uint32_t copyNb,
  const common::dataStructures::ArchiveRouteType& archiveRouteType) {
  return retryOnLostConnection(
    m_log,
    [this, &storageClassName, &copyNb, &archiveRouteType] {
      return m_catalogue.ArchiveRoute()->deleteArchiveRoute(storageClassName, copyNb, archiveRouteType);
    },
    m_maxTriesToConnect);
}

std::vector<common::dataStructures::ArchiveRoute> ArchiveRouteCatalogueRetryWrapper::getArchiveRoutes() const {
  return retryOnLostConnection(
    m_log,
    [this] { return m_catalogue.ArchiveRoute()->getArchiveRoutes(); },
    m_maxTriesToConnect);
}

std::vector<common::dataStructures::ArchiveRoute>
ArchiveRouteCatalogueRetryWrapper::getArchiveRoutes(const std::string& storageClassName,
                                                    const std::string& tapePoolName) const {
  return retryOnLostConnection(
    m_log,
    [this, &storageClassName, &tapePoolName] {
      return m_catalogue.ArchiveRoute()->getArchiveRoutes(storageClassName, tapePoolName);
    },
    m_maxTriesToConnect);
}

void ArchiveRouteCatalogueRetryWrapper::modifyArchiveRouteTapePoolName(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& storageClassName,
  const uint32_t copyNb,
  const common::dataStructures::ArchiveRouteType& archiveRouteType,
  const std::string& tapePoolName) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &storageClassName, &copyNb, &archiveRouteType, &tapePoolName] {
      return m_catalogue.ArchiveRoute()->modifyArchiveRouteTapePoolName(admin,
                                                                        storageClassName,
                                                                        copyNb,
                                                                        archiveRouteType,
                                                                        tapePoolName);
    },
    m_maxTriesToConnect);
}

void ArchiveRouteCatalogueRetryWrapper::modifyArchiveRouteComment(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& storageClassName,
  const uint32_t copyNb,
  const common::dataStructures::ArchiveRouteType& archiveRouteType,
  const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &storageClassName, &copyNb, &archiveRouteType, &comment] {
      return m_catalogue.ArchiveRoute()->modifyArchiveRouteComment(admin,
                                                                   storageClassName,
                                                                   copyNb,
                                                                   archiveRouteType,
                                                                   comment);
    },
    m_maxTriesToConnect);
}

}  // namespace cta::catalogue
