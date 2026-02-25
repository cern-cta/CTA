/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/retrywrappers/DiskInstanceCatalogueRetryWrapper.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta::catalogue {

DiskInstanceCatalogueRetryWrapper::DiskInstanceCatalogueRetryWrapper(Catalogue& catalogue,
                                                                     log::Logger& log,
                                                                     const uint32_t maxTriesToConnect)
    : m_catalogue(catalogue),
      m_log(log),
      m_maxTriesToConnect(maxTriesToConnect) {}

void DiskInstanceCatalogueRetryWrapper::createDiskInstance(const common::dataStructures::SecurityIdentity& admin,
                                                           const std::string& name,
                                                           const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &comment] { return m_catalogue.DiskInstance()->createDiskInstance(admin, name, comment); },
    m_maxTriesToConnect);
}

void DiskInstanceCatalogueRetryWrapper::deleteDiskInstance(const std::string& name) {
  return retryOnLostConnection(
    m_log,
    [this, &name] { return m_catalogue.DiskInstance()->deleteDiskInstance(name); },
    m_maxTriesToConnect);
}

void DiskInstanceCatalogueRetryWrapper::modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity& admin,
                                                                  const std::string& name,
                                                                  const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &comment] {
      return m_catalogue.DiskInstance()->modifyDiskInstanceComment(admin, name, comment);
    },
    m_maxTriesToConnect);
}

std::vector<common::dataStructures::DiskInstance> DiskInstanceCatalogueRetryWrapper::getAllDiskInstances() const {
  return retryOnLostConnection(
    m_log,
    [this] { return m_catalogue.DiskInstance()->getAllDiskInstances(); },
    m_maxTriesToConnect);
}

}  // namespace cta::catalogue
