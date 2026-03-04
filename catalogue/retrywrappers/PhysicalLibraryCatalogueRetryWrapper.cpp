/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/retrywrappers/PhysicalLibraryCatalogueRetryWrapper.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/PhysicalLibrary.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"

#include <memory>

namespace cta::catalogue {

PhysicalLibraryCatalogueRetryWrapper::PhysicalLibraryCatalogueRetryWrapper(Catalogue& catalogue,
                                                                           log::Logger& log,
                                                                           const uint32_t maxTriesToConnect)
    : m_catalogue(catalogue),
      m_log(log),
      m_maxTriesToConnect(maxTriesToConnect) {}

void PhysicalLibraryCatalogueRetryWrapper::createPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin,
                                                                 const common::dataStructures::PhysicalLibrary& pl) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &pl] { return m_catalogue.PhysicalLibrary()->createPhysicalLibrary(admin, pl); },
    m_maxTriesToConnect);
}

void PhysicalLibraryCatalogueRetryWrapper::deletePhysicalLibrary(const std::string& name) {
  return retryOnLostConnection(
    m_log,
    [this, &name] { return m_catalogue.PhysicalLibrary()->deletePhysicalLibrary(name); },
    m_maxTriesToConnect);
}

std::vector<common::dataStructures::PhysicalLibrary>
PhysicalLibraryCatalogueRetryWrapper::getPhysicalLibraries() const {
  return retryOnLostConnection(
    m_log,
    [this] { return m_catalogue.PhysicalLibrary()->getPhysicalLibraries(); },
    m_maxTriesToConnect);
}

void PhysicalLibraryCatalogueRetryWrapper::modifyPhysicalLibrary(
  const common::dataStructures::SecurityIdentity& admin,
  const common::dataStructures::UpdatePhysicalLibrary& pl) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &pl] { return m_catalogue.PhysicalLibrary()->modifyPhysicalLibrary(admin, pl); },
    m_maxTriesToConnect);
}

}  // namespace cta::catalogue
