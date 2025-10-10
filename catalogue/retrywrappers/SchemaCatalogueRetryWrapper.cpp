/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <memory>

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "catalogue/retrywrappers/SchemaCatalogueRetryWrapper.hpp"
#include "catalogue/SchemaVersion.hpp"

namespace cta::catalogue {

SchemaCatalogueRetryWrapper::SchemaCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
  log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {
}

SchemaVersion SchemaCatalogueRetryWrapper::getSchemaVersion() const {
  return retryOnLostConnection(m_log, [this] {
    return m_catalogue->Schema()->getSchemaVersion();
  }, m_maxTriesToConnect);
}

void SchemaCatalogueRetryWrapper::verifySchemaVersion() {
  return retryOnLostConnection(m_log, [this] {
    return m_catalogue->Schema()->verifySchemaVersion();
  }, m_maxTriesToConnect);
}

void SchemaCatalogueRetryWrapper::ping() {
  return retryOnLostConnection(m_log, [this] {
    return m_catalogue->Schema()->ping();
  }, m_maxTriesToConnect);
}

} // namespace cta::catalogue
