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
