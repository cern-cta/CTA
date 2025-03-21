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
#include "catalogue/TapePool.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "catalogue/retrywrappers/TapePoolCatalogueRetryWrapper.hpp"

namespace cta::catalogue {

TapePoolCatalogueRetryWrapper::TapePoolCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
  log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {
}

void TapePoolCatalogueRetryWrapper::createTapePool(const common::dataStructures::SecurityIdentity& admin,
                                                   const std::string& name,
                                                   const std::string& vo,
                                                   const uint64_t nbPartialTapes,
                                                   const std::optional<std::string>& encryptionKeyNameOpt,
                                                   const std::list<std::string>& supply_list,
                                                   const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &vo, &nbPartialTapes, &encryptionKeyNameOpt, &supply_list, &comment] {
      return m_catalogue->TapePool()->createTapePool(admin, name, vo, nbPartialTapes, encryptionKeyNameOpt, supply_list,
                                                     comment);
    },
    m_maxTriesToConnect);
}

void TapePoolCatalogueRetryWrapper::deleteTapePool(const std::string &name) {
  return retryOnLostConnection(m_log, [this,&name] {
    return m_catalogue->TapePool()->deleteTapePool(name);
  }, m_maxTriesToConnect);
}

std::list<TapePool> TapePoolCatalogueRetryWrapper::getTapePools(const TapePoolSearchCriteria &searchCriteria) const {
  return retryOnLostConnection(m_log, [this,&searchCriteria] {
    return m_catalogue->TapePool()->getTapePools(searchCriteria);
  }, m_maxTriesToConnect);
}

std::optional<TapePool> TapePoolCatalogueRetryWrapper::getTapePool(const std::string &tapePoolName) const {
  return retryOnLostConnection(m_log, [this,&tapePoolName] {
    return m_catalogue->TapePool()->getTapePool(tapePoolName);
  }, m_maxTriesToConnect);
}

void TapePoolCatalogueRetryWrapper::modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &vo) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&vo] {
    return m_catalogue->TapePool()->modifyTapePoolVo(admin, name, vo);
  }, m_maxTriesToConnect);
}

void TapePoolCatalogueRetryWrapper::modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t nbPartialTapes) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&nbPartialTapes] {
    return m_catalogue->TapePool()->modifyTapePoolNbPartialTapes(admin, name, nbPartialTapes);
  }, m_maxTriesToConnect);
}

void TapePoolCatalogueRetryWrapper::modifyTapePoolComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&comment] {
    return m_catalogue->TapePool()->modifyTapePoolComment(admin, name, comment);
  }, m_maxTriesToConnect);
}

void TapePoolCatalogueRetryWrapper::setTapePoolEncryption(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::optional<std::string>& encryptionKeyNameOpt) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&encryptionKeyNameOpt] {
    return m_catalogue->TapePool()->setTapePoolEncryption(admin, name, encryptionKeyNameOpt);
  }, m_maxTriesToConnect);
}

void TapePoolCatalogueRetryWrapper::modifyTapePoolSupply(const common::dataStructures::SecurityIdentity& admin,
                                                         const std::string& name,
                                                         const std::list<std::string>& supply_list) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &supply_list] {
      return m_catalogue->TapePool()->modifyTapePoolSupply(admin, name, supply_list);
    },
    m_maxTriesToConnect);
}

void TapePoolCatalogueRetryWrapper::modifyTapePoolName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &currentName, const std::string &newName) {
  return retryOnLostConnection(m_log, [this,&admin,&currentName,&newName] {
    return m_catalogue->TapePool()->modifyTapePoolName(admin, currentName, newName);
  }, m_maxTriesToConnect);
}

bool TapePoolCatalogueRetryWrapper::tapePoolExists(const std::string &tapePoolName) const {
  return retryOnLostConnection(m_log, [this,&tapePoolName] {
    return m_catalogue->TapePool()->tapePoolExists(tapePoolName);
  }, m_maxTriesToConnect);
}

void TapePoolCatalogueRetryWrapper::deleteAllTapePoolSupplyEntries() {
  return retryOnLostConnection(
    m_log, [this] { return m_catalogue->TapePool()->deleteAllTapePoolSupplyEntries(); }, m_maxTriesToConnect);
}

} // namespace cta::catalogue
