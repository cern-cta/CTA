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

void TapePoolCatalogueRetryWrapper::createTapePool(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &vo, const uint64_t nbPartialTapes, const bool encryptionValue,
  const std::optional<std::string> &supply, const std::string &comment) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&vo,&nbPartialTapes,&encryptionValue,&supply,&comment] {
    return m_catalogue->TapePool()->createTapePool(admin, name, vo, nbPartialTapes, encryptionValue, supply, comment);
  }, m_maxTriesToConnect);
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
  const std::string &name, const bool encryptionValue) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&encryptionValue] {
    return m_catalogue->TapePool()->setTapePoolEncryption(admin, name, encryptionValue);
  }, m_maxTriesToConnect);
}

void TapePoolCatalogueRetryWrapper::modifyTapePoolSupply(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &supply) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&supply] {
    return m_catalogue->TapePool()->modifyTapePoolSupply(admin, name, supply);
  }, m_maxTriesToConnect);
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

void TapePoolCatalogueRetryWrapper::verifyTapePoolSupply(const std::string &supply) {
  return retryOnLostConnection(m_log, [this,&supply] {
    // return m_catalogue->TapePool()->tapePoolExists(supply);
    const std::regex CTA_SUPPLY_OPTION_REGEX("^\\s*([^,]+\\s*,\\s*)*[^,]+\\s*$");

    bool valid = std::regex_match(supply, CTA_SUPPLY_OPTION_REGEX);
    if (!valid)
    {
      throw UserSpecifiedInvalidSupplyField("Cannot set tape pool supply because user specified an invalid supply string");
    }
    // for every submatch, call tapePoolExists
    std::regex pattern_between_commas("\\s*([^,]+)\\s*,?");
    auto it_begin = std::sregex_iterator(supply.begin(), supply.end(), pattern_between_commas);
    auto it_end = std::sregex_iterator();

    for (std::sregex_iterator it = it_begin; it != it_end; ++it){
      std::smatch match = *it;
      m_catalogue->TapePool()->tapePoolExists(match[1]);
    }
  }, m_maxTriesToConnect);
}


} // namespace cta::catalogue
