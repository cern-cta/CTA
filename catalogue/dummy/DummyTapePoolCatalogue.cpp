/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyTapePoolCatalogue.hpp"

#include "catalogue/TapePool.hpp"
#include "catalogue/TapePoolSearchCriteria.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

#include <optional>
#include <string>

namespace cta::catalogue {

void DummyTapePoolCatalogue::createTapePool(const common::dataStructures::SecurityIdentity& admin,
                                            const std::string& name,
                                            const std::string& vo,
                                            const uint64_t nbPartialTapes,
                                            const std::optional<std::string>& encryptionKeyNameOpt,
                                            const std::list<std::string>& supply_list,
                                            const std::string& comment) {
  throw exception::NotImplementedException();
}

void DummyTapePoolCatalogue::deleteTapePool(const std::string& name) {
  throw exception::NotImplementedException();
}

std::list<TapePool> DummyTapePoolCatalogue::getTapePools(const TapePoolSearchCriteria& searchCriteria) const {
  throw exception::NotImplementedException();
}

std::optional<TapePool> DummyTapePoolCatalogue::getTapePool(const std::string& tapePoolName) const {
  throw exception::NotImplementedException();
}

void DummyTapePoolCatalogue::modifyTapePoolVo(const common::dataStructures::SecurityIdentity& admin,
                                              const std::string& name,
                                              const std::string& vo) {
  throw exception::NotImplementedException();
}

void DummyTapePoolCatalogue::modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity& admin,
                                                          const std::string& name,
                                                          const uint64_t nbPartialTapes) {
  throw exception::NotImplementedException();
}

void DummyTapePoolCatalogue::modifyTapePoolComment(const common::dataStructures::SecurityIdentity& admin,
                                                   const std::string& name,
                                                   const std::string& comment) {
  throw exception::NotImplementedException();
}

void DummyTapePoolCatalogue::setTapePoolEncryption(const common::dataStructures::SecurityIdentity& admin,
                                                   const std::string& name,
                                                   const std::string& encryptionKeyName) {
  throw exception::NotImplementedException();
}

void DummyTapePoolCatalogue::modifyTapePoolSupply(const common::dataStructures::SecurityIdentity& admin,
                                                  const std::string& name,
                                                  const std::list<std::string>& supply_list) {
  throw exception::NotImplementedException();
}

void DummyTapePoolCatalogue::modifyTapePoolName(const common::dataStructures::SecurityIdentity& admin,
                                                const std::string& currentName,
                                                const std::string& newName) {
  throw exception::NotImplementedException();
}

bool DummyTapePoolCatalogue::tapePoolExists(const std::string& tapePoolName) const {
  throw exception::NotImplementedException();
}

void DummyTapePoolCatalogue::deleteAllTapePoolSupplyEntries() {
  throw exception::NotImplementedException();
}

}  // namespace cta::catalogue
