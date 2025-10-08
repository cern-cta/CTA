/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <optional>
#include <string>

#include "catalogue/dummy/DummyTapePoolCatalogue.hpp"
#include "catalogue/TapePool.hpp"
#include "catalogue/TapePoolSearchCriteria.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

void DummyTapePoolCatalogue::createTapePool(const common::dataStructures::SecurityIdentity& admin,
                                            const std::string& name,
                                            const std::string& vo,
                                            const uint64_t nbPartialTapes,
                                            const std::optional<std::string>& encryptionKeyNameOpt,
                                            const std::list<std::string>& supply_list,
                                            const std::string& comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapePoolCatalogue::deleteTapePool(const std::string &name) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

std::list<TapePool> DummyTapePoolCatalogue::getTapePools(const TapePoolSearchCriteria &searchCriteria) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

std::optional<TapePool> DummyTapePoolCatalogue::getTapePool(const std::string &tapePoolName) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapePoolCatalogue::modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &vo) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapePoolCatalogue::modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t nbPartialTapes) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapePoolCatalogue::modifyTapePoolComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapePoolCatalogue::setTapePoolEncryption(const common::dataStructures::SecurityIdentity &admin,
                                                   const std::string &name, const std::string &encryptionKeyName) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapePoolCatalogue::modifyTapePoolSupply(const common::dataStructures::SecurityIdentity& admin,
                                                  const std::string& name,
                                                  const std::list<std::string>& supply_list) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapePoolCatalogue::modifyTapePoolName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &currentName, const std::string &newName) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

bool DummyTapePoolCatalogue::tapePoolExists(const std::string &tapePoolName) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapePoolCatalogue::deleteAllTapePoolSupplyEntries() {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

} // namespace cta::catalogue
