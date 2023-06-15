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

#include <optional>
#include <string>

#include "catalogue/dummy/DummyTapePoolCatalogue.hpp"
#include "catalogue/TapePool.hpp"
#include "catalogue/TapePoolSearchCriteria.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace catalogue {

void DummyTapePoolCatalogue::createTapePool(const common::dataStructures::SecurityIdentity& admin,
                                            const std::string& name,
                                            const std::string& vo,
                                            const uint64_t nbPartialTapes,
                                            const bool encryptionValue,
                                            const std::optional<std::string>& supply,
                                            const std::string& comment) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyTapePoolCatalogue::deleteTapePool(const std::string& name) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

std::list<TapePool> DummyTapePoolCatalogue::getTapePools(const TapePoolSearchCriteria& searchCriteria) const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

std::optional<TapePool> DummyTapePoolCatalogue::getTapePool(const std::string& tapePoolName) const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyTapePoolCatalogue::modifyTapePoolVo(const common::dataStructures::SecurityIdentity& admin,
                                              const std::string& name,
                                              const std::string& vo) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyTapePoolCatalogue::modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity& admin,
                                                          const std::string& name,
                                                          const uint64_t nbPartialTapes) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyTapePoolCatalogue::modifyTapePoolComment(const common::dataStructures::SecurityIdentity& admin,
                                                   const std::string& name,
                                                   const std::string& comment) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyTapePoolCatalogue::setTapePoolEncryption(const common::dataStructures::SecurityIdentity& admin,
                                                   const std::string& name,
                                                   const bool encryptionValue) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyTapePoolCatalogue::modifyTapePoolSupply(const common::dataStructures::SecurityIdentity& admin,
                                                  const std::string& name,
                                                  const std::string& supply) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyTapePoolCatalogue::modifyTapePoolName(const common::dataStructures::SecurityIdentity& admin,
                                                const std::string& currentName,
                                                const std::string& newName) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

bool DummyTapePoolCatalogue::tapePoolExists(const std::string& tapePoolName) const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

}  // namespace catalogue
}  // namespace cta