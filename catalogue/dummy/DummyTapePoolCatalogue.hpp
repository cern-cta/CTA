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

#pragma once

#include "catalogue/interfaces/TapePoolCatalogue.hpp"

namespace cta::catalogue {

class DummyTapePoolCatalogue : public TapePoolCatalogue {
public:
  DummyTapePoolCatalogue() = default;
  ~DummyTapePoolCatalogue() override = default;

  void createTapePool(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
                      const std::string &vo, const uint64_t nbPartialTapes, const bool encryptionValue,
                      const std::list<std::string> &supply_list, const std::string &comment) override;

  void deleteTapePool(const std::string &name) override;

  std::list<TapePool> getTapePools(const TapePoolSearchCriteria &searchCriteria) const override;

  std::optional<TapePool> getTapePool(const std::string &tapePoolName) const override;

  void modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &vo) override;

  void modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const uint64_t nbPartialTapes) override;

  void modifyTapePoolComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &comment) override;

  void setTapePoolEncryption(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const bool encryptionValue) override;

  void modifyTapePoolSupply(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
                            const std::list<std::string> &supply_list) override;

  void modifyTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName,
    const std::string &newName) override;

  bool tapePoolExists(const std::string& tapePoolName) const override;

  void deleteAllTapePoolSupplyEntries() override;
};

} // namespace cta::catalogue
