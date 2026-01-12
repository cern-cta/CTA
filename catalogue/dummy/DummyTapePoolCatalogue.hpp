/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/TapePoolCatalogue.hpp"

namespace cta::catalogue {

class DummyTapePoolCatalogue : public TapePoolCatalogue {
public:
  DummyTapePoolCatalogue() = default;
  ~DummyTapePoolCatalogue() override = default;

  void createTapePool(const common::dataStructures::SecurityIdentity& admin,
                      const std::string& name,
                      const std::string& vo,
                      const uint64_t nbPartialTapes,
                      const std::optional<std::string>& encryptionKeyNameOpt,
                      const std::vector<std::string>& supply_list,
                      const std::string& comment) override;

  void deleteTapePool(const std::string& name) override;

  std::vector<TapePool> getTapePools(const TapePoolSearchCriteria& searchCriteria) const override;

  std::optional<TapePool> getTapePool(const std::string& tapePoolName) const override;

  void modifyTapePoolVo(const common::dataStructures::SecurityIdentity& admin,
                        const std::string& name,
                        const std::string& vo) override;

  void modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity& admin,
                                    const std::string& name,
                                    const uint64_t nbPartialTapes) override;

  void modifyTapePoolComment(const common::dataStructures::SecurityIdentity& admin,
                             const std::string& name,
                             const std::string& comment) override;

  void setTapePoolEncryption(const common::dataStructures::SecurityIdentity& admin,
                             const std::string& name,
                             const std::string& encryptionKeyName) override;

  void modifyTapePoolSupply(const common::dataStructures::SecurityIdentity& admin,
                            const std::string& name,
                            const std::vector<std::string>& supply_list) override;

  void modifyTapePoolName(const common::dataStructures::SecurityIdentity& admin,
                          const std::string& currentName,
                          const std::string& newName) override;

  bool tapePoolExists(const std::string& tapePoolName) const override;

  void deleteAllTapePoolSupplyEntries() override;
};

}  // namespace cta::catalogue
