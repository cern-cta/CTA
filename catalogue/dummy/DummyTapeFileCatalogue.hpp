/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "catalogue/interfaces/TapeFileCatalogue.hpp"

namespace cta::catalogue {

class DummyTapeFileCatalogue : public TapeFileCatalogue {
public:
  DummyTapeFileCatalogue() = default;
  ~DummyTapeFileCatalogue() override = default;

  void filesWrittenToTape(const std::set<TapeItemWrittenPointer> &event) override;

  void deleteTapeFileCopy(common::dataStructures::ArchiveFile &file, const std::string &reason) override;

  common::dataStructures::RetrieveFileQueueCriteria prepareToRetrieveFile(const std::string &diskInstanceName,
    const uint64_t archiveFileId, const common::dataStructures::RequesterIdentity &user,
    const std::optional<std::string> & activity, log::LogContext &lc,
    const std::optional<std::string> &mountPolicyName = std::nullopt) override;
};

} // namespace cta::catalogue
