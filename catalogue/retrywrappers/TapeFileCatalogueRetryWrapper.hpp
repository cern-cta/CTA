/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/TapeFileCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta::catalogue {

class Catalogue;

class TapeFileCatalogueRetryWrapper : public TapeFileCatalogue {
public:
  TapeFileCatalogueRetryWrapper(Catalogue& catalogue, log::Logger& m_log, const uint32_t maxTriesToConnect);
  ~TapeFileCatalogueRetryWrapper() override = default;

  void filesWrittenToTape(const std::set<TapeItemWrittenPointer>& event) override;

  void deleteTapeFileCopy(common::dataStructures::ArchiveFile& file, const std::string& reason) override;

  common::dataStructures::RetrieveFileQueueCriteria
  prepareToRetrieveFile(const std::string& diskInstanceName,
                        const uint64_t archiveFileId,
                        const common::dataStructures::RequesterIdentity& user,
                        const std::optional<std::string>& activity,
                        log::LogContext& lc,
                        const std::optional<std::string>& mountPolicyName = std::nullopt) override;

private:
  const Catalogue& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};  // class TapeFileCatalogueRetryWrapper

}  // namespace cta::catalogue
