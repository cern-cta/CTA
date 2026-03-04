/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/RecyleTapeFileSearchCriteria.hpp"
#include "catalogue/interfaces/FileRecycleLogCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <memory>
#include <string>

namespace cta {

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class FileRecycleLogCatalogueRetryWrapper : public FileRecycleLogCatalogue {
public:
  FileRecycleLogCatalogueRetryWrapper(Catalogue& catalogue, log::Logger& m_log, const uint32_t maxTriesToConnect);

  ~FileRecycleLogCatalogueRetryWrapper() override = default;

  FileRecycleLogItor getFileRecycleLogItor(
    const RecycleTapeFileSearchCriteria& searchCriteria = RecycleTapeFileSearchCriteria()) const override;

  void restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria& searchCriteria, const std::string& newFid) override;

  void deleteFilesFromRecycleLog(const std::string& vid, log::LogContext& lc) override;

private:
  Catalogue& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};

}  // namespace catalogue
}  // namespace cta
