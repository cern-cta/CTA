/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/retrywrappers/FileRecycleLogCatalogueRetryWrapper.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"
#include "common/log/LogContext.hpp"

#include <memory>

namespace cta::catalogue {

FileRecycleLogCatalogueRetryWrapper::FileRecycleLogCatalogueRetryWrapper(Catalogue& catalogue,
                                                                         log::Logger& log,
                                                                         const uint32_t maxTriesToConnect)
    : m_catalogue(catalogue),
      m_log(log),
      m_maxTriesToConnect(maxTriesToConnect) {}

FileRecycleLogItor
FileRecycleLogCatalogueRetryWrapper::getFileRecycleLogItor(const RecycleTapeFileSearchCriteria& searchCriteria) const {
  return retryOnLostConnection(
    m_log,
    [this, &searchCriteria] { return m_catalogue.FileRecycleLog()->getFileRecycleLogItor(searchCriteria); },
    m_maxTriesToConnect);
}

void FileRecycleLogCatalogueRetryWrapper::restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria& searchCriteria,
                                                                  const std::string& newFid) {
  return retryOnLostConnection(
    m_log,
    [this, &searchCriteria, &newFid] {
      return m_catalogue.FileRecycleLog()->restoreFileInRecycleLog(searchCriteria, newFid);
    },
    m_maxTriesToConnect);
}

void FileRecycleLogCatalogueRetryWrapper::deleteFilesFromRecycleLog(const std::string& vid, log::LogContext& lc) {
  return retryOnLostConnection(
    m_log,
    [this, &vid, &lc] { return m_catalogue.FileRecycleLog()->deleteFilesFromRecycleLog(vid, lc); },
    m_maxTriesToConnect);
}

}  // namespace cta::catalogue
