/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyFileRecycleLogCatalogue.hpp"

#include "catalogue/CatalogueItor.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"
#include "common/log/LogContext.hpp"

namespace cta::catalogue {

FileRecycleLogItor
DummyFileRecycleLogCatalogue::getFileRecycleLogItor(const RecycleTapeFileSearchCriteria& searchCriteria) const {
  throw exception::NotImplementedException();
}

void DummyFileRecycleLogCatalogue::restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria& searchCriteria,
                                                           const std::string& newFid) {
  throw exception::NotImplementedException();
}

void DummyFileRecycleLogCatalogue::deleteFilesFromRecycleLog(const std::string& vid, log::LogContext& lc) {
  throw exception::NotImplementedException();
}

}  // namespace cta::catalogue
