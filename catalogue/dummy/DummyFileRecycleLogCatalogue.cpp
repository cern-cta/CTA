/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/CatalogueItor.hpp"
#include "catalogue/dummy/DummyFileRecycleLogCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"

namespace cta::catalogue {

FileRecycleLogItor DummyFileRecycleLogCatalogue::getFileRecycleLogItor(
  const RecycleTapeFileSearchCriteria & searchCriteria) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyFileRecycleLogCatalogue::restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria & searchCriteria,
  const std::string &newFid) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyFileRecycleLogCatalogue::deleteFilesFromRecycleLog(const std::string& vid, log::LogContext& lc) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

} // namespace cta::catalogue
