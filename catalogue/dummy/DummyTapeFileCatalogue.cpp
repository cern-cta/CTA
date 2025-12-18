/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyTapeFileCatalogue.hpp"

#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>

namespace cta::catalogue {

void DummyTapeFileCatalogue::filesWrittenToTape(const std::set<TapeItemWrittenPointer>& event) {
  throw exception::NotImplementedException();
}

void DummyTapeFileCatalogue::deleteTapeFileCopy(common::dataStructures::ArchiveFile& file, const std::string& reason) {
  throw exception::NotImplementedException();
}

common::dataStructures::RetrieveFileQueueCriteria
DummyTapeFileCatalogue::prepareToRetrieveFile(const std::string& diskInstanceName,
                                              const uint64_t archiveFileId,
                                              const common::dataStructures::RequesterIdentity& user,
                                              const std::optional<std::string>& activity,
                                              log::LogContext& lc,
                                              const std::optional<std::string>& mountPolicyName) {
  throw exception::NotImplementedException();
}

}  // namespace cta::catalogue
