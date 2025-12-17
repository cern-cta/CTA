/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyArchiveFileCatalogue.hpp"

#include "catalogue/CatalogueItor.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta::catalogue {

uint64_t
DummyArchiveFileCatalogue::checkAndGetNextArchiveFileId(const std::string& diskInstanceName,
                                                        const std::string& storageClassName,
                                                        const common::dataStructures::RequesterIdentity& user) {
  throw exception::NotImplementedException();
}

common::dataStructures::ArchiveFileQueueCriteria
DummyArchiveFileCatalogue::getArchiveFileQueueCriteria(const std::string& diskInstanceName,
                                                       const std::string& storageClassName,
                                                       const common::dataStructures::RequesterIdentity& user) {
  throw exception::NotImplementedException();
}

ArchiveFileItor DummyArchiveFileCatalogue::getArchiveFilesItor(const TapeFileSearchCriteria& searchCriteria) const {
  throw exception::NotImplementedException();
}

common::dataStructures::ArchiveFile
DummyArchiveFileCatalogue::getArchiveFileCopyForDeletion(const TapeFileSearchCriteria& searchCriteria) const {
  throw exception::NotImplementedException();
}

std::list<common::dataStructures::ArchiveFile>
DummyArchiveFileCatalogue::getFilesForRepack(const std::string& vid,
                                             const uint64_t startFSeq,
                                             const uint64_t maxNbFiles) const {
  throw exception::NotImplementedException();
}

ArchiveFileItor DummyArchiveFileCatalogue::getArchiveFilesForRepackItor(const std::string& vid,
                                                                        const uint64_t startFSeq) const {
  throw exception::NotImplementedException();
}

common::dataStructures::ArchiveFileSummary
DummyArchiveFileCatalogue::getTapeFileSummary(const TapeFileSearchCriteria& searchCriteria) const {
  throw exception::NotImplementedException();
}

common::dataStructures::ArchiveFile DummyArchiveFileCatalogue::getArchiveFileById(const uint64_t id) const {
  throw exception::NotImplementedException();
}

void DummyArchiveFileCatalogue::modifyArchiveFileStorageClassId(const uint64_t archiveFileId,
                                                                const std::string& newStorageClassName) const {
  throw exception::NotImplementedException();
}

void DummyArchiveFileCatalogue::modifyArchiveFileFxIdAndDiskInstance(const uint64_t archiveId,
                                                                     const std::string& fxId,
                                                                     const std::string& diskInstance) const {
  throw exception::NotImplementedException();
}

void DummyArchiveFileCatalogue::moveArchiveFileToRecycleLog(const common::dataStructures::DeleteArchiveRequest& request,
                                                            log::LogContext& lc) {
  throw exception::NotImplementedException();
}

void DummyArchiveFileCatalogue::updateDiskFileId(uint64_t archiveFileId,
                                                 const std::string& diskInstance,
                                                 const std::string& diskFileId) {
  throw exception::NotImplementedException();
}

void DummyArchiveFileCatalogue::DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string& diskInstanceName,
                                                                        const uint64_t archiveFileId,
                                                                        log::LogContext& lc) {
  throw exception::NotImplementedException();
}

}  // namespace cta::catalogue
