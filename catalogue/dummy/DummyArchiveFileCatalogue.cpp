/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/CatalogueItor.hpp"
#include "catalogue/dummy/DummyArchiveFileCatalogue.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

uint64_t DummyArchiveFileCatalogue::checkAndGetNextArchiveFileId(const std::string &diskInstanceName,
  const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

common::dataStructures::ArchiveFileQueueCriteria DummyArchiveFileCatalogue::getArchiveFileQueueCriteria(
  const std::string &diskInstanceName, const std::string &storageClassName,
  const common::dataStructures::RequesterIdentity &user) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

ArchiveFileItor DummyArchiveFileCatalogue::getArchiveFilesItor(const TapeFileSearchCriteria &searchCriteria) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

common::dataStructures::ArchiveFile DummyArchiveFileCatalogue::getArchiveFileCopyForDeletion(
  const TapeFileSearchCriteria &searchCriteria) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

std::list<common::dataStructures::ArchiveFile> DummyArchiveFileCatalogue::getFilesForRepack(const std::string &vid,
  const uint64_t startFSeq, const uint64_t maxNbFiles) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

ArchiveFileItor DummyArchiveFileCatalogue::getArchiveFilesForRepackItor(const std::string &vid,
  const uint64_t startFSeq) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

common::dataStructures::ArchiveFileSummary DummyArchiveFileCatalogue::getTapeFileSummary(
  const TapeFileSearchCriteria &searchCriteria) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

common::dataStructures::ArchiveFile DummyArchiveFileCatalogue::getArchiveFileById(const uint64_t id) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyArchiveFileCatalogue::modifyArchiveFileStorageClassId(const uint64_t archiveFileId,
  const std::string &newStorageClassName) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyArchiveFileCatalogue::modifyArchiveFileFxIdAndDiskInstance(const uint64_t archiveId, const std::string& fxId,
  const std::string &diskInstance) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyArchiveFileCatalogue::moveArchiveFileToRecycleLog(const common::dataStructures::DeleteArchiveRequest &request,
  log::LogContext & lc) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyArchiveFileCatalogue::updateDiskFileId(uint64_t archiveFileId, const std::string &diskInstance,
  const std::string &diskFileId) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyArchiveFileCatalogue::DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string &diskInstanceName,
  const uint64_t archiveFileId, log::LogContext &lc) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

} // namespace cta::catalogue
