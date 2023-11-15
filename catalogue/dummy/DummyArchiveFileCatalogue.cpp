/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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

common::dataStructures::ArchiveFile DummyArchiveFileCatalogue::getArchiveFileForDeletion(
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
