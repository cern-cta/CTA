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

#include <memory>
#include <string>

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/retrywrappers/ArchiveFileCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"

namespace cta {
namespace catalogue {

ArchiveFileCatalogueRetryWrapper::ArchiveFileCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
  log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {}

uint64_t ArchiveFileCatalogueRetryWrapper::checkAndGetNextArchiveFileId(const std::string &diskInstanceName,
  const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) {
  return retryOnLostConnection(m_log, [&]{
    return m_catalogue->ArchiveFile()->checkAndGetNextArchiveFileId(diskInstanceName, storageClassName, user);
  }, m_maxTriesToConnect);
}

common::dataStructures::ArchiveFileQueueCriteria ArchiveFileCatalogueRetryWrapper::getArchiveFileQueueCriteria(
  const std::string &diskInstanceName, const std::string &storageClassName,
  const common::dataStructures::RequesterIdentity &user) {
  return retryOnLostConnection(m_log, [&]{
    return m_catalogue->ArchiveFile()->getArchiveFileQueueCriteria(diskInstanceName, storageClassName, user);
  }, m_maxTriesToConnect);
}

ArchiveFileItor ArchiveFileCatalogueRetryWrapper::getArchiveFilesItor(
  const TapeFileSearchCriteria &searchCriteria) const {
  return retryOnLostConnection(m_log, [&]{
    return m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
  }, m_maxTriesToConnect);
}

common::dataStructures::ArchiveFile ArchiveFileCatalogueRetryWrapper::getArchiveFileForDeletion(
  const TapeFileSearchCriteria &searchCriteria) const {
  return retryOnLostConnection(m_log, [&]{
    return m_catalogue->ArchiveFile()->getArchiveFileForDeletion(searchCriteria);
  }, m_maxTriesToConnect);
}

std::list<common::dataStructures::ArchiveFile> ArchiveFileCatalogueRetryWrapper::getFilesForRepack(
  const std::string &vid, const uint64_t startFSeq, const uint64_t maxNbFiles) const {
  return retryOnLostConnection(m_log, [&]{
    return m_catalogue->ArchiveFile()->getFilesForRepack(vid, startFSeq, maxNbFiles);
  }, m_maxTriesToConnect);
}

ArchiveFileItor ArchiveFileCatalogueRetryWrapper::getArchiveFilesForRepackItor(const std::string &vid,
  const uint64_t startFSeq) const {
  return retryOnLostConnection(m_log, [&]{
    return m_catalogue->ArchiveFile()->getArchiveFilesForRepackItor(vid, startFSeq);
  }, m_maxTriesToConnect);
}

common::dataStructures::ArchiveFileSummary ArchiveFileCatalogueRetryWrapper::getTapeFileSummary(
  const TapeFileSearchCriteria &searchCriteria) const {
  return retryOnLostConnection(m_log, [&]{
    return m_catalogue->ArchiveFile()->getTapeFileSummary(searchCriteria);
  }, m_maxTriesToConnect);
}

common::dataStructures::ArchiveFile ArchiveFileCatalogueRetryWrapper::getArchiveFileById(const uint64_t id) const {
  return retryOnLostConnection(m_log, [&]{
    return m_catalogue->ArchiveFile()->getArchiveFileById(id);
  }, m_maxTriesToConnect);
}

void ArchiveFileCatalogueRetryWrapper::modifyArchiveFileStorageClassId(const uint64_t archiveFileId,
  const std::string& newStorageClassName) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->ArchiveFile()->modifyArchiveFileStorageClassId(
    archiveFileId, newStorageClassName);}, m_maxTriesToConnect);
}

void ArchiveFileCatalogueRetryWrapper::modifyArchiveFileFxIdAndDiskInstance(const uint64_t archiveId,
  const std::string& fxId, const std::string &diskInstance) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->ArchiveFile()->modifyArchiveFileFxIdAndDiskInstance(
    archiveId, fxId, diskInstance);}, m_maxTriesToConnect);
}

void ArchiveFileCatalogueRetryWrapper::DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string &diskInstanceName,
  const uint64_t archiveFileId, log::LogContext &lc) {
  return retryOnLostConnection(m_log, [&]{
    return m_catalogue->ArchiveFile()->DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(diskInstanceName, archiveFileId, lc);
  }, m_maxTriesToConnect);
}

void ArchiveFileCatalogueRetryWrapper::updateDiskFileId(uint64_t archiveFileId, const std::string &diskInstance,
  const std::string &diskFileId) {
  return retryOnLostConnection(m_log, [&]{
    return m_catalogue->ArchiveFile()->updateDiskFileId(archiveFileId, diskInstance, diskFileId);
  }, m_maxTriesToConnect);
}

void ArchiveFileCatalogueRetryWrapper::moveArchiveFileToRecycleLog(
  const common::dataStructures::DeleteArchiveRequest &request, log::LogContext & lc) {
  return retryOnLostConnection(m_log, [&]{
    return m_catalogue->ArchiveFile()->moveArchiveFileToRecycleLog(request, lc);
  }, m_maxTriesToConnect);
}

}  // namespace catalogue
}  // namespace cta