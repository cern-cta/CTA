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

#pragma once

#include <memory>

#include "catalogue/interfaces/ArchiveFileCatalogue.hpp"

namespace cta {

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class ArchiveFileCatalogueRetryWrapper : public ArchiveFileCatalogue {
public:
  ArchiveFileCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
                                   log::Logger& m_log,
                                   const uint32_t maxTriesToConnect);
  ~ArchiveFileCatalogueRetryWrapper() override = default;

  uint64_t checkAndGetNextArchiveFileId(const std::string& diskInstanceName,
                                        const std::string& storageClassName,
                                        const common::dataStructures::RequesterIdentity& user) override;

  common::dataStructures::ArchiveFileQueueCriteria
    getArchiveFileQueueCriteria(const std::string& diskInstanceName,
                                const std::string& storageClassName,
                                const common::dataStructures::RequesterIdentity& user) override;

  ArchiveFileItor
    getArchiveFilesItor(const TapeFileSearchCriteria& searchCriteria = TapeFileSearchCriteria()) const override;

  common::dataStructures::ArchiveFile
    getArchiveFileForDeletion(const TapeFileSearchCriteria& searchCriteria = TapeFileSearchCriteria()) const override;

  std::list<common::dataStructures::ArchiveFile>
    getFilesForRepack(const std::string& vid, const uint64_t startFSeq, const uint64_t maxNbFiles) const override;

  ArchiveFileItor getArchiveFilesForRepackItor(const std::string& vid, const uint64_t startFSeq) const override;

  common::dataStructures::ArchiveFileSummary
    getTapeFileSummary(const TapeFileSearchCriteria& searchCriteria = TapeFileSearchCriteria()) const override;

  common::dataStructures::ArchiveFile getArchiveFileById(const uint64_t id) const override;

  void modifyArchiveFileStorageClassId(const uint64_t archiveFileId,
                                       const std::string& newStorageClassName) const override;

  void modifyArchiveFileFxIdAndDiskInstance(const uint64_t archiveId,
                                            const std::string& fxId,
                                            const std::string& diskInstance) const override;

  void moveArchiveFileToRecycleLog(const common::dataStructures::DeleteArchiveRequest& request,
                                   log::LogContext& lc) override;

  void
    updateDiskFileId(uint64_t archiveFileId, const std::string& diskInstance, const std::string& diskFileId) override;

  void DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string& diskInstanceName,
                                               const uint64_t archiveFileId,
                                               log::LogContext& lc) override;

private:
  const std::unique_ptr<Catalogue>& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};  // class SchemaCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta