/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "catalogue/interfaces/ArchiveFileCatalogue.hpp"

namespace cta::catalogue {

class DummyArchiveFileCatalogue : public ArchiveFileCatalogue {
public:
  DummyArchiveFileCatalogue() = default;
  ~DummyArchiveFileCatalogue() override = default;

  uint64_t checkAndGetNextArchiveFileId(const std::string &diskInstanceName, const std::string &storageClassName,
    const common::dataStructures::RequesterIdentity &user) override;

  common::dataStructures::ArchiveFileQueueCriteria getArchiveFileQueueCriteria(const std::string &diskInstanceName,
    const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) override;

  ArchiveFileItor getArchiveFilesItor(
    const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const override;

  common::dataStructures::ArchiveFile getArchiveFileCopyForDeletion(
    const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const override;

  std::list<common::dataStructures::ArchiveFile> getFilesForRepack(const std::string &vid, const uint64_t startFSeq,
    const uint64_t maxNbFiles) const override;

  ArchiveFileItor getArchiveFilesForRepackItor(const std::string &vid, const uint64_t startFSeq) const override;

  common::dataStructures::ArchiveFileSummary getTapeFileSummary(
    const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const override;

  common::dataStructures::ArchiveFile getArchiveFileById(const uint64_t id) const override;

  void modifyArchiveFileStorageClassId(const uint64_t archiveFileId,
    const std::string &newStorageClassName) const override;

  void modifyArchiveFileFxIdAndDiskInstance(const uint64_t archiveId, const std::string& fxId,
    const std::string &diskInstance) const override;

  void moveArchiveFileToRecycleLog(const common::dataStructures::DeleteArchiveRequest &request,
    log::LogContext & lc) override;\

  void updateDiskFileId(uint64_t archiveFileId, const std::string &diskInstance,
    const std::string &diskFileId) override;

  void DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string &diskInstanceName, const uint64_t archiveFileId,
    log::LogContext &lc) override;
};

} // namespace cta::catalogue
