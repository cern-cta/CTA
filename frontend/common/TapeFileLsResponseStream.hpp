/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "common/dataStructures/ArchiveFile.hpp"

#include <list>

#include "cta_admin.pb.h"

namespace cta::frontend {

class TapeFileLsResponseStream : public CtaAdminResponseStream {
public:
  TapeFileLsResponseStream(cta::catalogue::Catalogue& catalogue,
                           cta::Scheduler& scheduler,
                           const std::string& instanceName,
                           const admin::AdminCmd& adminCmd);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  catalogue::ArchiveFileItor m_tapeFileItor;
  std::optional<common::dataStructures::ArchiveFile> m_currentArchiveFile;
  std::vector<common::dataStructures::TapeFile>::const_iterator m_currentTapeFileIter;
  std::vector<common::dataStructures::TapeFile>::const_iterator m_currentTapeFileEnd;

  void validateSearchCriteria(bool hasAnySearchOption) const;
  void populateDataItem(cta::xrd::Data& data,
                        const common::dataStructures::ArchiveFile& archiveFile,
                        const common::dataStructures::TapeFile& tapeFile) const;
};

}  // namespace cta::frontend
