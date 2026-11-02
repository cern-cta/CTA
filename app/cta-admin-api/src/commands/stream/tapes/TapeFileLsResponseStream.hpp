/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/CatalogueItor.hpp"
#include "commands/stream/CtaAdminResponseStream.hpp"
#include "common/dataStructures/ArchiveFile.hpp"

#include <catalogue/Catalogue.hpp>
#include <list>
#include <scheduler/Scheduler.hpp>

#include "cta_admin.pb.h"

namespace cta::admin_api::commands::stream {

class TapeFileLsResponseStream : public CtaAdminResponseStream {
public:
  TapeFileLsResponseStream(cta::catalogue::Catalogue& catalogue,
                           const std::string& instanceName,
                           const admin::AdminCmd& adminCmd,
                           cta::admin::HeaderType headerType);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  cta::catalogue::Catalogue& m_catalogue;
  catalogue::ArchiveFileItor m_tapeFileItor;
  std::optional<common::dataStructures::ArchiveFile> m_currentArchiveFile;
  std::vector<common::dataStructures::TapeFile>::const_iterator m_currentTapeFileIter;
  std::vector<common::dataStructures::TapeFile>::const_iterator m_currentTapeFileEnd;

  void validateSearchCriteria(bool hasAnySearchOption) const;
  void populateDataItem(cta::xrd::Data& data,
                        const common::dataStructures::ArchiveFile& archiveFile,
                        const common::dataStructures::TapeFile& tapeFile) const;
};

}  // namespace cta::admin_api::commands::stream
