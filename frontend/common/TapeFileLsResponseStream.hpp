/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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

#include <list>
#include "cta_admin.pb.h"

#include "CtaAdminResponseStream.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "common/dataStructures/ArchiveFile.hpp"

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
  std::list<common::dataStructures::TapeFile>::const_iterator m_currentTapeFileIter;
  std::list<common::dataStructures::TapeFile>::const_iterator m_currentTapeFileEnd;

  void validateSearchCriteria(bool hasAnySearchOption) const;
  void populateDataItem(cta::xrd::Data& data,
                        const common::dataStructures::ArchiveFile& archiveFile,
                        const common::dataStructures::TapeFile& tapeFile) const;
};

}  // namespace cta::frontend