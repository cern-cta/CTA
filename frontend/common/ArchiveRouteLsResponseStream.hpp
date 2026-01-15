/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"

#include "cta_admin.pb.h"

namespace cta::frontend {

class ArchiveRouteLsResponseStream final : public CtaAdminResponseStream {
public:
  ArchiveRouteLsResponseStream(cta::catalogue::Catalogue& catalogue,
                               cta::Scheduler& scheduler,
                               const std::string& instanceName);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::vector<cta::common::dataStructures::ArchiveRoute> m_archiveRoutes;
  std::size_t m_archiveRoutesIdx = 0;
};

}  // namespace cta::frontend
