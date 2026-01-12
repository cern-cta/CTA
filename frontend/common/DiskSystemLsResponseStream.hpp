/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "disk/DiskSystem.hpp"

#include "cta_admin.pb.h"

namespace cta::frontend {

class DiskSystemLsResponseStream final : public CtaAdminResponseStream {
public:
  DiskSystemLsResponseStream(cta::catalogue::Catalogue& catalogue,
                             cta::Scheduler& scheduler,
                             const std::string& instanceName);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  cta::disk::DiskSystemList m_diskSystems;
  std::size_t m_diskSystemsIdx = 0;
};

}  // namespace cta::frontend
