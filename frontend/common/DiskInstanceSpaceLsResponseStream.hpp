/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/DiskInstanceSpace.hpp"

#include "cta_admin.pb.h"

namespace cta::frontend {

class DiskInstanceSpaceLsResponseStream final : public CtaAdminResponseStream {
public:
  DiskInstanceSpaceLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                    cta::Scheduler& scheduler,
                                    const std::string& instanceName);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::vector<cta::common::dataStructures::DiskInstanceSpace> m_diskInstanceSpaces;
  std::size_t m_diskInstanceSpacesIdx = 0;
};

}  // namespace cta::frontend
