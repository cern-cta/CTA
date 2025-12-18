/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "catalogue/TapePool.hpp"

#include <list>

#include "cta_admin.pb.h"

namespace cta::frontend {

class TapePoolLsResponseStream final : public CtaAdminResponseStream {
public:
  TapePoolLsResponseStream(cta::catalogue::Catalogue& catalogue,
                           cta::Scheduler& scheduler,
                           const std::string& instanceName,
                           const admin::AdminCmd& adminCmd);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::list<cta::catalogue::TapePool> m_tapePools;
  std::list<cta::catalogue::TapePool> buildTapePoolList(const admin::AdminCmd& admincmd);
};

}  // namespace cta::frontend
