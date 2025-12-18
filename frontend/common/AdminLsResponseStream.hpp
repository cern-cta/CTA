/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/AdminUser.hpp"

#include <list>

#include "cta_admin.pb.h"

namespace cta::frontend {

class AdminLsResponseStream final : public CtaAdminResponseStream {
public:
  AdminLsResponseStream(cta::catalogue::Catalogue& catalogue,
                        cta::Scheduler& scheduler,
                        const std::string& instanceName);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::list<cta::common::dataStructures::AdminUser> m_adminUsers;
};

}  // namespace cta::frontend
