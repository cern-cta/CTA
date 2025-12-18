/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/RequesterActivityMountRule.hpp"

#include <list>

#include "cta_admin.pb.h"

namespace cta::frontend {

class ActivityMountRuleLsResponseStream final : public CtaAdminResponseStream {
public:
  ActivityMountRuleLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                    cta::Scheduler& scheduler,
                                    const std::string& instanceName);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::list<cta::common::dataStructures::RequesterActivityMountRule> m_activityMountRules;
};

}  // namespace cta::frontend
