/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/MountPolicy.hpp"

#include <list>

#include "cta_admin.pb.h"

namespace cta::frontend {

class MountPolicyLsResponseStream final : public CtaAdminResponseStream {
public:
  MountPolicyLsResponseStream(cta::catalogue::Catalogue& catalogue,
                              cta::Scheduler& scheduler,
                              const std::string& instanceName);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::vector<common::dataStructures::MountPolicy> m_mountPolicies;
  std::size_t m_mountPoliciesIdx = 0;
};

}  // namespace cta::frontend
