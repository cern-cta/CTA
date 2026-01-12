/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"

#include "cta_admin.pb.h"

namespace cta::frontend {

class VirtualOrganizationLsResponseStream final : public CtaAdminResponseStream {
public:
  VirtualOrganizationLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                      cta::Scheduler& scheduler,
                                      const std::string& instanceName);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::vector<cta::common::dataStructures::VirtualOrganization> m_virtualOrganizations;
  std::size_t m_virtualOrganizationsIdx = 0;
};

}  // namespace cta::frontend
