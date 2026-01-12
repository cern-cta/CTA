/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/QueueAndMountSummary.hpp"

#include "cta_admin.pb.h"

namespace cta::frontend {

class ShowQueuesResponseStream final : public CtaAdminResponseStream {
public:
  ShowQueuesResponseStream(cta::catalogue::Catalogue& catalogue,
                           cta::Scheduler& scheduler,
                           const std::string& instanceName,
                           cta::log::LogContext& lc);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  cta::log::LogContext& m_lc;
  std::optional<std::string> m_schedulerBackendName;
  std::vector<cta::common::dataStructures::QueueAndMountSummary> m_queuesAndMounts;
  std::size_t m_queuesAndMountsIdx = 0;
};

}  // namespace cta::frontend
