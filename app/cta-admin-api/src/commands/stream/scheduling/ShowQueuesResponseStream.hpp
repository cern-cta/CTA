/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "commands/stream/CtaAdminResponseStream.hpp"
#include "common/dataStructures/QueueAndMountSummary.hpp"

#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_admin.pb.h"

namespace cta::admin_api::commands::stream {

class ShowQueuesResponseStream final : public CtaAdminResponseStream {
public:
  ShowQueuesResponseStream(cta::Scheduler& scheduler,
                           const std::string& instanceName,
                           cta::log::LogContext& lc,
                           cta::admin::HeaderType headerType);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  cta::Scheduler& m_scheduler;
  cta::log::LogContext& m_lc;
  std::optional<std::string> m_schedulerBackendName;
  std::vector<cta::common::dataStructures::QueueAndMountSummary> m_queuesAndMounts;
  std::size_t m_queuesAndMountsIdx = 0;
};

}  // namespace cta::admin_api::commands::stream
