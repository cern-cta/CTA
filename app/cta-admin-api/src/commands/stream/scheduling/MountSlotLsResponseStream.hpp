/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "commands/stream/CtaAdminResponseStream.hpp"
#include "scheduler/SchedulerDatabase.hpp"

#include <scheduler/Scheduler.hpp>

#ifdef CTA_PGSCHED
#include "mountdecision/MountDecisionDB.hpp"
#endif

#include "cta_admin.pb.h"

namespace cta::admin_api::commands::stream {

/// Stream the mount slots calculated by maintd for tape servers to reserve; requires the PostgreSQL scheduler.
class MountSlotLsResponseStream final : public CtaAdminResponseStream {
public:
  MountSlotLsResponseStream(cta::Scheduler& scheduler,
                            cta::SchedulerDatabase& schedulerDb,
                            const std::string& instanceName,
                            cta::log::LogContext& lc,
                            cta::admin::HeaderType headerType);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::string m_schedulerBackendName;
#ifdef CTA_PGSCHED
  std::vector<cta::mountdecision::MountSlot> m_mountSlots;
#endif
  std::size_t m_mountSlotsIdx = 0;
};

}  // namespace cta::admin_api::commands::stream
