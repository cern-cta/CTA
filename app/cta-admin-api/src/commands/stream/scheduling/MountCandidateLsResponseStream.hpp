/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "commands/stream/CtaAdminResponseStream.hpp"
#include "mountdecision/MountDecisionDB.hpp"

#include <scheduler/Scheduler.hpp>

#include "cta_admin.pb.h"

namespace cta::admin_api::commands::stream {

/// Stream the mount candidates calculated by maintd for tape servers to reserve (PostgreSQL scheduler only).
class MountCandidateLsResponseStream final : public CtaAdminResponseStream {
public:
  MountCandidateLsResponseStream(cta::Scheduler& scheduler,
                                 cta::mountdecision::MountDecisionDB& mountDecisionDb,
                                 const std::string& instanceName,
                                 cta::log::LogContext& lc,
                                 cta::admin::HeaderType headerType);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::string m_schedulerBackendName;
  std::vector<cta::mountdecision::MountCandidateRecord> m_mountCandidateRecords;
  std::size_t m_mountCandidateRecordsIdx = 0;
};

}  // namespace cta::admin_api::commands::stream
