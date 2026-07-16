/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "mountdecision/MountDecisionDB.hpp"
#include "scheduler/SchedulerDatabase.hpp"

#include "cta_admin.pb.h"

namespace cta::frontend {

class MountCandidateLsResponseStream final : public CtaAdminResponseStream {
public:
  MountCandidateLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                 cta::Scheduler& scheduler,
                                 cta::SchedulerDatabase& schedulerDb,
                                 const std::string& instanceName,
                                 cta::log::LogContext& lc);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::string m_schedulerBackendName;
  std::vector<cta::mountdecision::MountCandidateRecord> m_mountCandidateRecords;
  std::size_t m_mountCandidateRecordsIdx = 0;
};

}  // namespace cta::frontend
