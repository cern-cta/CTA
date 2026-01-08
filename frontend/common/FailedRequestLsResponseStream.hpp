/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"

#include <list>

#include "cta_admin.pb.h"

namespace cta::frontend {

class FailedRequestLsResponseStream final : public CtaAdminResponseStream {
public:
  FailedRequestLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                cta::Scheduler& scheduler,
                                const std::string& instanceName,
                                const admin::AdminCmd& adminCmd,
                                SchedulerDatabase& schedDb,
                                cta::log::LogContext& lc);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  SchedulerDatabase& m_schedDb;
  cta::log::LogContext& m_lc;

  // Configuration options
  bool m_isSummary = false;
  bool m_isLogEntries = false;
  std::optional<std::string> m_schedulerBackendName;

  // Data storage
  std::list<cta::xrd::Data> m_summaryData;  // Only 3 items max
  std::unique_ptr<SchedulerDatabase::IArchiveJobQueueItor> m_archiveJobQueueItorPtr;
  std::unique_ptr<SchedulerDatabase::IRetrieveJobQueueItor> m_retrieveJobQueueItorPtr;

  // Helper methods
  cta::xrd::Data getNextArchiveJobsData();
  cta::xrd::Data getNextRetrieveJobsData();

  void collectSummaryData(bool hasArchive, bool hasRetrieve);
};

}  // namespace cta::frontend
