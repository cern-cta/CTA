/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "cta_admin.pb.h"

#include <list>

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
  std::list<cta::xrd::Data> m_items;

  // Helper methods
  void collectArchiveJobs(const std::optional<std::string>& tapepool);
  void collectRetrieveJobs(const std::optional<std::string>& vid);
  void collectSummaryData(bool hasArchive, bool hasRetrieve);
};

}  // namespace cta::frontend
