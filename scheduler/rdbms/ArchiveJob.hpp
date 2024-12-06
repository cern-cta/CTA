/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include "scheduler/rdbms/RelationalDB.hpp"
#include "common/log/LogContext.hpp"

#include <list>
#include <memory>
#include <optional>
#include <string>
#include <cstdint>
#include <time.h>

namespace cta::schedulerdb {

class ArchiveJob : public SchedulerDatabase::ArchiveJob {
  friend class cta::RelationalDB;

public:
  ArchiveJob();
  ArchiveJob(bool jobOwned, uint64_t jid, uint64_t mountID, std::string_view tapePool);

  /*
   * Sets the status of the job as failed in the Scheduler DB
   *
   * @param failureReason  The reason of the failure as string
   * @param lc             The log context
   *
   * @return void
   */
  void failTransfer(const std::string& failureReason, log::LogContext& lc) override;

  /*
   * Sets the status of the report of the archive job to failed in Scheduler DB
   *
   * @param failureReason   The failure reason as string
   * @param lc              The log context
   *
   * @return void
   */
  void failReport(const std::string& failureReason, log::LogContext& lc) override;

  /*
   * Currently unused function throwing an exception
   */
  void bumpUpTapeFileCount(uint64_t newFileCount) override;

  bool m_jobOwned = false;
  uint64_t m_mountId = 0;
  std::string m_tapePool;
};

}  // namespace cta::schedulerdb
