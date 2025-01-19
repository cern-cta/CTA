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

#include "common/log/LogContext.hpp"
#include "rdbms/ConnPool.hpp"
#include "scheduler/rdbms/postgres/RetrieveJobQueue.hpp"

#include <list>
#include <memory>
#include <optional>
#include <string>
#include <cstdint>
#include <time.h>

namespace cta {
class RelationalDB;
}

namespace cta::schedulerdb {

class RetrieveRdbJob : public SchedulerDatabase::RetrieveJob {
  friend class cta::RelationalDB;

public:
  // Constructor to convert RetrieveJobQueueRow to RetrieveRdbJob
  explicit RetrieveRdbJob(rdbms::ConnPool& connPool, const rdbms::Rset& rset);

  // Constructor to create empty RetrieveJob object with a reference to the connection pool
  explicit RetrieveRdbJob(rdbms::ConnPool& connPool);

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
   * Sets the status of the report of the retrieve job to failed in Scheduler DB
   *
   * @param failureReason   The failure reason as string
   * @param lc              The log context
   *
   * @return void
   */
  void failReport(const std::string& failureReason, log::LogContext& lc) override;

  /**
   * Reinitialise the job object data members with
   * new values after it has been poped from the pool
   *
   * @param connPool
   * @param rset
   */
  void initialize(const rdbms::Rset& rset, log::LogContext& lc) override;
  /**
   * Reset all data members to return the job object to the pool
   */
  void reset();

  postgres::RetrieveJobQueueRow m_jobRow;  // Job data is encapsulated in this member
  bool m_jobOwned = false;
  uint64_t m_mountId = 0;
  std::string m_tapePool;
  rdbms::ConnPool& m_connPool;
  //std::shared_ptr<rdbms::Conn> m_conn;

  void abort(const std::string& abortReason, log::LogContext& lc) override;
  void asyncSetSuccessful() override;
  void fail() override;
};


}  // namespace cta::schedulerdb
