/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "rdbms/ConnPool.hpp"
#include "scheduler/rdbms/postgres/RetrieveJobQueue.hpp"
#include "scheduler/rdbms/JobPool.hpp"

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

class RetrieveRdbJob final : public SchedulerDatabase::RetrieveJob {
  friend class cta::RelationalDB;

public:

  // Constructor to create empty RetrieveJob object with a reference to the connection pool
  explicit RetrieveRdbJob(rdbms::ConnPool& connPool);
  explicit RetrieveRdbJob(rdbms::ConnPool& connPool, const rdbms::Rset& rset);

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
  void initialize(const rdbms::Rset& rset) final;

  /**
   * @brief Returns this job instance back to its originating pool.
   *
   *        Used when the job has completed and can be recycled.
   *        Assumes the job is exclusively owned and managed by the pool.
   */
  bool releaseToPool() override {
    if (!m_pool || !m_pool->releaseJob(this)) {
      // Pool is full, allow destruction
     return false;
    }
    return true;
  }

  /**
   * @brief Injects the pointer to the pool this job belongs to.
   *
   *        This allows the job to release itself properly once processing
   *        is complete.
   *
   * @param pool  Pointer to the corresponding job pool.
   */
  void setPool(std::shared_ptr<JobPool<RetrieveRdbJob>> pool) { m_pool = pool; }

  /**
   * Reset all data members to return the job object to the pool
   */
  void reset();

  /**
   * Handles the case when the job has exceeded its total retry limit.
   * Marks the job for failure reporting and updates its status in the database.
   *
   * @param txn The transaction context for database operations.
   * @param lc The logging context for structured logging.
   * @param reason The textual explanation for the failure.
   */
  void handleExceedTotalRetries(cta::schedulerdb::Transaction& txn, log::LogContext& lc, const std::string& reason);

  /**
   * Requeues the job to a new mount after a failure. Resets mount-related retry counters and updates the job in the DB.
   *
   * @param txn The transaction context for database operations.
   * @param lc The logging context for structured logging.
   * @param reason The textual explanation for the failure.
   */
  void requeueToNewMount(cta::schedulerdb::Transaction& txn, log::LogContext& lc, const std::string& reason);

  /**
   * Requeues the job to the same mount after a recoverable failure.
   * Keeps the mount context but marks it for retry, updating the job in the DB.
   *
   * @param txn The transaction context for database operations.
   * @param lc The logging context for structured logging.
   * @param reason The textual explanation for the failure.
   */
  void requeueToSameMount(cta::schedulerdb::Transaction& txn, log::LogContext& lc, const std::string& reason);

  postgres::RetrieveJobQueueRow m_jobRow;  // Job data is encapsulated in this member
  bool m_jobOwned = false;
  uint64_t m_mountId = 0;
  std::string m_tapePool="";
  rdbms::ConnPool& m_connPool;
  //std::shared_ptr<rdbms::Conn> m_conn;

  void abort(const std::string& abortReason, log::LogContext& lc) override;
  void asyncSetSuccessful() override;
  void fail() override;

private:
  std::shared_ptr<JobPool<RetrieveRdbJob>> m_pool = nullptr;
};

}  // namespace cta::schedulerdb
