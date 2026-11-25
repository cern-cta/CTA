/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "rdbms/ConnPool.hpp"
#include "scheduler/rdbms/JobPool.hpp"
#include "scheduler/rdbms/postgres/RetrieveJobQueue.hpp"

#include <cstdint>
#include <list>
#include <memory>
#include <optional>
#include <string>
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
  explicit RetrieveRdbJob(rdbms::ConnPool& connPool, const rdbms::Rset& rset, bool rowFromRepack);

  /**
   * Not supported under pgsched: RecallReportPacker never calls this - it accumulates failures
   * via prepareFailTransfer() and flushes them through RetrieveMount::failTransferBatch()
   * instead. Kept only because SchedulerDatabase::RetrieveJob declares it unconditionally (the
   * objectstore backend's implementation is the real one). Throws
   * cta::exception::NotImplementedException.
   */
  void failTransfer(const std::string& failureReason, log::LogContext& lc) override;

#ifdef CTA_PGSCHED
  /**
   * Prepares the in-memory job state for batch failure reporting.
   * Updates failure log and retry counts without touching the DB.
   * Also rotates the VID to an alternate copy if within-mount retries are exhausted.
   *
   * @param failureReason  The reason of the failure as string
   */
  void prepareFailTransfer(const std::string& failureReason) override;

  /**
   * Prepares the in-memory job state for batch report-failure handling.
   * Updates reportFailureLogs and increments totalReportRetries without DB access.
   *
   * @param failureReason  The reason of the failure as string
   */
  void prepareFailReport(const std::string& failureReason) override;
#endif

  /**
   * Not supported under pgsched: RecallReportPacker never calls this - it accumulates report
   * failures via prepareFailReport() and flushes them through
   * RelationalDB::failRetrieveJobBatchReport() instead. Kept only because
   * SchedulerDatabase::RetrieveJob declares it unconditionally (the objectstore backend's
   * implementation is the real one). Throws cta::exception::NotImplementedException.
   */
  void failReport(const std::string& failureReason, log::LogContext& lc) override;

  /**
   * Reinitialise the job object data members with
   * new values after it has been poped from the pool
   *
   * @param connPool
   * @param rset
   */
  void initialize(const rdbms::Rset& rset, bool jobIsRepack) final;

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

  postgres::RetrieveJobQueueRow m_jobRow;  // Job data is encapsulated in this member
  bool m_jobOwned = false;
  uint64_t m_mountId = 0;
  std::string m_tapePool = "";
  rdbms::ConnPool& m_connPool;

  void abort(const std::string& abortReason, log::LogContext& lc) override;
  void asyncSetSuccessful() override;
  void fail() override;

private:
  std::shared_ptr<JobPool<RetrieveRdbJob>> m_pool = nullptr;
};

}  // namespace cta::schedulerdb
