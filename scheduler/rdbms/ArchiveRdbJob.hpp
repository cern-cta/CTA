/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "rdbms/ConnPool.hpp"
#include "scheduler/rdbms/JobPool.hpp"
#include "scheduler/rdbms/postgres/ArchiveJobQueue.hpp"

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

class ArchiveRdbJob final : public SchedulerDatabase::ArchiveJob {
  friend class cta::RelationalDB;

public:
  // Constructor to convert ArchiveJobQueueRow to ArchiveRdbJob
  explicit ArchiveRdbJob(rdbms::ConnPool& connPool, const rdbms::Rset& rset, bool rowFromRepack);

  // Constructor to create empty ArchiveJob object with a reference to the connection pool
  explicit ArchiveRdbJob(rdbms::ConnPool& connPool);

  /**
   * Not supported under pgsched: MigrationReportPacker/RecallReportPacker never call this -
   * they accumulate failures via prepareFailTransfer() and flush them through
   * ArchiveMount::failTransferBatch() instead. Kept only because SchedulerDatabase::ArchiveJob
   * declares it unconditionally (the objectstore backend's implementation is the real one).
   * Throws cta::exception::NotImplementedException.
   */
  void failTransfer(const std::string& failureReason, log::LogContext& lc) override;

  /**
   * Updates in-memory retry counters and failure log for a subsequent failTransferBatch()
   * call. Called from ArchiveMount::failTransferBatch() on each job before the batch SQL
   * runs - either for accumulated transfer failures or for jobs that failed catalogue
   * update (FailedReportCatalogueUpdate).
   */
  void prepareFailTransfer(const std::string& failureReason) override;

  /**
   * Same as prepareFailTransfer but for the report-failure path: increments
   * totalReportRetries and appends to reportFailureLogs. Called from
   * ArchiveMount::failReportBatch() on each job after FailedReportMoveToQueue is thrown.
   */
  void prepareFailReport(const std::string& failureReason) override;

  /**
   * Not supported under pgsched: MigrationReportPacker/RecallReportPacker never call this -
   * they accumulate report failures via prepareFailReport() and flush them through
   * ArchiveMount::failReportBatch() instead. Kept only because SchedulerDatabase::ArchiveJob
   * declares it unconditionally (the objectstore backend's implementation is the real one).
   * Throws cta::exception::NotImplementedException.
   */
  void failReport(const std::string& failureReason, log::LogContext& lc) override;

  /**
  * Currently unused function throwing an exception
  */
  void bumpUpTapeFileCount(uint64_t newFileCount) override;

  /**
  * Reinitialise the job object data members with
  * new values after it has been poped from the pool
  *
  * @param connPool
  * @param rset
  */
  void initialize(const rdbms::Rset& rset, bool jobIsRepack) final;

  /**
  * @brief Returns the job instance back to its originating JobPool.
  *
  * This method should be called instead of deleting the object when the job
  * is no longer needed. It enables object reuse by placing the job back into
  * the pool, avoiding unnecessary allocations.
  *
  * @note After calling releaseToPool(), the caller must not use or delete the object.
  *       It is the caller's responsibility to also call .release() on any
  *       std::unique_ptr managing this object to prevent double-deletion.
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
  void setPool(std::shared_ptr<JobPool<ArchiveRdbJob>> pool) { m_pool = pool; }

  /**
  * Reset all data members to return the job object to the pool
  */
  void reset();

  postgres::ArchiveJobQueueRow m_jobRow;  // Job data is encapsulated in this member
  bool m_jobOwned = false;
  uint64_t m_mountId = 0;
  std::string m_tapePool;
  rdbms::ConnPool& m_connPool;

private:
  std::shared_ptr<JobPool<ArchiveRdbJob>> m_pool = nullptr;
};

}  // namespace cta::schedulerdb
