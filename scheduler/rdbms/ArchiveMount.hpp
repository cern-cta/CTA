/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/log/LogContext.hpp"
#include "scheduler/rdbms/ArchiveRdbJob.hpp"
#include "scheduler/rdbms/JobPool.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"
#include "scheduler/rdbms/postgres/Enums.hpp"

#include <atomic>
#include <cstdint>
#include <list>
#include <memory>
#include <optional>
#include <time.h>
#include <vector>

namespace cta::schedulerdb {

class TapeMountDecisionInfo;

class ArchiveMount : public SchedulerDatabase::ArchiveMount {
  friend class cta::RelationalDB;
  friend class TapeMountDecisionInfo;

public:
  ArchiveMount(RelationalDB& rdb_instance, const std::string& ownerId, common::dataStructures::JobQueueType queueType)
      : m_RelationalDB(rdb_instance),
        m_connPool(rdb_instance.m_connPool),
        m_ownerId(ownerId),
        m_queueType(queueType) {
    m_jobPool = std::make_shared<schedulerdb::JobPool<schedulerdb::ArchiveRdbJob>>(m_connPool);
  }

  const MountInfo& getMountInfo() override;

  /*
   * Fetch next bach of jobs from the Scheduler DB backend
   *
   * @param filesRequested  The maximum number of jobs to fetch
   * @param bytesRequested  The maximum number of bytes the files in these jobs can sum up to
   * @param logContext      The logging context
   *
   * @return list of pointers to SchedulerDatabase::ArchiveJob objects
   */
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>
  getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested, log::LogContext& logContext) override;

  /// Publish activity with this mount's metadata, without changing desired state.
  void reportTapeSessionState(common::dataStructures::TapeSessionState state, time_t reportTime) override;

  /*
   * Set Tape session statistics summary
   *
   * @param stats       The tape-side session statistics
   *
   * @return void
   */
  void setTapeSessionStats(const cta::tape::daemon::TapeTransferStats& stats) override;

  /*
   * Setting a batch of jobs to state which informs
   * the Scheduler that they are ready for reporting
   *
   * @param jobsBatch  The list of SchedulerDatabase::ArchiveJob objects
   * @param lc         The log context
   *
   * @return void
   */
  void setJobBatchTransferred(std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>& jobsBatch,
                              log::LogContext& lc) override;

  /*
   * Re-queue batch of jobs
   * Serves PGSCHED purpose only
   * This method does not update retry job statistics on purpose.
   * It is used after exception during tape write to requeue last batch for which flush() could not be called.
   * Second use-case is to clean up the task queue for tasks which no longer can be processed (due to exception thrown)
   *
   * @param jobIDsList
   * @return number of jobs re-queued in the DB
   */
  uint64_t requeueJobBatch(const std::list<std::string>& jobIDsList, cta::log::LogContext& logContext) const override;
  void failTransferBatch(std::vector<cta::SchedulerDatabase::ArchiveJob*>& batch, log::LogContext& lc) override;

  /**
   * Batch-fail reporting for jobs whose catalogue write succeeded but whose scheduler-DB
   * state update failed (FailedReportMoveToQueue path).  prepareFailReport() must have
   * been called on each job before this method to update in-memory retry counters.
   * Jobs that have exhausted report retries (or need no report) are moved to
   * ARCHIVE_FAILED_QUEUE; the rest have IS_REPORTING reset so they will be retried.
   */
  void failReportBatch(std::vector<cta::SchedulerDatabase::ArchiveJob*>& batch, log::LogContext& lc) override;
  void setIsRepack(log::LogContext& logContext);

private:
  cta::RelationalDB& m_RelationalDB;
  cta::rdbms::ConnPool& m_connPool;
  const std::string& m_ownerId;
  common::dataStructures::JobQueueType m_queueType;
  std::shared_ptr<schedulerdb::JobPool<schedulerdb::ArchiveRdbJob>> m_jobPool;
  bool m_isRepack = false;

  void recycleTransferredJobs(std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>& jobsBatch,
                              log::LogContext& lc);

  /*
   * Refreshes MOUNT_QUEUE_LAST_FETCH for this mount so the dead-mount reclaim routine never
   * mistakes it for inactive while still fetching OR reporting. Called from getNextJobBatch()
   * after fetch, and from setJobBatchTransferred()/setRepackJobBatchTransferred() after a
   * successful report. Runs in its own transaction: piggybacking onto the caller's transaction
   * would overwrite its query-summary/row-count telemetry (see updateMountQueueLastFetch()).
   * Non-fatal on failure.
   *
   * Skips the round-trip if called again within 5s (tracked by m_lastHeartbeatRefreshEpoch) -
   * the UPSERT's own WHERE clause already makes that call a server-side no-op, but finding that
   * out still costs a full round-trip. Matters at scale: small flushMaxFiles/fetchMaxFiles make
   * a fetch and its following report land close together often enough for this to add up.
   */
  void refreshMountLivenessHeartbeat(log::LogContext& lc);

  // Epoch-second of this mount's last heartbeat attempt (0 = never), set BEFORE the attempt so
  // near-simultaneous callers from different threads only ever race to skip, never to double-call.
  std::atomic<std::time_t> m_lastHeartbeatRefreshEpoch {0};
  /*
   * Setting a batch of REPACK mount jobs to state which informs
   * the Scheduler that they are ready for reporting
   *
   * @param jobsBatch  The list of SchedulerDatabase::ArchiveJob objects
   * @param lc         The log context
   *
   * @return void
   */
  void setRepackJobBatchTransferred(std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>& jobsBatch,
                                    log::LogContext& lc);
};

}  // namespace cta::schedulerdb
