/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/DiskSpaceReservationRequest.hpp"
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/exception/NotImplementedException.hpp"
#include "common/log/LogContext.hpp"
#include "scheduler/rdbms/JobPool.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"
#include "scheduler/rdbms/RetrieveRdbJob.hpp"
#include "scheduler/rdbms/postgres/Enums.hpp"

#include <atomic>
#include <cstdint>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <time.h>

namespace cta::schedulerdb {

class RetrieveMount : public SchedulerDatabase::RetrieveMount {
  friend class cta::RelationalDB;
  friend class TapeMountDecisionInfo;

public:
  explicit RetrieveMount(RelationalDB& rdb_instance)
      : m_RelationalDB(rdb_instance),
        m_connPool(rdb_instance.m_connPool) {
    m_jobPool = std::make_shared<schedulerdb::JobPool<schedulerdb::RetrieveRdbJob>>(m_connPool);
  }

  const MountInfo& getMountInfo() override;
  void setIsRepack(std::string_view defaultRepackVO, log::LogContext& logContext);

  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>
  getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested, log::LogContext& logContext) override;

  /**
    * Re-queue batch of jobs
    * Serves PGSCHED purpose only
    * This method purposely does not update retry statistics since used only
    * in RecallTaskInjector when testing for available disk space !
    *
    * @param jobIDsList
    * @return number of jobs re-queued in the DB
    */
  uint64_t requeueJobBatch(const std::list<std::string>& jobIDsList, cta::log::LogContext& logContext) const override;

  void requeueJobBatch(std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>& jobBatch,
                       cta::log::LogContext& logContext) override;

  void setDriveStatus(common::dataStructures::DriveStatus status,
                      common::dataStructures::MountType mountType,
                      time_t completionTime,
                      const std::optional<std::string>& reason) override;

  void setTapeSessionStats(const cta::tape::daemon::TapeSessionStats& stats) override;

  void setJobBatchTransferred(std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>& jobsBatch,
                              log::LogContext& lc) override;

  void updateRetrieveJobStatusWrapper(const std::vector<std::string>& jobIDs,
                                      cta::schedulerdb::RetrieveJobStatus newStatus,
                                      log::LogContext& lc);

  //------------------------------------------------------------------------------
  // for compliance with OStoreDB only
  //------------------------------------------------------------------------------
  void flushAsyncSuccessReports(std::list<cta::SchedulerDatabase::RetrieveJob*>& jobsBatch,
                                cta::log::LogContext& lc) override {
    throw cta::exception::NotImplementedException();
  };

  void addDiskSystemToSkip(const DiskSystemToSkip& diskSystemToSkip) override;

  void
  putQueueToSleep(const std::string& diskSystemName, const uint64_t sleepTime, log::LogContext& logContext) override;

private:
  cta::RelationalDB& m_RelationalDB;
  cta::rdbms::ConnPool& m_connPool;
  std::shared_ptr<schedulerdb::JobPool<schedulerdb::RetrieveRdbJob>> m_jobPool;
  void recycleTransferredJobs(std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>& jobsBatch,
                              log::LogContext& lc);
  bool m_isRepack = false;

  /*
   * Refresh MOUNT_QUEUE_LAST_FETCH for this mount outside of (and after) any other transaction,
   * so the dead-mount reclaim routine never mistakes this mount for inactive while it is still
   * genuinely fetching OR reporting. The single call site for both: getNextJobBatch() calls this
   * after its own fetch, and setJobBatchTransferred() calls it after a successful report. Runs in
   * its own transaction deliberately: piggybacking this UPSERT onto the caller's own transaction
   * would overwrite ITS query-summary/row-count telemetry (see updateMountQueueLastFetch()).
   * Non-fatal on failure.
   *
   * Skips the DB round-trip entirely if called again within 5s of this mount's own last attempt
   * from EITHER call site (tracked by m_lastHeartbeatRefreshEpoch below) - see
   * ArchiveMount::refreshMountLivenessHeartbeat()'s equivalent comment for the full rationale.
   */
  void refreshMountLivenessHeartbeat(log::LogContext& lc);

  // Last epoch-second this mount attempted the heartbeat UPSERT above (0 = never), set BEFORE the
  // attempt so two near-simultaneous callers racing in from different threads only ever race to
  // skip, never to double-call: the harmless outcome either way.
  std::atomic<std::time_t> m_lastHeartbeatRefreshEpoch {0};
};

}  // namespace cta::schedulerdb
