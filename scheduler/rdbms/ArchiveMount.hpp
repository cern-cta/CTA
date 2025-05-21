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
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/MountType.hpp"
#include "scheduler/rdbms/ArchiveRdbJob.hpp"
#include "scheduler/rdbms/postgres/Enums.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"
#include "scheduler/rdbms/JobPool.hpp"

#include <list>
#include <memory>
#include <optional>
#include <cstdint>
#include <time.h>

namespace cta::schedulerdb {

class TapeMountDecisionInfo;

class ArchiveMount : public SchedulerDatabase::ArchiveMount {
  friend class cta::RelationalDB;
  friend class TapeMountDecisionInfo;

public:
  ArchiveMount(RelationalDB& pdb, const std::string& ownerId, common::dataStructures::JobQueueType queueType)
      : m_RelationalDB(pdb),
        m_connPool(pdb.m_connPool),
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

  /*
   * Set the status of the drive
   *
   * @param status           The drive status
   * @param mountType       The type of the mount
   * @param completionTime  The time the operation took it took
   * @param reason          An optional parameter containing the reason fo the drive status change
   *
   * @return void
   */
  void setDriveStatus(common::dataStructures::DriveStatus status,
                      common::dataStructures::MountType mountType,
                      time_t completionTime,
                      const std::optional<std::string>& reason = std::nullopt) override;

  /*
   * Set Tape session statistics summary
   *
   * @param stats       The TapeSessionStats object
   *
   * @return void
   */
  void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats& stats) override;

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
  /**
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

private:
  cta::RelationalDB& m_RelationalDB;
  cta::rdbms::ConnPool& m_connPool;
  const std::string& m_ownerId;
  common::dataStructures::JobQueueType m_queueType;
  std::shared_ptr<schedulerdb::JobPool<schedulerdb::ArchiveRdbJob>> m_jobPool;

  void recycleTransferredJobs(std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>& jobsBatch, log::LogContext& lc);
};

}  // namespace cta::schedulerdb
