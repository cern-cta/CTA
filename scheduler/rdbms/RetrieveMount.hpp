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
#include "common/dataStructures/DiskSpaceReservationRequest.hpp"
#include "scheduler/rdbms/RetrieveRdbJob.hpp"
#include "scheduler/rdbms/postgres/Enums.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"
#include "scheduler/rdbms/JobPool.hpp"

#include <list>
#include <memory>
#include <optional>
#include <string>
#include <cstdint>
#include <time.h>

namespace cta::schedulerdb {

class RetrieveMount : public SchedulerDatabase::RetrieveMount {
  friend class cta::RelationalDB;
  friend class TapeMountDecisionInfo;

public:
  //RetrieveMount(const std::string& ownerId, Transaction& txn, const std::string &vid) :
  //   , m_txn(txn), m_vid(vid) { }
  RetrieveMount(RelationalDB& pdb) : m_RelationalDB(pdb), m_connPool(pdb.m_connPool), m_jobPool(pdb.m_connPool) {}

  const MountInfo& getMountInfo() override;

  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>
  getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested, log::LogContext& logContext) override;

  bool reserveDiskSpace(const cta::DiskSpaceReservationRequest& diskSpaceReservationRequest,
                        const std::string& externalFreeDiskSpaceScript,
                        log::LogContext& logContext) override;

  bool testReserveDiskSpace(const cta::DiskSpaceReservationRequest& request,
                            const std::string& externalFreeDiskSpaceScript,
                            log::LogContext& logContext) override;

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
                      const std::optional<std::string>& reason = std::nullopt) override;

  void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats& stats) override;

  void flushAsyncSuccessReports(std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>& jobsBatch,
                                log::LogContext& lc) override;

  //------------------------------------------------------------------------------
  // for compliance with OStoreDB only
  //------------------------------------------------------------------------------
  void flushAsyncSuccessReports(std::list<cta::SchedulerDatabase::RetrieveJob*>& jobsBatch,
                                cta::log::LogContext& lc) override {};

  void addDiskSystemToSkip(const DiskSystemToSkip& diskSystemToSkip) override;

  void
  putQueueToSleep(const std::string& diskSystemName, const uint64_t sleepTime, log::LogContext& logContext) override;

private:
  cta::RelationalDB& m_RelationalDB;
  cta::rdbms::ConnPool& m_connPool;
  schedulerdb::JobPool<schedulerdb::RetrieveRdbJob> m_jobPool;
};

}  // namespace cta::schedulerdb
