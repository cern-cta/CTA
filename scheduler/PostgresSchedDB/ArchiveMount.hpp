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
#include "scheduler/PostgresSchedDB/sql/Transaction.hpp"
#include "scheduler/PostgresSchedDB/sql/Enums.hpp"
#include "scheduler/PostgresSchedDB/PostgresSchedDB.hpp"

#include <list>
#include <memory>
#include <optional>
#include <cstdint>
#include <time.h>


namespace cta::postgresscheddb {

class TapeMountDecisionInfo;

class ArchiveMount : public SchedulerDatabase::ArchiveMount {
 friend class cta::PostgresSchedDB;
 friend class TapeMountDecisionInfo;
 public:

   ArchiveMount(PostgresSchedDB &pdb, const std::string& ownerId, Transaction& txn, common::dataStructures::JobQueueType queueType) :
                m_PostgresSchedDB(pdb), m_ownerId(ownerId), m_txn(txn), m_queueType(queueType) { }

   const MountInfo & getMountInfo() override;

   std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> getNextJobBatch(uint64_t filesRequested,
      uint64_t bytesRequested, log::LogContext& logContext) override;

   void setDriveStatus(common::dataStructures::DriveStatus status, common::dataStructures::MountType mountType,
                       time_t completionTime, const std::optional<std::string>& reason = std::nullopt) override;

   void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) override;

   void setJobBatchTransferred(
      std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> & jobsBatch, log::LogContext & lc) override;

private:

   cta::PostgresSchedDB& m_PostgresSchedDB;
   const std::string& m_ownerId;
   Transaction& m_txn;
   common::dataStructures::JobQueueType m_queueType;
};

} // namespace cta::postgresscheddb
