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

#include "PostgresSchedDB.hpp"
#include "common/log/LogContext.hpp"
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/dataStructures/DiskSpaceReservationRequest.hpp"

#include <list>
#include <memory>
#include <optional>
#include <string>
#include <cstdint>
#include <time.h>

namespace cta {

class PostgresSchedDB::RetrieveMount : public SchedulerDatabase::RetrieveMount {
 public:

   RetrieveMount();

   const MountInfo & getMountInfo() override;

   std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> getNextJobBatch(uint64_t filesRequested,
     uint64_t bytesRequested, log::LogContext& logContext) override;

   bool reserveDiskSpace(const cta::DiskSpaceReservationRequest &request,
      const std::string &externalFreeDiskSpaceScript, log::LogContext& logContext) override;

   bool testReserveDiskSpace(const cta::DiskSpaceReservationRequest &request,
      const std::string &externalFreeDiskSpaceScript, log::LogContext& logContext) override;

   void requeueJobBatch(std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>& jobBatch,
      log::LogContext& logContext) override;

   void setDriveStatus(common::dataStructures::DriveStatus status, common::dataStructures::MountType mountType,
                                time_t completionTime, const std::optional<std::string> & reason = std::nullopt) override;

   void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) override;

   void flushAsyncSuccessReports(std::list<SchedulerDatabase::RetrieveJob *> & jobsBatch, log::LogContext & lc) override;

   void addDiskSystemToSkip(const DiskSystemToSkip &diskSystemToSkip) override;

   void putQueueToSleep(const std::string &diskSystemName, const uint64_t sleepTime, log::LogContext &logContext) override;
};

} //namespace cta
