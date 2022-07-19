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

#include "RetrieveMount.hpp"
#include "common/exception/Exception.hpp"

namespace cta {

PostgresSchedDB::RetrieveMount::RetrieveMount()
{
   throw cta::exception::Exception("Not implemented");
}

const SchedulerDatabase::RetrieveMount::MountInfo & PostgresSchedDB::RetrieveMount::getMountInfo()
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> PostgresSchedDB::RetrieveMount::getNextJobBatch(uint64_t filesRequested,
     uint64_t bytesRequested, log::LogContext& logContext)
{
   throw cta::exception::Exception("Not implemented");
}

bool PostgresSchedDB::RetrieveMount::reserveDiskSpace(const cta::DiskSpaceReservationRequest &request,
      const std::string &externalFreeDiskSpaceScript, log::LogContext& logContext)
{
   throw cta::exception::Exception("Not implemented");
}

bool PostgresSchedDB::RetrieveMount::testReserveDiskSpace(const cta::DiskSpaceReservationRequest &request,
      const std::string &externalFreeDiskSpaceScript, log::LogContext& logContext)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::RetrieveMount::requeueJobBatch(std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>& jobBatch,
      log::LogContext& logContext)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::RetrieveMount::setDriveStatus(common::dataStructures::DriveStatus status, common::dataStructures::MountType mountType,
                                time_t completionTime, const std::optional<std::string> & reason)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::RetrieveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::RetrieveMount::flushAsyncSuccessReports(std::list<SchedulerDatabase::RetrieveJob *> & jobsBatch, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::RetrieveMount::addDiskSystemToSkip(const DiskSystemToSkip &diskSystemToSkip)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::RetrieveMount::putQueueToSleep(const std::string &diskSystemName, const uint64_t sleepTime, log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

} //namespace cta
