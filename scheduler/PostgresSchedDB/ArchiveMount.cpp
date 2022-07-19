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

#include "ArchiveMount.hpp"
#include "common/exception/Exception.hpp"

namespace cta {

PostgresSchedDB::ArchiveMount::ArchiveMount()
{
   throw cta::exception::Exception("Not implemented");
}

const SchedulerDatabase::ArchiveMount::MountInfo & PostgresSchedDB::ArchiveMount::getMountInfo()
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> PostgresSchedDB::ArchiveMount::getNextJobBatch(uint64_t filesRequested,
      uint64_t bytesRequested, log::LogContext& logContext)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::ArchiveMount::setDriveStatus(common::dataStructures::DriveStatus status, common::dataStructures::MountType mountType,
                                time_t completionTime, const std::optional<std::string>& reason)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::ArchiveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::ArchiveMount::setJobBatchTransferred(
      std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> & jobsBatch, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

} //namespace cta
