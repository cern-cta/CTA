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

#include "ArchiveJob.hpp"
#include "common/exception/Exception.hpp"

namespace cta::postgresscheddb {

ArchiveJob::ArchiveJob() = default;
ArchiveJob::ArchiveJob(bool jobOwned, uint64_t jobID, uint64_t mountID, std::string_view tapePool) :
           m_jobOwned(jobOwned), m_mountId(mountID), m_jobId(jobID), m_tapePool(tapePool) { };

void ArchiveJob::failTransfer(const std::string & failureReason, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

void ArchiveJob::failReport(const std::string & failureReason, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

void ArchiveJob::bumpUpTapeFileCount(uint64_t newFileCount)
{
   throw cta::exception::Exception("Not implemented");
}

} // namespace cta::postgresscheddb
