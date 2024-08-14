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
#include "scheduler/rdbms/postgres/ArchiveJobQueue.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"

namespace cta::schedulerdb {

ArchiveJob::ArchiveJob() = default;
ArchiveJob::ArchiveJob(rdbms::ConnPool &pool, bool jobOwned, uint64_t jid, uint64_t mountID, std::string_view tapePool) :
                       m_connPool(pool), m_jobOwned(jobOwned), m_mountId(mountID), m_tapePool(tapePool) { jobID = jid; };

void ArchiveJob::failTransfer(const std::string & failureReason, log::LogContext & lc)
{
  if (!m_jobOwned) {
    throw JobNotOwned("In schedulerdb::ArchiveJob::failTransfer: cannot fail a job not owned");
  }
  std::string failureLog = cta::utils::getCurrentLocalTime() + " " + cta::utils::getShortHostname() +
                           " " + failureReason;

  lc.log(log::WARNING,
         "In schedulerdb::ArchiveJob::failTransfer(): passes as half-dummy implementation !");
  log::ScopedParamContainer(lc)
            .add("jobID", m_jobId)
            .add("mountId", m_mountId)
            .add("tapePool", m_tapePool)
            .log(log::INFO,
                 "In schedulerdb::ArchiveJob::failTransfer(): received failed job to be reported.");
  /* Update Status in ARCHIVE_JOB_QUEUE and table to failed status:
   * AJS_ToReportToUserForFailure
   */
  cta::schedulerdb::Transaction txn(m_connPool);
  try {
    postgres::ArchiveJobQueueRow::updateJobStatus(txn, ArchiveJobStatus::AJS_ToReportToUserForFailure, jobIDsList);
    txn.commit();
  } catch (exception::Exception &ex) {
    lc.log(cta::log::DEBUG,
           "In schedulerdb::ArchiveJob::failTransfer(): failed to update job status for reporting. Aborting the transaction." +
           ex.getMessageValue());
    txn.abort();
  }
}

void ArchiveJob::failReport(const std::string & failureReason, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

void ArchiveJob::bumpUpTapeFileCount(uint64_t newFileCount)
{
   throw cta::exception::Exception("Not implemented");
}

} // namespace cta::schedulerdb
