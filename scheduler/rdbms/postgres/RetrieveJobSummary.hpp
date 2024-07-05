/**
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright © 2023 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "scheduler/rdbms/postgres/Enums.hpp"

namespace cta::schedulerdb::postgres {

struct RetrieveJobSummaryRow {

  uint64_t jobsCount;
  uint64_t jobsTotalSize;
  std::string vid;
  uint64_t priority;
  schedulerdb::RetrieveJobStatus status;

  RetrieveJobSummaryRow() = default;

  /**
   * Constructor from row
   *
   * @param row  A single row from the result of a query
   */
  explicit RetrieveJobSummaryRow(const rdbms::Rset &rset) {
    *this = rset;
  }

  RetrieveJobSummaryRow& operator=(const rdbms::Rset &rset) {
    vid                  = rset.columnString("VID");
    status               = from_string<schedulerdb::RetrieveJobStatus>(
                           rset.columnString("STATUS") );
    jobsCount            = rset.columnUint64("JOBS_COUNT");
    jobsTotalSize        = rset.columnUint64("JOBS_TOTAL_SIZE");
    priority             = rset.columnUint16("PRIORITY");
    return *this;
  }

  void addParamsToLogContext(log::ScopedParamContainer& params) const {
    params.add("vid", vid);
    params.add("jobsCount", jobsCount);
    params.add("jobsTotalSize", jobsTotalSize);
    params.add("priority", priority);
  }

  /**
   * Select all rows
   *
   * @return result set containing all rows in the table
   */
  static rdbms::Rset selectVid(const std::string &vid,
                               common::dataStructures::JobQueueType type,
                               Transaction &txn) {
    const char *const sql = "SELECT "
      "VID,"
      "STATUS,"
      "JOBS_COUNT,"
      "JOBS_TOTAL_SIZE,"
    "FROM RETRIEVE_JOB_SUMMARY WHERE "
      "VID = :VID AND "
      "STATUS = :STATUS";

    std::string statusStr;
    switch(type) {
      case common::dataStructures::JobQueueType::JobsToTransferForUser:
        statusStr = to_string(schedulerdb::RetrieveJobStatus::RJS_ToTransfer);
        break;
      case common::dataStructures::JobQueueType::JobsToReportToUser:
        statusStr = to_string(schedulerdb::RetrieveJobStatus::RJS_ToReportToUserForFailure);
        break;
      case common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess:
        statusStr = to_string(schedulerdb::RetrieveJobStatus::RJS_ToReportToRepackForSuccess);
        break;
      case common::dataStructures::JobQueueType::JobsToReportToRepackForFailure:
        statusStr = to_string(schedulerdb::RetrieveJobStatus::RJS_ToReportToRepackForFailure);
        break;
      case common::dataStructures::JobQueueType::JobsToTransferForRepack:
        // not used for Retrieve
        throw cta::exception::Exception(
          "Did not expect queue type JobsToTransferForRepack in RetrieveJobSummaryRow::selectVid"
        );
        break;
      case common::dataStructures::JobQueueType::FailedJobs:
        statusStr = to_string(schedulerdb::RetrieveJobStatus::RJS_Failed);
        break;
    }

    auto stmt = txn.conn().createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.bindString(":STATUS", statusStr);
    return stmt.executeQuery();
  }
};

} // namespace cta::schedulerdb::postgres
