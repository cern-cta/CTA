/**
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright © 2021-2022 CERN
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

#include "scheduler/PostgresSchedDB/sql/Enums.hpp"

namespace cta::postgresscheddb::sql {

struct ArchiveJobSummaryRow {
  uint64_t mountId;
  ArchiveJobStatus status;
  std::string tapePool;
  std::string mountPolicy;
  uint64_t jobsCount;
  uint64_t jobsTotalSize;
  time_t oldestJobStartTime;
  uint16_t archivePriority;
  uint32_t archiveMinRequestAge;

  ArchiveJobSummaryRow() :
    mountId(0),
    status(ArchiveJobStatus::AJS_ToTransferForUser),
    jobsCount(0),
    jobsTotalSize(0),
    oldestJobStartTime(std::numeric_limits<time_t>::max()),
    archivePriority(0),
    archiveMinRequestAge(0) { }

  /**
   * Constructor from row
   *
   * @param row  A single row from the result of a query
   */
  ArchiveJobSummaryRow(const rdbms::Rset &rset) {
    *this = rset;
  }

  ArchiveJobSummaryRow& operator=(const rdbms::Rset &rset) {
    mountId              = rset.columnUint64("MOUNT_ID");
    status               = from_string<ArchiveJobStatus>(
                           rset.columnString("STATUS") );
    tapePool             = rset.columnString("TAPE_POOL");
    mountPolicy          = rset.columnString("MOUNT_POLICY");
    jobsCount            = rset.columnUint64("JOBS_COUNT");
    jobsTotalSize        = rset.columnUint64("JOBS_TOTAL_SIZE");
    oldestJobStartTime   = rset.columnUint64("OLDEST_JOB_START_TIME");
    archivePriority      = rset.columnUint16("ARCHIVE_PRIORITY");
    archiveMinRequestAge = rset.columnUint32("ARCHIVE_MIN_REQUEST_AGE");
    return *this;
  }

  void addParamsToLogContext(log::ScopedParamContainer& params) const {
    params.add("mountId", mountId);
    params.add("status", to_string(status));
    params.add("tapePool", tapePool);
    params.add("mountPolicy", mountPolicy);
    params.add("jobsCount", jobsCount);
    params.add("jobsTotalSize", jobsTotalSize);
    params.add("oldestJobStartTime", oldestJobStartTime);
    params.add("archivePriority", archivePriority);
    params.add("archiveMinRequestAge", archiveMinRequestAge);
  }

  /**
   * Select all rows
   *
   * @return result set containing all rows in the table
   */
  static rdbms::Rset selectNotOwned(Transaction &txn) {
    const char *const sql = "SELECT "
      "MOUNT_ID,"
      "STATUS,"
      "TAPE_POOL,"
      "MOUNT_POLICY,"
      "JOBS_COUNT,"
      "JOBS_TOTAL_SIZE,"
      "OLDEST_JOB_START_TIME,"
      "ARCHIVE_PRIORITY,"
      "ARCHIVE_MIN_REQUEST_AGE "
    "FROM ARCHIVE_JOB_SUMMARY WHERE "
    "MOUNT_ID IS NULL";

    auto stmt = txn.conn().createStmt(sql);
    return stmt.executeQuery();
  }
};

} // namespace cta::postgresscheddb::sql
