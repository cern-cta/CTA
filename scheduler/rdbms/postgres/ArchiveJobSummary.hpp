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

#include "scheduler/rdbms/postgres/Enums.hpp"
#include "rdbms/NullDbValue.hpp"

namespace cta::schedulerdb::postgres {

struct ArchiveJobSummaryRow {
  std::optional<std::uint64_t> mountId = std::nullopt;
  ArchiveJobStatus status = ArchiveJobStatus::AJS_ToTransferForUser;
  std::string tapePool;
  std::string mountPolicy;
  uint64_t jobsCount = 0;
  uint64_t jobsTotalSize = 0;
  time_t oldestJobStartTime = std::numeric_limits<time_t>::max();
  uint16_t archivePriority = 0;
  uint32_t archiveMinRequestAge = 0;
  uint32_t lastUpdateTime = 0;

  ArchiveJobSummaryRow() = default;

  /**
   * Constructor from row
   *
   * @param row  A single row from the result of a query
   */
  explicit ArchiveJobSummaryRow(const rdbms::Rset &rset) {
    *this = rset;
  }

  ArchiveJobSummaryRow& operator=(const rdbms::Rset &rset) {
    mountId = rset.columnOptionalUint64("MOUNT_ID");
    status               = from_string<ArchiveJobStatus>(
                           rset.columnString("STATUS") );
    tapePool             = rset.columnString("TAPE_POOL");
    mountPolicy          = rset.columnString("MOUNT_POLICY");
    jobsCount            = rset.columnUint64("JOBS_COUNT");
    jobsTotalSize        = rset.columnUint64("JOBS_TOTAL_SIZE");
    oldestJobStartTime   = rset.columnUint64("OLDEST_JOB_START_TIME");
    archivePriority      = rset.columnUint16("ARCHIVE_PRIORITY");
    archiveMinRequestAge = rset.columnUint32("ARCHIVE_MIN_REQUEST_AGE");
    lastUpdateTime     = rset.columnUint32("LAST_UPDATE_TIME");
    return *this;
  }

  void addParamsToLogContext(log::ScopedParamContainer& params) const {
    params.add("mountId", mountId.has_value() ? std::to_string(mountId.value()) : "no value");
    params.add("status", to_string(status));
    params.add("tapePool", tapePool);
    params.add("mountPolicy", mountPolicy);
    params.add("jobsCount", jobsCount);
    params.add("jobsTotalSize", jobsTotalSize);
    params.add("oldestJobStartTime", oldestJobStartTime);
    params.add("archivePriority", archivePriority);
    params.add("archiveMinRequestAge", archiveMinRequestAge);
    params.add("lastUpdateTime", lastUpdateTime);
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
      "ARCHIVE_MIN_REQUEST_AGE, "
      "LAST_UPDATE_TIME"
    "FROM ARCHIVE_JOB_SUMMARY WHERE "
    "MOUNT_ID IS NULL";

    auto stmt = txn.conn().createStmt(sql);
    return stmt.executeQuery();
  }

  /**
   * Refresh Materialized View
   * @param txn transaction
   * @param table_name
   *
   * @return void/exception
   */
  static void refreshMaterializedView(Transaction &txn, std::string_view table_name) {
    std::string sql = "REFRESH MATERIALIZED VIEW " + table_name;
    auto stmt = txn.conn().createStmt(sql);
    return stmt.executeNonQuery();
  }
};

} // namespace cta::schedulerdb::postgres
