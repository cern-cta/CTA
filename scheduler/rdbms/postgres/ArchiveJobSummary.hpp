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
#include <sstream>

namespace cta::schedulerdb::postgres {
/*
 * Archive job table summary object
 */
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
  uint32_t lastJobUpdateTime = 0;

  ArchiveJobSummaryRow() = default;

  /**
   * Constructor from row
   *
   * @param row  A single row from the result of a query
   */
  explicit ArchiveJobSummaryRow(const rdbms::Rset& rset) { *this = rset; }

  ArchiveJobSummaryRow& operator=(const rdbms::Rset& rset) {
    mountId = rset.columnOptionalUint64("MOUNT_ID");
    status = from_string<ArchiveJobStatus>(rset.columnString("STATUS"));
    tapePool = rset.columnString("TAPE_POOL");
    mountPolicy = rset.columnString("MOUNT_POLICY");
    jobsCount = rset.columnUint64("JOBS_COUNT");
    jobsTotalSize = rset.columnUint64("JOBS_TOTAL_SIZE");
    oldestJobStartTime = rset.columnUint64("OLDEST_JOB_START_TIME");
    archivePriority = rset.columnUint16("ARCHIVE_PRIORITY");
    archiveMinRequestAge = rset.columnUint32("ARCHIVE_MIN_REQUEST_AGE");
    lastUpdateTime = rset.columnUint32("LAST_SUMMARY_UPDATE_TIME");
    lastJobUpdateTime = rset.columnUint32("LAST_JOB_UPDATE_TIME");
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
    params.add("lastJobUpdateTime", lastJobUpdateTime);
  }

  /**
   * Select jobs which do not belong to any drive yet.
   * This is used for deciding if a new mount shall be created
   * uint64_t gc_delay = 43200
   * @param txn        Transaction to use for this query
   * @param gc_delay   Delay for garbage collection of jobs which were not processed
   *                   until a final state by the mount where they started processing
   *                   default is 1 hours
   * @return result set containing all rows in the table
   */
  static rdbms::Rset selectJobsExceptDriveQueue(Transaction& txn, uint64_t gc_delay = 10800) {
    // locking the view until commit (DB lock released)
    // this is to prevent tape servers counting the rows all at the same time
    const char* const lock_sql = R"SQL(
    LOCK TABLE ARCHIVE_JOB_SUMMARY IN ACCESS EXCLUSIVE MODE
    )SQL";
    auto stmt = txn.getConn().createStmt(lock_sql);
    stmt.executeNonQuery();
    //update archive_job_queue set in_drive_queue='f',mount_id=NULL; for all which
    // are pending since a defined period of time
    // gc_delay logic and liberating stuck mounts should be later moved elsewhere !
    // this is currently responsible for reprocessing of tasks which were sent to task queue
    // but not picked up yet but the drive - if they were not updated during the gc_delay they will get assigned agan !
    // we could make a queue cleaner doing something much smarter than this
    uint64_t gc_now_minus_delay = (uint64_t) cta::utils::getCurrentEpochTime() - gc_delay;
    const char* const update_sql = R"SQL(
    UPDATE ARCHIVE_JOB_QUEUE SET
      MOUNT_ID = NULL,
      IN_DRIVE_QUEUE = FALSE
    WHERE MOUNT_ID IS NOT NULL AND IN_DRIVE_QUEUE = TRUE AND STATUS = :STATUS AND LAST_UPDATE_TIME < :NOW_MINUS_DELAY
    )SQL";
    stmt = txn.getConn().createStmt(update_sql);
    ArchiveJobStatus status = ArchiveJobStatus::AJS_ToTransferForUser;
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindUint64(":NOW_MINUS_DELAY", gc_now_minus_delay);
    stmt.executeNonQuery();

    const char* const sql = R"SQL(
      SELECT 
        MOUNT_ID,
        STATUS,
        TAPE_POOL,
        MOUNT_POLICY,
        JOBS_COUNT,
        JOBS_TOTAL_SIZE,
        OLDEST_JOB_START_TIME,
        ARCHIVE_PRIORITY,
        ARCHIVE_MIN_REQUEST_AGE, 
        LAST_JOB_UPDATE_TIME, 
        LAST_SUMMARY_UPDATE_TIME
      FROM 
        ARCHIVE_JOB_SUMMARY 
      WHERE 
        MOUNT_ID IS NULL
        AND IN_DRIVE_QUEUE IS FALSE
    )SQL";

    stmt = txn.getConn().createStmt(sql);
    return stmt.executeQuery();
  }

  /**
   * Select jobs which do not belong to any drive yet.
   * This is used for deciding if a new mount shall be created
   *
   * @return result set containing all rows in the table
   */
  static rdbms::Rset selectFailedJobSummary(Transaction& txn) {
    // locking the view until commit (DB lock released)
    // this is to prevent tape servers counting the rows all at the same time
    const char* const sql = R"SQL(
      SELECT
        COUNT(*) AS JOBS_COUNT,
        SUM(SIZE_IN_BYTES) AS JOBS_TOTAL_SIZE
      FROM
        ARCHIVE_FAILED_JOB_QUEUE
      WHERE
        STATUS = :STATUS::ARCHIVE_JOB_STATUS
    )SQL";
    auto stmt = txn.getConn().createStmt(sql);
    stmt.bindString(":STATUS", "AJS_Failed");
    return stmt.executeQuery();
  }
};

}  // namespace cta::schedulerdb::postgres
