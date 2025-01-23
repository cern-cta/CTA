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
/*
 * Retrieve job table summary object
 */
struct RetrieveJobSummaryRow {
  uint64_t jobsCount;
  uint64_t jobsTotalSize;
  uint64_t oldestJobStartTime;
  uint64_t youngestJobStartTime;
  std::string vid;
  std::optional<std::string> activity;
  uint64_t priority;
  //schedulerdb::RetrieveJobStatus status;
  std::string mountPolicy;
  uint64_t minRetrieveRequestAge;

  RetrieveJobSummaryRow() = default;

  /**
   * Constructor from row
   *
   * @param row  A single row from the result of a query
   */
  explicit RetrieveJobSummaryRow(const rdbms::Rset& rset) { *this = rset; }

  RetrieveJobSummaryRow& operator=(const rdbms::Rset& rset) {
    vid = rset.columnString("VID");
    //status = from_string<schedulerdb::RetrieveJobStatus>(rset.columnString("STATUS"));
    activity = rset.columnOptionalString("ACTIVITY");
    jobsCount = rset.columnUint64("JOBS_COUNT");
    jobsTotalSize = rset.columnUint64("JOBS_TOTAL_SIZE");
    priority = rset.columnUint64("PRIORITY");
    minRetrieveRequestAge = rset.columnUint64("RETRIEVE_MIN_REQUEST_AGE");
    mountPolicy = rset.columnString("MOUNT_POLICY");
    oldestJobStartTime = rset.columnUint64("OLDEST_JOB_START_TIME");
    youngestJobStartTime = rset.columnUint64("YOUNGEST_JOB_START_TIME");
    return *this;
  }

  void addParamsToLogContext(log::ScopedParamContainer& params) const {
    params.add("vid", vid);
    params.add("jobsCount", jobsCount);
    params.add("jobsTotalSize", jobsTotalSize);
    params.add("activity", activity.value_or(""));
    params.add("mountPolicy", mountPolicy);
    params.add("priority", priority);
    params.add("minRetrieveRequestAge", minRetrieveRequestAge);
    params.add("oldestJobStartTime", oldestJobStartTime);
    params.add("youngestJobStartTime", youngestJobStartTime);
  }

  /**
   * Select all rows
   *
   * @return result set containing all rows in the table
   */
  static rdbms::Rset selectVid(const std::string& vid, common::dataStructures::JobQueueType type, Transaction& txn) {
    // for the moment ignoring status as we will query only one table
    // where all the jobs wait to be popped to the drive task queues
    const char* const sql = R"SQL(
      SELECT 
        VID,
        JOBS_COUNT,
        JOBS_TOTAL_SIZE
      FROM RETRIEVE_JOB_SUMMARY WHERE 
        VID = :VID AND 
    )SQL";
    /*
    std::string statusStr;
    switch (type) {
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
          "Did not expect queue type JobsToTransferForRepack in RetrieveJobSummaryRow::selectVid");
        break;
      case common::dataStructures::JobQueueType::FailedJobs:
        statusStr = to_string(schedulerdb::RetrieveJobStatus::RJS_Failed);
        break;
    }
     */

    auto stmt = txn.getConn().createStmt(sql);
    stmt.bindString(":VID", vid);
    //stmt.bindString(":STATUS", statusStr);
    return stmt.executeQuery();
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
  static rdbms::Rset selectNewJobs(Transaction& txn, uint64_t gc_delay = 10800) {
    // locking the view until commit (DB lock released)
    // this is to prevent tape servers counting the rows all at the same time
    //const char* const lock_sql = R"SQL(
    //LOCK TABLE ARCHIVE_JOB_SUMMARY IN ACCESS EXCLUSIVE MODE
    //)SQL";
    //auto stmt = txn.getConn().createStmt(lock_sql);
    //stmt.executeNonQuery();
    //update archive_job_queue set in_drive_queue='f',mount_id=NULL; for all which
    // are pending since a defined period of time
    // gc_delay logic and liberating stuck mounts should be later moved elsewhere !
    // this is currently responsible for reprocessing of tasks which were sent to task queue
    // but not picked up yet but the drive - if they were not updated during the gc_delay they will get assigned agan !
    // we could make a queue cleaner doing something much smarter than this
    /* uint64_t gc_now_minus_delay = (uint64_t) cta::utils::getCurrentEpochTime() - gc_delay;
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
    stmt.executeNonQuery(); */

    const char* const sql = R"SQL(
      SELECT
        VID,
        MOUNT_POLICY,
        ACTIVITY,
        PRIORITY,
        JOBS_COUNT,
        JOBS_TOTAL_SIZE,
        OLDEST_JOB_START_TIME,
        YOUNGEST_JOB_START_TIME,
        RETRIEVE_MIN_REQUEST_AGE,
        LAST_JOB_UPDATE_TIME
      FROM
        RETRIEVE_JOB_SUMMARY
    )SQL";

    auto stmt = txn.getConn().createStmt(sql);
    return stmt.executeQuery();
  }

};

}  // namespace cta::schedulerdb::postgres
