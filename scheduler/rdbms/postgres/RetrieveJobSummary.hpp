/**
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
  std::string mountPolicy;
  uint64_t minRetrieveRequestAge;
  std::optional<std::string> diskSystemName;

  RetrieveJobSummaryRow() = default;

  /**
   * Constructor from row
   *
   * @param row  A single row from the result of a query
   */
  explicit RetrieveJobSummaryRow(const rdbms::Rset& rset) { *this = rset; }

  RetrieveJobSummaryRow& operator=(const rdbms::Rset& rset) {
    vid = rset.columnString("VID");
    activity = rset.columnOptionalString("ACTIVITY");
    diskSystemName = rset.columnOptionalString("DISK_SYSTEM_NAME");
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
  static rdbms::Rset
  selectVid(const std::string& vid, [[maybe_unused]] common::dataStructures::JobQueueType type, rdbms::Conn& conn) {
    // for the moment ignoring status as we will query only one table
    // where all the jobs wait to be popped to the drive task queues
    const char* const sql = R"SQL(
      SELECT
        VID,
        MOUNT_POLICY,
        ACTIVITY,
        DISK_SYSTEM_NAME,
        PRIORITY,
        JOBS_COUNT,
        JOBS_TOTAL_SIZE,
        OLDEST_JOB_START_TIME,
        YOUNGEST_JOB_START_TIME,
        RETRIEVE_MIN_REQUEST_AGE,
        LAST_JOB_UPDATE_TIME
      FROM RETRIEVE_QUEUE_SUMMARY WHERE
        VID = :VID
    )SQL";

    auto stmt = conn.createStmt(sql);
    stmt.setDbQuerySummary("selectRetrieveJobSummaryForVid");
    stmt.bindString(":VID", vid);
    return stmt.executeQuery();
  }

  /**
   * Select jobs which do not belong to any drive yet.
   * This is used for deciding if a new mount shall be created
   * @param txn        Transaction to use for this query
   * @return result set containing all rows in the table
   */
  static rdbms::Rset selectNewJobs(Transaction& txn) {
    const char* const sql = R"SQL(
      SELECT
        VID,
        MOUNT_POLICY,
        ACTIVITY,
        DISK_SYSTEM_NAME,
        PRIORITY,
        JOBS_COUNT,
        JOBS_TOTAL_SIZE,
        OLDEST_JOB_START_TIME,
        YOUNGEST_JOB_START_TIME,
        RETRIEVE_MIN_REQUEST_AGE,
        LAST_JOB_UPDATE_TIME
      FROM
        RETRIEVE_QUEUE_SUMMARY
    )SQL";

    auto stmt = txn.getConn().createStmt(sql);
    stmt.setDbQuerySummary("selectRetrieveJobSummary");
    return stmt.executeQuery();
  }

  /**
   * Select jobs which do not belong to any drive yet.
   * This is used for deciding if a new mount shall be created
   * @param txn        Transaction to use for this query
   * @return result set containing all rows in the table
   */
  static rdbms::Rset selectNewRepackJobs(Transaction& txn) {
    const char* const sql = R"SQL(
      SELECT
        VID,
        MOUNT_POLICY,
        ACTIVITY,
        DISK_SYSTEM_NAME,
        PRIORITY,
        JOBS_COUNT,
        JOBS_TOTAL_SIZE,
        OLDEST_JOB_START_TIME,
        YOUNGEST_JOB_START_TIME,
        RETRIEVE_MIN_REQUEST_AGE,
        LAST_JOB_UPDATE_TIME
      FROM
        REPACK_RETRIEVE_QUEUE_SUMMARY
    )SQL";

    auto stmt = txn.getConn().createStmt(sql);
    stmt.setDbQuerySummary("selectRepackRetrieveJobSummary");
    return stmt.executeQuery();
  }
};

}  // namespace cta::schedulerdb::postgres
