/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/postgres/ArchiveJobQueue.hpp"

#include "common/utils/utils.hpp"
#include "rdbms/wrapper/PostgresColumn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"
#include "scheduler/rdbms/ArchiveMount.hpp"

namespace cta::schedulerdb::postgres {

rdbms::Rset ArchiveJobQueueRow::moveJobsToDbActiveQueue(Transaction& txn,
                                                        ArchiveJobStatus newStatus,
                                                        const SchedulerDatabase::ArchiveMount::MountInfo& mountInfo,
                                                        uint64_t maxBytesRequested,
                                                        uint64_t limit,
                                                        bool isRepack) {
  /* using write row lock FOR UPDATE for the select statement
   * since it is the same lock used for UPDATE
   * we first apply the LIMIT on the selection to limit
   * the number of rows and only after calculate the
   * running cumulative sum of bytes in the consequent step */
  std::string prefix = isRepack ? "REPACK_" : "";
  std::string sql = R"SQL(
    WITH SET_SELECTION AS (
      SELECT JOB_ID, ARCHIVE_FILE_ID, PRIORITY, SIZE_IN_BYTES
      FROM
    )SQL";
  sql += prefix + "ARCHIVE_PENDING_QUEUE ";
  sql += R"SQL(
      WHERE TAPE_POOL = :TAPE_POOL
      AND STATUS = :STATUS
      AND ( MOUNT_ID IS NULL OR MOUNT_ID = :SAME_MOUNT_ID )
      ORDER BY PRIORITY DESC, ARCHIVE_FILE_ID
      LIMIT :LIMIT
      FOR UPDATE SKIP LOCKED
    ),
    SELECTION_WITH_CUMULATIVE_SUMS AS (
      SELECT JOB_ID FROM (
        SELECT JOB_ID, SUM(SIZE_IN_BYTES) OVER (ORDER BY PRIORITY DESC, JOB_ID) AS CUMULATIVE_SIZE
          FROM SET_SELECTION ) s
      WHERE s.CUMULATIVE_SIZE <= :BYTES_REQUESTED
    ),
    MOVED_ROWS AS (
        DELETE FROM
      )SQL";
  sql += prefix + "ARCHIVE_PENDING_QUEUE ";
  sql += R"SQL( AIQ
        USING SELECTION_WITH_CUMULATIVE_SUMS CSEL
        WHERE AIQ.JOB_ID = CSEL.JOB_ID
        RETURNING AIQ.*
    )
    INSERT INTO
    )SQL";
  sql += prefix + "ARCHIVE_ACTIVE_QUEUE (";
  if (isRepack) {
    sql += " REPACK_REQUEST_ID,";
  }
  sql += R"SQL(
        JOB_ID,
        ARCHIVE_REQUEST_ID,
        REQUEST_JOB_COUNT,
        STATUS,
        TAPE_POOL,
        MOUNT_POLICY,
        PRIORITY,
        MIN_ARCHIVE_REQUEST_AGE,
        ARCHIVE_FILE_ID,
        SIZE_IN_BYTES,
        COPY_NB,
        START_TIME,
        CHECKSUMBLOB,
        CREATION_TIME,
        DISK_INSTANCE,
        DISK_FILE_ID,
        DISK_FILE_OWNER_UID,
        DISK_FILE_GID,
        DISK_FILE_PATH,
        ARCHIVE_REPORT_URL,
        ARCHIVE_ERROR_REPORT_URL,
        REQUESTER_NAME,
        REQUESTER_GROUP,
        SRC_URL,
        STORAGE_CLASS,
        RETRIES_WITHIN_MOUNT,
        MAX_RETRIES_WITHIN_MOUNT,
        TOTAL_RETRIES,
        LAST_MOUNT_WITH_FAILURE,
        MAX_TOTAL_RETRIES,
        TOTAL_REPORT_RETRIES,
        MAX_REPORT_RETRIES,
        MOUNT_ID,
        VID,
        DRIVE,
        HOST,
        MOUNT_TYPE,
        LOGICAL_LIBRARY)
            SELECT
  )SQL";
  if (isRepack) {
    sql += " M.REPACK_REQUEST_ID,";
  }
  sql += R"SQL( M.JOB_ID,
                M.ARCHIVE_REQUEST_ID,
                M.REQUEST_JOB_COUNT,
                M.STATUS,
                M.TAPE_POOL,
                M.MOUNT_POLICY,
                M.PRIORITY,
                M.MIN_ARCHIVE_REQUEST_AGE,
                M.ARCHIVE_FILE_ID,
                M.SIZE_IN_BYTES,
                M.COPY_NB,
                M.START_TIME,
                M.CHECKSUMBLOB,
                M.CREATION_TIME,
                M.DISK_INSTANCE,
                M.DISK_FILE_ID,
                M.DISK_FILE_OWNER_UID,
                M.DISK_FILE_GID,
                M.DISK_FILE_PATH,
                M.ARCHIVE_REPORT_URL,
                M.ARCHIVE_ERROR_REPORT_URL,
                M.REQUESTER_NAME,
                M.REQUESTER_GROUP,
                M.SRC_URL,
                M.STORAGE_CLASS,
                M.RETRIES_WITHIN_MOUNT,
                M.MAX_RETRIES_WITHIN_MOUNT,
                M.TOTAL_RETRIES,
                M.LAST_MOUNT_WITH_FAILURE,
                M.MAX_TOTAL_RETRIES,
                M.TOTAL_REPORT_RETRIES,
                M.MAX_REPORT_RETRIES,
                :MOUNT_ID AS MOUNT_ID,
                :VID AS VID,
                :DRIVE AS DRIVE,
                :HOST AS HOST,
                :MOUNT_TYPE AS MOUNT_TYPE,
                :LOGICAL_LIBRARY AS LOGICAL_LIBRARY
            FROM MOVED_ROWS M
    RETURNING *;
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":TAPE_POOL", mountInfo.tapePool);
  stmt.bindString(":STATUS", to_string(newStatus));
  stmt.bindUint32(":LIMIT", limit);
  stmt.bindUint64(":MOUNT_ID", mountInfo.mountId);
  stmt.bindUint64(":SAME_MOUNT_ID", mountInfo.mountId);
  stmt.bindString(":VID", mountInfo.vid);
  stmt.bindString(":DRIVE", mountInfo.drive);
  stmt.bindString(":HOST", mountInfo.host);
  stmt.bindString(":MOUNT_TYPE", cta::common::dataStructures::toString(mountInfo.mountType));
  stmt.bindString(":LOGICAL_LIBRARY", mountInfo.logicalLibrary);
  stmt.bindUint64(":BYTES_REQUESTED", maxBytesRequested);
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbMoveArchiveToActive);
  auto result = stmt.executeQuery();
  return result;
}

uint64_t ArchiveJobQueueRow::updateMultiCopyJobSuccess(Transaction& txn, const std::vector<std::string>& jobIDs) {
  if (jobIDs.empty()) {
    return 0;
  }
  ArchiveJobStatus newStatus = ArchiveJobStatus::AJS_ToReportToUserForSuccess;
  std::string jobids_sqlpart;
  for (const auto& piece : jobIDs) {
    if (!jobids_sqlpart.empty()) {
      jobids_sqlpart += ",";
    }
    jobids_sqlpart += piece;
  }
  std::string sql = R"SQL(
      WITH target_success_multicopy AS (
        SELECT JOB_ID, ARCHIVE_REQUEST_ID, STATUS, REQUEST_JOB_COUNT
        FROM ARCHIVE_ACTIVE_QUEUE scj
        WHERE scj.JOB_ID IN (
      )SQL";
  sql += jobids_sqlpart;
  sql += R"SQL( )
      ),
      ready_for_reporting_to_disk AS (
        SELECT combined.ARCHIVE_REQUEST_ID
        FROM (
          SELECT t.JOB_ID, t.ARCHIVE_REQUEST_ID, t.STATUS, t.REQUEST_JOB_COUNT
          FROM target_success_multicopy t

          UNION ALL

          SELECT aj.JOB_ID, aj.ARCHIVE_REQUEST_ID, aj.STATUS, aj.REQUEST_JOB_COUNT
          FROM ARCHIVE_ACTIVE_QUEUE aj
          JOIN target_success_multicopy t USING (ARCHIVE_REQUEST_ID)
          WHERE aj.JOB_ID <> t.JOB_ID AND aj.STATUS = :STATUS_COND_REPLICAS::ARCHIVE_JOB_STATUS
        ) AS combined
        GROUP BY combined.ARCHIVE_REQUEST_ID
        HAVING COUNT(*) = MAX(combined.REQUEST_JOB_COUNT)
      )
      UPDATE ARCHIVE_ACTIVE_QUEUE aj2
      SET STATUS = CASE
          WHEN rfr.ARCHIVE_REQUEST_ID IS NOT NULL
            THEN :STATUS_READY_FOR_REPORTING::ARCHIVE_JOB_STATUS
          ELSE :STATUS_WAIT_FOR_ALL_BEFORE_REPORT::ARCHIVE_JOB_STATUS
      END,
      LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
      FROM target_success_multicopy tsm
          LEFT JOIN ready_for_reporting_to_disk rfr
              ON rfr.ARCHIVE_REQUEST_ID = tsm.ARCHIVE_REQUEST_ID
      WHERE aj2.ARCHIVE_REQUEST_ID = tsm.ARCHIVE_REQUEST_ID
        AND aj2.STATUS <> :STATUS_FAILED::ARCHIVE_JOB_STATUS;
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS_READY_FOR_REPORTING", to_string(newStatus));
  stmt.bindString(":STATUS_COND_REPLICAS", to_string(ArchiveJobStatus::AJS_WaitReplicasBeforeReportingSuccessToDisk));
  stmt.bindString(":STATUS_WAIT_FOR_ALL_BEFORE_REPORT",
                  to_string(ArchiveJobStatus::AJS_WaitReplicasBeforeReportingSuccessToDisk));
  stmt.bindString(":STATUS_FAILED", to_string(ArchiveJobStatus::AJS_ToReportToUserForFailure));
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbUpdateArchiveMulticopy);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

uint64_t ArchiveJobQueueRow::updateJobStatus(Transaction& txn,
                                             ArchiveJobStatus newStatus,
                                             const std::vector<std::string>& jobIDs) {
  if (jobIDs.empty()) {
    return 0;
  }
  std::string sqlpart;
  for (const auto& piece : jobIDs) {
    sqlpart += piece + ",";
  }
  if (!sqlpart.empty()) {
    sqlpart.pop_back();
  }
  if (newStatus == ArchiveJobStatus::AJS_Complete || newStatus == ArchiveJobStatus::AJS_Failed
      || newStatus == ArchiveJobStatus::ReadyForDeletion) {
    std::string sql = R"SQL(
        DELETE FROM ARCHIVE_ACTIVE_QUEUE
        WHERE
          JOB_ID IN (
        )SQL";
    sql += sqlpart + std::string(")");
    auto stmt1 = txn.getConn().createStmt(sql);
    txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbDeleteArchive);
    stmt1.executeNonQuery();
    auto nrows = stmt1.getNbAffectedRows();
    txn.setRowCountForTelemetry(nrows);
    return nrows;
  }
  std::string sql = "UPDATE ARCHIVE_ACTIVE_QUEUE SET STATUS = :NEWSTATUS1::ARCHIVE_JOB_STATUS, ";
  sql += "        LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT WHERE JOB_ID IN (" + sqlpart + ")";
  auto stmt2 = txn.getConn().createStmt(sql);
  stmt2.bindString(":NEWSTATUS1", to_string(newStatus));
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbUpdateArchive);
  stmt2.executeNonQuery();
  auto nrows = stmt2.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
};

uint64_t ArchiveJobQueueRow::updateRepackJobSuccess(Transaction& txn, const std::vector<std::string>& jobIDs) {
  if (jobIDs.empty()) {
    return 0;
  }
  std::string sqlpart;
  for (const auto& piece : jobIDs) {
    if (!sqlpart.empty()) {
      sqlpart += ",";
    }
    sqlpart += piece;
  }
  /* For Repack, when we report success this query handles the check of all
   * other replica rows/job with the same archive_file_id
   * which needed to be archived too. If all the rows with the same archive_file_id
   * are in AJS_ToReportToRepackForSuccess except the one being currently updated,
   * then update all of them to ReadyForDeletion. This will signal the next step
   * that the source file can be deleted from disk otherwise it updates the status
   * just to AJS_ToReportToRepackForSuccess. If the required update status is anything
   * else than AJS_ToReportToRepackForSuccess it just updates to that status.
   */
  std::string sql = R"SQL(
  WITH updated_single_copy_jobs AS (
      UPDATE REPACK_ARCHIVE_ACTIVE_QUEUE rscj
    SET STATUS = :STATUS_READY_FOR_DELETION1::ARCHIVE_JOB_STATUS
  )SQL";
  sql += " WHERE rscj.JOB_ID IN (" + sqlpart + ") AND rscj.REQUEST_JOB_COUNT = 1";
  sql += R"SQL(
  ),
  target_success_multicopy AS (
      SELECT JOB_ID, ARCHIVE_FILE_ID, STATUS, REQUEST_JOB_COUNT
      FROM REPACK_ARCHIVE_ACTIVE_QUEUE rscj2
      WHERE rscj2.REQUEST_JOB_COUNT > 1 AND rscj2.JOB_ID IN (
  )SQL";
  sql += sqlpart;
  sql += R"SQL( )
  ),
  ready_for_deletion AS (
    SELECT combined.ARCHIVE_FILE_ID
    FROM (
      SELECT t.JOB_ID, t.ARCHIVE_FILE_ID, t.STATUS, t.REQUEST_JOB_COUNT
      FROM target_success_multicopy t

      UNION ALL

      SELECT aj.JOB_ID, aj.ARCHIVE_FILE_ID, aj.STATUS, aj.REQUEST_JOB_COUNT
       FROM REPACK_ARCHIVE_ACTIVE_QUEUE aj
       JOIN target_success_multicopy t USING (ARCHIVE_FILE_ID)
       WHERE aj.JOB_ID <> t.JOB_ID AND aj.STATUS = :STATUS_COND_REPLICAS::ARCHIVE_JOB_STATUS
     ) AS combined
     GROUP BY combined.ARCHIVE_FILE_ID
     HAVING COUNT(*) = MAX(combined.REQUEST_JOB_COUNT)
   )
  UPDATE REPACK_ARCHIVE_ACTIVE_QUEUE aj2
  SET STATUS = CASE
      WHEN rfd.ARCHIVE_FILE_ID IS NOT NULL
        THEN :STATUS_READY_FOR_DELETION2::ARCHIVE_JOB_STATUS
      ELSE :STATUS_SUCCESS::ARCHIVE_JOB_STATUS
  END
  FROM target_success_multicopy tsm
        LEFT JOIN ready_for_deletion rfd
            ON rfd.ARCHIVE_FILE_ID = tsm.ARCHIVE_FILE_ID
  WHERE aj2.ARCHIVE_FILE_ID = tsm.ARCHIVE_FILE_ID;
  )SQL";
  auto stmt1 = txn.getConn().createStmt(sql);
  stmt1.bindString(":STATUS_COND_REPLICAS", to_string(ArchiveJobStatus::AJS_ToReportToRepackForSuccess));
  stmt1.bindString(":STATUS_SUCCESS", to_string(ArchiveJobStatus::AJS_ToReportToRepackForSuccess));
  stmt1.bindString(":STATUS_READY_FOR_DELETION1", to_string(ArchiveJobStatus::ReadyForDeletion));
  stmt1.bindString(":STATUS_READY_FOR_DELETION2", to_string(ArchiveJobStatus::ReadyForDeletion));
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbUpdateRepackArchiveSuccess);
  stmt1.executeNonQuery();
  auto nrows = stmt1.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
};

rdbms::Rset ArchiveJobQueueRow::getNextSuccessfulArchiveRepackReportBatch(Transaction& txn, const size_t limit) {
  std::string sql = R"SQL(
        SELECT SRC_URL, JOB_ID FROM REPACK_ARCHIVE_ACTIVE_QUEUE
            WHERE STATUS = :STATUS
                ORDER BY PRIORITY DESC, JOB_ID
                LIMIT :LIMIT FOR UPDATE SKIP LOCKED
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(ArchiveJobStatus::ReadyForDeletion));
  stmt.bindUint32(":LIMIT", static_cast<uint32_t>(limit));
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbSelectRepackArchiveToReport);
  auto rset = stmt.executeQuery();
  return rset;
}

rdbms::Rset ArchiveJobQueueRow::deleteSuccessfulRepackArchiveJobBatch(Transaction& txn,
                                                                      std::vector<std::string>& jobIDs) {
  std::string sql = R"SQL(
    WITH DELETED_ROWS AS (
        DELETE FROM REPACK_ARCHIVE_ACTIVE_QUEUE
  )SQL";
  if (!jobIDs.empty()) {
    std::string sqlpart;
    for (const auto& jid : jobIDs) {
      sqlpart += jid + ",";
    }
    if (!sqlpart.empty()) {
      sqlpart.pop_back();
    }
    sql += std::string("WHERE JOB_ID IN (") + sqlpart + std::string(")");
  } else {
    rdbms::Rset rset;
    return rset;
  }
  sql += R"SQL(
        RETURNING REPACK_REQUEST_ID, VID, SIZE_IN_BYTES
    )
    SELECT
        REPACK_REQUEST_ID, VID,
        COUNT(*) AS ARCHIVED_COUNT,
        COALESCE(SUM(SIZE_IN_BYTES), 0) AS ARCHIVED_BYTES
    FROM DELETED_ROWS
    GROUP BY REPACK_REQUEST_ID, VID;
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbDeleteRepackArchive);
  auto rset = stmt.executeQuery();
  return rset;
}

uint64_t ArchiveJobQueueRow::updateFailedJobStatus(Transaction& txn, bool isRepack) {
  ArchiveJobStatus newStatus;
  std::string repack_prefix = "";
  if (isRepack) {
    repack_prefix = "REPACK_";
    newStatus = ArchiveJobStatus::AJS_ToReportToRepackForFailure;
  } else {
    newStatus = ArchiveJobStatus::AJS_ToReportToUserForFailure;
  }
  std::string sql = R"SQL(
      UPDATE )SQL";
  sql += repack_prefix + R"SQL(ARCHIVE_ACTIVE_QUEUE SET
        STATUS = :STATUS,
        TOTAL_RETRIES = :TOTAL_RETRIES,
        RETRIES_WITHIN_MOUNT = :RETRIES_WITHIN_MOUNT,
        LAST_MOUNT_WITH_FAILURE = :LAST_MOUNT_WITH_FAILURE,
        FAILURE_LOG = :FAILURE_LOG,
        LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
  )SQL";
  // Plain assignment, not FAILURE_LOG || :FAILURE_LOG: :FAILURE_LOG is bound from
  // failureLogs.value_or(""), which updateJobRowFailureLog() already appended the new entry onto
  // the value loaded from this same column - appending again in SQL would duplicate the old text.
  // Multi-copy user requests: fail all copies together, since the request can no longer meet
  // its redundancy goal. updateMultiCopyJobSuccess guards against a still-writing sibling copy
  // overwriting this terminal status.
  if (reqJobCount > 1 && !isRepack) {
    sql += R"SQL(
        WHERE ARCHIVE_REQUEST_ID = :ARCHIVE_REQUEST_ID
    )SQL";
  } else {
    sql += R"SQL(
        WHERE JOB_ID = :JOB_ID
    )SQL";
  }
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(newStatus));
  stmt.bindUint32(":TOTAL_RETRIES", totalRetries);
  stmt.bindUint32(":RETRIES_WITHIN_MOUNT", retriesWithinMount);
  stmt.bindUint64(":LAST_MOUNT_WITH_FAILURE", lastMountWithFailure);
  stmt.bindString(":FAILURE_LOG", failureLogs.value_or(""));
  if (reqJobCount > 1 && !isRepack) {
    stmt.bindUint64(":ARCHIVE_REQUEST_ID", reqId);
  } else {
    stmt.bindUint64(":JOB_ID", jobId);
  }
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbUpdateArchive);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
};

void ArchiveJobQueueRow::updateJobRowFailureLog(const std::string& reason, bool is_report_log) {
  std::string failureLog = cta::utils::getCurrentLocalTime() + " " + cta::utils::getShortHostname() + " " + reason;

  if (auto& logField = is_report_log ? reportFailureLogs : failureLogs; logField.has_value()) {
    logField.value() += failureLog;
  } else {
    logField.emplace(failureLog);
  }
  if (is_report_log) {
    ++totalReportRetries;
  }
};

void ArchiveJobQueueRow::updateRetryCounts(uint64_t mountId) {
  if (lastMountWithFailure == mountId) {
    retriesWithinMount += 1;
  } else {
    retriesWithinMount = 1;
    lastMountWithFailure = mountId;
  }
  totalRetries += 1;
};

// requeueFailedJob is used to requeue jobs which were not processed due to finished mount or failed jobs
// The SQL below selects :FAILURE_LOG AS FAILURE_LOG with a plain reference, no SQL-side
// concatenation: :FAILURE_LOG is bound from failureLogs.value_or(""), which already contains
// the row's old FAILURE_LOG text plus the new entry (updateJobRowFailureLog() appends onto a
// field loaded from this same column) - concatenating again here would duplicate the old text.
uint64_t ArchiveJobQueueRow::requeueFailedJob(Transaction& txn,
                                              ArchiveJobStatus newStatus,
                                              bool keepMountId,
                                              bool isRepack,
                                              std::optional<std::list<std::string>> jobIDs) {
  std::string repack_prefix = isRepack ? "REPACK_" : "";
  std::string sql = R"SQL(
    WITH MOVED_ROWS AS (
        DELETE FROM
  )SQL";
  sql += repack_prefix + "ARCHIVE_ACTIVE_QUEUE ";
  bool userowjid = true;
  if (jobIDs.has_value() && !jobIDs.value().empty()) {
    userowjid = false;
    std::string sqlpart;
    for (const auto& jid : jobIDs.value()) {
      sqlpart += jid + ",";
    }
    if (!sqlpart.empty()) {
      sqlpart.pop_back();
    }
    sql += std::string("WHERE JOB_ID IN (") + sqlpart + std::string(")");
  } else {
    sql += R"SQL(
        WHERE JOB_ID = :JOB_ID
    )SQL";
  }
  sql += R"SQL(
        RETURNING *
    )
  )SQL";
  if (keepMountId) {
    // To optimise this query, one may consider creating separate call
    // similar to updateMountQueueLastFetch and remove UPDATED_MOUNT_QUEUE_LAST_FETCH
    sql += R"SQL(
      , UPDATED_MOUNT_QUEUE_LAST_FETCH AS (
       INSERT INTO MOUNT_QUEUE_LAST_FETCH (MOUNT_ID, LAST_UPDATE_TIME, QUEUE_TYPE)
         SELECT DISTINCT
           M.MOUNT_ID,
           EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
           :QUEUE_TYPE
         FROM MOVED_ROWS M
       ON CONFLICT (MOUNT_ID, QUEUE_TYPE)
         DO UPDATE SET
           LAST_UPDATE_TIME = EXCLUDED.LAST_UPDATE_TIME,
           QUEUE_TYPE = EXCLUDED.QUEUE_TYPE
             WHERE MOUNT_QUEUE_LAST_FETCH.LAST_UPDATE_TIME
               < EXCLUDED.LAST_UPDATE_TIME - 5
    ) )SQL";
  }
  sql += R"SQL(
    INSERT INTO
    )SQL";
  sql += repack_prefix + "ARCHIVE_PENDING_QUEUE ( ";
  if (isRepack) {
    sql += "REPACK_REQUEST_ID,";
  }
  sql += R"SQL(
    JOB_ID,
    ARCHIVE_REQUEST_ID,
    REQUEST_JOB_COUNT,
    TAPE_POOL,
    MOUNT_POLICY,
    PRIORITY,
    MIN_ARCHIVE_REQUEST_AGE,
    ARCHIVE_FILE_ID,
    SIZE_IN_BYTES,
    COPY_NB,
    START_TIME,
    CHECKSUMBLOB,
    CREATION_TIME,
    DISK_INSTANCE,
    DISK_FILE_ID,
    DISK_FILE_OWNER_UID,
    DISK_FILE_GID,
    DISK_FILE_PATH,
    ARCHIVE_REPORT_URL,
    ARCHIVE_ERROR_REPORT_URL,
    REQUESTER_NAME,
    REQUESTER_GROUP,
    SRC_URL,
    STORAGE_CLASS,
    MAX_RETRIES_WITHIN_MOUNT,
    MAX_TOTAL_RETRIES,
    TOTAL_REPORT_RETRIES,
    MAX_REPORT_RETRIES,
    VID,
    DRIVE,
    HOST,
    MOUNT_TYPE,
    LOGICAL_LIBRARY,
    STATUS,
    TOTAL_RETRIES,
    RETRIES_WITHIN_MOUNT,
    LAST_MOUNT_WITH_FAILURE,
    FAILURE_LOG,
    MOUNT_ID)
        SELECT
)SQL";
  if (isRepack) {
    sql += "M.REPACK_REQUEST_ID,";
  }
  sql += R"SQL(
            M.JOB_ID,
            M.ARCHIVE_REQUEST_ID,
            M.REQUEST_JOB_COUNT,
            M.TAPE_POOL,
            M.MOUNT_POLICY,
            M.PRIORITY,
            M.MIN_ARCHIVE_REQUEST_AGE,
            M.ARCHIVE_FILE_ID,
            M.SIZE_IN_BYTES,
            M.COPY_NB,
            M.START_TIME,
            M.CHECKSUMBLOB,
            M.CREATION_TIME,
            M.DISK_INSTANCE,
            M.DISK_FILE_ID,
            M.DISK_FILE_OWNER_UID,
            M.DISK_FILE_GID,
            M.DISK_FILE_PATH,
            M.ARCHIVE_REPORT_URL,
            M.ARCHIVE_ERROR_REPORT_URL,
            M.REQUESTER_NAME,
            M.REQUESTER_GROUP,
            M.SRC_URL,
            M.STORAGE_CLASS,
            M.MAX_RETRIES_WITHIN_MOUNT,
            M.MAX_TOTAL_RETRIES,
            M.TOTAL_REPORT_RETRIES,
            M.MAX_REPORT_RETRIES,
            M.VID AS VID,
            M.DRIVE AS DRIVE,
            M.HOST AS HOST,
            M.MOUNT_TYPE AS MOUNT_TYPE,
            M.LOGICAL_LIBRARY AS LOGICAL_LIBRARY,
            :STATUS AS STATUS,
            :TOTAL_RETRIES AS TOTAL_RETRIES,
            :RETRIES_WITHIN_MOUNT AS RETRIES_WITHIN_MOUNT,
            :LAST_MOUNT_WITH_FAILURE AS LAST_MOUNT_WITH_FAILURE,
            :FAILURE_LOG AS FAILURE_LOG,
  )SQL";
  if (!keepMountId) {
    sql += " NULL AS MOUNT_ID";
  } else {
    sql += " M.MOUNT_ID AS MOUNT_ID";
  }
  // Continue the query
  sql += R"SQL(
          FROM MOVED_ROWS M;
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  if (keepMountId) {
    std::string queueType = isRepack ? "REPACK_ARCHIVE_PENDING" : "ARCHIVE_PENDING";
    stmt.bindString(":QUEUE_TYPE", queueType);
  }
  stmt.bindString(":STATUS", to_string(newStatus));
  stmt.bindUint32(":TOTAL_RETRIES", totalRetries);
  stmt.bindUint32(":RETRIES_WITHIN_MOUNT", retriesWithinMount);
  stmt.bindUint64(":LAST_MOUNT_WITH_FAILURE", lastMountWithFailure);
  stmt.bindString(":FAILURE_LOG", failureLogs.value_or(""));
  if (userowjid) {
    stmt.bindUint64(":JOB_ID", jobId);
  }
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbMoveArchiveBackToPending);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
};

uint64_t ArchiveJobQueueRow::requeueJobBatch(Transaction& txn,
                                             ArchiveJobStatus newStatus,
                                             const std::list<std::string>& jobIDs,
                                             bool isRepack) {
  std::string repack_prefix = isRepack ? "REPACK_" : "";
  std::string sql = R"SQL(
    WITH MOVED_ROWS AS (
        DELETE FROM
  )SQL";
  sql += repack_prefix + "ARCHIVE_ACTIVE_QUEUE ";
  if (!jobIDs.empty()) {
    std::string sqlpart;
    for (const auto& jid : jobIDs) {
      sqlpart += jid + ",";
    }
    if (!sqlpart.empty()) {
      sqlpart.pop_back();
    }
    sql += std::string("WHERE JOB_ID IN (") + sqlpart + std::string(")");
  } else {
    return 0;
  }
  sql += R"SQL(
        RETURNING *
    )
    INSERT INTO
  )SQL";
  sql += repack_prefix + "ARCHIVE_PENDING_QUEUE ( ";
  if (isRepack) {
    sql += "REPACK_REQUEST_ID,";
  }
  sql += R"SQL(
    JOB_ID,
    ARCHIVE_REQUEST_ID,
    REQUEST_JOB_COUNT,
    TAPE_POOL,
    MOUNT_POLICY,
    PRIORITY,
    MIN_ARCHIVE_REQUEST_AGE,
    ARCHIVE_FILE_ID,
    SIZE_IN_BYTES,
    COPY_NB,
    START_TIME,
    CHECKSUMBLOB,
    CREATION_TIME,
    DISK_INSTANCE,
    DISK_FILE_ID,
    DISK_FILE_OWNER_UID,
    DISK_FILE_GID,
    DISK_FILE_PATH,
    ARCHIVE_REPORT_URL,
    ARCHIVE_ERROR_REPORT_URL,
    REQUESTER_NAME,
    REQUESTER_GROUP,
    SRC_URL,
    STORAGE_CLASS,
    RETRIES_WITHIN_MOUNT,
    MAX_RETRIES_WITHIN_MOUNT,
    TOTAL_RETRIES,
    MAX_TOTAL_RETRIES,
    TOTAL_REPORT_RETRIES,
    MAX_REPORT_RETRIES,
    LAST_MOUNT_WITH_FAILURE,
    VID,
    DRIVE,
    HOST,
    MOUNT_TYPE,
    LOGICAL_LIBRARY,
    STATUS,
    MOUNT_ID,
    FAILURE_LOG)
        SELECT
  )SQL";
  if (isRepack) {
    sql += " M.REPACK_REQUEST_ID,";
  }
  sql += R"SQL(
            M.JOB_ID,
            M.ARCHIVE_REQUEST_ID,
            M.REQUEST_JOB_COUNT,
            M.TAPE_POOL,
            M.MOUNT_POLICY,
            M.PRIORITY,
            M.MIN_ARCHIVE_REQUEST_AGE,
            M.ARCHIVE_FILE_ID,
            M.SIZE_IN_BYTES,
            M.COPY_NB,
            M.START_TIME,
            M.CHECKSUMBLOB,
            M.CREATION_TIME,
            M.DISK_INSTANCE,
            M.DISK_FILE_ID,
            M.DISK_FILE_OWNER_UID,
            M.DISK_FILE_GID,
            M.DISK_FILE_PATH,
            M.ARCHIVE_REPORT_URL,
            M.ARCHIVE_ERROR_REPORT_URL,
            M.REQUESTER_NAME,
            M.REQUESTER_GROUP,
            M.SRC_URL,
            M.STORAGE_CLASS,
            M.RETRIES_WITHIN_MOUNT,
            M.MAX_RETRIES_WITHIN_MOUNT,
            M.TOTAL_RETRIES,
            M.MAX_TOTAL_RETRIES,
            M.TOTAL_REPORT_RETRIES,
            M.MAX_REPORT_RETRIES,
            M.LAST_MOUNT_WITH_FAILURE,
            M.VID,
            M.DRIVE,
            M.HOST,
            M.MOUNT_TYPE,
            M.LOGICAL_LIBRARY,
            :STATUS AS STATUS,
            NULL AS MOUNT_ID,
            FAILURE_LOG || :FAILURE_LOG AS FAILURE_LOG
        FROM MOVED_ROWS M;
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(newStatus));
  stmt.bindString(":FAILURE_LOG", "UNPROCESSED_TASK_QUEUE_JOB_REQUEUED");
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbMoveArchiveBackToPending);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

uint64_t ArchiveJobQueueRow::moveJobToFailedQueueTable(Transaction& txn) {
  std::string sql = R"SQL(
    WITH MOVED_ROWS AS (
        DELETE FROM ARCHIVE_ACTIVE_QUEUE
  )SQL";
  if (reqJobCount > 1) {
    sql += R"SQL(
        WHERE ARCHIVE_REQUEST_ID = :ARCHIVE_REQUEST_ID
    )SQL";
  } else {
    sql += R"SQL(
        WHERE JOB_ID = :JOB_ID
    )SQL";
  }
  sql += R"SQL(
        RETURNING *
    ) INSERT INTO ARCHIVE_FAILED_QUEUE SELECT * FROM MOVED_ROWS;
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  if (reqJobCount > 1) {
    stmt.bindUint64(":ARCHIVE_REQUEST_ID", reqId);
  } else {
    stmt.bindUint64(":JOB_ID", jobId);
  }
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbMoveRepackArchiveToFailed);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

rdbms::Rset ArchiveJobQueueRow::moveFailedRepackJobBatchToFailedQueueTable(Transaction& txn, uint64_t limit) {
  std::string sql = R"SQL(
    WITH MOVED_ROWS AS (
        DELETE FROM REPACK_ARCHIVE_ACTIVE_QUEUE
        WHERE JOB_ID IN (
            SELECT JOB_ID
            FROM REPACK_ARCHIVE_ACTIVE_QUEUE
            WHERE STATUS = 'AJS_ToReportToRepackForFailure'
            ORDER BY PRIORITY DESC, JOB_ID
            LIMIT :LIMIT
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
    )
    INSERT INTO REPACK_ARCHIVE_FAILED_QUEUE
        SELECT * FROM MOVED_ROWS
        RETURNING REPACK_REQUEST_ID, SIZE_IN_BYTES, SRC_URL
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindUint64(":LIMIT", limit);
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbMoveRepackArchiveToFailed);
  auto rset = stmt.executeQuery();
  return rset;
}

uint64_t
ArchiveJobQueueRow::updateJobStatusForFailedReport(Transaction& txn, ArchiveJobStatus newStatus, bool isRepack) {
  // otherwise update the statistics and requeue the job
  std::string repack_prefix = isRepack ? "REPACK_" : "";
  std::string sql = R"SQL(
      UPDATE )SQL";
  sql += repack_prefix + "ARCHIVE_ACTIVE_QUEUE SET ";
  // For repack, STATUS is left for updateRepackJobSuccess() below to set correctly - see comment.
  if (!isRepack) {
    sql += "STATUS = :STATUS, ";
  }
  sql += R"SQL(TOTAL_REPORT_RETRIES = :TOTAL_REPORT_RETRIES,
        IS_REPORTING =:IS_REPORTING,
        REPORT_FAILURE_LOG = :REPORT_FAILURE_LOG,
        LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
      WHERE JOB_ID = :JOB_ID
    )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  if (!isRepack) {
    stmt.bindString(":STATUS", to_string(newStatus));
  }
  stmt.bindUint32(":TOTAL_REPORT_RETRIES", totalReportRetries);
  stmt.bindBool(":IS_REPORTING", isReporting);
  stmt.bindString(":REPORT_FAILURE_LOG", reportFailureLogs.value_or(""));
  stmt.bindUint64(":JOB_ID", jobId);
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbUpdateArchive);
  txn.setRowCountForTelemetry(1);
  stmt.executeNonQuery();
  /* USER job: on the terminal failure (newStatus=ReadyForDeletion), move the row to
   * ARCHIVE_FAILED_QUEUE via moveJobToFailedQueueTable(), multi-copy requests included. Checks
   * newStatus, not status (stale in-memory value) - status missed the real transition and left
   * rows stuck in ARCHIVE_ACTIVE_QUEUE forever once retries were exhausted.
   *
   * REPACK job: repack transfer failures never reach this function (updateFailedJobStatus()
   * handles those). The only way here is updateRepackJobSuccess() itself failing to persist an
   * already-successful transfer, so newStatus=ReadyForDeletion means "recover that lost write",
   * not "report a failure". STATUS is left for updateRepackJobSuccess() to set, since only it
   * knows whether a multi-copy request's other copies have also succeeded - forcing
   * ReadyForDeletion here directly would let getNextSuccessfulArchiveRepackReportBatch() delete
   * the source file before every copy exists. A second failure on this recovery attempt (fresh
   * connection, same write) is caught by failReport()'s own try/catch and falls back to
   * dead-mount GC: one wasted re-transfer, not lost data, not a retry loop.
   */
  if (isRepack && newStatus == ArchiveJobStatus::ReadyForDeletion) {
    return ArchiveJobQueueRow::updateRepackJobSuccess(txn, {std::to_string(jobId)});
  }
  if (newStatus == ArchiveJobStatus::ReadyForDeletion && !isRepack) {
    return ArchiveJobQueueRow::moveJobToFailedQueueTable(txn);
  }
  return stmt.getNbAffectedRows();
};

rdbms::Rset ArchiveJobQueueRow::flagReportingJobsByStatus(Transaction& txn,
                                                          std::list<ArchiveJobStatus> statusList,
                                                          uint64_t limit) {
  std::string sql = R"SQL(
      WITH SET_SELECTION AS (
        SELECT JOB_ID FROM ARCHIVE_ACTIVE_QUEUE
        WHERE STATUS = ANY(ARRAY[
    )SQL";
  // we can move this to new bindArray method for stmt
  std::vector<std::string> statusVec;
  std::vector<std::string> placeholderVec;
  size_t j = 1;
  for (const auto& jstatus : statusList) {
    statusVec.emplace_back(to_string(jstatus));
    std::string plch = std::string(":STATUS") + std::to_string(j);
    placeholderVec.emplace_back(plch);
    sql += plch;
    if (&jstatus != &statusList.back()) {
      sql += std::string(",");
    }
    j++;
  }
  sql += R"SQL(
        ]::ARCHIVE_JOB_STATUS[]) AND IS_REPORTING IS FALSE
        LIMIT :LIMIT FOR UPDATE SKIP LOCKED)
      UPDATE ARCHIVE_ACTIVE_QUEUE SET
        IS_REPORTING = TRUE,
        LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
      FROM SET_SELECTION
      WHERE ARCHIVE_ACTIVE_QUEUE.JOB_ID = SET_SELECTION.JOB_ID
      RETURNING ARCHIVE_ACTIVE_QUEUE.*
    )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  // we can move the array binding to new bindArray method for STMT
  size_t sz = statusVec.size();
  for (size_t i = 0; i < sz; ++i) {
    stmt.bindString(placeholderVec[i], statusVec[i]);
  }
  stmt.bindUint64(":LIMIT", limit);
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbUpdateArchive);
  return stmt.executeQuery();
}

uint64_t ArchiveJobQueueRow::getNextArchiveRequestID(rdbms::Conn& conn) {
  const char* const sql = R"SQL(
        SELECT NEXTVAL('ARCHIVE_REQUEST_ID_SEQ') AS ARCHIVE_REQUEST_ID
      )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw exception::Exception("Result set is unexpectedly empty");
  }
  return rset.columnUint64("ARCHIVE_REQUEST_ID");
}

uint64_t
ArchiveJobQueueRow::cancelArchiveJob(Transaction& txn, const std::string& diskInstance, uint64_t archiveFileID) {
  /* All jobs which were already picked up by the
   * mount to the task queue will run and not be deleted.
   * Deletes only from ARCHIVE_PENDING_QUEUE
   */
  std::string sqlActive = R"SQL(
    DELETE FROM ARCHIVE_PENDING_QUEUE
    WHERE
      DISK_INSTANCE = :DISK_INSTANCE AND
      ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID
  )SQL";
  auto stmt = txn.getConn().createStmt(sqlActive);
  stmt.bindString(":DISK_INSTANCE", diskInstance);
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileID);
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbDeleteArchive);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

uint64_t ArchiveJobQueueRow::deleteFailedArchiveJob(Transaction& txn, uint64_t jobID, bool repack) {
  std::string tableName = "ARCHIVE_FAILED_QUEUE";
  if (repack) {
    tableName = "REPACK_ARCHIVE_FAILED_QUEUE";
  }
  std::string sqlActive = R"SQL(
    DELETE FROM
 )SQL";
  sqlActive += tableName;
  sqlActive += R"SQL(
    WHERE
      JOB_ID = :JOB_ID
  )SQL";
  auto stmt = txn.getConn().createStmt(sqlActive);
  stmt.bindUint64(":JOB_ID", jobID);
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbDeleteArchive);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

// The SQL below selects d.failure_log AS FAILURE_LOG with a plain reference, no SQL-side
// concatenation: d.failure_log (bound from row->failureLogs.value_or("")) already contains
// m.FAILURE_LOG's old text plus the new entry, since updateJobRowFailureLog() appends onto a
// field that was itself loaded from this same column - concatenating again here would
// duplicate the old text.
uint64_t ArchiveJobQueueRow::failTransferBatchRequeue(Transaction& txn,
                                                      const std::vector<ArchiveJobQueueRow*>& jobs,
                                                      bool isRepack) {
  if (jobs.empty()) {
    return 0;
  }
  const std::string repack_prefix = isRepack ? "REPACK_" : "";
  const std::string pending_status = isRepack ? to_string(ArchiveJobStatus::AJS_ToTransferForRepack) :
                                                to_string(ArchiveJobStatus::AJS_ToTransferForUser);

  // Build per-row VALUES CTE with numbered bind params
  std::string values_rows;
  for (size_t i = 0; i < jobs.size(); ++i) {
    if (i > 0) {
      values_rows += ",\n    ";
    }
    const std::string s = std::to_string(i);
    values_rows += "(:JID" + s + "::BIGINT, :TR" + s + "::SMALLINT, :RWM" + s + "::SMALLINT, :LMF" + s + "::BIGINT, :FL"
                   + s + "::TEXT, :KM" + s + "::BOOL)";
  }

  std::string sql = R"SQL(
  WITH per_job(job_id, total_retries, retries_within_mount, last_mount, failure_log, keep_mount) AS (
    VALUES )SQL";
  sql += values_rows;
  sql += R"SQL(
  ),
  moved AS (
    DELETE FROM )SQL";
  sql += repack_prefix + R"SQL(ARCHIVE_ACTIVE_QUEUE
    WHERE JOB_ID IN (SELECT job_id FROM per_job)
    RETURNING *
  ),
  updated_last_fetch AS (
    INSERT INTO MOUNT_QUEUE_LAST_FETCH (MOUNT_ID, LAST_UPDATE_TIME, QUEUE_TYPE)
      SELECT DISTINCT m.MOUNT_ID,
        EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
        :QUEUE_TYPE
      FROM moved m
      JOIN per_job d ON m.JOB_ID = d.job_id
      WHERE m.MOUNT_ID IS NOT NULL AND d.keep_mount
    ON CONFLICT (MOUNT_ID, QUEUE_TYPE) DO UPDATE SET
      LAST_UPDATE_TIME = EXCLUDED.LAST_UPDATE_TIME
      WHERE MOUNT_QUEUE_LAST_FETCH.LAST_UPDATE_TIME < EXCLUDED.LAST_UPDATE_TIME - 5
  )
  INSERT INTO )SQL";
  sql += repack_prefix + "ARCHIVE_PENDING_QUEUE (";
  if (isRepack) {
    sql += "\n  REPACK_REQUEST_ID,";
  }
  sql += R"SQL(
  JOB_ID, ARCHIVE_REQUEST_ID, REQUEST_JOB_COUNT, TAPE_POOL, MOUNT_POLICY, PRIORITY,
  MIN_ARCHIVE_REQUEST_AGE, ARCHIVE_FILE_ID, SIZE_IN_BYTES, COPY_NB, START_TIME,
  CHECKSUMBLOB, CREATION_TIME, DISK_INSTANCE, DISK_FILE_ID, DISK_FILE_OWNER_UID,
  DISK_FILE_GID, DISK_FILE_PATH, ARCHIVE_REPORT_URL, ARCHIVE_ERROR_REPORT_URL,
  REQUESTER_NAME, REQUESTER_GROUP, SRC_URL, STORAGE_CLASS,
  MAX_RETRIES_WITHIN_MOUNT, MAX_TOTAL_RETRIES, TOTAL_REPORT_RETRIES, MAX_REPORT_RETRIES,
  VID, DRIVE, HOST, MOUNT_TYPE, LOGICAL_LIBRARY,
  STATUS, TOTAL_RETRIES, RETRIES_WITHIN_MOUNT, LAST_MOUNT_WITH_FAILURE, FAILURE_LOG, MOUNT_ID
  )
  SELECT )SQL";
  if (isRepack) {
    sql += "\n  m.REPACK_REQUEST_ID,";
  }
  sql += R"SQL(
  m.JOB_ID, m.ARCHIVE_REQUEST_ID, m.REQUEST_JOB_COUNT, m.TAPE_POOL, m.MOUNT_POLICY, m.PRIORITY,
  m.MIN_ARCHIVE_REQUEST_AGE, m.ARCHIVE_FILE_ID, m.SIZE_IN_BYTES, m.COPY_NB, m.START_TIME,
  m.CHECKSUMBLOB, m.CREATION_TIME, m.DISK_INSTANCE, m.DISK_FILE_ID, m.DISK_FILE_OWNER_UID,
  m.DISK_FILE_GID, m.DISK_FILE_PATH, m.ARCHIVE_REPORT_URL, m.ARCHIVE_ERROR_REPORT_URL,
  m.REQUESTER_NAME, m.REQUESTER_GROUP, m.SRC_URL, m.STORAGE_CLASS,
  m.MAX_RETRIES_WITHIN_MOUNT, m.MAX_TOTAL_RETRIES, m.TOTAL_REPORT_RETRIES, m.MAX_REPORT_RETRIES,
  m.VID, m.DRIVE, m.HOST, m.MOUNT_TYPE, m.LOGICAL_LIBRARY,
  :PENDING_STATUS::ARCHIVE_JOB_STATUS AS STATUS,
  d.total_retries, d.retries_within_mount, d.last_mount,
  d.failure_log AS FAILURE_LOG,
  CASE WHEN d.keep_mount THEN m.MOUNT_ID ELSE NULL END AS MOUNT_ID
  FROM moved m
  JOIN per_job d ON m.JOB_ID = d.job_id
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":QUEUE_TYPE", isRepack ? "REPACK_ARCHIVE_PENDING" : "ARCHIVE_PENDING");
  stmt.bindString(":PENDING_STATUS", pending_status);

  for (size_t i = 0; i < jobs.size(); ++i) {
    const auto* row = jobs[i];
    const std::string s = std::to_string(i);
    // keepMountId: stay on same mount if within-mount retry budget not exhausted. Removed a dead
    // "force a new mount if the SESSION itself crashed" check that matched failureLogs against a
    // TapeWriteSingleThread log message which is never actually produced (see git history for
    // details) - it could not have worked by construction either: a crash orphans jobs via
    // TapeWriteSingleThread::requeueUnprocessedTasks() -> ArchiveMount::requeueJobBatch(), a
    // separate path that never reaches this query. The case it was trying to cover - a job left
    // pinned to a mount that dies later - is already handled by the garbage collector
    // (ArchiveInactiveMountPendingQueueRoutine), which resets MOUNT_ID once it detects the mount
    // is gone. Matches ArchiveRdbJob.cpp's failTransfer() single-job equivalent, which had the
    // same (equally dead) check. Retrieve's equivalent never had this check at all.
    const bool keepMountId = row->retriesWithinMount < row->maxRetriesWithinMount;
    stmt.bindUint64(":JID" + s, row->jobId);
    stmt.bindUint32(":TR" + s, row->totalRetries);
    stmt.bindUint32(":RWM" + s, row->retriesWithinMount);
    stmt.bindUint64(":LMF" + s, row->lastMountWithFailure);
    stmt.bindString(":FL" + s, row->failureLogs.value_or(""));
    stmt.bindBool(":KM" + s, keepMountId);
  }

  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbMoveArchiveBackToPending);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

// The SQL below sets FAILURE_LOG = d.failure_log with a plain assignment, no SQL-side
// concatenation: d.failure_log (bound from row->failureLogs.value_or("")) already has
// a.FAILURE_LOG's old text plus the new entry appended in memory (updateJobRowFailureLog()) -
// concatenating again here would duplicate the old text.
uint64_t ArchiveJobQueueRow::failTransferBatchExceedRetries(Transaction& txn,
                                                            const std::vector<ArchiveJobQueueRow*>& jobs,
                                                            bool isRepack) {
  if (jobs.empty()) {
    return 0;
  }
  const std::string repack_prefix = isRepack ? "REPACK_" : "";
  const ArchiveJobStatus newStatus =
    isRepack ? ArchiveJobStatus::AJS_ToReportToRepackForFailure : ArchiveJobStatus::AJS_ToReportToUserForFailure;

  // Non-repack multi-copy requests fail all copies atomically, by ARCHIVE_REQUEST_ID (matching
  // the single-job path); repack copies are independent and use per-JOB_ID semantics.
  std::string values_rows;
  for (size_t i = 0; i < jobs.size(); ++i) {
    if (i > 0) {
      values_rows += ",\n    ";
    }
    const std::string s = std::to_string(i);
    values_rows += "(:JID" + s + "::BIGINT, :TR" + s + "::SMALLINT, :RWM" + s + "::SMALLINT, :LMF" + s + "::BIGINT, :FL"
                   + s + "::TEXT" + ", :REQID" + s + "::BIGINT, :RJC" + s + "::SMALLINT)";
  }

  std::string sql = R"SQL(
  WITH per_job(job_id, total_retries, retries_within_mount, last_mount, failure_log,
               archive_request_id, req_job_count) AS (
    VALUES )SQL";
  sql += values_rows;
  sql += R"SQL(
  )
  UPDATE )SQL";
  sql += repack_prefix + R"SQL(ARCHIVE_ACTIVE_QUEUE a
  SET
    STATUS = :STATUS::ARCHIVE_JOB_STATUS,
    TOTAL_RETRIES = d.total_retries,
    RETRIES_WITHIN_MOUNT = d.retries_within_mount,
    LAST_MOUNT_WITH_FAILURE = d.last_mount,
    FAILURE_LOG = d.failure_log,
    LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
  FROM per_job d
  WHERE (d.req_job_count = 1 AND a.JOB_ID = d.job_id)
     OR (d.req_job_count > 1 AND a.ARCHIVE_REQUEST_ID = d.archive_request_id)
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(newStatus));

  for (size_t i = 0; i < jobs.size(); ++i) {
    const auto* row = jobs[i];
    const std::string s = std::to_string(i);
    stmt.bindUint64(":JID" + s, row->jobId);
    stmt.bindUint32(":TR" + s, row->totalRetries);
    stmt.bindUint32(":RWM" + s, row->retriesWithinMount);
    stmt.bindUint64(":LMF" + s, row->lastMountWithFailure);
    stmt.bindString(":FL" + s, row->failureLogs.value_or(""));
    stmt.bindUint64(":REQID" + s, row->reqId);
    stmt.bindUint32(":RJC" + s, row->reqJobCount);
  }

  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbUpdateArchive);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

uint64_t ArchiveJobQueueRow::failReportBatchExceedRetries(Transaction& txn,
                                                          const std::vector<ArchiveJobQueueRow*>& jobs,
                                                          bool isRepack) {
  if (jobs.empty()) {
    return 0;
  }

  // Non-repack multi-copy requests move all copies atomically, by ARCHIVE_REQUEST_ID (matching
  // single-job failReport()); repack always has req_job_count = 1 (its multi-copy grouping is by
  // ARCHIVE_FILE_ID, handled entirely inside updateRepackJobSuccess() below).
  std::string values_rows;
  for (size_t i = 0; i < jobs.size(); ++i) {
    if (i > 0) {
      values_rows += ",\n    ";
    }
    const std::string s = std::to_string(i);
    values_rows += "(:JID" + s + "::BIGINT, :TRR" + s + "::SMALLINT, :RFL" + s + "::TEXT, :REQID" + s + "::BIGINT, :RJC"
                   + s + "::SMALLINT)";
  }

  if (isRepack) {
    // REPACK: "exceeded report retries" here means the DB WRITE failed repeatedly, not that the
    // job failed - repack transfer failures never reach this function (updateFailedJobStatus()
    // handles those). The only way here is updateRepackJobSuccess() itself failing to persist an
    // already-successful transfer, so this means "recover that lost write", not "report a
    // failure" - mirrors ArchiveRdbJob::failReport()'s single-job handling of the same case.
    // STATUS is intentionally NOT set by the UPDATE below: only updateRepackJobSuccess() knows
    // whether a multi-copy request's other copies have also succeeded - moving straight to
    // ARCHIVE_FAILED_QUEUE here (as the non-repack path below does) would let
    // getNextSuccessfulArchiveRepackReportBatch() delete the source file before every copy exists.
    std::string sql = R"SQL(
    WITH per_job(job_id, total_report_retries, report_failure_log,
                 archive_request_id, req_job_count) AS (
      VALUES )SQL";
    sql += values_rows;
    sql += R"SQL(
    )
    UPDATE REPACK_ARCHIVE_ACTIVE_QUEUE a SET
      TOTAL_REPORT_RETRIES = d.total_report_retries,
      IS_REPORTING = FALSE,
      REPORT_FAILURE_LOG = d.report_failure_log,
      LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
    FROM per_job d
    WHERE a.JOB_ID = d.job_id
    )SQL";

    auto stmt = txn.getConn().createStmt(sql);
    for (size_t i = 0; i < jobs.size(); ++i) {
      const auto* row = jobs[i];
      const std::string s = std::to_string(i);
      stmt.bindUint64(":JID" + s, row->jobId);
      stmt.bindUint32(":TRR" + s, row->totalReportRetries);
      stmt.bindString(":RFL" + s, row->reportFailureLogs.value_or(""));
      stmt.bindUint64(":REQID" + s, row->reqId);
      stmt.bindUint32(":RJC" + s, row->reqJobCount);
    }
    txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbFailReportBatchArchive);
    stmt.executeNonQuery();

    std::vector<std::string> jobIds;
    jobIds.reserve(jobs.size());
    for (const auto* row : jobs) {
      jobIds.push_back(std::to_string(row->jobId));
    }
    return ArchiveJobQueueRow::updateRepackJobSuccess(txn, jobIds);
  }

  // USER: update STATUS to ReadyForDeletion, move to failed queue, and delete the original row
  // from the active queue - all in one CTE chain, one round trip. For multi-copy requests the
  // WHERE clause matches all sibling copies by ARCHIVE_REQUEST_ID so the entire request is moved
  // atomically. The final DELETE targets exactly the JOB_IDs returned by the INSERT, which
  // already covers sibling copies pulled in via ARCHIVE_REQUEST_ID - without it, the row would be
  // duplicated into ARCHIVE_FAILED_QUEUE but left behind (stuck forever) in ARCHIVE_ACTIVE_QUEUE.
  std::string sql = R"SQL(
  WITH per_job(job_id, total_report_retries, report_failure_log,
               archive_request_id, req_job_count) AS (
    VALUES )SQL";
  sql += values_rows;
  sql += R"SQL(
  ),
  updated AS (
    UPDATE ARCHIVE_ACTIVE_QUEUE a SET
      STATUS = 'ReadyForDeletion'::ARCHIVE_JOB_STATUS,
      TOTAL_REPORT_RETRIES = d.total_report_retries,
      IS_REPORTING = FALSE,
      REPORT_FAILURE_LOG = d.report_failure_log,
      LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
    FROM per_job d
    WHERE (d.req_job_count = 1 AND a.JOB_ID = d.job_id)
       OR (d.req_job_count > 1 AND a.ARCHIVE_REQUEST_ID = d.archive_request_id)
    RETURNING a.*
  ),
  inserted AS (
    INSERT INTO ARCHIVE_FAILED_QUEUE SELECT * FROM updated RETURNING JOB_ID
  )
  DELETE FROM ARCHIVE_ACTIVE_QUEUE WHERE JOB_ID IN (SELECT JOB_ID FROM inserted)
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  for (size_t i = 0; i < jobs.size(); ++i) {
    const auto* row = jobs[i];
    const std::string s = std::to_string(i);
    stmt.bindUint64(":JID" + s, row->jobId);
    stmt.bindUint32(":TRR" + s, row->totalReportRetries);
    stmt.bindString(":RFL" + s, row->reportFailureLogs.value_or(""));
    stmt.bindUint64(":REQID" + s, row->reqId);
    stmt.bindUint32(":RJC" + s, row->reqJobCount);
  }

  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbFailReportBatchArchive);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

uint64_t ArchiveJobQueueRow::failReportBatchRequeue(Transaction& txn,
                                                    const std::vector<ArchiveJobQueueRow*>& jobs,
                                                    bool isRepack) {
  if (jobs.empty()) {
    return 0;
  }
  const std::string repack_prefix = isRepack ? "REPACK_" : "";

  // Build per-row VALUES CTE: (job_id, total_report_retries, report_failure_log)
  std::string values_rows;
  for (size_t i = 0; i < jobs.size(); ++i) {
    if (i > 0) {
      values_rows += ",\n    ";
    }
    const std::string s = std::to_string(i);
    values_rows += "(:JID" + s + "::BIGINT, :TRR" + s + "::SMALLINT, :RFL" + s + "::TEXT)";
  }

  // Reset IS_REPORTING so the flagging loop will pick these jobs up for another attempt.
  std::string sql = R"SQL(
  WITH per_job(job_id, total_report_retries, report_failure_log) AS (
    VALUES )SQL";
  sql += values_rows;
  sql += R"SQL(
  )
  UPDATE )SQL";
  sql += repack_prefix + R"SQL(ARCHIVE_ACTIVE_QUEUE a SET
    IS_REPORTING = FALSE,
    TOTAL_REPORT_RETRIES = d.total_report_retries,
    REPORT_FAILURE_LOG = d.report_failure_log,
    LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
  FROM per_job d
  WHERE a.JOB_ID = d.job_id
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  for (size_t i = 0; i < jobs.size(); ++i) {
    const auto* row = jobs[i];
    const std::string s = std::to_string(i);
    stmt.bindUint64(":JID" + s, row->jobId);
    stmt.bindUint32(":TRR" + s, row->totalReportRetries);
    stmt.bindString(":RFL" + s, row->reportFailureLogs.value_or(""));
  }

  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbFailReportBatchArchive);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

}  // namespace cta::schedulerdb::postgres
