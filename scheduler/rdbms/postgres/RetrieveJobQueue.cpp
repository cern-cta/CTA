/*
 * SPDX-FileCopyrightText: 2023 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/postgres/RetrieveJobQueue.hpp"

#include "rdbms/wrapper/PostgresColumn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"
#include "scheduler/rdbms/postgres/ArchiveJobQueue.hpp"

namespace cta::schedulerdb::postgres {

uint64_t
RetrieveJobQueueRow::updateMountQueueLastFetch(Transaction& txn, uint64_t mountId, bool isActive, bool isRepack) {
  std::string sql = R"SQL(
       INSERT INTO MOUNT_QUEUE_LAST_FETCH (MOUNT_ID, LAST_UPDATE_TIME, QUEUE_TYPE)
         VALUES (:MOUNT_ID, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER, :QUEUE_TYPE)
       ON CONFLICT (MOUNT_ID, QUEUE_TYPE)
         DO UPDATE SET
           LAST_UPDATE_TIME = EXCLUDED.LAST_UPDATE_TIME,
           QUEUE_TYPE = EXCLUDED.QUEUE_TYPE
             WHERE MOUNT_QUEUE_LAST_FETCH.LAST_UPDATE_TIME
             < EXCLUDED.LAST_UPDATE_TIME - 5
)SQL";
  auto stmt = txn.getConn().createStmt(sql);
  std::string queueType = isRepack ? "REPACK_RETRIEVE" : "RETRIEVE";
  queueType += isActive ? "_ACTIVE" : "_PENDING";
  stmt.bindString(":QUEUE_TYPE", queueType);
  stmt.bindUint64(":MOUNT_ID", mountId);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.getConn().setDbQuerySummary("update mount_queue_last_fetch");
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

std::pair<rdbms::Rset, uint64_t>
RetrieveJobQueueRow::moveJobsToDbActiveQueue(Transaction& txn,
                                             RetrieveJobStatus newStatus,
                                             const SchedulerDatabase::RetrieveMount::MountInfo& mountInfo,
                                             std::vector<std::string>& noSpaceDiskSystemNames,
                                             uint64_t maxBytesRequested,
                                             uint64_t limit,
                                             bool isRepack) {
  // we first check if there are any disk systems
  // we should avoid querying jobs for
  std::string sql_dsn_exclusion_part = "";
  if (!noSpaceDiskSystemNames.empty()) {
    sql_dsn_exclusion_part = R"SQL( AND DISK_SYSTEM_NAME != ALL(ARRAY[)SQL";
    size_t j = 1;
    for (const auto& dsn : noSpaceDiskSystemNames) {
      std::string plch = std::string(":") + dsn + std::to_string(j);
      sql_dsn_exclusion_part += plch;
      if (j != noSpaceDiskSystemNames.size()) {
        sql_dsn_exclusion_part += std::string(",");
      }
      j++;
    }
    sql_dsn_exclusion_part += R"SQL( ]) )SQL";
  }

  std::string repack_table_name_prefix = isRepack ? "REPACK_" : "";

  /* using write row lock FOR UPDATE for the select statement
   * since it is the same lock used for UPDATE
   * we first apply the LIMIT on the selection to limit
   * the number of rows and only after calculate the
   * running cumulative sum of bytes in the consequent step */
  std::string sql = R"SQL(
    WITH SET_SELECTION AS (
      SELECT JOB_ID, PRIORITY, SIZE_IN_BYTES
      FROM )SQL";
  sql += repack_table_name_prefix + "RETRIEVE_PENDING_QUEUE";
  sql += R"SQL(
      WHERE VID = :VID
      AND STATUS = :STATUS
      AND (MOUNT_ID IS NULL OR MOUNT_ID = :SAME_MOUNT_ID)
      )SQL";
  sql += sql_dsn_exclusion_part;
  sql += R"SQL(
      ORDER BY PRIORITY DESC, JOB_ID
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
  sql += repack_table_name_prefix + "RETRIEVE_PENDING_QUEUE";
  sql += R"SQL( RIQ
        USING SELECTION_WITH_CUMULATIVE_SUMS CSEL
        WHERE RIQ.JOB_ID = CSEL.JOB_ID
        RETURNING RIQ.*
    )
    INSERT INTO )SQL";
  sql += repack_table_name_prefix + "RETRIEVE_ACTIVE_QUEUE ( ";
  if (isRepack) {
    sql += " REPACK_REQUEST_ID,";
    sql += " REPACK_REARCHIVE_COPY_NBS,";
    sql += " REPACK_REARCHIVE_TAPE_POOLS,";
  }
  sql += R"SQL(
        JOB_ID,
        RETRIEVE_REQUEST_ID,
        REQUEST_JOB_COUNT,
        STATUS,
        TAPE_POOL,
        MOUNT_POLICY,
        PRIORITY,
        MIN_RETRIEVE_REQUEST_AGE,
        ARCHIVE_FILE_ID,
        SIZE_IN_BYTES,
        COPY_NB,
        ALTERNATE_COPY_NBS,
        ALTERNATE_FSEQS,
        ALTERNATE_BLOCK_IDS,
        START_TIME,
        CHECKSUMBLOB,
        FSEQ,
        BLOCK_ID,
        CREATION_TIME,
        DISK_INSTANCE,
        DISK_FILE_ID,
        DISK_FILE_OWNER_UID,
        DISK_FILE_GID,
        DISK_FILE_PATH,
        RETRIEVE_REPORT_URL,
        RETRIEVE_ERROR_REPORT_URL,
        REQUESTER_NAME,
        REQUESTER_GROUP,
        DST_URL,
        STORAGE_CLASS,
        RETRIES_WITHIN_MOUNT,
        TOTAL_RETRIES,
        LAST_MOUNT_WITH_FAILURE,
        MAX_TOTAL_RETRIES,
        MAX_RETRIES_WITHIN_MOUNT,
        TOTAL_REPORT_RETRIES,
        MAX_REPORT_RETRIES,
        MOUNT_ID,
        VID,
        ALTERNATE_VIDS,
        DRIVE,
        HOST,
        LOGICAL_LIBRARY,
        ACTIVITY,
        SRR_USERNAME,
        SRR_HOST,
        SRR_TIME,
        SRR_MOUNT_POLICY,
        SRR_ACTIVITY,
        LIFECYCLE_CREATION_TIME,
        LIFECYCLE_FIRST_SELECTED_TIME,
        LIFECYCLE_COMPLETED_TIME,
        DISK_SYSTEM_NAME
    )
    SELECT
    )SQL";
  if (isRepack) {
    sql += " M.REPACK_REQUEST_ID,";
    sql += " M.REPACK_REARCHIVE_COPY_NBS,";
    sql += " M.REPACK_REARCHIVE_TAPE_POOLS,";
  }
  sql += R"SQL(
        M.JOB_ID,
        M.RETRIEVE_REQUEST_ID,
        M.REQUEST_JOB_COUNT,
        M.STATUS,
        :TAPE_POOL AS TAPE_POOL,
        M.MOUNT_POLICY,
        M.PRIORITY,
        M.MIN_RETRIEVE_REQUEST_AGE,
        M.ARCHIVE_FILE_ID,
        M.SIZE_IN_BYTES,
        M.COPY_NB,
        M.ALTERNATE_COPY_NBS,
        M.ALTERNATE_FSEQS,
        M.ALTERNATE_BLOCK_IDS,
        M.START_TIME,
        M.CHECKSUMBLOB,
        M.FSEQ,
        M.BLOCK_ID,
        M.CREATION_TIME,
        M.DISK_INSTANCE,
        M.DISK_FILE_ID,
        M.DISK_FILE_OWNER_UID,
        M.DISK_FILE_GID,
        M.DISK_FILE_PATH,
        M.RETRIEVE_REPORT_URL,
        M.RETRIEVE_ERROR_REPORT_URL,
        M.REQUESTER_NAME,
        M.REQUESTER_GROUP,
        M.DST_URL,
        M.STORAGE_CLASS,
        M.RETRIES_WITHIN_MOUNT,
        M.TOTAL_RETRIES,
        M.LAST_MOUNT_WITH_FAILURE,
        M.MAX_TOTAL_RETRIES,
        M.MAX_RETRIES_WITHIN_MOUNT,
        M.TOTAL_REPORT_RETRIES,
        M.MAX_REPORT_RETRIES,
        :MOUNT_ID AS MOUNT_ID,
        M.VID AS VID,
        M.ALTERNATE_VIDS AS ALTERNATE_VIDS,
        :DRIVE AS DRIVE,
        :HOST AS HOST,
        :LOGICAL_LIBRARY AS LOGICAL_LIBRARY,
        M.ACTIVITY,
        M.SRR_USERNAME,
        M.SRR_HOST,
        M.SRR_TIME,
        M.SRR_MOUNT_POLICY,
        M.SRR_ACTIVITY,
        M.LIFECYCLE_CREATION_TIME,
        M.LIFECYCLE_FIRST_SELECTED_TIME,
        M.LIFECYCLE_COMPLETED_TIME,
        M.DISK_SYSTEM_NAME
    FROM MOVED_ROWS M
    RETURNING *
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":VID", mountInfo.vid);
  stmt.bindString(":STATUS", to_string(newStatus));
  stmt.bindUint32(":LIMIT", limit);
  stmt.bindUint64(":MOUNT_ID", mountInfo.mountId);
  stmt.bindUint64(":SAME_MOUNT_ID", mountInfo.mountId);
  size_t j = 1;
  for (const auto& dsn : noSpaceDiskSystemNames) {
    std::string plch = std::string(":") + dsn + std::to_string(j);
    stmt.bindString(plch, dsn);
    sql_dsn_exclusion_part += plch;
    j++;
  }
  stmt.bindString(":DRIVE", mountInfo.drive);
  stmt.bindString(":HOST", mountInfo.host);
  stmt.bindString(":LOGICAL_LIBRARY", mountInfo.logicalLibrary);
  stmt.bindString(":TAPE_POOL", mountInfo.tapePool.empty() ? "NOT_PROVIDED" : mountInfo.tapePool);
  stmt.bindUint64(":BYTES_REQUESTED", maxBytesRequested);
  txn.getConn().setDbQuerySummary("move retrieve to active queue");
  auto result = stmt.executeQuery();
  auto nrows = stmt.getNbAffectedRows();
  return std::make_pair(std::move(result), nrows);
}

uint64_t RetrieveJobQueueRow::updateJobStatus(Transaction& txn,
                                              RetrieveJobStatus newStatus,
                                              const std::vector<std::string>& jobIDs,
                                              bool isRepack) {
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

  std::string repack_table_name_prefix = isRepack ? "REPACK_" : "";

  // DISABLE DELETION FOR DEBUGGING
  if (newStatus == RetrieveJobStatus::RJS_Complete || newStatus == RetrieveJobStatus::RJS_Failed
      || newStatus == RetrieveJobStatus::ReadyForDeletion) {
    std::string sql = R"SQL(
        DELETE FROM
      )SQL";
    sql += repack_table_name_prefix + "RETRIEVE_ACTIVE_QUEUE ";
    sql += R"SQL(
        WHERE
          JOB_ID IN (
        )SQL";
    sql += sqlpart + std::string(")");
    auto stmt2 = txn.getConn().createStmt(sql);
    txn.getConn().setDbQuerySummary("delete retrieve");
    stmt2.executeNonQuery();
    auto nrows = stmt2.getNbAffectedRows();
    txn.setRowCountForTelemetry(nrows);
    return nrows;
  }

  // END OF DISABLE DELETION FOR DEBUGGING
  // the following is here for debugging purposes (row deletion gets disabled)
  // if (status == RetrieveJobStatus::RJS_Complete) {
  //   status = RetrieveJobStatus::ReadyForDeletion;
  // } else if (status == RetrieveJobStatus::RJS_Failed) {
  //   status = RetrieveJobStatus::ReadyForDeletion;
  //   RetrieveJobQueueRow::copyToFailedJobTable(txn, jobIDs);
  // }
  std::string sql = "UPDATE ";
  sql += repack_table_name_prefix + "RETRIEVE_ACTIVE_QUEUE ";
  sql += " SET STATUS = :STATUS, LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER WHERE JOB_ID IN ("
         + sqlpart + ")";
  auto stmt1 = txn.getConn().createStmt(sql);
  stmt1.bindString(":STATUS", to_string(newStatus));
  txn.getConn().setDbQuerySummary("update retrieve");
  stmt1.executeNonQuery();
  auto nrows = stmt1.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
};

uint64_t RetrieveJobQueueRow::updateFailedJobStatus(Transaction& txn, bool isRepack) {
  RetrieveJobStatus newStatus;
  std::string repack_table_name_prefix = "";
  if (isRepack) {
    repack_table_name_prefix = "REPACK_";
    newStatus = RetrieveJobStatus::RJS_ToReportToRepackForFailure;
  } else {
    newStatus = RetrieveJobStatus::RJS_ToReportToUserForFailure;
  }
  std::string sql = R"SQL(
      UPDATE )SQL";
  sql += repack_table_name_prefix + R"SQL(RETRIEVE_ACTIVE_QUEUE SET
        STATUS = :STATUS,
        TOTAL_RETRIES = :TOTAL_RETRIES,
        RETRIES_WITHIN_MOUNT = :RETRIES_WITHIN_MOUNT,
        LAST_MOUNT_WITH_FAILURE = :LAST_MOUNT_WITH_FAILURE,
        FAILURE_LOG = FAILURE_LOG || :FAILURE_LOG,
        LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER
        WHERE JOB_ID = :JOB_ID
    )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(newStatus));
  stmt.bindUint32(":TOTAL_RETRIES", totalRetries);
  stmt.bindUint32(":RETRIES_WITHIN_MOUNT", retriesWithinMount);
  stmt.bindUint64(":LAST_MOUNT_WITH_FAILURE", lastMountWithFailure);
  stmt.bindString(":FAILURE_LOG", failureLogs.value_or(""));
  stmt.bindUint64(":JOB_ID", jobId);
  txn.getConn().setDbQuerySummary("update retrieve");
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
};

void RetrieveJobQueueRow::updateJobRowFailureLog(const std::string& reason, bool is_report_log) {
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

void RetrieveJobQueueRow::updateRetryCounts(uint64_t mountId) {
  if (lastMountWithFailure == mountId) {
    retriesWithinMount += 1;
  } else {
    retriesWithinMount = 1;
    lastMountWithFailure = mountId;
  }
  totalRetries += 1;
};

// requeueFailedJob is used to requeue jobs which were not processed due to finished mount or failed jobs
// In case of unexpected crashed the job stays in the RETRIEVE_PENDING_QUEUE and needs to be identified
// in some garbage collection process - TO-BE-DONE. ALTERNATE_VIDS
uint64_t RetrieveJobQueueRow::requeueFailedJob(Transaction& txn,
                                               RetrieveJobStatus newStatus,
                                               bool keepMountId,
                                               bool isRepack,
                                               std::optional<std::list<std::string>> jobIDs) {
  // To optimise this query, one shall call updateMountQueueLastFetch
  // separatelly and remove UPDATED_MOUNT_QUEUE_LAST_FETCH
  std::string repack_table_name_prefix = isRepack ? "REPACK_" : "";
  std::string sql = R"SQL(
    WITH MOVED_ROWS AS (
        DELETE FROM
  )SQL";
  sql += repack_table_name_prefix + "RETRIEVE_ACTIVE_QUEUE ";
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
    sql += R"SQL(
      , UPDATED_MOUNT_QUEUE_LAST_FETCH AS (
       INSERT INTO MOUNT_QUEUE_LAST_FETCH (MOUNT_ID, LAST_UPDATE_TIME, QUEUE_TYPE)
         SELECT DISTINCT
           M.MOUNT_ID,
           EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER,
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
  sql += repack_table_name_prefix + "RETRIEVE_PENDING_QUEUE (";
  if (isRepack) {
    sql += " REPACK_REQUEST_ID,";
    sql += " REPACK_REARCHIVE_COPY_NBS,";
    sql += " REPACK_REARCHIVE_TAPE_POOLS,";
  }
  sql += R"SQL(
      JOB_ID,
      RETRIEVE_REQUEST_ID,
      REQUEST_JOB_COUNT,
      TAPE_POOL,
      MOUNT_POLICY,
      PRIORITY,
      MIN_RETRIEVE_REQUEST_AGE,
      ARCHIVE_FILE_ID,
      SIZE_IN_BYTES,
      COPY_NB,
      START_TIME,
      CHECKSUMBLOB,
      FSEQ,
      BLOCK_ID,
      CREATION_TIME,
      DISK_INSTANCE,
      DISK_FILE_ID,
      DISK_FILE_OWNER_UID,
      DISK_FILE_GID,
      DISK_FILE_PATH,
      RETRIEVE_REPORT_URL,
      RETRIEVE_ERROR_REPORT_URL,
      REQUESTER_NAME,
      REQUESTER_GROUP,
      DST_URL,
      STORAGE_CLASS,
      MAX_TOTAL_RETRIES,
      MAX_RETRIES_WITHIN_MOUNT,
      TOTAL_REPORT_RETRIES,
      MAX_REPORT_RETRIES,
      VID,
      ALTERNATE_FSEQS,
      ALTERNATE_BLOCK_IDS,
      ALTERNATE_VIDS,
      ALTERNATE_COPY_NBS,
      DRIVE,
      HOST,
      LOGICAL_LIBRARY,
      ACTIVITY,
      SRR_USERNAME,
      SRR_HOST,
      SRR_TIME,
      SRR_MOUNT_POLICY,
      SRR_ACTIVITY,
      LIFECYCLE_CREATION_TIME,
      LIFECYCLE_FIRST_SELECTED_TIME,
      LIFECYCLE_COMPLETED_TIME,
      DISK_SYSTEM_NAME,
      FAILURE_LOG,
      RETRIES_WITHIN_MOUNT,
      TOTAL_RETRIES,
      LAST_MOUNT_WITH_FAILURE,
      STATUS,
      MOUNT_ID
    )
    SELECT
   )SQL";
  if (isRepack) {
    sql += " M.REPACK_REQUEST_ID,";
    sql += " M.REPACK_REARCHIVE_COPY_NBS,";
    sql += " M.REPACK_REARCHIVE_TAPE_POOLS,";
  }
  sql += R"SQL(
      M.JOB_ID,
      M.RETRIEVE_REQUEST_ID,
      M.REQUEST_JOB_COUNT,
      M.TAPE_POOL,
      M.MOUNT_POLICY,
      M.PRIORITY,
      M.MIN_RETRIEVE_REQUEST_AGE,
      M.ARCHIVE_FILE_ID,
      M.SIZE_IN_BYTES,
      :COPY_NB AS COPY_NB,
      M.START_TIME,
      M.CHECKSUMBLOB,
      :FSEQ AS FSEQ,
      :BLOCK_ID AS BLOCK_ID,
      M.CREATION_TIME,
      M.DISK_INSTANCE,
      M.DISK_FILE_ID,
      M.DISK_FILE_OWNER_UID,
      M.DISK_FILE_GID,
      M.DISK_FILE_PATH,
      M.RETRIEVE_REPORT_URL,
      M.RETRIEVE_ERROR_REPORT_URL,
      M.REQUESTER_NAME,
      M.REQUESTER_GROUP,
      M.DST_URL,
      M.STORAGE_CLASS,
      M.MAX_TOTAL_RETRIES,
      M.MAX_RETRIES_WITHIN_MOUNT,
      M.TOTAL_REPORT_RETRIES,
      M.MAX_REPORT_RETRIES,
      :VID AS VID,
      M.ALTERNATE_FSEQS,
      M.ALTERNATE_BLOCK_IDS,
      M.ALTERNATE_VIDS,
      M.ALTERNATE_COPY_NBS,
      M.DRIVE,
      M.HOST,
      M.LOGICAL_LIBRARY,
      M.ACTIVITY,
      M.SRR_USERNAME,
      M.SRR_HOST,
      M.SRR_TIME,
      M.SRR_MOUNT_POLICY,
      M.SRR_ACTIVITY,
      M.LIFECYCLE_CREATION_TIME,
      M.LIFECYCLE_FIRST_SELECTED_TIME,
      M.LIFECYCLE_COMPLETED_TIME,
      M.DISK_SYSTEM_NAME,
      M.FAILURE_LOG || :FAILURE_LOG AS FAILURE_LOG,
      :RETRIES_WITHIN_MOUNT AS RETRIES_WITHIN_MOUNT,
      :TOTAL_RETRIES AS TOTAL_RETRIES,
      :LAST_MOUNT_WITH_FAILURE AS LAST_MOUNT_WITH_FAILURE,
      :STATUS AS STATUS,
  )SQL";
  // Add MOUNT_ID to the query if mountId is provided
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
    std::string queueType = isRepack ? "REPACK_RETRIEVE_PENDING" : "RETRIEVE_PENDING";
    stmt.bindString(":QUEUE_TYPE", queueType);
  }
  stmt.bindString(":STATUS", to_string(newStatus));
  stmt.bindUint32(":TOTAL_RETRIES", totalRetries);
  stmt.bindUint32(":RETRIES_WITHIN_MOUNT", retriesWithinMount);
  stmt.bindUint64(":LAST_MOUNT_WITH_FAILURE", lastMountWithFailure);
  stmt.bindString(":VID", vid);
  stmt.bindUint64(":FSEQ", fSeq);
  stmt.bindUint8(":COPY_NB", copyNb);
  stmt.bindUint64(":BLOCK_ID", blockId);
  stmt.bindString(":FAILURE_LOG", failureLogs.value_or(""));
  if (userowjid) {
    stmt.bindUint64(":JOB_ID", jobId);
  }
  txn.getConn().setDbQuerySummary("move retrieve back to pending");
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
};

uint64_t RetrieveJobQueueRow::requeueJobBatch(Transaction& txn,
                                              RetrieveJobStatus newStatus,
                                              const std::list<std::string>& jobIDs,
                                              bool isRepack) {
  std::string repack_table_name_prefix = isRepack ? "REPACK_" : "";
  std::string sql = R"SQL(
    WITH MOVED_ROWS AS (
        DELETE FROM
  )SQL";
  sql += repack_table_name_prefix + "RETRIEVE_ACTIVE_QUEUE ";
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
  sql += repack_table_name_prefix + "RETRIEVE_PENDING_QUEUE (";
  if (isRepack) {
    sql += " REPACK_REQUEST_ID,";
    sql += " REPACK_REARCHIVE_COPY_NBS,";
    sql += " REPACK_REARCHIVE_TAPE_POOLS,";
  }
  sql += R"SQL(
      JOB_ID,
      RETRIEVE_REQUEST_ID,
      REQUEST_JOB_COUNT,
      TAPE_POOL,
      MOUNT_POLICY,
      PRIORITY,
      MIN_RETRIEVE_REQUEST_AGE,
      ARCHIVE_FILE_ID,
      SIZE_IN_BYTES,
      COPY_NB,
      START_TIME,
      CHECKSUMBLOB,
      FSEQ,
      BLOCK_ID,
      CREATION_TIME,
      DISK_INSTANCE,
      DISK_FILE_ID,
      DISK_FILE_OWNER_UID,
      DISK_FILE_GID,
      DISK_FILE_PATH,
      RETRIEVE_REPORT_URL,
      RETRIEVE_ERROR_REPORT_URL,
      REQUESTER_NAME,
      REQUESTER_GROUP,
      DST_URL,
      STORAGE_CLASS,
      MAX_TOTAL_RETRIES,
      MAX_RETRIES_WITHIN_MOUNT,
      TOTAL_REPORT_RETRIES,
      MAX_REPORT_RETRIES,
      VID,
      ALTERNATE_FSEQS,
      ALTERNATE_BLOCK_IDS,
      ALTERNATE_VIDS,
      ALTERNATE_COPY_NBS,
      DRIVE,
      HOST,
      LOGICAL_LIBRARY,
      ACTIVITY,
      SRR_USERNAME,
      SRR_HOST,
      SRR_TIME,
      SRR_MOUNT_POLICY,
      SRR_ACTIVITY,
      LIFECYCLE_CREATION_TIME,
      LIFECYCLE_FIRST_SELECTED_TIME,
      LIFECYCLE_COMPLETED_TIME,
      DISK_SYSTEM_NAME,
      FAILURE_LOG,
      RETRIES_WITHIN_MOUNT,
      TOTAL_RETRIES,
      LAST_MOUNT_WITH_FAILURE,
      STATUS,
      MOUNT_ID
    )
    SELECT
    )SQL";
  if (isRepack) {
    sql += " M.REPACK_REQUEST_ID,";
    sql += " M.REPACK_REARCHIVE_COPY_NBS,";
    sql += " M.REPACK_REARCHIVE_TAPE_POOLS,";
  }
  sql += R"SQL(
      M.JOB_ID,
      M.RETRIEVE_REQUEST_ID,
      M.REQUEST_JOB_COUNT,
      M.TAPE_POOL,
      M.MOUNT_POLICY,
      M.PRIORITY,
      M.MIN_RETRIEVE_REQUEST_AGE,
      M.ARCHIVE_FILE_ID,
      M.SIZE_IN_BYTES,
      M.COPY_NB,
      M.START_TIME,
      M.CHECKSUMBLOB,
      M.FSEQ,
      M.BLOCK_ID,
      M.CREATION_TIME,
      M.DISK_INSTANCE,
      M.DISK_FILE_ID,
      M.DISK_FILE_OWNER_UID,
      M.DISK_FILE_GID,
      M.DISK_FILE_PATH,
      M.RETRIEVE_REPORT_URL,
      M.RETRIEVE_ERROR_REPORT_URL,
      M.REQUESTER_NAME,
      M.REQUESTER_GROUP,
      M.DST_URL,
      M.STORAGE_CLASS,
      M.MAX_TOTAL_RETRIES,
      M.MAX_RETRIES_WITHIN_MOUNT,
      M.TOTAL_REPORT_RETRIES,
      M.MAX_REPORT_RETRIES,
      M.VID,
      M.ALTERNATE_FSEQS,
      M.ALTERNATE_BLOCK_IDS,
      M.ALTERNATE_VIDS,
      M.ALTERNATE_COPY_NBS,
      M.DRIVE,
      M.HOST,
      M.LOGICAL_LIBRARY,
      M.ACTIVITY,
      M.SRR_USERNAME,
      M.SRR_HOST,
      M.SRR_TIME,
      M.SRR_MOUNT_POLICY,
      M.SRR_ACTIVITY,
      M.LIFECYCLE_CREATION_TIME,
      M.LIFECYCLE_FIRST_SELECTED_TIME,
      M.LIFECYCLE_COMPLETED_TIME,
      M.DISK_SYSTEM_NAME,
      M.FAILURE_LOG || :FAILURE_LOG AS FAILURE_LOG,
      M.RETRIES_WITHIN_MOUNT + 1 AS RETRIES_WITHIN_MOUNT,
      M.TOTAL_RETRIES + 1 AS TOTAL_RETRIES,
      M.LAST_MOUNT_WITH_FAILURE,
      :STATUS AS STATUS,
      NULL AS MOUNT_ID
        FROM MOVED_ROWS M
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(newStatus));
  stmt.bindString(":FAILURE_LOG", "UNPROCESSED_TASK_QUEUE_JOB_REQUEUED");
  txn.getConn().setDbQuerySummary("move retrieve back to pending");
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

rdbms::Rset RetrieveJobQueueRow::transformJobBatchToArchive(Transaction& txn, const size_t limit) {
  // Notes for dev process: we assume the DST_URL becomes SRC_URL, the ARCHIVE_REPORT_URL
  // and ARCHIVE_ERROR_REPORT_URL should not need to be used - must be checked
  // we assume these should be poiniting to the same disk URL as for retrieve
  // MIN_ARCHIVE_REQUEST_AGE ? - to be checked if could be used same as for the
  // BASE_INSERT - the original copy NB from the retrieve
  // ALTERNATE_INSERT - the alternative COPY_NB to re-archive if any
  std::string sql = R"SQL(
  WITH MOVED_ROWS AS (
    DELETE FROM REPACK_RETRIEVE_ACTIVE_QUEUE rraq
    WHERE rraq.JOB_ID IN (
        SELECT JOB_ID
        FROM REPACK_RETRIEVE_ACTIVE_QUEUE
        WHERE STATUS = :RETRIEVESTATUS
        ORDER BY PRIORITY DESC, JOB_ID
        LIMIT :LIMIT
        FOR UPDATE SKIP LOCKED
    )
    RETURNING rraq.*,
              COALESCE(
                NULLIF(CARDINALITY(string_to_array(rraq.REPACK_REARCHIVE_COPY_NBS, ',')), 0),
              0) AS REQUEST_JOB_NEW_COUNT
  ),
  BASE_INSERT AS (
      INSERT INTO REPACK_ARCHIVE_PENDING_QUEUE (
        ARCHIVE_REQUEST_ID,
        REPACK_REQUEST_ID,
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
        MAX_TOTAL_RETRIES,
        MAX_RETRIES_WITHIN_MOUNT,
        TOTAL_REPORT_RETRIES,
        MAX_REPORT_RETRIES,
        RETRIES_WITHIN_MOUNT,
        TOTAL_RETRIES,
        LAST_MOUNT_WITH_FAILURE,
        STATUS,
        MOUNT_ID
      )
      SELECT
        M.RETRIEVE_REQUEST_ID,
        M.REPACK_REQUEST_ID,
        M.REQUEST_JOB_NEW_COUNT AS REQUEST_JOB_COUNT,
        M.TAPE_POOL,
        M.MOUNT_POLICY,
        M.PRIORITY,
        M.MIN_RETRIEVE_REQUEST_AGE,
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
        M.RETRIEVE_REPORT_URL,
        M.RETRIEVE_ERROR_REPORT_URL,
        M.REQUESTER_NAME,
        M.REQUESTER_GROUP,
        M.DST_URL,
        M.STORAGE_CLASS,
        M.MAX_TOTAL_RETRIES,
        M.MAX_RETRIES_WITHIN_MOUNT,
        M.TOTAL_REPORT_RETRIES,
        M.MAX_REPORT_RETRIES,
        :RETRIES_WITHIN_MOUNT_BASE AS RETRIES_WITHIN_MOUNT,
        :TOTAL_RETRIES_BASE AS TOTAL_RETRIES,
        M.LAST_MOUNT_WITH_FAILURE,
        :STATUS_BASE AS STATUS,
        NULL AS MOUNT_ID
      FROM MOVED_ROWS M
      RETURNING REPACK_REQUEST_ID, SIZE_IN_BYTES
  ),
  ALTERNATE_INSERT AS (
      INSERT INTO REPACK_ARCHIVE_PENDING_QUEUE (
        ARCHIVE_REQUEST_ID,
        REPACK_REQUEST_ID,
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
        MAX_TOTAL_RETRIES,
        MAX_RETRIES_WITHIN_MOUNT,
        TOTAL_REPORT_RETRIES,
        MAX_REPORT_RETRIES,
        RETRIES_WITHIN_MOUNT,
        TOTAL_RETRIES,
        LAST_MOUNT_WITH_FAILURE,
        STATUS,
        MOUNT_ID
      )
      SELECT
        M.RETRIEVE_REQUEST_ID,
        M.REPACK_REQUEST_ID,
        M.REQUEST_JOB_NEW_COUNT AS REQUEST_JOB_COUNT,
        REARCHIVE_POOL.REARCHIVE_TAPE_POOL_VAL AS TAPE_POOL,
        M.MOUNT_POLICY,
        M.PRIORITY,
        M.MIN_RETRIEVE_REQUEST_AGE,
        M.ARCHIVE_FILE_ID,
        M.SIZE_IN_BYTES,
        REARCHIVE_COPY.REARCHIVE_COPY_VAL AS COPY_NB,
        M.START_TIME,
        M.CHECKSUMBLOB,
        M.CREATION_TIME,
        M.DISK_INSTANCE,
        M.DISK_FILE_ID,
        M.DISK_FILE_OWNER_UID,
        M.DISK_FILE_GID,
        M.DISK_FILE_PATH,
        M.RETRIEVE_REPORT_URL,
        M.RETRIEVE_ERROR_REPORT_URL,
        M.REQUESTER_NAME,
        M.REQUESTER_GROUP,
        M.DST_URL,
        M.STORAGE_CLASS,
        M.MAX_TOTAL_RETRIES,
        M.MAX_RETRIES_WITHIN_MOUNT,
        M.TOTAL_REPORT_RETRIES,
        M.MAX_REPORT_RETRIES,
        :RETRIES_WITHIN_MOUNT_ALTERNATE AS RETRIES_WITHIN_MOUNT,
        :TOTAL_RETRIES_ALTERNATE AS TOTAL_RETRIES,
        M.LAST_MOUNT_WITH_FAILURE,
        :STATUS_ALTERNATE AS STATUS,
        NULL AS MOUNT_ID
      FROM MOVED_ROWS M
      CROSS JOIN LATERAL (
        SELECT CAST(trim(VAL) AS BIGINT) AS REARCHIVE_COPY_VAL, ORD
        FROM unnest(string_to_array(M.REPACK_REARCHIVE_COPY_NBS, ',')) WITH ORDINALITY AS t(VAL, ORD) )
        AS REARCHIVE_COPY
      CROSS JOIN LATERAL (
        SELECT trim(VAL) AS REARCHIVE_TAPE_POOL_VAL, ORD
        FROM unnest(string_to_array(M.REPACK_REARCHIVE_TAPE_POOLS, ',')) WITH ORDINALITY AS t(VAL, ORD) )
        AS REARCHIVE_POOL
      WHERE REARCHIVE_COPY.ORD = REARCHIVE_POOL.ORD
            AND REARCHIVE_POOL.REARCHIVE_TAPE_POOL_VAL IS NOT NULL
            AND REARCHIVE_COPY.REARCHIVE_COPY_VAL IS NOT NULL
            AND REARCHIVE_COPY.REARCHIVE_COPY_VAL <> M.COPY_NB
            AND REARCHIVE_COPY.REARCHIVE_COPY_VAL <> 0
      RETURNING REPACK_REQUEST_ID, SIZE_IN_BYTES
  )
  SELECT
      COALESCE(b.REPACK_REQUEST_ID, a.REPACK_REQUEST_ID) AS REPACK_REQUEST_ID,
      COALESCE(b.BASE_INSERTED_COUNT, 0) AS BASE_INSERTED_COUNT,
      COALESCE(b.BASE_INSERTED_BYTES, 0) AS BASE_INSERTED_BYTES,
      COALESCE(a.ALTERNATE_INSERTED_COUNT, 0) AS ALTERNATE_INSERTED_COUNT,
      COALESCE(a.ALTERNATE_INSERTED_BYTES, 0) AS ALTERNATE_INSERTED_BYTES
  FROM (
      SELECT REPACK_REQUEST_ID,
             COUNT(*) AS ALTERNATE_INSERTED_COUNT,
             SUM(SIZE_IN_BYTES) AS ALTERNATE_INSERTED_BYTES
      FROM ALTERNATE_INSERT
      GROUP BY REPACK_REQUEST_ID
  ) a
  FULL OUTER JOIN (
      SELECT REPACK_REQUEST_ID,
             COUNT(*) AS BASE_INSERTED_COUNT,
             SUM(SIZE_IN_BYTES) AS BASE_INSERTED_BYTES
      FROM BASE_INSERT
      GROUP BY REPACK_REQUEST_ID
  ) b
  ON a.REPACK_REQUEST_ID = b.REPACK_REQUEST_ID
  )SQL";
  //  WHERE MOVED_ROWS.ALTERNATE_COPY_NBS LIKE '%,%';

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":RETRIEVESTATUS", to_string(RetrieveJobStatus::RJS_ToReportToRepackForSuccess));
  stmt.bindString(":STATUS_BASE", to_string(ArchiveJobStatus::AJS_ToTransferForRepack));
  stmt.bindString(":STATUS_ALTERNATE", to_string(ArchiveJobStatus::AJS_ToTransferForRepack));
  stmt.bindUint32(":TOTAL_RETRIES_BASE", 0);
  stmt.bindUint32(":TOTAL_RETRIES_ALTERNATE", 0);
  stmt.bindUint32(":RETRIES_WITHIN_MOUNT_BASE", 0);
  stmt.bindUint32(":RETRIES_WITHIN_MOUNT_ALTERNATE", 0);
  stmt.bindUint32(":LIMIT", limit);
  txn.getConn().setDbQuerySummary("move repack retrieve to archive");
  return stmt.executeQuery();
}

uint64_t RetrieveJobQueueRow::handlePendingRetrieveJobsAfterTapeStateChange(Transaction& txn, std::string vid) {
  std::string sql = R"SQL(
    WITH MOVED_ROWS AS (
      DELETE FROM RETRIEVE_PENDING_QUEUE
      WHERE VID = :VID
      RETURNING *
    ),
    TO_MOVE AS (
        SELECT * FROM MOVED_ROWS
        WHERE RETRIEVE_ERROR_REPORT_URL IS NOT NULL
          AND RETRIEVE_ERROR_REPORT_URL <> ''
    )
    INSERT INTO RETRIEVE_ACTIVE_QUEUE (
      JOB_ID,
      RETRIEVE_REQUEST_ID,
      REQUEST_JOB_COUNT,
      TAPE_POOL,
      MOUNT_POLICY,
      PRIORITY,
      MIN_RETRIEVE_REQUEST_AGE,
      ARCHIVE_FILE_ID,
      SIZE_IN_BYTES,
      COPY_NB,
      START_TIME,
      CHECKSUMBLOB,
      FSEQ,
      BLOCK_ID,
      CREATION_TIME,
      DISK_INSTANCE,
      DISK_FILE_ID,
      DISK_FILE_OWNER_UID,
      DISK_FILE_GID,
      DISK_FILE_PATH,
      RETRIEVE_REPORT_URL,
      RETRIEVE_ERROR_REPORT_URL,
      REQUESTER_NAME,
      REQUESTER_GROUP,
      DST_URL,
      STORAGE_CLASS,
      MAX_TOTAL_RETRIES,
      MAX_RETRIES_WITHIN_MOUNT,
      TOTAL_REPORT_RETRIES,
      MAX_REPORT_RETRIES,
      VID,
      ALTERNATE_FSEQS,
      ALTERNATE_BLOCK_IDS,
      ALTERNATE_VIDS,
      ALTERNATE_COPY_NBS,
      DRIVE,
      HOST,
      LOGICAL_LIBRARY,
      ACTIVITY,
      SRR_USERNAME,
      SRR_HOST,
      SRR_TIME,
      SRR_MOUNT_POLICY,
      SRR_ACTIVITY,
      LIFECYCLE_CREATION_TIME,
      LIFECYCLE_FIRST_SELECTED_TIME,
      LIFECYCLE_COMPLETED_TIME,
      DISK_SYSTEM_NAME,
      FAILURE_LOG,
      RETRIES_WITHIN_MOUNT,
      TOTAL_RETRIES,
      LAST_MOUNT_WITH_FAILURE,
      STATUS,
      MOUNT_ID
    )
    SELECT
      M.JOB_ID,
      M.RETRIEVE_REQUEST_ID,
      M.REQUEST_JOB_COUNT,
      :TAPE_POOL AS TAPE_POOL,
      M.MOUNT_POLICY,
      M.PRIORITY,
      M.MIN_RETRIEVE_REQUEST_AGE,
      M.ARCHIVE_FILE_ID,
      M.SIZE_IN_BYTES,
      M.COPY_NB,
      M.START_TIME,
      M.CHECKSUMBLOB,
      M.FSEQ,
      M.BLOCK_ID,
      M.CREATION_TIME,
      M.DISK_INSTANCE,
      M.DISK_FILE_ID,
      M.DISK_FILE_OWNER_UID,
      M.DISK_FILE_GID,
      M.DISK_FILE_PATH,
      M.RETRIEVE_REPORT_URL,
      M.RETRIEVE_ERROR_REPORT_URL,
      M.REQUESTER_NAME,
      M.REQUESTER_GROUP,
      M.DST_URL,
      M.STORAGE_CLASS,
      M.MAX_TOTAL_RETRIES,
      M.MAX_RETRIES_WITHIN_MOUNT,
      M.TOTAL_REPORT_RETRIES,
      M.MAX_REPORT_RETRIES,
      M.VID,
      M.ALTERNATE_FSEQS,
      M.ALTERNATE_BLOCK_IDS,
      M.ALTERNATE_VIDS,
      M.ALTERNATE_COPY_NBS,
      :DRIVE AS DRIVE,
      :HOST AS HOST,
      :LOGICAL_LIBRARY AS LOGICAL_LIBRARY,
      M.ACTIVITY,
      M.SRR_USERNAME,
      M.SRR_HOST,
      M.SRR_TIME,
      M.SRR_MOUNT_POLICY,
      M.SRR_ACTIVITY,
      M.LIFECYCLE_CREATION_TIME,
      M.LIFECYCLE_FIRST_SELECTED_TIME,
      M.LIFECYCLE_COMPLETED_TIME,
      M.DISK_SYSTEM_NAME,
      M.FAILURE_LOG || :FAILURE_LOG AS FAILURE_LOG,
      M.RETRIES_WITHIN_MOUNT AS RETRIES_WITHIN_MOUNT,
      M.TOTAL_RETRIES AS TOTAL_RETRIES,
      M.LAST_MOUNT_WITH_FAILURE,
      :STATUS AS STATUS,
      NULL AS MOUNT_ID
    FROM TO_MOVE M
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":DRIVE", "NONE");
  stmt.bindString(":HOST", "NONE");
  stmt.bindString(":LOGICAL_LIBRARY", "NONE");
  stmt.bindString(":TAPE_POOL", "NONE");
  stmt.bindString(":VID", vid);
  stmt.bindString(":STATUS", to_string(RetrieveJobStatus::RJS_ToReportToUserForFailure));
  stmt.bindString(":FAILURE_LOG", "TAPE_STATE_CHANGE_JOBS_TO_REPORT_FOR_FAILURE");
  txn.getConn().setDbQuerySummary("move pending retrieve to report failure");
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

uint64_t RetrieveJobQueueRow::moveJobToFailedQueueTable(Transaction& txn) {
  // DISABLE DELETION FOR DEBUGGING
  std::string sql = R"SQL(
    WITH MOVED_ROWS AS (
        DELETE FROM RETRIEVE_ACTIVE_QUEUE
          WHERE JOB_ID = :JOB_ID
        RETURNING *
    ) INSERT INTO RETRIEVE_FAILED_QUEUE SELECT * FROM MOVED_ROWS
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindUint64(":JOB_ID", jobId);
  txn.getConn().setDbQuerySummary("move failed retrieve");
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

uint64_t RetrieveJobQueueRow::moveJobBatchToFailedQueueTable(Transaction& txn,
                                                             const std::vector<std::string>& jobIDs,
                                                             bool isRepack) {
  std::string sqlpart;
  for (const auto& piece : jobIDs) {
    sqlpart += piece + ",";
  }
  if (!sqlpart.empty()) {
    sqlpart.pop_back();
  }
  std::string repack_table_name_prefix = isRepack ? "REPACK_" : "";
  std::string sql = R"SQL(
    WITH MOVED_ROWS AS (
        DELETE FROM
  )SQL";
  sql += repack_table_name_prefix + "RETRIEVE_ACTIVE_QUEUE ";
  sql += R"SQL(
          WHERE JOB_ID IN (
  )SQL";
  sql += sqlpart + ")";
  sql += R"SQL(
        RETURNING *
    ) INSERT INTO
  )SQL";
  sql += repack_table_name_prefix + "RETRIEVE_FAILED_QUEUE ";
  sql += R"SQL(
  SELECT * FROM MOVED_ROWS
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  txn.getConn().setDbQuerySummary("move failed retrieve");
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

rdbms::Rset RetrieveJobQueueRow::moveFailedRepackJobBatchToFailedQueueTable(Transaction& txn, uint64_t limit) {
  std::string sql = R"SQL(
    WITH MOVED_ROWS AS (
        DELETE FROM REPACK_RETRIEVE_ACTIVE_QUEUE
        WHERE JOB_ID IN (
            SELECT JOB_ID
            FROM REPACK_RETRIEVE_ACTIVE_QUEUE
            WHERE STATUS = 'RJS_ToReportToRepackForFailure'
            ORDER BY PRIORITY DESC, JOB_ID
            LIMIT :LIMIT
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
    ),
    INSERTED_ROWS AS (
        INSERT INTO REPACK_RETRIEVE_FAILED_QUEUE
        SELECT * FROM MOVED_ROWS
        RETURNING *
    )
    SELECT
        REPACK_REQUEST_ID,
        COUNT(*) AS FILE_COUNT,
        SUM(SIZE_IN_BYTES) AS FILE_BYTES
    FROM INSERTED_ROWS
    GROUP BY REPACK_REQUEST_ID
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindUint64(":LIMIT", limit);
  txn.getConn().setDbQuerySummary("move failed repack retrieve");
  auto rset = stmt.executeQuery();
  return rset;
}

uint64_t RetrieveJobQueueRow::updateJobStatusForFailedReport(Transaction& txn, RetrieveJobStatus newStatus) {
  // otherwise update the statistics and requeue the job
  std::string sql = R"SQL(
      UPDATE RETRIEVE_ACTIVE_QUEUE SET
        STATUS = :STATUS,
        TOTAL_REPORT_RETRIES = :TOTAL_REPORT_RETRIES,
        IS_REPORTING =:IS_REPORTING,
        REPORT_FAILURE_LOG = :REPORT_FAILURE_LOG,
        LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER
      WHERE JOB_ID = :JOB_ID
    )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(newStatus));
  stmt.bindUint32(":TOTAL_REPORT_RETRIES", totalReportRetries);
  stmt.bindBool(":IS_REPORTING", isReporting);
  stmt.bindString(":REPORT_FAILURE_LOG", reportFailureLogs.value_or(""));
  stmt.bindUint64(":JOB_ID", jobId);
  txn.getConn().setDbQuerySummary("update retrieve");
  txn.setRowCountForTelemetry(1);
  stmt.executeNonQuery();
  // if this was the final reporting failure,
  // move the row to failed jobs and delete the entry from the queue
  if (newStatus == RetrieveJobStatus::ReadyForDeletion) {
    return RetrieveJobQueueRow::moveJobToFailedQueueTable(txn);
  }
  return stmt.getNbAffectedRows();
};

rdbms::Rset RetrieveJobQueueRow::getRetrieveJobs(Transaction& txn, uint64_t filesRequested, bool fetchFailed) {
  std::string sql = "SELECT * FROM RETRIEVE_";
  if (fetchFailed) {
    sql += "FAILED";
  } else {
    sql += "PENDING";
  }
  sql += R"SQL(_QUEUE
    ORDER BY PRIORITY DESC, JOB_ID
    LIMIT :LIMIT
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindUint64(":LIMIT", filesRequested);
  return stmt.executeQuery();
}

rdbms::Rset RetrieveJobQueueRow::flagReportingJobsByStatus(Transaction& txn,
                                                           std::list<RetrieveJobStatus> statusList,
                                                           uint64_t limit) {
  std::string sql = R"SQL(
      WITH SET_SELECTION AS (
        SELECT JOB_ID FROM RETRIEVE_ACTIVE_QUEUE
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
        ]::RETRIEVE_JOB_STATUS[]) AND IS_REPORTING IS FALSE
        LIMIT :LIMIT FOR UPDATE SKIP LOCKED)
      UPDATE RETRIEVE_ACTIVE_QUEUE SET
        IS_REPORTING = TRUE,
        LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER
      FROM SET_SELECTION
      WHERE RETRIEVE_ACTIVE_QUEUE.JOB_ID = SET_SELECTION.JOB_ID
      RETURNING RETRIEVE_ACTIVE_QUEUE.*
    )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  // we can move the array binding to new bindArray method for STMT
  size_t sz = statusVec.size();
  for (size_t i = 0; i < sz; ++i) {
    stmt.bindString(placeholderVec[i], statusVec[i]);
  }
  stmt.bindUint64(":LIMIT", limit);

  txn.getConn().setDbQuerySummary("update retrieve report");
  return stmt.executeQuery();
}

uint64_t RetrieveJobQueueRow::getNextRetrieveRequestID(rdbms::Conn& conn) {
  const char* const sql = R"SQL(
        SELECT NEXTVAL('RETRIEVE_REQUEST_ID_SEQ') AS RETRIEVE_REQUEST_ID
      )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw exception::Exception("Result set is unexpectedly empty");
  }
  return rset.columnUint64("RETRIEVE_REQUEST_ID");
}

uint64_t RetrieveJobQueueRow::cancelRetrieveJob(Transaction& txn, uint64_t archiveFileID) {
  /* All jobs which were already picked up by the
   * mount to the task queue will run and not be deleted.
   * Deletes only from RETRIEVE_PENDING_QUEUE
   */
  std::string sqlActive = R"SQL(
    DELETE FROM RETRIEVE_PENDING_QUEUE
    WHERE
      ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID
  )SQL";
  auto stmt = txn.getConn().createStmt(sqlActive);
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileID);
  txn.getConn().setDbQuerySummary("delete retrieve");
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}
}  // namespace cta::schedulerdb::postgres
