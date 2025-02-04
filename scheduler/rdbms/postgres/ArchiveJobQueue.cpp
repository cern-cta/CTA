/**
 * @project        The CERN Tape Archive (CTA)
 * @copyright	   Copyright © 2021-2022 CERN
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

#include "common/utils/utils.hpp"
#include "scheduler/rdbms/postgres/ArchiveJobQueue.hpp"
#include "scheduler/rdbms/ArchiveMount.hpp"

#include "rdbms/wrapper/PostgresColumn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"

namespace cta::schedulerdb::postgres {
std::pair<rdbms::Rset, uint64_t>
ArchiveJobQueueRow::moveJobsToDbQueue(Transaction& txn,
                                      ArchiveJobStatus status,
                                      const SchedulerDatabase::ArchiveMount::MountInfo& mountInfo,
                                      uint64_t maxBytesRequested,
                                      uint64_t limit) {
  /* using write row lock FOR UPDATE for the select statement
   * since it is the same lock used for UPDATE
   */
  /* for paritioned queue table replace CREATION TIME by: EXTRACT(EPOCH FROM CREATION_TIME)::BIGINT */
  /* below I first apply the LIMIT on the selection to limit
   * the number of rows and only after calculate the running cumulative sun of bytes in the consequent step */
  const char* const sql = R"SQL(
    WITH SET_SELECTION AS (
      SELECT JOB_ID, PRIORITY, SIZE_IN_BYTES
      FROM ARCHIVE_PENDING_QUEUE
      WHERE TAPE_POOL = :TAPE_POOL
      AND STATUS = :STATUS
      AND ( MOUNT_ID IS NULL OR MOUNT_ID = :SAME_MOUNT_ID )
      ORDER BY PRIORITY DESC, JOB_ID
      LIMIT :LIMIT
      FOR UPDATE SKIP LOCKED
    ),
    SELECTION_WITH_CUMULATIVE_SUMS AS (
      SELECT JOB_ID, PRIORITY,
             SUM(SIZE_IN_BYTES) OVER (ORDER BY PRIORITY DESC, JOB_ID) AS CUMULATIVE_SIZE
      FROM SET_SELECTION
    ),
    CUMULATIVE_SELECTION AS (
        SELECT JOB_ID, PRIORITY, CUMULATIVE_SIZE
        FROM SELECTION_WITH_CUMULATIVE_SUMS
        WHERE CUMULATIVE_SIZE <= :BYTES_REQUESTED
    ),
    MOVED_ROWS AS (
        DELETE FROM ARCHIVE_PENDING_QUEUE AIQ
        USING CUMULATIVE_SELECTION CSEL
        WHERE AIQ.JOB_ID = CSEL.JOB_ID
        RETURNING AIQ.*
    )
    INSERT INTO ARCHIVE_ACTIVE_QUEUE (
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
                M.JOB_ID,
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
                M.RETRIES_WITHIN_MOUNT + 1 AS RETRIES_WITHIN_MOUNT,
                M.MAX_RETRIES_WITHIN_MOUNT,
                M.TOTAL_RETRIES + 1 AS TOTAL_RETRIES,
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
  stmt.bindString(":STATUS", to_string(status));
  stmt.bindUint32(":LIMIT", limit);
  stmt.bindUint64(":MOUNT_ID", mountInfo.mountId);
  stmt.bindUint64(":SAME_MOUNT_ID", mountInfo.mountId);
  stmt.bindString(":VID", mountInfo.vid);
  stmt.bindString(":DRIVE", mountInfo.drive);
  stmt.bindString(":HOST", mountInfo.host);
  stmt.bindString(":MOUNT_TYPE", cta::common::dataStructures::toString(mountInfo.mountType));
  stmt.bindString(":LOGICAL_LIBRARY", mountInfo.logicalLibrary);
  stmt.bindUint64(":BYTES_REQUESTED", maxBytesRequested);
  auto result = stmt.executeQuery();
  auto nrows = stmt.getNbAffectedRows();
  return std::make_pair(std::move(result), nrows);
}

uint64_t
ArchiveJobQueueRow::updateJobStatus(Transaction& txn, ArchiveJobStatus status, const std::vector<std::string>& jobIDs) {
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
  // DISABLE DELETION FOR DEBUGGING
  if (status == ArchiveJobStatus::AJS_Complete || status == ArchiveJobStatus::AJS_Failed ||
      status == ArchiveJobStatus::ReadyForDeletion) {
    if (status == ArchiveJobStatus::AJS_Failed) {
      return ArchiveJobQueueRow::moveJobBatchToFailedQueueTable(txn, jobIDs);
    } else {
      std::string sql = R"SQL(
        DELETE FROM ARCHIVE_ACTIVE_QUEUE
        WHERE
          JOB_ID IN (
        )SQL";
      sql += sqlpart + std::string(")");
      auto stmt2 = txn.getConn().createStmt(sql);
      stmt2.executeNonQuery();
      return stmt2.getNbAffectedRows();
    }
  }
  // END OF DISABLE DELETION FOR DEBUGGING
  // the following is here for debugging purposes (row deletion gets disabled)
  // if (status == ArchiveJobStatus::AJS_Complete) {
  //   status = ArchiveJobStatus::ReadyForDeletion;
  // } else if (status == ArchiveJobStatus::AJS_Failed) {
  //   status = ArchiveJobStatus::ReadyForDeletion;
  //   ArchiveJobQueueRow::copyToFailedJobTable(txn, jobIDs);
  // }
  std::string sql = "UPDATE ARCHIVE_ACTIVE_QUEUE SET STATUS = :STATUS WHERE JOB_ID IN (" + sqlpart + ")";
  auto stmt1 = txn.getConn().createStmt(sql);
  stmt1.bindString(":STATUS", to_string(status));
  stmt1.executeNonQuery();
  return stmt1.getNbAffectedRows();
};

uint64_t ArchiveJobQueueRow::updateFailedJobStatus(Transaction& txn, ArchiveJobStatus status) {
  std::string sql = R"SQL(
      UPDATE ARCHIVE_ACTIVE_QUEUE SET
        STATUS = :STATUS,
        TOTAL_RETRIES = :TOTAL_RETRIES,
        RETRIES_WITHIN_MOUNT = :RETRIES_WITHIN_MOUNT,
        LAST_MOUNT_WITH_FAILURE = :LAST_MOUNT_WITH_FAILURE,
        FAILURE_LOG = FAILURE_LOG || :FAILURE_LOG
        WHERE JOB_ID = :JOB_ID
    )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(status));
  stmt.bindUint32(":TOTAL_RETRIES", totalRetries);
  stmt.bindUint32(":RETRIES_WITHIN_MOUNT", retriesWithinMount);
  stmt.bindUint64(":LAST_MOUNT_WITH_FAILURE", lastMountWithFailure);
  stmt.bindString(":FAILURE_LOG", failureLogs.value_or(""));
  stmt.bindUint64(":JOB_ID", jobId);
  stmt.executeNonQuery();
  return stmt.getNbAffectedRows();
};

// the job can stay in the ARCHIVE_PENDING_QUEUE in case the current Mount for this it was requeued
// dies in the meantime and the same MOundID will not be picking up jobs anymore
// this needs to be caught up in some cleaner process.
uint64_t ArchiveJobQueueRow::requeueFailedJob(Transaction& txn,
                                              ArchiveJobStatus status,
                                              bool keepMountId,
                                              std::optional<std::list<std::string>> jobIDs) {
  std::string sql = R"SQL(
    WITH MOVED_ROWS AS (
        DELETE FROM ARCHIVE_ACTIVE_QUEUE
  )SQL";
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
    INSERT INTO ARCHIVE_PENDING_QUEUE (
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
            FAILURE_LOG || :FAILURE_LOG AS FAILURE_LOG,
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
  stmt.bindString(":STATUS", to_string(status));
  stmt.bindUint32(":TOTAL_RETRIES", totalRetries);
  stmt.bindUint32(":RETRIES_WITHIN_MOUNT", retriesWithinMount);
  stmt.bindUint64(":LAST_MOUNT_WITH_FAILURE", lastMountWithFailure);
  stmt.bindString(":FAILURE_LOG", failureLogs.value_or(""));
  if (userowjid) {
    stmt.bindUint64(":JOB_ID", jobId);
  }
  stmt.executeNonQuery();
  return stmt.getNbAffectedRows();
};

uint64_t
ArchiveJobQueueRow::requeueJobBatch(Transaction& txn, ArchiveJobStatus status, const std::list<std::string>& jobIDs) {
  std::string sql = R"SQL(
    WITH MOVED_ROWS AS (
        DELETE FROM ARCHIVE_ACTIVE_QUEUE
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
    return 0;
  }
  sql += R"SQL(
        RETURNING *
    )
    INSERT INTO ARCHIVE_PENDING_QUEUE (
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
  stmt.bindString(":STATUS", to_string(status));
  stmt.bindString(":FAILURE_LOG", "UNPROCESSED_TASK_QUEUE_JOB_REQUEUED");
  stmt.executeNonQuery();
  return stmt.getNbAffectedRows();
}

uint64_t ArchiveJobQueueRow::moveJobToFailedQueueTable(Transaction& txn) {
  // DISABLE DELETION FOR DEBUGGING
  std::string sql = R"SQL(
    WITH MOVED_ROWS AS (
        DELETE FROM ARCHIVE_ACTIVE_QUEUE
          WHERE JOB_ID = :JOB_ID
        RETURNING *
    ) INSERT INTO ARCHIVE_FAILED_QUEUE SELECT * FROM MOVED_ROWS;
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindUint64(":JOB_ID", jobId);
  stmt.executeQuery();
  return stmt.getNbAffectedRows();
}

uint64_t ArchiveJobQueueRow::moveJobBatchToFailedQueueTable(Transaction& txn, const std::vector<std::string>& jobIDs) {
  std::string sqlpart;
  for (const auto& piece : jobIDs) {
    sqlpart += piece + ",";
  }
  if (!sqlpart.empty()) {
    sqlpart.pop_back();
  }
  std::string sql = R"SQL(
    WITH MOVED_ROWS AS (
        DELETE FROM ARCHIVE_ACTIVE_QUEUE
          WHERE JOB_ID IN (
  )SQL";
  sql += sqlpart + ")";
  sql += R"SQL(
        RETURNING *
    ) INSERT INTO ARCHIVE_FAILED_QUEUE SELECT * FROM MOVED_ROWS;
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  //stmt.bindString(":STATUS", to_string(ArchiveJobStatus::AJS_Failed));
  stmt.executeNonQuery();
  return stmt.getNbAffectedRows();
  ;
}

uint64_t ArchiveJobQueueRow::updateJobStatusForFailedReport(Transaction& txn, ArchiveJobStatus status) {
  // if this was the final reporting failure,
  // move the row to failed jobs and delete the entry from the queue
  if (status == ArchiveJobStatus::ReadyForDeletion) {
    return ArchiveJobQueueRow::moveJobToFailedQueueTable(txn);
    // END OF DISABLING DELETION FOR DEBUGGING
  }
  // otherwise update the statistics and requeue the job
  std::string sql = R"SQL(
      UPDATE ARCHIVE_ACTIVE_QUEUE SET
        STATUS = :STATUS,
        TOTAL_REPORT_RETRIES = :TOTAL_REPORT_RETRIES,
        IS_REPORTING =:IS_REPORTING,
        REPORT_FAILURE_LOG = REPORT_FAILURE_LOG || :REPORT_FAILURE_LOG
      WHERE JOB_ID = :JOB_ID
    )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(status));
  stmt.bindUint32(":TOTAL_REPORT_RETRIES", totalReportRetries);
  stmt.bindBool(":IS_REPORTING", isReporting);
  stmt.bindString(":REPORT_FAILURE_LOG", reportFailureLogs.value_or(""));
  stmt.bindUint64(":JOB_ID", jobId);
  stmt.executeNonQuery();
  return stmt.getNbAffectedRows();
};

rdbms::Rset ArchiveJobQueueRow::flagReportingJobsByStatus(Transaction& txn,
                                                          std::list<ArchiveJobStatus> statusList,
                                                          uint64_t gc_delay,
                                                          uint64_t limit) {
  uint64_t gc_now_minus_delay = (uint64_t) cta::utils::getCurrentEpochTime() - gc_delay;
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
    statusVec.push_back(to_string(jstatus));
    std::string plch = std::string(":STATUS") + std::to_string(j);
    placeholderVec.push_back(plch);
    sql += plch;
    if (&jstatus != &statusList.back()) {
      sql += std::string(",");
    }
    j++;
  }
  sql += R"SQL(
        ]::ARCHIVE_JOB_STATUS[]) AND IS_REPORTING IS FALSE
        OR (IS_REPORTING IS TRUE AND LAST_UPDATE_TIME < :NOW_MINUS_DELAY)
        ORDER BY PRIORITY DESC, JOB_ID
        LIMIT :LIMIT FOR UPDATE SKIP LOCKED)
      UPDATE ARCHIVE_ACTIVE_QUEUE SET
        IS_REPORTING = TRUE
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
  stmt.bindUint64(":NOW_MINUS_DELAY", gc_now_minus_delay);

  return stmt.executeQuery();
}

uint64_t ArchiveJobQueueRow::getNextArchiveRequestID(rdbms::Conn& conn) {
  try {
    const char* const sql = R"SQL(
          SELECT NEXTVAL('ARCHIVE_REQUEST_ID_SEQ') AS ARCHIVE_REQUEST_ID
        )SQL";
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    if (!rset.next()) {
      throw exception::Exception("Result set is unexpectedly empty");
    }
    return rset.columnUint64("ARCHIVE_REQUEST_ID");
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

uint64_t
ArchiveJobQueueRow::cancelArchiveJob(Transaction& txn, const std::string& diskInstance, uint64_t archiveFileID) {
  std::string sqlpart;
  /* flagging jobs ReadyForDeletion - alternative strategy
     * for deletion by dropping partitions
     std::string sql = R"SQL(
      UPDATE ARCHIVE_ACTIVE_QUEUE SET
        STATUS = :NEWSTATUS
      WHERE
        DISK_INSTANCE = :DISK_INSTANCE AND
        ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID AND
        STATUS NOT IN (:COMPLETE, :FAILED, :FORDELETION)
    )SQL";
    std::string sql = R"SQL(
      UPDATE ARCHIVE_ACTIVE_QUEUE SET
        STATUS = :NEWSTATUS
      WHERE
        DISK_INSTANCE = :DISK_INSTANCE AND
        ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID AND
        STATUS NOT IN (:COMPLETE, :FAILED, :FORDELETION)
    )SQL";

    stmt.bindString(":NEWSTATUS",
                    to_string(ArchiveJobStatus::ReadyForDeletion));
    stmt.bindString(":COMPLETE",
                    to_string(ArchiveJobStatus::AJS_Complete));
    stmt.bindString(":FORDELETION",
                    to_string(ArchiveJobStatus::ReadyForDeletion));
    stmt.bindString(":FAILED",
                    to_string(ArchiveJobStatus::AJS_Failed));
     */
  // directly deleting the archive request irrespectively in which state it is
  // this can result in attempts to update rows of the DB which will not exist anymore
  // better strategy might be needed
  std::string sql = R"SQL(
      DELETE FROM ARCHIVE_ACTIVE_QUEUE
      WHERE
        DISK_INSTANCE = :DISK_INSTANCE AND
        ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID
    )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":DISK_INSTANCE", diskInstance);
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileID);

  stmt.executeNonQuery();
  uint64_t nrows = stmt.getNbAffectedRows();
  sql = R"SQL(
    DELETE FROM ARCHIVE_PENDING_QUEUE
    WHERE
      DISK_INSTANCE = :DISK_INSTANCE AND
      ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID
  )SQL";
  stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":DISK_INSTANCE", diskInstance);
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileID);
  stmt.executeNonQuery();
  nrows += stmt.getNbAffectedRows();
  return nrows;
}
}  // namespace cta::schedulerdb::postgres
