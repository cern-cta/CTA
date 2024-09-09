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

#include "scheduler/rdbms/postgres/ArchiveJobQueue.hpp"
#include "scheduler/rdbms/ArchiveMount.hpp"

#include "rdbms/wrapper/PostgresColumn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"

namespace cta::schedulerdb::postgres {
  rdbms::Rset ArchiveJobQueueRow::updateMountInfo(Transaction &txn, ArchiveJobStatus status, const SchedulerDatabase::ArchiveMount::MountInfo &mountInfo, uint64_t limit, uint64_t gc_delay){
    /* using write row lock FOR UPDATE for the select statement
     * since it is the same lock used for UPDATE
     */
    const char* const sql = R"SQL(
    WITH SET_SELECTION AS (
      SELECT JOB_ID FROM ARCHIVE_JOB_QUEUE
    WHERE TAPE_POOL = :TAPE_POOL
    AND STATUS = :STATUS
    AND ( MOUNT_ID IS NULL OR MOUNT_ID = :SAME_MOUNT_ID OR
          (MOUNT_ID != :DRIVE_MOUNT_ID AND (LAST_UPDATE_TIME - CREATION_TIME) > :GC_DELAY) )
    AND IN_DRIVE_QUEUE IS FALSE
    ORDER BY PRIORITY DESC, JOB_ID
    LIMIT :LIMIT FOR UPDATE)
    UPDATE ARCHIVE_JOB_QUEUE SET
      MOUNT_ID = :MOUNT_ID,
      VID = :VID,
      DRIVE = :DRIVE,
      HOST = :HOST,
      MOUNT_TYPE = :MOUNT_TYPE,
      LOGICAL_LIBRARY = :LOGICAL_LIB,
      IN_DRIVE_QUEUE = TRUE
    FROM SET_SELECTION
    WHERE ARCHIVE_JOB_QUEUE.JOB_ID = SET_SELECTION.JOB_ID
    RETURNING SET_SELECTION.JOB_ID
    )SQL";

    auto stmt = txn.conn().createStmt(sql);
    stmt.bindString(":TAPE_POOL", mountInfo.tapePool);
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindUint64(":SAME_MOUNT_ID", mountInfo.mountId);
    stmt.bindUint64(":DRIVE_MOUNT_ID", mountInfo.mountId);
    stmt.bindUint32(":LIMIT", limit);
    stmt.bindUint64(":MOUNT_ID", mountInfo.mountId);
    stmt.bindUint64(":GC_DELAY", gc_delay);
    stmt.bindString(":VID", mountInfo.vid);
    stmt.bindString(":DRIVE", mountInfo.drive);
    stmt.bindString(":HOST", mountInfo.host);
    stmt.bindString(":MOUNT_TYPE", cta::common::dataStructures::toString(mountInfo.mountType));
    stmt.bindString(":LOGICAL_LIB", mountInfo.logicalLibrary);
    return stmt.executeQuery();
  }

  void ArchiveJobQueueRow::updateJobStatus(Transaction &txn, ArchiveJobStatus status, const std::vector<std::string>& jobIDs){
    if(jobIDs.empty()) {
      return;
    }
    std::string sqlpart;
    for (const auto &piece : jobIDs) sqlpart += piece + ",";
    if (!sqlpart.empty()) { sqlpart.pop_back(); }
    std::string sql = "UPDATE ARCHIVE_JOB_QUEUE SET STATUS = :STATUS WHERE JOB_ID IN (" + sqlpart + ")";
    auto stmt = txn.conn().createStmt(sql);
    if (status == ArchiveJobStatus::AJS_Complete) {
      status = ArchiveJobStatus::ReadyForDeletion;
    } else if (status == ArchiveJobStatus::AJS_Failed) {
      status = ArchiveJobStatus::ReadyForDeletion;
      ArchiveJobQueueRow::copyToFailedJobTable(txn, jobIDs);
    }
    stmt.bindString(":STATUS", to_string(status));
    stmt.executeNonQuery();
    return;
  };

  void ArchiveJobQueueRow::updateFailedJobStatus(Transaction &txn, ArchiveJobStatus status, std::optional<uint64_t> mountId){
    std::string sql = R"SQL(
      UPDATE ARCHIVE_JOB_QUEUE SET
        STATUS = :STATUS,
        TOTAL_RETRIES = :TOTAL_RETRIES,
        RETRIES_WITHIN_MOUNT = :RETRIES_WITHIN_MOUNT,
        LAST_MOUNT_WITH_FAILURE = :LAST_MOUNT_WITH_FAILURE,
        IN_DRIVE_QUEUE = FALSE,
        FAILURE_LOG = FAILURE_LOG || :FAILURE_LOG
      )SQL";
    // Add MOUNT_ID to the query if mountId is provided
    if (mountId.has_value() && *mountId == 0) {
      sql += ", MOUNT_ID = NULL ";
    }
    // Continue the query
    sql += R"SQL(
      WHERE JOB_ID = :JOB_ID
    )SQL";
    auto stmt = txn.conn().createStmt(sql);
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindUint32(":TOTAL_RETRIES", totalRetries);
    stmt.bindUint32(":RETRIES_WITHIN_MOUNT", retriesWithinMount);
    stmt.bindUint64(":LAST_MOUNT_WITH_FAILURE", lastMountWithFailure);
    stmt.bindString(":FAILURE_LOG", failureLogs.value_or(""));
    stmt.bindUint64(":JOB_ID", jobId);
    stmt.executeNonQuery();
    return;
  };

  void ArchiveJobQueueRow::copyToFailedJobTable(Transaction &txn){
    std::string sql = R"SQL(
    INSERT INTO ARCHIVE_FAILED_JOB_QUEUE (
            JOB_ID,
            ARCHIVE_REQUEST_ID,
            REQUEST_JOB_COUNT,
    :STATUS AS STATUS
    CREATION_TIME,
            MOUNT_POLICY,
            TAPE_POOL,
            VID,
            MOUNT_ID,
            DRIVE,
            HOST,
            MOUNT_TYPE,
            LOGICAL_LIBRARY,
            START_TIME,
            PRIORITY,
            STORAGE_CLASS,
            MIN_ARCHIVE_REQUEST_AGE,
            COPY_NB,
            SIZE_IN_BYTES,
            ARCHIVE_FILE_ID,
            CHECKSUMBLOB,
            REQUESTER_NAME,
            REQUESTER_GROUP,
            SRC_URL,
            DISK_INSTANCE,
            DISK_FILE_PATH,
            DISK_FILE_ID,
            DISK_FILE_GID,
            DISK_FILE_OWNER_UID,
            ARCHIVE_ERROR_REPORT_URL,
            ARCHIVE_REPORT_URL,
            TOTAL_RETRIES,
            MAX_TOTAL_RETRIES,
            RETRIES_WITHIN_MOUNT,
            MAX_RETRIES_WITHIN_MOUNT,
            LAST_MOUNT_WITH_FAILURE,
            IS_REPORTING,
            IN_DRIVE_QUEUE,
            FAILURE_LOG,
            LAST_UPDATE_TIME,
            REPORT_FAILURE_LOG,
            TOTAL_REPORT_RETRIES,
            MAX_REPORT_RETRIES) FROM ARCHIVE_JOB_QUEUE
    WHERE JOB_ID = :JOB_ID
    )SQL";
    auto stmt = txn.conn().createStmt(sql);
    stmt.bindString(":STATUS", to_string(ArchiveJobStatus::AJS_Failed));
    stmt.bindUint64(":JOB_ID", jobId);
    stmt.executeNonQuery();
    return;
  }

  void ArchiveJobQueueRow::copyToFailedJobTable(Transaction &txn, const std::vector<std::string>& jobIDs){
    std::string sqlpart;
    for (const auto &piece : jobIDs) sqlpart += piece + ",";
    if (!sqlpart.empty()) { sqlpart.pop_back(); }
    std::string sql = R"SQL(
    INSERT INTO ARCHIVE_FAILED_JOB_QUEUE (
            JOB_ID,
            ARCHIVE_REQUEST_ID,
            REQUEST_JOB_COUNT,
    :STATUS AS STATUS
    CREATION_TIME,
            MOUNT_POLICY,
            TAPE_POOL,
            VID,
            MOUNT_ID,
            DRIVE,
            HOST,
            MOUNT_TYPE,
            LOGICAL_LIBRARY,
            START_TIME,
            PRIORITY,
            STORAGE_CLASS,
            MIN_ARCHIVE_REQUEST_AGE,
            COPY_NB,
            SIZE_IN_BYTES,
            ARCHIVE_FILE_ID,
            CHECKSUMBLOB,
            REQUESTER_NAME,
            REQUESTER_GROUP,
            SRC_URL,
            DISK_INSTANCE,
            DISK_FILE_PATH,
            DISK_FILE_ID,
            DISK_FILE_GID,
            DISK_FILE_OWNER_UID,
            ARCHIVE_ERROR_REPORT_URL,
            ARCHIVE_REPORT_URL,
            TOTAL_RETRIES,
            MAX_TOTAL_RETRIES,
            RETRIES_WITHIN_MOUNT,
            MAX_RETRIES_WITHIN_MOUNT,
            LAST_MOUNT_WITH_FAILURE,
            IS_REPORTING,
            IN_DRIVE_QUEUE,
            FAILURE_LOG,
            LAST_UPDATE_TIME,
            REPORT_FAILURE_LOG,
            TOTAL_REPORT_RETRIES,
            MAX_REPORT_RETRIES) FROM ARCHIVE_JOB_QUEUE
    WHERE JOB_ID IN (
    )SQL";
    sql += sqlpart + ")";
    auto stmt = txn.conn().createStmt(sql);
    stmt.bindString(":STATUS", to_string(ArchiveJobStatus::AJS_Failed));
    stmt.executeNonQuery();
    return;
  }

  void ArchiveJobQueueRow::updateJobStatusForFailedReport(Transaction &txn, ArchiveJobStatus status){

    std::string sql = R"SQL(
      UPDATE ARCHIVE_JOB_QUEUE SET
        STATUS = :STATUS,
        TOTAL_REPORT_RETRIES = :TOTAL_REPORT_RETRIES,
        IS_REPORTING =:IS_REPORTING,
        IN_DRIVE_QUEUE = FALSE,
        REPORT_FAILURE_LOG = REPORT_FAILURE_LOG || :REPORT_FAILURE_LOG
      WHERE JOB_ID = :JOB_ID
    )SQL";

    auto stmt = txn.conn().createStmt(sql);
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindUint32(":TOTAL_REPORT_RETRIES", totalReportRetries);
    stmt.bindBool(":IS_REPORTING", is_reporting);
    stmt.bindString(":REPORT_FAILURE_LOG", reportFailureLogs.value_or(""));
    stmt.bindUint64(":JOB_ID", jobId);
    stmt.executeNonQuery();
    if (status == ArchiveJobStatus::ReadyForDeletion){
      ArchiveJobQueueRow::copyToFailedJobTable(txn);
    }
    return;
  };

  rdbms::Rset ArchiveJobQueueRow::flagReportingJobsByStatus(Transaction &txn, std::list<ArchiveJobStatus> statusList, uint64_t limit) {
    std::string sql = R"SQL(
      WITH SET_SELECTION AS (
        SELECT JOB_ID FROM ARCHIVE_JOB_QUEUE
        WHERE STATUS = ANY(ARRAY[
    )SQL";
    // we can move this to new bindArray method for stmt
    std::vector<std::string> statusVec;
    std::vector<std::string> placeholderVec;
    size_t j = 1;
    for (const auto &jstatus : statusList) {
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
        ORDER BY PRIORITY DESC, JOB_ID
        LIMIT :LIMIT FOR UPDATE)
      UPDATE ARCHIVE_JOB_QUEUE SET
        IS_REPORTING = TRUE
      FROM SET_SELECTION
      WHERE ARCHIVE_JOB_QUEUE.JOB_ID = SET_SELECTION.JOB_ID
      RETURNING SET_SELECTION.JOB_ID
    )SQL";
    auto stmt = txn.conn().createStmt(sql);
    // we can move the array binding to new bindArray method for STMT
    size_t sz = statusVec.size();
    for (size_t i = 0; i < sz; ++i) {
      stmt.bindString(placeholderVec[i], statusVec[i]);
    }
    stmt.bindUint64(":LIMIT", limit);
    return stmt.executeQuery();
  }

  uint64_t ArchiveJobQueueRow::getNextArchiveRequestID(Transaction &txn) {
    try {
      const char *const sql = R"SQL(
          SELECT NEXTVAL('ARCHIVE_REQUEST_ID_SEQ') AS ARCHIVE_REQUEST_ID
        )SQL";
      txn.start();
      auto stmt = txn.conn().createStmt(sql);
      auto rset = stmt.executeQuery();
      if (!rset.next()) {
        throw exception::Exception("Result set is unexpectedly empty");
      }
      return rset.columnUint64("ARCHIVE_REQUEST_ID");
    } catch (exception::Exception &ex) {
      ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
      throw;
    }
  }
} // namespace cta::schedulerdb::postgres
