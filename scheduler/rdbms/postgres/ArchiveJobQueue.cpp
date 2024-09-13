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
  rdbms::Rset ArchiveJobQueueRow::updateMountInfo(Transaction &txn, ArchiveJobStatus status, const SchedulerDatabase::ArchiveMount::MountInfo &mountInfo, uint64_t limit, uint64_t gc_delay){
    /* using write row lock FOR UPDATE for the select statement
     * since it is the same lock used for UPDATE
     */
    /* for paritioned queue table replace CREATION TIME by: EXTRACT(EPOCH FROM CREATION_TIME)::BIGINT */
    uint64_t gc_now_minus_delay = (uint64_t)cta::utils::getCurrentEpochTime()  - gc_delay;
    const char* const sql = R"SQL(
    WITH SET_SELECTION AS (
      SELECT JOB_ID FROM ARCHIVE_JOB_QUEUE
    WHERE TAPE_POOL = :TAPE_POOL
    AND STATUS = :STATUS
    AND ( MOUNT_ID IS NULL OR MOUNT_ID = :SAME_MOUNT_ID OR
          (MOUNT_ID != :DRIVE_MOUNT_ID AND LAST_UPDATE_TIME < :NOW_MINUS_DELAY) )
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

    auto stmt = txn.getConn()->createStmt(sql);
    stmt.bindString(":TAPE_POOL", mountInfo.tapePool);
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindUint64(":SAME_MOUNT_ID", mountInfo.mountId);
    stmt.bindUint64(":DRIVE_MOUNT_ID", mountInfo.mountId);
    stmt.bindUint32(":LIMIT", limit);
    stmt.bindUint64(":MOUNT_ID", mountInfo.mountId);
    stmt.bindUint64(":NOW_MINUS_DELAY", gc_now_minus_delay);
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
    if (status == ArchiveJobStatus::AJS_Complete) {
      status = ArchiveJobStatus::ReadyForDeletion;
    } else if (status == ArchiveJobStatus::AJS_Failed) {
      status = ArchiveJobStatus::ReadyForDeletion;
      ArchiveJobQueueRow::copyToFailedJobTable(txn, jobIDs);
    } else {
      std::string sql = "UPDATE ARCHIVE_JOB_QUEUE SET STATUS = :STATUS WHERE JOB_ID IN (" + sqlpart + ")";
      auto stmt1 = txn.getConn()->createStmt(sql);
      stmt1.bindString(":STATUS", to_string(status));
      stmt1.executeNonQuery();
    }
    if (status == ArchiveJobStatus::ReadyForDeletion) {
      std::string sql = R"SQL(
      DELETE FROM ARCHIVE_JOB_QUEUE
      WHERE
        JOB_ID IN ("
      )SQL";
      sql += sqlpart + std::string(")");
      auto stmt2 = txn.getConn()->createStmt(sql);
      stmt2.bindString(":STATUS", to_string(status));
      stmt2.executeNonQuery();
    }
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
    auto stmt = txn.getConn()->createStmt(sql);
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
    INSERT INTO ARCHIVE_FAILED_JOB_QUEUE
        SELECT *
    FROM ARCHIVE_JOB_QUEUE
    WHERE JOB_ID = :JOB_ID
    )SQL";
    auto stmt = txn.getConn()->createStmt(sql);
    //stmt.bindString(":STATUS", to_string(ArchiveJobStatus::AJS_Failed));
    stmt.bindUint64(":JOB_ID", jobId);
    stmt.executeNonQuery();
    return;
  }

  void ArchiveJobQueueRow::copyToFailedJobTable(Transaction &txn, const std::vector<std::string>& jobIDs){
    std::string sqlpart;
    for (const auto &piece : jobIDs) sqlpart += piece + ",";
    if (!sqlpart.empty()) { sqlpart.pop_back(); }
    std::string sql = R"SQL(
    INSERT INTO ARCHIVE_FAILED_JOB_QUEUE
        SELECT *
    FROM ARCHIVE_JOB_QUEUE
    WHERE JOB_ID IN (
    )SQL";
    sql += sqlpart + ")";
    auto stmt = txn.getConn()->createStmt(sql);
    //stmt.bindString(":STATUS", to_string(ArchiveJobStatus::AJS_Failed));
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

    auto stmt = txn.getConn()->createStmt(sql);
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindUint32(":TOTAL_REPORT_RETRIES", totalReportRetries);
    stmt.bindBool(":IS_REPORTING", is_reporting);
    stmt.bindString(":REPORT_FAILURE_LOG", reportFailureLogs.value_or(""));
    stmt.bindUint64(":JOB_ID", jobId);
    stmt.executeNonQuery();
    if (status == ArchiveJobStatus::ReadyForDeletion){
      ArchiveJobQueueRow::copyToFailedJobTable(txn);
      std::string sql = R"SQL(
      DELETE FROM ARCHIVE_JOB_QUEUE
      WHERE
        JOB_ID = :JOB_ID
      )SQL";
      auto stmt = txn.getConn()->createStmt(sql);
      stmt.bindUint64(":JOB_ID", jobId);
      stmt.executeNonQuery();
    }
    return;
  };

  rdbms::Rset ArchiveJobQueueRow::flagReportingJobsByStatus(Transaction &txn, std::list<ArchiveJobStatus> statusList, uint64_t gc_delay, uint64_t limit) {
    uint64_t gc_now_minus_delay = (uint64_t)cta::utils::getCurrentEpochTime()  - gc_delay;
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
        OR (IS_REPORTING IS TRUE AND LAST_UPDATE_TIME < :NOW_MINUS_DELAY)
        ORDER BY PRIORITY DESC, JOB_ID
        LIMIT :LIMIT FOR UPDATE)
      UPDATE ARCHIVE_JOB_QUEUE SET
        IS_REPORTING = TRUE
      FROM SET_SELECTION
      WHERE ARCHIVE_JOB_QUEUE.JOB_ID = SET_SELECTION.JOB_ID
      RETURNING SET_SELECTION.JOB_ID
    )SQL";
    auto stmt = txn.getConn()->createStmt(sql);
    // we can move the array binding to new bindArray method for STMT
    size_t sz = statusVec.size();
    for (size_t i = 0; i < sz; ++i) {
      stmt.bindString(placeholderVec[i], statusVec[i]);
    }
    stmt.bindUint64(":LIMIT", limit);
    stmt.bindUint64(":NOW_MINUS_DELAY", gc_now_minus_delay);

    return stmt.executeQuery();
  }

  uint64_t ArchiveJobQueueRow::getNextArchiveRequestID(rdbms::Conn &conn) {
    try {
      const char *const sql = R"SQL(
          SELECT NEXTVAL('ARCHIVE_REQUEST_ID_SEQ') AS ARCHIVE_REQUEST_ID
        )SQL";
      auto stmt = conn.createStmt(sql);
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

  uint64_t ArchiveJobQueueRow::cancelArchiveJob(Transaction &txn, const std::string& diskInstance, uint64_t archiveFileID) {
    std::string sqlpart;
    /* flagging jobs ReadyForDeletion - alternative strategy
     * for deletion by dropping partitions
     std::string sql = R"SQL(
      UPDATE ARCHIVE_JOB_QUEUE SET
        STATUS = :NEWSTATUS
      WHERE
        DISK_INSTANCE = :DISK_INSTANCE AND
        ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID AND
        STATUS NOT IN (:COMPLETE, :FAILED, :FORDELETION)
    )SQL";
    std::string sql = R"SQL(
      UPDATE ARCHIVE_JOB_QUEUE SET
        STATUS = :NEWSTATUS
      WHERE
        DISK_INSTANCE = :DISK_INSTANCE AND
        ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID AND
        STATUS NOT IN (:COMPLETE, :FAILED, :FORDELETION)
    )SQL";
     */
    std::string sql = R"SQL(
      DELETE FROM ARCHIVE_JOB_QUEUE
      WHERE
        DISK_INSTANCE = :DISK_INSTANCE AND
        ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID
    )SQL";
    auto stmt = txn.getConn()->createStmt(sql);
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileID);
    stmt.bindString(":DISK_INSTANCE", diskInstance);
    /* stmt.bindString(":NEWSTATUS",
                    to_string(ArchiveJobStatus::ReadyForDeletion));
    stmt.bindString(":COMPLETE",
                    to_string(ArchiveJobStatus::AJS_Complete));
    stmt.bindString(":FORDELETION",
                    to_string(ArchiveJobStatus::ReadyForDeletion));
    stmt.bindString(":FAILED",
                    to_string(ArchiveJobStatus::AJS_Failed));
    */
    stmt.executeNonQuery();
    return stmt.getNbAffectedRows();
  }
} // namespace cta::schedulerdb::postgres
