/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/postgres/CommonQueueUtils.hpp"

#include "rdbms/Conn.hpp"
#include "rdbms/NullDbValue.hpp"
#include "rdbms/wrapper/PostgresColumn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"

namespace cta::schedulerdb::postgres {

std::string getQueueType(bool isActive, bool isRepack, bool isArchive) {
  std::string queueType = isArchive ? "ARCHIVE" : "RETRIEVE";

  if (isRepack) {
    queueType = "REPACK_" + queueType;
  }

  queueType += isActive ? "_ACTIVE" : "_PENDING";
  return queueType;
}

uint64_t updateMountQueueLastFetch(Transaction& txn, uint64_t mountId, bool isActive, bool isRepack, bool isArchive) {
  std::string sql = R"SQL(
       INSERT INTO MOUNT_QUEUE_LAST_FETCH (MOUNT_ID, LAST_UPDATE_TIME, QUEUE_TYPE)
         VALUES (:MOUNT_ID, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT, :QUEUE_TYPE)
       ON CONFLICT (MOUNT_ID, QUEUE_TYPE)
         DO UPDATE SET
           LAST_UPDATE_TIME = EXCLUDED.LAST_UPDATE_TIME,
           QUEUE_TYPE = EXCLUDED.QUEUE_TYPE
             WHERE MOUNT_QUEUE_LAST_FETCH.LAST_UPDATE_TIME
             < EXCLUDED.LAST_UPDATE_TIME - 5
)SQL";
  auto stmt = txn.getConn().createStmt(sql);
  const std::string queueType = getQueueType(isActive, isRepack, isArchive);
  stmt.bindString(":QUEUE_TYPE", queueType);
  stmt.bindUint64(":MOUNT_ID", mountId);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

std::string repackRequestIsCompleteSql(const RepackRequestStatusSqlInputs& i) {
  return R"SQL(
    (
      )SQL"
         + i.isExpandFinished + R"SQL(
      AND ()SQL"
         + i.retrievedFiles + " + " + i.failedRetrieveFiles + R"SQL() >= )SQL" + i.totalRetrieveFiles + R"SQL(
      AND ()SQL"
         + i.archivedFiles + " + " + i.failedArchiveFiles + " + " + i.failedCreateArchiveReq + R"SQL() >= )SQL"
         + i.totalArchiveFiles + R"SQL(
    )
  )SQL";
}

std::string repackRequestStatusSql(const RepackRequestStatusSqlInputs& i) {
  const auto isComplete = repackRequestIsCompleteSql(i);

  return R"SQL(
    CASE
      WHEN )SQL"
         + isComplete + R"SQL(
      THEN
        CASE
          WHEN ()SQL"
         + i.failedRetrieveFiles + R"SQL( > 0
            OR )SQL"
         + i.failedArchiveFiles + R"SQL( > 0 )
          THEN :STATUS_FAILED::REPACK_REQ_STATUS
          ELSE :STATUS_COMPLETE::REPACK_REQ_STATUS
        END

      WHEN )SQL"
         + i.retrievedFiles + R"SQL( > 0
        OR )SQL"
         + i.failedRetrieveFiles + R"SQL( > 0
        OR )SQL"
         + i.archivedFiles + R"SQL( > 0
        OR )SQL"
         + i.failedArchiveFiles + R"SQL( > 0
      THEN :STATUS_RUNNING::REPACK_REQ_STATUS

      ELSE :STATUS_STARTING::REPACK_REQ_STATUS
    END
  )SQL";
}

}  // namespace cta::schedulerdb::postgres
