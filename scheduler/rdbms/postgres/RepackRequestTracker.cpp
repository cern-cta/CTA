/*
 * SPDX-FileCopyrightText: 2023 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/postgres/RepackRequestTracker.hpp"

#include "rdbms/wrapper/PostgresColumn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"
#include "scheduler/rdbms/postgres/CommonQueueUtils.hpp"

namespace cta::schedulerdb::postgres {

uint64_t RepackRequestTrackingRow::cancelRepack(Transaction& txn, const std::string& vid) {
  // Cancelling repack has never been used with objectstore. Whenever repack wound need to be cancelled,
  // the entire objectstore had to be wiped. This is going towards enabling this functionality for the first time.
  // It will need a bit more logic to be implemented to clean-up properly all the tables. For the moment, this does
  // work only for repack which has not started yet. The cancelRepack() call will just delete all the rows for which
  // the process has not started yet. Curently this functionality is not used at all in production.
  // Marking as 'Cancelled' all active jobs and deleting files from disk will be an upgrade implemented in the next MR.
  std::string sql = R"SQL(
    DELETE FROM REPACK_REQUEST_TRACKING
    WHERE
      VID = :VID
      AND STATUS = ANY(ARRAY['RRS_Pending','RRS_ToExpand','RRS_Starting','RRS_Running','RRS_Complete','RRS_Failed']::REPACK_REQ_STATUS[])
    )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":VID", vid);
  txn.getConn().setDbQuerySummary("cancel repack");
  stmt.executeNonQuery();
  return stmt.getNbAffectedRows();
}

uint64_t RepackRequestTrackingRow::updateRepackRequestForExpansion(Transaction& txn, const uint32_t requestCount) {
  const char* const sql = R"SQL(
      WITH TO_BE_UPDATED AS (
        SELECT REPACK_REQUEST_ID
        FROM REPACK_REQUEST_TRACKING
        WHERE STATUS = 'RRS_Pending'
        ORDER BY CREATE_TIME ASC
        LIMIT :REQUEST_COUNT
        FOR UPDATE SKIP LOCKED
      )
      UPDATE REPACK_REQUEST_TRACKING r
      SET STATUS = :STATUS::REPACK_REQ_STATUS
      WHERE r.REPACK_REQUEST_ID IN (SELECT REPACK_REQUEST_ID FROM TO_BE_UPDATED)
    )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindUint64(":REQUEST_COUNT", requestCount);
  stmt.bindString(":STATUS", "RRS_ToExpand");
  txn.getConn().setDbQuerySummary("update repack expansion");
  stmt.executeNonQuery();
  return stmt.getNbAffectedRows();
}

uint64_t RepackRequestTrackingRow::updateRepackRequestWithExpansionStats(
  Transaction& txn,
  const uint64_t& reqId,
  const cta::SchedulerDatabase::RepackRequest::TotalStatsFiles& totalStatsFiles,
  const uint64_t nbRetrieveSubrequestsCreated,
  const uint64_t lastExpandedFseq,
  const RepackJobStatus& newStatus) {
  const char* const sql = R"SQL(
    UPDATE REPACK_REQUEST_TRACKING
    SET STATUS = :STATUS::REPACK_REQ_STATUS,
        TOTAL_FILES_ON_TAPE_AT_START = :TOTAL_FILES_ON_TAPE_AT_START,
        TOTAL_BYTES_ON_TAPE_AT_START = :TOTAL_BYTES_ON_TAPE_AT_START,
        ALL_FILES_SELECTED_AT_START   = :ALL_FILES_SELECTED_AT_START,
        TOTAL_FILES_TO_RETRIEVE       = :TOTAL_FILES_TO_RETRIEVE,
        TOTAL_BYTES_TO_RETRIEVE       = :TOTAL_BYTES_TO_RETRIEVE,
        TOTAL_FILES_TO_ARCHIVE        = :TOTAL_FILES_TO_ARCHIVE,
        TOTAL_BYTES_TO_ARCHIVE        = :TOTAL_BYTES_TO_ARCHIVE,
        USER_PROVIDED_FILES           = :USER_PROVIDED_FILES,
        LAST_EXPANDED_FSEQ            = :LAST_EXPANDED_FSEQ,
        RETRIEVE_SUBREQUESTS_EXPANDED = :RETRIEVE_SUBREQUESTS_EXPANDED,
        IS_COMPLETE = :IS_COMPLETE
    WHERE REPACK_REQUEST_ID = :REQID
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);

  // Bind job status
  if (nbRetrieveSubrequestsCreated == 0) {
    stmt.bindString(":STATUS", to_string(cta::schedulerdb::RepackJobStatus::RRS_Complete));
    stmt.bindBool(":IS_COMPLETE", true);
  } else {
    stmt.bindString(":STATUS", to_string(newStatus));
    stmt.bindBool(":IS_COMPLETE", false);
  }

  // Bind TotalStatsFiles values
  stmt.bindUint64(":TOTAL_FILES_ON_TAPE_AT_START", totalStatsFiles.totalFilesOnTapeAtStart);
  stmt.bindUint64(":TOTAL_BYTES_ON_TAPE_AT_START", totalStatsFiles.totalBytesOnTapeAtStart);
  stmt.bindBool(":ALL_FILES_SELECTED_AT_START", totalStatsFiles.allFilesSelectedAtStart);
  stmt.bindUint64(":TOTAL_FILES_TO_RETRIEVE", totalStatsFiles.totalFilesToRetrieve);
  stmt.bindUint64(":TOTAL_BYTES_TO_RETRIEVE", totalStatsFiles.totalBytesToRetrieve);
  stmt.bindUint64(":TOTAL_FILES_TO_ARCHIVE", totalStatsFiles.totalFilesToArchive);
  stmt.bindUint64(":TOTAL_BYTES_TO_ARCHIVE", totalStatsFiles.totalBytesToArchive);
  stmt.bindUint64(":USER_PROVIDED_FILES", totalStatsFiles.userProvidedFiles);

  // Bind other function parameters
  stmt.bindUint64(":LAST_EXPANDED_FSEQ", lastExpandedFseq);
  stmt.bindUint64(":RETRIEVE_SUBREQUESTS_EXPANDED", nbRetrieveSubrequestsCreated);
  stmt.bindUint64(":REQID", reqId);

  // Execute
  txn.getConn().setDbQuerySummary("update repack expansion");
  stmt.executeNonQuery();
  return stmt.getNbAffectedRows();
}

rdbms::Rset RepackRequestTrackingRow::updateRepackRequestsProgress(Transaction& txn,
                                                                   const std::vector<RepackRequestProgress>& updates) {
  if (updates.empty()) {
    rdbms::Rset rset;
    return rset;
  }

  // Build SQL with placeholders for each row
  std::string sql = "WITH UPDATE_TABLE (REPACK_REQUEST_ID, DEST_VID, RETRIEVED_FILES, RETRIEVED_BYTES, ARCHIVED_FILES, "
                    "ARCHIVED_BYTES, REARCHIVE_COPYNBS, REARCHIVE_BYTES) AS (VALUES ";

  for (size_t i = 0; i < updates.size(); ++i) {
    if (i > 0) {
      sql += ",";
    }
    sql += "("
           ":REQID"
           + std::to_string(i)
           + "::BIGINT,"
             ":DEST_VID"
           + std::to_string(i)
           + "::TEXT,"
             ":RETR_FILES"
           + std::to_string(i)
           + "::BIGINT,"
             ":RETR_BYTES"
           + std::to_string(i)
           + "::BIGINT,"
             ":ARCH_FILES"
           + std::to_string(i)
           + "::BIGINT,"
             ":ARCH_BYTES"
           + std::to_string(i)
           + "::BIGINT,"
             ":REARCH_COPY"
           + std::to_string(i)
           + "::BIGINT,"
             ":REARCH_BYTES"
           + std::to_string(i) + "::BIGINT" + ")";
  }

  sql += R"SQL()
           , UPSERT_DEST AS (INSERT INTO REPACK_REQUEST_DESTINATION_STATISTICS (
                   REPACK_REQUEST_ID, VID,
                   ARCHIVED_FILES, ARCHIVED_BYTES
               )
               SELECT
                   upd.REPACK_REQUEST_ID, upd.DEST_VID,
                   upd.ARCHIVED_FILES, upd.ARCHIVED_BYTES
               FROM UPDATE_TABLE upd
               WHERE upd.DEST_VID IS NOT NULL AND upd.DEST_VID <> ''
               ON CONFLICT (REPACK_REQUEST_ID, VID) DO UPDATE
               SET
                   ARCHIVED_FILES = REPACK_REQUEST_DESTINATION_STATISTICS.ARCHIVED_FILES + EXCLUDED.ARCHIVED_FILES,
                   ARCHIVED_BYTES = REPACK_REQUEST_DESTINATION_STATISTICS.ARCHIVED_BYTES + EXCLUDED.ARCHIVED_BYTES
           ),
           AGG AS (
             SELECT REPACK_REQUEST_ID,
                    SUM(ARCHIVED_FILES) AS ARCHIVED_FILES_INC,
                    SUM(ARCHIVED_BYTES) AS ARCHIVED_BYTES_INC,
                    SUM(RETRIEVED_FILES) AS RETRIEVED_FILES_INC,
                    SUM(RETRIEVED_BYTES) AS RETRIEVED_BYTES_INC,
                    SUM(REARCHIVE_COPYNBS) AS REARCHIVE_COPYNBS_INC,
                    SUM(REARCHIVE_BYTES) AS REARCHIVE_BYTES_INC
             FROM UPDATE_TABLE
             GROUP BY REPACK_REQUEST_ID
           ),
    )SQL";
  RepackRequestStatusSqlInputs statusInputs {"trk.IS_EXPAND_FINISHED",
                                             "trk.RETRIEVED_FILES + agg.RETRIEVED_FILES_INC",
                                             "trk.FAILED_TO_RETRIEVE_FILES",
                                             "trk.TOTAL_FILES_TO_RETRIEVE",
                                             "trk.ARCHIVED_FILES + agg.ARCHIVED_FILES_INC",
                                             "trk.FAILED_TO_ARCHIVE_FILES",
                                             "trk.FAILED_TO_CREATE_ARCHIVE_REQ",
                                             "trk.TOTAL_FILES_TO_ARCHIVE"};
  sql += R"SQL(
           CALC AS (
             SELECT trk.REPACK_REQUEST_ID,
                    trk.VID,
                    trk.BUFFER_URL,
                    trk.RETRIEVED_FILES + agg.RETRIEVED_FILES_INC AS NEW_RETRIEVED_FILES,
                    trk.RETRIEVED_BYTES + agg.RETRIEVED_BYTES_INC AS NEW_RETRIEVED_BYTES,
                    trk.ARCHIVED_FILES + agg.ARCHIVED_FILES_INC AS NEW_ARCHIVED_FILES,
                    trk.ARCHIVED_BYTES + agg.ARCHIVED_BYTES_INC AS NEW_ARCHIVED_BYTES,
                    trk.REARCHIVE_COPYNBS + agg.REARCHIVE_COPYNBS_INC AS NEW_REARCHIVE_COPYNBS,
                    trk.REARCHIVE_BYTES + agg.REARCHIVE_BYTES_INC AS NEW_REARCHIVE_BYTES,
                    trk.IS_COMPLETE AS OLD_IS_COMPLETE,
             )SQL";
  sql += repackRequestIsCompleteSql(statusInputs) + " AS NEW_IS_COMPLETE, ";
  sql += repackRequestStatusSql(statusInputs) + " AS NEW_STATUS ";
  sql += R"SQL(
             FROM REPACK_REQUEST_TRACKING trk
             JOIN AGG agg ON trk.REPACK_REQUEST_ID = agg.REPACK_REQUEST_ID
             WHERE trk.IS_COMPLETE = FALSE
             FOR UPDATE of trk
           )

    )SQL";
  sql += R"SQL(
    UPDATE REPACK_REQUEST_TRACKING trkfn
    SET
      STATUS = calc.NEW_STATUS,
      IS_COMPLETE = calc.NEW_IS_COMPLETE,
      RETRIEVED_FILES = calc.NEW_RETRIEVED_FILES,
      RETRIEVED_BYTES = calc.NEW_RETRIEVED_BYTES,
      ARCHIVED_FILES = calc.NEW_ARCHIVED_FILES,
      ARCHIVED_BYTES = calc.NEW_ARCHIVED_BYTES,
      REARCHIVE_COPYNBS = calc.NEW_REARCHIVE_COPYNBS,
      REARCHIVE_BYTES = calc.NEW_REARCHIVE_BYTES,
      LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
    FROM CALC calc
    WHERE trkfn.REPACK_REQUEST_ID = calc.REPACK_REQUEST_ID
    RETURNING
      calc.VID,
      (calc.OLD_IS_COMPLETE = FALSE AND calc.NEW_IS_COMPLETE = TRUE) AS IS_COMPLETE_CHANGED,
      calc.BUFFER_URL
    )SQL";

  auto stmt = txn.getConn().createStmt(sql);

  // Bind each row's values
  for (size_t i = 0; i < updates.size(); ++i) {
    const auto& u = updates[i];
    std::string idx = std::to_string(i);

    stmt.bindUint64(":REQID" + idx, u.reqId);
    if (u.vid == "") {
      stmt.bindString(":DEST_VID" + idx, std::nullopt);
    } else {
      stmt.bindString(":DEST_VID" + idx, u.vid);
    }
    stmt.bindUint64(":RETR_FILES" + idx, u.retrievedFiles);
    stmt.bindUint64(":RETR_BYTES" + idx, u.retrievedBytes);
    stmt.bindUint64(":ARCH_FILES" + idx, u.archivedFiles);
    stmt.bindUint64(":ARCH_BYTES" + idx, u.archivedBytes);
    stmt.bindUint64(":REARCH_COPY" + idx, u.rearchiveCopyNbs);
    stmt.bindUint64(":REARCH_BYTES" + idx, u.rearchiveBytes);
  }
  stmt.bindString(":STATUS_RUNNING", to_string(cta::schedulerdb::RepackJobStatus::RRS_Running));
  stmt.bindString(":STATUS_STARTING", to_string(cta::schedulerdb::RepackJobStatus::RRS_Starting));
  stmt.bindString(":STATUS_COMPLETE", to_string(cta::schedulerdb::RepackJobStatus::RRS_Complete));
  stmt.bindString(":STATUS_FAILED", to_string(cta::schedulerdb::RepackJobStatus::RRS_Failed));
  txn.getConn().setDbQuerySummary("update repack progress");

  return stmt.executeQuery();
}

uint64_t RepackRequestTrackingRow::updateRRRetrieveCreationFailures(Transaction& txn,
                                                                    uint64_t reqId,
                                                                    uint64_t failedFilesToRetrieve,
                                                                    uint64_t failedBytesToRetrieve,
                                                                    uint64_t failedToCreateArchiveReq) {
  std::string sql = R"SQL(
        UPDATE REPACK_REQUEST_TRACKING
        SET FAILED_TO_RETRIEVE_FILES     = COALESCE(FAILED_TO_RETRIEVE_FILES, 0) + :FAILED_FILES,
            FAILED_TO_RETRIEVE_BYTES     = COALESCE(FAILED_TO_RETRIEVE_BYTES, 0) + :FAILED_BYTES,
            FAILED_TO_CREATE_ARCHIVE_REQ = COALESCE(FAILED_TO_CREATE_ARCHIVE_REQ, 0) + :FAILED_ARCH_REQS
        WHERE REPACK_REQUEST_ID = :REQID
    )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindUint64(":FAILED_FILES", failedFilesToRetrieve);
  stmt.bindUint64(":FAILED_BYTES", failedBytesToRetrieve);
  stmt.bindUint64(":FAILED_ARCH_REQS", failedToCreateArchiveReq);
  stmt.bindUint64(":REQID", reqId);

  txn.getConn().setDbQuerySummary("update repack failures");
  stmt.executeNonQuery();
  return stmt.getNbAffectedRows();
}

uint64_t RepackRequestTrackingRow::updateRepackRequestFailuresBatch(Transaction& txn,
                                                                    const std::vector<uint64_t>& reqIds,
                                                                    const std::vector<uint64_t>& failedFiles,
                                                                    const std::vector<uint64_t>& failedBytes,
                                                                    bool isRetrieve) {
  if (reqIds.empty()) {
    return 0;
  }
  if (reqIds.size() != failedFiles.size() || reqIds.size() != failedBytes.size()) {
    throw cta::exception::Exception(
      "In RepackRequestTrackingRow::updateRepackRequestFailuresBatch(): Input vector size mismatch.");
  }
  std::string sql = "WITH FAILURE_TABLE (REPACK_REQUEST_ID, FAILED_FILES, FAILED_BYTES) AS (VALUES ";

  for (size_t i = 0; i < reqIds.size(); ++i) {
    if (i > 0) {
      sql += ",";
    }
    sql += "("
           ":REQID"
           + std::to_string(i)
           + "::BIGINT,"
             ":FAILED_FILES"
           + std::to_string(i)
           + "::BIGINT,"
             ":FAILED_BYTES"
           + std::to_string(i)
           + "::BIGINT"
             ")";
  }
  sql += R"SQL(
    ),
    AGG AS (
      SELECT
        REPACK_REQUEST_ID,
        SUM(FAILED_FILES) AS FAILED_FILES_INC,
        SUM(FAILED_BYTES) AS FAILED_BYTES_INC
      FROM FAILURE_TABLE
      GROUP BY REPACK_REQUEST_ID
    ),
  )SQL";
  RepackRequestStatusSqlInputs statusInputs {
    "trk.IS_EXPAND_FINISHED",

    // Successful retrieval count is unchanged by this method.
    "trk.RETRIEVED_FILES",

    isRetrieve ? "trk.FAILED_TO_RETRIEVE_FILES + agg.FAILED_FILES_INC" : "trk.FAILED_TO_RETRIEVE_FILES",

    "trk.TOTAL_FILES_TO_RETRIEVE",

    // Successful archive count is unchanged by this method.
    "trk.ARCHIVED_FILES",

    isRetrieve ? "trk.FAILED_TO_ARCHIVE_FILES" : "trk.FAILED_TO_ARCHIVE_FILES + agg.FAILED_FILES_INC",

    "trk.FAILED_TO_CREATE_ARCHIVE_REQ",
    "trk.TOTAL_FILES_TO_ARCHIVE"};
  sql += R"SQL(
    CALC AS (
      SELECT
        trk.REPACK_REQUEST_ID,
        trk.IS_COMPLETE AS OLD_IS_COMPLETE,
        trk.REPACK_FINISHED_TIME AS OLD_REPACK_FINISHED_TIME,
  )SQL";
  if (isRetrieve) {
    sql += R"SQL(
        trk.FAILED_TO_RETRIEVE_FILES + agg.FAILED_FILES_INC AS NEW_FAILED_TO_RETRIEVE_FILES,
        trk.FAILED_TO_RETRIEVE_BYTES + agg.FAILED_BYTES_INC AS NEW_FAILED_TO_RETRIEVE_BYTES,
        trk.FAILED_TO_ARCHIVE_FILES AS NEW_FAILED_TO_ARCHIVE_FILES,
        trk.FAILED_TO_ARCHIVE_BYTES AS NEW_FAILED_TO_ARCHIVE_BYTES,
    )SQL";
  } else {
    sql += R"SQL(
        trk.FAILED_TO_RETRIEVE_FILES AS NEW_FAILED_TO_RETRIEVE_FILES,
        trk.FAILED_TO_RETRIEVE_BYTES AS NEW_FAILED_TO_RETRIEVE_BYTES,
        trk.FAILED_TO_ARCHIVE_FILES + agg.FAILED_FILES_INC AS NEW_FAILED_TO_ARCHIVE_FILES,
        trk.FAILED_TO_ARCHIVE_BYTES + agg.FAILED_BYTES_INC AS NEW_FAILED_TO_ARCHIVE_BYTES,
    )SQL";
  }
  sql += repackRequestIsCompleteSql(statusInputs) + " AS NEW_IS_COMPLETE,";

  sql += repackRequestStatusSql(statusInputs) + " AS NEW_STATUS";
  sql += R"SQL(
      FROM REPACK_REQUEST_TRACKING trk
      JOIN AGG agg
        ON trk.REPACK_REQUEST_ID = agg.REPACK_REQUEST_ID
      WHERE trk.IS_COMPLETE = FALSE
      FOR UPDATE OF trk
    )
     UPDATE REPACK_REQUEST_TRACKING trk
     SET
       LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
       FAILED_TO_RETRIEVE_FILES = calc.NEW_FAILED_TO_RETRIEVE_FILES,
       FAILED_TO_RETRIEVE_BYTES = calc.NEW_FAILED_TO_RETRIEVE_BYTES,
       FAILED_TO_ARCHIVE_FILES = calc.NEW_FAILED_TO_ARCHIVE_FILES,
       FAILED_TO_ARCHIVE_BYTES = calc.NEW_FAILED_TO_ARCHIVE_BYTES,
       STATUS = calc.NEW_STATUS,
       IS_COMPLETE = calc.NEW_IS_COMPLETE,
       REPACK_FINISHED_TIME =
         CASE
             WHEN calc.OLD_IS_COMPLETE = FALSE
               AND calc.NEW_IS_COMPLETE = TRUE
             THEN EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
             ELSE calc.OLD_REPACK_FINISHED_TIME
         END
       FROM CALC calc
       WHERE trk.REPACK_REQUEST_ID = calc.REPACK_REQUEST_ID
       )SQL";

  auto stmt = txn.getConn().createStmt(sql);

  // Bind each row's values
  for (size_t i = 0; i < reqIds.size(); ++i) {
    const auto& rId = reqIds[i];
    const auto& ffl = failedFiles[i];
    const auto& fb = failedBytes[i];
    const std::string idx = std::to_string(i);

    stmt.bindUint64(":REQID" + idx, rId);
    stmt.bindUint64(":FAILED_FILES" + idx, ffl);
    stmt.bindUint64(":FAILED_BYTES" + idx, fb);
  }
  stmt.bindString(":STATUS_RUNNING", to_string(cta::schedulerdb::RepackJobStatus::RRS_Running));
  stmt.bindString(":STATUS_STARTING", to_string(cta::schedulerdb::RepackJobStatus::RRS_Starting));
  stmt.bindString(":STATUS_COMPLETE", to_string(cta::schedulerdb::RepackJobStatus::RRS_Complete));
  stmt.bindString(":STATUS_FAILED", to_string(cta::schedulerdb::RepackJobStatus::RRS_Failed));

  txn.getConn().setDbQuerySummary("update repack failures");
  stmt.executeNonQuery();

  return stmt.getNbAffectedRows();
}

rdbms::Rset RepackRequestTrackingRow::markStartOfExpansion(Transaction& txn) {
  const char* const sql = R"SQL(
    WITH NEXT_JOB AS (
      SELECT REPACK_REQUEST_ID
      FROM REPACK_REQUEST_TRACKING
      WHERE STATUS = :STATUS::REPACK_REQ_STATUS
        AND IS_EXPAND_STARTED = '0'
      ORDER BY CREATE_TIME ASC
      LIMIT 1
      FOR UPDATE
    )
    UPDATE REPACK_REQUEST_TRACKING
    SET IS_EXPAND_STARTED = '1',
    LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
    WHERE REPACK_REQUEST_ID IN (SELECT REPACK_REQUEST_ID FROM NEXT_JOB)
    RETURNING *
    )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", "RRS_ToExpand");
  txn.getConn().setDbQuerySummary("update repack expansion");
  auto result = stmt.executeQuery();
  return result;
}

uint64_t RepackRequestTrackingRow::updateRepackRequestExpansionFailure(Transaction& txn,
                                                                       const uint64_t& reqId,
                                                                       const bool isExpandFinished,
                                                                       const uint64_t& finishTime) {
  // This method is called only in case the expansion fails.
  // We need to notify the operator that the request is failed
  // so we change the status to Failed. We prevent further statistics update by
  // setting IS_COMPLETE to TRUE and we delete retrieve pending job
  // to limit the workload on the tape drive.
  std::string sql = R"SQL(
    UPDATE REPACK_REQUEST_TRACKING
    SET STATUS = :STATUS,
        IS_COMPLETE = TRUE,
        LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
        REPACK_FINISHED_TIME = :REPACK_FINISHED_TIME
  )SQL";
  if (isExpandFinished) {
    sql += ", IS_EXPAND_FINISHED = '1' ";
  }
  sql += " WHERE REPACK_REQUEST_ID = :REQID";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(cta::schedulerdb::RepackJobStatus::RRS_Failed));
  stmt.bindUint64(":REPACK_FINISHED_TIME", finishTime);
  stmt.bindUint64(":REQID", reqId);

  txn.getConn().setDbQuerySummary("update repack status");
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  // At the same time we need to prevent this request to be picked up by the taped,
  // so we delete the jobs from the pending table.
  std::string sql2 = R"SQL(
    DELETE FROM REPACK_RETRIEVE_PENDING_QUEUE
    WHERE
      REPACK_REQUEST_ID = :REQID
    )SQL";
  auto stmt2 = txn.getConn().createStmt(sql2);
  stmt2.bindUint64(":REQID", reqId);
  stmt2.executeNonQuery();

  return nrows;
}

uint64_t RepackRequestTrackingRow::updateRepackRequestExpansionStatus(Transaction& txn,
                                                                      const uint64_t& reqId,
                                                                      const bool isExpandFinished) {
  std::string sql = R"SQL(
    UPDATE REPACK_REQUEST_TRACKING
    SET STATUS = :STATUS::REPACK_REQ_STATUS
  )SQL";
  if (isExpandFinished) {
    sql += ", IS_EXPAND_FINISHED = '1' ";
  }
  sql += R"SQL(
    WHERE REPACK_REQUEST_ID = :REQID
      AND IS_COMPLETE = FALSE
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(cta::schedulerdb::RepackJobStatus::RRS_Starting));
  stmt.bindUint64(":REQID", reqId);

  txn.getConn().setDbQuerySummary("update repack status");
  stmt.executeNonQuery();
  return stmt.getNbAffectedRows();
}
}  // namespace cta::schedulerdb::postgres
