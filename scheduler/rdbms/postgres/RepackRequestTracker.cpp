/**
 * @project        The CERN Tape Archive (CTA)
 * @copyright	   Copyright © 2023 CERN
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

#include "scheduler/rdbms/postgres/RepackRequestTracker.hpp"
#include "rdbms/wrapper/PostgresColumn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"

namespace cta::schedulerdb::postgres {

  uint64_t
  RepackRequestTrackingRow::cancelRepack(Transaction &txn, const std::string &vid) {
    // The request will be cancelled via the packer checking all tracked repack requests
    std::string sql = R"SQL(
    DELETE FROM REPACK_REQUEST_TRACKING
    WHERE
      VID = :VID
      AND STATUS = ANY(ARRAY[
    )SQL";
    // we can move this to new bindArray method for stmt
    std::vector <std::string> statusVec;
    std::vector <std::string> placeholderVec;
    std::list <RepackJobStatus> statusList;
    statusList.emplace_back(RepackJobStatus::RRS_Pending);
    statusList.emplace_back(RepackJobStatus::RRS_ToExpand);
    size_t j = 1;
    for (const auto &jstatus: statusList) {
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
        ]::REPACK_REQ_STATUS[])
  )SQL";
    auto stmt = txn.getConn().createStmt(sql);
    stmt.bindString(":VID", vid);
    // we can move the array binding to new bindArray method for STMT
    size_t sz = statusVec.size();
    for (size_t i = 0; i < sz; ++i) {
      stmt.bindString(placeholderVec[i], statusVec[i]);
    }
    stmt.executeNonQuery();
    return stmt.getNbAffectedRows();
  }

  uint64_t RepackRequestTrackingRow::updateRepackRequestForExpansion(Transaction &txn,
                                            const uint32_t requestCount) {
    const char *const sql = R"SQL(
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
    stmt.executeNonQuery();
    return stmt.getNbAffectedRows();
  }

  uint64_t RepackRequestTrackingRow::updateRepackRequest(Transaction &txn,
                                                         const uint64_t &reqId,
                                                         const cta::SchedulerDatabase::RepackRequest::TotalStatsFiles &totalStatsFiles,
                                                         const uint64_t nbRetrieveSubrequestsCreated,
                                                         const uint64_t lastExpandedFseq,
                                                         const RepackJobStatus &newStatus) {
  const char *const sql = R"SQL(
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
        RETRIEVE_SUBREQUESTS_EXPANDED = :RETRIEVE_SUBREQUESTS_EXPANDED
    WHERE REPACK_REQUEST_ID = :REQID
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);

  // Bind job status
  stmt.bindString(":STATUS", to_string(newStatus));

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
  stmt.executeNonQuery();
  return stmt.getNbAffectedRows();
}



//uint64_t RepackRequestTrackingRow::updateRepackRequestProgress(Transaction &txn, std::vector<RepackRequestProgress> progressReport,
//                                                         const RepackJobStatus &newStatus) {
//  const char *const sql = R"SQL(
//    UPDATE REPACK_REQUEST_TRACKING
//    SET STATUS = :STATUS::REPACK_REQ_STATUS,
//        RETRIEVED_FILES += :RETRIEVED_FILES,
//        RETRIEVED_BYTES += :RETRIEVED_BYTES,
//        ARCHIVED_FILES += :ARCHIVED_FILES,
//        ARCHIVED_BYTES += :ARCHIVED_BYTES,
//        REARCHIVE_COPYNBS += :REARCHIVE_COPYNBS,
//    WHERE REPACK_REQUEST_ID = :REQID
//  )SQL";
//
//  auto stmt = txn.getConn().createStmt(sql);
//
//  stmt.bindString(":STATUS", to_string(newStatus));
//  stmt.bindUint64(":RETRIEVED_FILES", retrieved_files);
//  stmt.bindUint64(":RETRIEVED_BYTES", retrieved_bytes);
//  stmt.bindUint64(":ARCHIVED_FILES", archived_files);
//  stmt.bindUint64(":ARCHIVED_BYTES", archived_bytes);
//  stmt.bindUint64(":REARCHIVE_COPYNBS", rearchive_copynbs);
//  stmt.bindUint64(":REQID", reqId);
//
//  // Execute
//  stmt.executeNonQuery();
//  return stmt.getNbAffectedRows();
//}

rdbms::Rset RepackRequestTrackingRow::updateRepackRequestsProgress(
    Transaction& txn,
    const std::vector<RepackRequestProgress>& updates) {

    if (updates.empty()) {
      rdbms::Rset rset;
      return rset;
    }

    // Build SQL with placeholders for each row
    std::string sql =
        "WITH UPDATE_TABLE (REPACK_REQUEST_ID, RETRIEVED_FILES, RETRIEVED_BYTES, ARCHIVED_FILES, ARCHIVED_BYTES, REARCHIVE_COPYNBS, REARCHIVE_BYTES) AS (VALUES ";

    for (size_t i = 0; i < updates.size(); ++i) {
        if (i > 0) sql += ",";
        sql += "("
            ":REQID" + std::to_string(i) + "::BIGINT" + ","
            ":RETR_FILES" + std::to_string(i) + "::BIGINT" + ","
            ":RETR_BYTES" + std::to_string(i) + "::BIGINT" + ","
            ":ARCH_FILES" + std::to_string(i) + "::BIGINT" + ","
            ":ARCH_BYTES" + std::to_string(i) + "::BIGINT" + ","
            ":REARCH_COPY" + std::to_string(i) + "::BIGINT" + ","
            ":REARCH_BYTES" + std::to_string(i) + "::BIGINT" +
            ")";
    }

    sql += R"SQL()
           UPDATE REPACK_REQUEST_TRACKING trk
           SET
           STATUS = CASE
             WHEN (trk.ARCHIVED_FILES + upd.ARCHIVED_FILES) = trk.TOTAL_FILES_TO_ARCHIVE
               THEN :STATUS_COMPLETE::REPACK_REQ_STATUS
             ELSE :STATUS_RUNNING::REPACK_REQ_STATUS
           END,
           RETRIEVED_FILES = trk.RETRIEVED_FILES + upd.RETRIEVED_FILES,
           RETRIEVED_BYTES = trk.RETRIEVED_BYTES + upd.RETRIEVED_BYTES,
           ARCHIVED_FILES = trk.ARCHIVED_FILES + upd.ARCHIVED_FILES,
           ARCHIVED_BYTES = trk.ARCHIVED_BYTES + upd.ARCHIVED_BYTES,
           REARCHIVE_COPYNBS = trk.REARCHIVE_COPYNBS + upd.REARCHIVE_COPYNBS,
           REARCHIVE_BYTES = trk.REARCHIVE_BYTES + upd.REARCHIVE_BYTES
           FROM UPDATE_TABLE upd
           WHERE trk.REPACK_REQUEST_ID = upd.REPACK_REQUEST_ID
           RETURNING trk.VID
    )SQL";

    auto stmt = txn.getConn().createStmt(sql);

    // Bind each row's values
    for (size_t i = 0; i < updates.size(); ++i) {
        const auto& u = updates[i];
        std::string idx = std::to_string(i);

        stmt.bindUint64(":REQID" + idx, u.reqId);
        stmt.bindUint64(":RETR_FILES" + idx, u.retrievedFiles);
        stmt.bindUint64(":RETR_BYTES" + idx, u.retrievedBytes);
        stmt.bindUint64(":ARCH_FILES" + idx, u.archivedFiles);
        stmt.bindUint64(":ARCH_BYTES" + idx, u.archivedBytes);
        stmt.bindUint64(":REARCH_COPY" + idx, u.rearchiveCopyNbs);
        stmt.bindUint64(":REARCH_BYTES" + idx, u.rearchiveBytes);
    }
    stmt.bindString(":STATUS_COMPLETE", to_string(cta::schedulerdb::RepackJobStatus::RRS_Complete));
    stmt.bindString(":STATUS_RUNNING", to_string(cta::schedulerdb::RepackJobStatus::RRS_Running));

    return stmt.executeQuery();
}


  uint64_t RepackRequestTrackingRow::updateRepackRequestFailures(
          Transaction &txn,
          uint64_t reqId,
          uint64_t failedFilesToRetrieve,
          uint64_t failedBytesToRetrieve,
          uint64_t failedToCreateArchiveReq,
          RepackJobStatus newStatus) {
    std::string sql = R"SQL(
        UPDATE REPACK_REQUEST_TRACKING
        SET FAILED_TO_RETRIEVE_FILES            = :FAILED_FILES,
            FAILED_TO_RETRIEVE_BYTES            = :FAILED_BYTES,
            FAILED_TO_CREATE_ARCHIVE_REQ        = :FAILED_ARCH_REQS,
            STATUS                     = :STATUS::REPACK_REQ_STATUS
        WHERE REPACK_REQUEST_ID = :REQID
    )SQL";

    auto stmt = txn.getConn().createStmt(sql);
    stmt.bindUint64(":FAILED_FILES", failedFilesToRetrieve);
    stmt.bindUint64(":FAILED_BYTES", failedBytesToRetrieve);
    stmt.bindUint64(":FAILED_ARCH_REQS", failedToCreateArchiveReq);
    stmt.bindString(":STATUS", to_string(newStatus));
    stmt.bindUint64(":REQID", reqId);

    stmt.executeNonQuery();
    return stmt.getNbAffectedRows();
  }


  rdbms::Rset RepackRequestTrackingRow::markStartOfExpansion(Transaction &txn) {
    const char *const sql = R"SQL(
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
    SET IS_EXPAND_STARTED = '1'
    WHERE REPACK_REQUEST_ID IN (SELECT REPACK_REQUEST_ID FROM NEXT_JOB)
    RETURNING *
    )SQL";
    auto stmt = txn.getConn().createStmt(sql);
    stmt.bindString(":STATUS", "RRS_ToExpand");
    auto result = stmt.executeQuery();
    return result;
  }


  uint64_t RepackRequestTrackingRow::updateStatusAndFinishTime(
    Transaction &txn,
    const uint64_t &reqId,
    const bool isExpandFinished,
    const RepackJobStatus &newStatus,
    const uint64_t &finishTime)
{
  std::string sql = R"SQL(
    UPDATE REPACK_REQUEST_TRACKING
    SET STATUS = :STATUS::REPACK_REQ_STATUS,
        FINISH_TIME = :FINISH_TIME
  )SQL";
    if (isExpandFinished){
   sql += ", IS_EXPAND_FINISHED = '1' ";
  }
  sql += " WHERE REPACK_REQUEST_ID = :REQID";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(newStatus));
  stmt.bindUint64(":FINISH_TIME", finishTime);
  stmt.bindUint64(":REQID", reqId);

  stmt.executeNonQuery();
  return stmt.getNbAffectedRows();
}

 uint64_t RepackRequestTrackingRow::updateStatus(
    Transaction &txn,
    const uint64_t &reqId,
    const bool isExpandFinished,
    const RepackJobStatus &newStatus)
{
  std::string sql = R"SQL(
    UPDATE REPACK_REQUEST_TRACKING
    SET STATUS = :STATUS::REPACK_REQ_STATUS
  )SQL";
  if (isExpandFinished){
   sql += ", IS_EXPAND_FINISHED = '1' ";
  }
  sql += " WHERE REPACK_REQUEST_ID = :REQID";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(newStatus));
  stmt.bindUint64(":REQID", reqId);

  stmt.executeNonQuery();
  return stmt.getNbAffectedRows();
}
} // namespace cta::schedulerdb::postgres
