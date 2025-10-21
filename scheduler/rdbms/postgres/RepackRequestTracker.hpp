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

#include "common/log/LogContext.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"
#include "scheduler/rdbms/postgres/Enums.hpp"
#include "common/dataStructures/EntryLog.hpp"

namespace cta::schedulerdb::postgres {

  struct RepackRequestProgress {
    uint64_t reqId = 0;
    std::string vid  = "";
    uint64_t retrievedFiles = 0;
    uint64_t retrievedBytes = 0;
    uint64_t archivedFiles = 0;
    uint64_t archivedBytes = 0;
    uint64_t rearchiveCopyNbs = 0;
    uint64_t rearchiveBytes = 0;
  };

  struct RepackRequestTrackingRow {
    uint64_t repackReqId = 0;
    std::string vid;
    std::string bufferUrl;
    RepackJobStatus status = RepackJobStatus::RRS_Pending;
    bool isAddCopies = true;
    bool isMove = true;
    uint64_t maxFilesToSelect = 0;
    uint64_t totalFilesOnTapeAtStart = 0;
    uint64_t totalBytesOnTapeAtStart = 0;
    bool allFilesSelectedAtStart = true;
    uint64_t totalFilesToRetrieve = 0;
    uint64_t totalBytesToRetrieve = 0;
    uint64_t totalFilesToArchive = 0;
    uint64_t totalBytesToArchive = 0;
    uint64_t userProvidedFiles = 0;
    uint64_t userProvidedBytes = 0;
    uint64_t retrievedFiles = 0;
    uint64_t retrievedBytes = 0;
    uint64_t archivedFiles = 0;
    uint64_t archivedBytes = 0;
    uint64_t rearchiveCopyNbs = 0;
    uint64_t rearchiveBytes = 0;
    uint64_t failedToRetrieveFiles = 0;
    uint64_t failedToRetrieveBytes = 0;
    uint64_t failedToCreateArchiveReq = 0;
    uint64_t failedToArchiveFiles = 0;
    uint64_t failedToArchiveBytes = 0;
    uint64_t lastExpandedFseq = 0;
    bool isExpandFinished = false;
    bool isExpandStarted = false;
    std::string mountPolicyName;
    bool isComplete = false;
    bool isNoRecall = false;
    std::string subReqProtoBuf;
    std::string destInfoProtoBuf;
    common::dataStructures::EntryLog createLog;
    time_t repackFinishedTime = 0;
    time_t lastUpdateTime = 0;

    RepackRequestTrackingRow() = default;

    /**
     * Constructor from row
     *
     * @param row  A single row from the current row of the rset
     */
    explicit RepackRequestTrackingRow(const rdbms::Rset &rset) { *this = rset; }

    RepackRequestTrackingRow &operator=(const rdbms::Rset &rset) {
      repackReqId = rset.columnUint64("REPACK_REQUEST_ID");
      vid = rset.columnString("VID");
      bufferUrl = rset.columnString("BUFFER_URL");
      status = from_string<RepackJobStatus>(rset.columnString("STATUS"));
      isAddCopies = rset.columnBool("IS_ADD_COPIES");
      isMove = rset.columnBool("IS_MOVE");
      isNoRecall = rset.columnBool("IS_NO_RECALL");
      maxFilesToSelect = rset.columnUint64("MAX_FILES_TO_EXPAND");
      totalFilesOnTapeAtStart = rset.columnUint64("TOTAL_FILES_ON_TAPE_AT_START");
      totalBytesOnTapeAtStart = rset.columnUint64("TOTAL_BYTES_ON_TAPE_AT_START");
      allFilesSelectedAtStart = rset.columnBool("ALL_FILES_SELECTED_AT_START");
      totalFilesToRetrieve = rset.columnUint64("TOTAL_FILES_TO_RETRIEVE");
      totalBytesToRetrieve = rset.columnUint64("TOTAL_BYTES_TO_RETRIEVE");
      totalFilesToArchive = rset.columnUint64("TOTAL_FILES_TO_ARCHIVE");
      rearchiveCopyNbs = rset.columnUint64("REARCHIVE_COPYNBS");
      rearchiveBytes = rset.columnUint64("REARCHIVE_BYTES");
      totalBytesToArchive = rset.columnUint64("TOTAL_BYTES_TO_ARCHIVE");
      userProvidedFiles = rset.columnUint64("USER_PROVIDED_FILES");
      userProvidedBytes = rset.columnUint64("USER_PROVIDED_BYTES");
      retrievedFiles = rset.columnUint64("RETRIEVED_FILES");
      retrievedBytes = rset.columnUint64("RETRIEVED_BYTES");
      archivedFiles = rset.columnUint64("ARCHIVED_FILES");
      archivedBytes = rset.columnUint64("ARCHIVED_BYTES");
      failedToRetrieveFiles = rset.columnUint64("FAILED_TO_RETRIEVE_FILES");
      failedToRetrieveBytes = rset.columnUint64("FAILED_TO_RETRIEVE_BYTES");
      failedToCreateArchiveReq = rset.columnUint64("FAILED_TO_CREATE_ARCHIVE_REQ");
      failedToArchiveFiles = rset.columnUint64("FAILED_TO_ARCHIVE_FILES");
      failedToArchiveBytes = rset.columnUint64("FAILED_TO_ARCHIVE_BYTES");
      lastExpandedFseq = rset.columnUint64("LAST_EXPANDED_FSEQ");
      isExpandFinished = rset.columnBool("IS_EXPAND_FINISHED");
      isExpandStarted = rset.columnBool("IS_EXPAND_STARTED");
      mountPolicyName = rset.columnString("MOUNT_POLICY");
      isComplete = rset.columnBool("IS_COMPLETE");
      subReqProtoBuf = rset.columnBlob("SUBREQ_PB");
      destInfoProtoBuf = rset.columnBlob("DESTINFO_PB");
      createLog.username = rset.columnString("CREATE_USERNAME");
      createLog.host = rset.columnString("CREATE_HOST");
      createLog.time = rset.columnUint64("CREATE_TIME");
      repackFinishedTime = rset.columnUint64("REPACK_FINISHED_TIME");
      lastUpdateTime = rset.columnUint64("LAST_UPDATE_TIME");

      return *this;
    }

    void insert(rdbms::Conn &conn) const {
      // setting repackReqId; todo
      const char *const sql = R"SQL(
      INSERT INTO REPACK_REQUEST_TRACKING(
        VID,
        BUFFER_URL,
        STATUS,
        IS_ADD_COPIES,
        IS_MOVE,
        MAX_FILES_TO_EXPAND,
        TOTAL_FILES_ON_TAPE_AT_START,
        TOTAL_BYTES_ON_TAPE_AT_START,
        ALL_FILES_SELECTED_AT_START,
        TOTAL_FILES_TO_RETRIEVE,
        TOTAL_BYTES_TO_RETRIEVE,
        TOTAL_FILES_TO_ARCHIVE,
        TOTAL_BYTES_TO_ARCHIVE,
        USER_PROVIDED_FILES,
        USER_PROVIDED_BYTES,
        RETRIEVED_FILES,
        RETRIEVED_BYTES,
        ARCHIVED_FILES,
        ARCHIVED_BYTES,
        REARCHIVE_COPYNBS,
        REARCHIVE_BYTES,
        FAILED_TO_RETRIEVE_FILES,
        FAILED_TO_RETRIEVE_BYTES,
        FAILED_TO_CREATE_ARCHIVE_REQ,
        FAILED_TO_ARCHIVE_FILES,
        FAILED_TO_ARCHIVE_BYTES,
        LAST_EXPANDED_FSEQ,
        IS_EXPAND_FINISHED,
        IS_EXPAND_STARTED,
        MOUNT_POLICY,
        IS_COMPLETE,
        IS_NO_RECALL,
        SUBREQ_PB,
        DESTINFO_PB,
        CREATE_USERNAME,
        CREATE_HOST,
        CREATE_TIME,
        REPACK_FINISHED_TIME) VALUES (
        :VID,
        :BUFFER_URL,
        :STATUS,
        :IS_ADD_COPIES,
        :IS_MOVE,
        :MAX_FILES_TO_EXPAND,
        :TOTAL_FILES_ON_TAPE_AT_START,
        :TOTAL_BYTES_ON_TAPE_AT_START,
        :ALL_FILES_SELECTED_AT_START,
        :TOTAL_FILES_TO_RETRIEVE,
        :TOTAL_BYTES_TO_RETRIEVE,
        :TOTAL_FILES_TO_ARCHIVE,
        :TOTAL_BYTES_TO_ARCHIVE,
        :USER_PROVIDED_FILES,
        :USER_PROVIDED_BYTES,
        :RETRIEVED_FILES,
        :RETRIEVED_BYTES,
        :ARCHIVED_FILES,
        :ARCHIVED_BYTES,
        :REARCHIVE_COPYNBS,
        :REARCHIVE_BYTES,
        :FAILED_TO_RETRIEVE_FILES,
        :FAILED_TO_RETRIEVE_BYTES,
        :FAILED_TO_CREATE_ARCHIVE_REQ,
        :FAILED_TO_ARCHIVE_FILES,
        :FAILED_TO_ARCHIVE_BYTES,
        :LAST_EXPANDED_FSEQ,
        :IS_EXPAND_FINISHED,
        :IS_EXPAND_STARTED,
        :MOUNT_POLICY,
        :IS_COMPLETE,
        :IS_NO_RECALL,
        :SUBREQ_PB,
        :DESTINFO_PB,
        :CREATE_USERNAME,
        :CREATE_HOST,
        :CREATE_TIME,
        :REPACK_FINISHED_TIME)
    )SQL";

      auto stmt = conn.createStmt(sql);
      stmt.bindString(":VID", vid);
      stmt.bindString(":BUFFER_URL", bufferUrl);
      stmt.bindString(":STATUS", to_string(status));
      stmt.bindBool(":IS_ADD_COPIES", isAddCopies);
      stmt.bindBool(":IS_MOVE", isMove);
      stmt.bindUint64(":MAX_FILES_TO_EXPAND", maxFilesToSelect);
      stmt.bindUint64(":TOTAL_FILES_ON_TAPE_AT_START", totalFilesOnTapeAtStart);
      stmt.bindUint64(":TOTAL_BYTES_ON_TAPE_AT_START", totalBytesOnTapeAtStart);
      stmt.bindBool(":ALL_FILES_SELECTED_AT_START", allFilesSelectedAtStart);
      stmt.bindUint64(":TOTAL_FILES_TO_RETRIEVE", totalFilesToRetrieve);
      stmt.bindUint64(":TOTAL_BYTES_TO_RETRIEVE", totalBytesToRetrieve);
      stmt.bindUint64(":TOTAL_FILES_TO_ARCHIVE", totalFilesToArchive);
      stmt.bindUint64(":TOTAL_BYTES_TO_ARCHIVE", totalBytesToArchive);
      stmt.bindUint64(":USER_PROVIDED_FILES", userProvidedFiles);
      stmt.bindUint64(":USER_PROVIDED_BYTES", userProvidedBytes);
      stmt.bindUint64(":RETRIEVED_FILES", retrievedFiles);
      stmt.bindUint64(":RETRIEVED_BYTES", retrievedBytes);
      stmt.bindUint64(":ARCHIVED_FILES", archivedFiles);
      stmt.bindUint64(":ARCHIVED_BYTES", archivedBytes);
      stmt.bindUint64(":REARCHIVE_COPYNBS", rearchiveCopyNbs);
      stmt.bindUint64(":REARCHIVE_BYTES", rearchiveBytes);
      stmt.bindUint64(":FAILED_TO_RETRIEVE_FILES", failedToRetrieveFiles);
      stmt.bindUint64(":FAILED_TO_RETRIEVE_BYTES", failedToRetrieveBytes);
      stmt.bindUint64(":FAILED_TO_CREATE_ARCHIVE_REQ", failedToCreateArchiveReq);
      stmt.bindUint64(":FAILED_TO_ARCHIVE_FILES", failedToArchiveFiles);
      stmt.bindUint64(":FAILED_TO_ARCHIVE_BYTES", failedToArchiveBytes);
      stmt.bindUint64(":LAST_EXPANDED_FSEQ", lastExpandedFseq);
      stmt.bindBool(":IS_EXPAND_FINISHED", isExpandFinished);
      stmt.bindBool(":IS_EXPAND_STARTED", isExpandStarted);
      stmt.bindString(":MOUNT_POLICY", mountPolicyName);
      stmt.bindBool(":IS_COMPLETE", isComplete);
      stmt.bindBool(":IS_NO_RECALL", isNoRecall);
      stmt.bindBlob(":SUBREQ_PB", subReqProtoBuf);
      stmt.bindBlob(":DESTINFO_PB", destInfoProtoBuf);
      stmt.bindString(":CREATE_USERNAME", createLog.username);
      stmt.bindString(":CREATE_HOST", createLog.host);
      stmt.bindUint64(":CREATE_TIME", createLog.time);
      stmt.bindUint64(":REPACK_FINISHED_TIME", repackFinishedTime);

      stmt.executeNonQuery();
    }

    void addParamsToLogContext(log::ScopedParamContainer &params) const {
      // does not set repackReqId
      params.add("vid", vid);
      params.add("bufferUrl", bufferUrl);
      params.add("status", to_string(status));
      params.add("isAddCopies", isAddCopies);
      params.add("isMove", isMove);
      params.add("maxFilesToSelect", maxFilesToSelect);
      params.add("totalFilesOnTapeAtStart", totalFilesOnTapeAtStart);
      params.add("totalBytesOnTapeAtStart", totalBytesOnTapeAtStart);
      params.add("allFilesSelectedAtStart", allFilesSelectedAtStart);
      params.add("totalFilesToRetrieve", totalFilesToRetrieve);
      params.add("totalBytesToRetrieve", totalBytesToRetrieve);
      params.add("totalFilesToArchive", totalFilesToArchive);
      params.add("totalBytesToArchive", totalBytesToArchive);
      params.add("userProvidedFiles", userProvidedFiles);
      params.add("userProvidedBytes", userProvidedBytes);
      params.add("retrievedFiles", retrievedFiles);
      params.add("retrievedBytes", retrievedBytes);
      params.add("archivedFiles", archivedFiles);
      params.add("archivedBytes", archivedBytes);
      params.add("failedToRetrieveFiles", failedToRetrieveFiles);
      params.add("failedToRetrieveBytes", failedToRetrieveBytes);
      params.add("failedToCreateArchiveReq", failedToCreateArchiveReq);
      params.add("failedToArchiveFiles", failedToArchiveFiles);
      params.add("failedToArchiveBytes", failedToArchiveBytes);
      params.add("lastExpandedFseq", lastExpandedFseq);
      params.add("isExpandFinished", isExpandFinished);
      params.add("isExpandStarted", isExpandStarted);
      params.add("mountPolicyName", mountPolicyName);
      params.add("isComplete", isComplete);
      params.add("isNoRecall", isNoRecall);

      /* Columns to be replaced by other DB columns than protobuf filled columns
       * params.add("subReqProtoBuf", subReqProtoBuf);
       * params.add("destInfoProtoBuf", destInfoProtoBuf);
       */
      params.add("createUsername", createLog.username);
      params.add("createHost", createLog.host);
      params.add("createTime", createLog.time);
      params.add("repackFinishedTime", repackFinishedTime);
    }

    /**
     * Get Repack Request Statistics
     *
     * @return result set containing grouped statistics from REPACK_REQUEST_TRACKING
     */
    static rdbms::Rset getRepackRequestStatistics(rdbms::Conn &conn) {
      const char *const sql = R"SQL(
      SELECT
        STATUS,
        COUNT(*) AS JOBS_COUNT,
        COALESCE(SUM(TOTAL_BYTES_TO_RETRIEVE), 0) AS RETRIEVE_TOTAL_SIZE,
        COALESCE(SUM(TOTAL_BYTES_TO_ARCHIVE), 0) AS ARCHIVE_TOTAL_SIZE,
        MIN(CREATE_TIME) AS OLDEST_JOB_START_TIME,
        MAX(CREATE_TIME) AS YOUNGEST_JOB_START_TIME,
        MAX(REPACK_FINISHED_TIME) AS LAST_JOB_UPDATE_TIME
      FROM REPACK_REQUEST_TRACKING
      GROUP BY STATUS
    )SQL";

      auto stmt = conn.createStmt(sql);
      return stmt.executeQuery();
    }

    /**
     * Select unowned jobs from the queue
     *
     * @param conn    DB connection
     *
     * @return  result set
     */
    static rdbms::Rset selectRepackRows(rdbms::Conn &conn, const std::string &vid) {
      std::string sql = R"SQL(
            SELECT
        trk.REPACK_REQUEST_ID,
        trk.VID,
        trk.BUFFER_URL,
        trk.STATUS,
        trk.IS_ADD_COPIES,
        trk.IS_MOVE,
        trk.MAX_FILES_TO_EXPAND,
        trk.TOTAL_FILES_ON_TAPE_AT_START,
        trk.TOTAL_BYTES_ON_TAPE_AT_START,
        trk.ALL_FILES_SELECTED_AT_START,
        trk.TOTAL_FILES_TO_RETRIEVE,
        trk.TOTAL_BYTES_TO_RETRIEVE,
        trk.TOTAL_FILES_TO_ARCHIVE,
        trk.TOTAL_BYTES_TO_ARCHIVE,
        trk.USER_PROVIDED_FILES,
        trk.USER_PROVIDED_BYTES,
        trk.RETRIEVED_FILES,
        trk.RETRIEVED_BYTES,
        trk.ARCHIVED_FILES,
        trk.ARCHIVED_BYTES,
        trk.REARCHIVE_COPYNBS,
        trk.REARCHIVE_BYTES,
        trk.FAILED_TO_RETRIEVE_FILES,
        trk.FAILED_TO_RETRIEVE_BYTES,
        trk.FAILED_TO_CREATE_ARCHIVE_REQ,
        trk.FAILED_TO_ARCHIVE_FILES,
        trk.FAILED_TO_ARCHIVE_BYTES,
        trk.LAST_EXPANDED_FSEQ,
        trk.RETRIEVE_SUBREQUESTS_EXPANDED,
        trk.IS_EXPAND_FINISHED,
        trk.IS_EXPAND_STARTED,
        trk.MOUNT_POLICY,
        trk.IS_COMPLETE,
        trk.IS_NO_RECALL,
        trk.SUBREQ_PB,
        trk.DESTINFO_PB,
        trk.CREATE_USERNAME,
        trk.CREATE_HOST,
        trk.CREATE_TIME,
        trk.REPACK_FINISHED_TIME,
        trk.LAST_UPDATE_TIME,
        dst.VID AS DESTINATION_VID,
        dst.ARCHIVED_FILES AS DESTINATION_ARCHIVED_FILES,
        dst.ARCHIVED_BYTES AS DESTINATION_ARCHIVED_BYTES
      FROM
        REPACK_REQUEST_TRACKING trk
      LEFT JOIN REPACK_REQUEST_DESTINATION_STATISTICS dst
           ON dst.REPACK_REQUEST_ID = trk.REPACK_REQUEST_ID
     )SQL";
     if (vid != "all") {
        sql += " WHERE trk.VID = :VID ";
      }
      sql += R"SQL(
      ORDER BY trk.REPACK_REQUEST_ID, dst.VID;
    )SQL";
       auto stmt = conn.createStmt(sql);
      if (vid != "all") {
        stmt.bindString(":VID", vid);
      }
      return stmt.executeQuery();
    }

    /**
    * @brief Update progress counters for multiple repack requests in a single query.
    *
    * This method applies incremental progress updates (retrieved/archived files and bytes,
    * rearchive copy numbers, and status) for a vector of repack requests. All updates are
    * executed in one SQL statement using a VALUES list and joined against the
    * REPACK_REQUEST_TRACKING table, which reduces round trips to the database compared
    * to updating each request individually.
    *
    * @param txn     Active transaction context used to execute the update.
    * @param updates Vector of per-request progress updates, each containing a request ID,
    *                progress counters, and the new status.
    *
    * @return The result set with VID list where changes were made.
    */
    static rdbms::Rset updateRepackRequestsProgress(
            Transaction &txn,
            const std::vector <RepackRequestProgress> &updates);

    /**
     * When CTA received the deleteArchive request from the disk buffer,
     * the following method ensures removal from the pending queue
     *
     * @param txn    Transaction handling the connection to the backend database
     * @param vid    VID of the tape for which the repack operation shall be marked for cancellation
     *
     * @return  The number of affected jobs
     */
    static uint64_t cancelRepack(Transaction &txn, const std::string &vid);

    /**
     * Update a limited number of pending repack requests to "ToExpand"
     * and return updated summary statistics.
     *
     * @param txn Transaction object
     * @param requestCount maximum number of requests to update
     * @return uint64_t number of rows effectively updated in REPACK_REQUEST_TRACKING
     */
    static uint64_t updateRepackRequestForExpansion(Transaction &txn, const uint32_t requestCount);

    /**
     * Atomically fetch and mark the next request to expand.
     *
     * This will:
     *  - Select the oldest request with STATUS = 'RRS_ToExpand' and IS_EXPAND_STARTED = '0'
     *  - Update IS_EXPAND_STARTED = '1'
     *  - Return the updated row in a result set
     *
     * Concurrency safety:
     *  - Uses FOR UPDATE SKIP LOCKED so concurrent workers don't process the same row.
     *
     * @param txn Active transaction
     * @return rdbms::Rset containing one row (or empty if none found)
     */
    static rdbms::Rset markStartOfExpansion(Transaction &txn);

/**
   * @brief Updates the status of a repack request.
   *
   * @param txn Active transaction to run the update within.
   * @param reqId The REPACK_REQUEST_ID of the request to update.
   * @param isExpandFinished boolean expressing if expansion finished
   * @param newStatus The new job status to set.
   * @return The number of affected rows (should be 1 if successful).
   */
    static uint64_t updateRepackRequestStatus(
            Transaction &txn,
            const uint64_t &reqId,
            const bool isExpandFinished,
            const RepackJobStatus &newStatus);

    /**
     * @brief Updates the status and finish time of a repack request.
     *
     * @param txn Active transaction to run the update within.
     * @param reqId The REPACK_REQUEST_ID of the request to update.
     * @param isExpandFinished boolean expressing if expansion finished
     * @param newStatus The new job status to set.
     * @param finishTime The finish time (epoch or timestamp value) to set.
     * @return The number of affected rows (should be 1 if successful).
     */
    static uint64_t updateRepackRequestStatusAndFinishTime(
            Transaction &txn,
            const uint64_t &reqId,
            const bool isExpandFinished,
            const RepackJobStatus &newStatus,
            const uint64_t &finishTime);

    /**
     * @brief Update an existing repack request in the REPACK_REQUEST_TRACKING table.
     *
     * This method updates the status and key statistics of a repack request row,
     * including file/byte counts, expansion progress, and completion time.
     *
     * @param txn                          Active transaction context used to execute the update.
     * @param reqId                        The unique identifier of the repack request to update.
     * @param totalStatsFiles              Aggregated statistics about files and bytes for this repack job.
     * @param nbRetrieveSubrequestsCreated Number of retrieve subrequests expanded for this repack request.
     * @param lastExpandedFseq             Last file sequence number expanded in this repack request.
     * @param newStatus                    The new status value for the repack request.
     *
     * @return The number of rows affected (should be 1 if the update succeeded).
     */
    static uint64_t updateRepackRequest(Transaction &txn,
                                        const uint64_t &reqId,
                                        const cta::SchedulerDatabase::RepackRequest::TotalStatsFiles &totalStatsFiles,
                                        const uint64_t nbRetrieveSubrequestsCreated,
                                        const uint64_t lastExpandedFseq,
                                        const RepackJobStatus &newStatus);


    // Update failure counters and status (overwrite values)
    static uint64_t updateRepackRequestFailures(
            Transaction &txn,
            const uint64_t reqId,
            const uint64_t failedFilesToRetrieve,
            const uint64_t failedBytesToRetrieve,
            const uint64_t failedToCreateArchiveReq,
            const RepackJobStatus newStatus);


    static uint64_t updateRepackRequestFailuresBatch(
            Transaction &txn,
            const std::vector <uint64_t> &reqIds,
            const std::vector <uint64_t> &failedFiles,
            const std::vector <uint64_t> &failedBytes,
            bool isRetrieve);

  };

}// namespace cta::schedulerdb::postgres
