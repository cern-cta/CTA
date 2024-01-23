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
#include "scheduler/PostgresSchedDB/sql/Transaction.hpp"
#include "scheduler/PostgresSchedDB/sql/Enums.hpp"
#include "common/dataStructures/EntryLog.hpp"

namespace cta::postgresscheddb::sql {

struct RepackJobQueueRow {
  uint64_t repackReqId = 0;
  std::string vid;
  std::string bufferUrl;
  RepackJobStatus status = RepackJobStatus::RRS_Pending;
  bool isAddCopies = true;
  bool isMove = true;
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

  RepackJobQueueRow() = default;

  /**
   * Constructor from row
   *
   * @param row  A single row from the current row of the rset
   */
  explicit RepackJobQueueRow(const rdbms::Rset &rset) noexcept {
    *this = rset;
  }

  RepackJobQueueRow& operator=(const rdbms::Rset &rset) noexcept {
    repackReqId               = rset.columnUint64("REPACK_REQID");
    vid                       = rset.columnString("VID");
    bufferUrl                 = rset.columnString("BUFFER_URL");
    status                    = from_string<RepackJobStatus>(
                                rset.columnString("STATUS") );
    isAddCopies               = rset.columnBool("IS_ADD_COPIES");
    isMove                    = rset.columnBool("IS_MOVE");
    totalFilesOnTapeAtStart   = rset.columnUint64("TOTAL_FILES_ON_TAPE_AT_START");
    totalBytesOnTapeAtStart   = rset.columnUint64("TOTAL_BYTES_ON_TAPE_AT_START");
    allFilesSelectedAtStart   = rset.columnBool("ALL_FILES_SELECTED_AT_START");
    totalFilesToRetrieve      = rset.columnUint64("TOTAL_FILES_TO_RETRIEVE");
    totalBytesToRetrieve      = rset.columnUint64("TOTAL_BYTES_TO_RETRIEVE");
    totalFilesToArchive       = rset.columnUint64("TOTAL_FILES_TO_ARCHIVE");
    totalBytesToArchive       = rset.columnUint64("TOTAL_BYTES_TO_ARCHIVE");
    userProvidedFiles         = rset.columnUint64("USER_PROVIDED_FILES");
    userProvidedBytes         = rset.columnUint64("USER_PROVIDED_BYTES");
    retrievedFiles            = rset.columnUint64("RETRIEVED_FILES");
    retrievedBytes            = rset.columnUint64("RETRIEVED_BYTES");
    archivedFiles             = rset.columnUint64("ARCHIVED_FILES");
    archivedBytes             = rset.columnUint64("ARCHIVED_BYTES");
    failedToRetrieveFiles     = rset.columnUint64("FAILED_TO_RETRIEVE_FILES");
    failedToRetrieveBytes     = rset.columnUint64("FAILED_TO_RETRIEVE_BYTES");
    failedToCreateArchiveReq  = rset.columnUint64("FAILED_TO_CREATE_ARCHIVE_REQ");
    failedToArchiveFiles      = rset.columnUint64("FAILED_TO_ARCHIVE_FILES");
    failedToArchiveBytes      = rset.columnUint64("FAILED_TO_ARCHIVE_BYTES");
    lastExpandedFseq          = rset.columnUint64("LAST_EXPANDED_FSEQ");
    isExpandFinished          = rset.columnBool("IS_EXPAND_FINISHED");
    isExpandStarted           = rset.columnBool("IS_EXPAND_STARTED");
    mountPolicyName           = rset.columnString("MOUNT_POLICY");
    isComplete                = rset.columnBool("IS_COMPLETE");
    isNoRecall                = rset.columnBool("IS_NO_RECALL");
    subReqProtoBuf            = rset.columnBlob("SUBREQ_PB");
    destInfoProtoBuf          = rset.columnBlob("DESTINFO_PB");
    createLog.username        = rset.columnString("CREATE_USERNAME");
    createLog.host            = rset.columnString("CREATE_HOST");
    createLog.time            = rset.columnUint64("CREATE_TIME");
    repackFinishedTime        = rset.columnUint64("REPACK_FINIHSED_TIME");

    return *this;
  }

  void insert(Transaction &txn) const {
    // setting repackReqId; todo
    const char *const sql =
      "INSERT INTO REPACK_JOB_QUEUE("
        "VID,"
        "BUFFER_URL,"
        "STATUS,"
        "IS_ADD_COPIES,"
        "IS_MOVE,"
        "TOTAL_FILES_ON_TAPE_AT_START,"
        "TOTAL_BYTES_ON_TAPE_AT_START,"
        "ALL_FILES_SELECTED_AT_START,"
        "TOTAL_FILES_TO_RETRIEVE,"
        "TOTAL_BYTES_TO_RETRIEVE,"
        "TOTAL_FILES_TO_ARCHIVE,"
        "TOTAL_BYTES_TO_ARCHIVE,"
        "USER_PROVIDED_FILES,"
        "USER_PROVIDED_BYTES,"
        "RETRIEVED_FILES,"
        "RETRIEVED_BYTES,"
        "ARCHIVED_FILES,"
        "ARCHIVED_BYTES,"
        "FAILED_TO_RETRIEVE_FILES,"
        "FAILED_TO_RETRIEVE_BYTES,"
        "FAILED_TO_CREATE_ARCHIVE_REQ,"
        "FAILED_TO_ARCHIVE_FILES,"
        "FAILED_TO_ARCHIVE_BYTES,"
        "LAST_EXPANDED_FSEQ,"
        "IS_EXPAND_FINISHED,"
        "IS_EXPAND_STARTED,"
        "MOUNT_POLICY,"
        "IS_COMPLETE,"
        "IS_NO_RECALL,"
        "SUBREQ_PB,"
        "DESTINFO_PB,"
        "CREATE_USERNAME,"
        "CREATE_HOST,"
        "CREATE_TIME,"
        "REPACK_FINISHED_TIME) VALUES ("
        ":VID,"
        ":BUFFER_URL,"
        ":STATUS,"
        ":IS_ADD_COPIES,"
        ":IS_MOVE,"
        ":TOTAL_FILES_ON_TAPE_AT_START,"
        ":TOTAL_BYTES_ON_TAPE_AT_START,"
        ":ALL_FILES_SELECTED_AT_START,"
        ":TOTAL_FILES_TO_RETRIEVE,"
        ":TOTAL_BYTES_TO_RETRIEVE,"
        ":TOTAL_FILES_TO_ARCHIVE,"
        ":TOTAL_BYTES_ARCHIVE,"
        ":USER_PROVIDED_FILES,"
        ":USER_PROVIDED_BYTES,"
        ":RETRIEVED_FILES,"
        ":RETRIEVED_BYTES,"
        ":ARCHIVED_FILES,"
        ":ARCHIVED_BYTES,"
        ":FAILED_TO_RETRIEVE_FILES,"
        ":FAILED_TO_RETRIEVE_BYTES,"
        ":FAILED_TO_CREATE_ARCHIVE_REQ,"
        ":FAILED_TO_ARCHIVE_FILES,"
        ":FAILED_TO_ARCHIVE_BYTES,"
        ":LAST_EXPANDED_FSEQ,"
        ":IS_EXPAND_FINISHED,"
        ":IS_EXPAND_STARTED,"
        ":MOUNT_POLICY,"
        ":IS_COMPLETE,"
        ":IS_NO_RECALL,"
        ":SUBREQ_PB,"
        ":DESTINFO_PB,"
        ":CREATE_USERNAME,"
        ":CREATE_HOST,"
        ":CREATE_TIME,"
        ":REPACK_FINISHED_TIME)";

    auto stmt = txn.conn().createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.bindString(":BUFFER_URL", bufferUrl);
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindBool(":IS_ADD_COPIES", isAddCopies);
    stmt.bindBool(":IS_MOVE", isMove);
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

  void addParamsToLogContext(log::ScopedParamContainer& params) const {
    // does not set repackReqId
    params.add("vid", vid);
    params.add("bufferUrl", bufferUrl);
    params.add("status", to_string(status));
    params.add("isAddCopies", isAddCopies);
    params.add("isMove", isMove);
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
//    params.add("subReqProtoBuf", subReqProtoBuf);
//    params.add("destInfoProtoBuf", destInfoProtoBuf);
    params.add("createUsername", createLog.username);
    params.add("createHost", createLog.host);
    params.add("createTime", createLog.time);
    params.add("repackFinishedTime", repackFinishedTime);
  }

  /**
   * Select unowned jobs from the queue
   *
   * @param txn        Transaction to use for this query
   * @param status     Archive Job Status to select on
   * @param tapepool   Tapepool to select on
   * @param limit      Maximum number of rows to return
   *
   * @return  result set
   */
  static rdbms::Rset select(Transaction &txn, RepackJobStatus status, uint32_t limit) {

    const char *const sql =
    "SELECT "
      "REPACK_REQID AS REPACK_REQID,"
      "VID AS VID,"
      "BUFFER_URL AS BUFFER_URL,"
      "STATUS AS STATUS,"
      "IS_ADD_COPIES AS IS_ADD_COPIES,"
      "IS_MOVE AS IS_MOVE,"
      "TOTAL_FILES_ON_TAPE_AT_START AS TOTAL_FILES_ON_TAPE_AT_START,"
      "TOTAL_BYTES_ON_TAPE_AT_START AS TOTAL_BYTES_ON_TAPE_AT_START,"
      "ALL_FILES_SELECTED_AT_START AS ALL_FILES_SELECTED_AT_START,"
      "TOTAL_FILES_TO_RETRIEVE AS TOTAL_FILES_TO_RETRIEVE,"
      "TOTAL_BYTES_TO_RETRIEVE AS TOTAL_BYTES_TO_RETRIEVE,"
      "TOTAL_FILES_TO_ARCHIVE AS TOTAL_FILES_TO_ARCHIVE,"
      "TOTAL_BYTES_TO_ARCHIVE AS TOTAL_BYTES_TO_ARCHIVE,"
      "USER_PROVIDED_FILES AS USER_PROVIDED_FILES,"
      "USER_PROVIDED_BYTES AS USER_PROVIDED_BYTES,"
      "RETRIEVED_FILES AS RETRIEVED_FILES,"
      "RETRIEVED_BYTES AS RETRIEVED_BYTES,"
      "ARCHIVED_FILES AS ARCHIVED_FILES,"
      "ARCHIVED_BYTES AS ARCHIVED_BYTES,"
      "FAILED_TO_RETRIEVE_FILES AS FAILED_TO_RETRIEVE_FILES,"
      "FAILED_TO_RETRIEVE_BYTES AS FAILED_TO_RETRIEVE_BYTES,"
      "FAILED_TO_CREATE_ARCHIVE_REQ AS FAILED_TO_CREATE_ARCHIVE_REQ,"
      "FAILED_TO_ARCHIVE_FILES AS FAILED_TO_ARCHIVE_FILES,"
      "FAILED_TO_ARCHIVE_BYTES AS FAILED_TO_ARCHIVE_BYTES,"
      "LAST_EXPANDED_FSEQ AS LAST_EXPANDED_FSEQ,"
      "IS_EXPAND_FINISHED AS IS_EXPAND_FINISHED,"
      "IS_EXPAND_STARTED AS IS_EXPAND_STARTED,"
      "MOUNT_POLICY AS MOUNT_POLICY,"
      "IS_COMPLETE AS IS_COMPLETE,"
      "IS_NO_RECALL AS IS_NO_RECALL,"
      "SUBREQ_PB AS SUBREQ_PB,"
      "CREATE_USERNAME AS CREATE_USERNAME,"
      "CREATE_HOST AS CREATE_HOST,"
      "CREATE_TIME AS CREATE_TIME,"
      "REPACK_FINISHED_TIME AS REPACK_FINISHED_TIME "
    "FROM REPACK_JOB_QUEUE "
    "WHERE "
      "STATUS = :STATUS "
    "ORDER BY REPACK_REQID "
      "LIMIT :LIMIT";

    auto stmt = txn.conn().createStmt(sql);
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindUint32(":LIMIT", limit);

    return stmt.executeQuery();
  }

};

} // namespace cta::postgresscheddb::sql
