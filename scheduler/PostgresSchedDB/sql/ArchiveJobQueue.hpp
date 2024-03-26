/**
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright © 2021-2022 CERN
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
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/checksum/ChecksumBlob.hpp"
#include "scheduler/PostgresSchedDB/sql/Transaction.hpp"
#include "scheduler/PostgresSchedDB/sql/Enums.hpp"

#include <vector>
#include <any>

namespace cta::postgresscheddb::sql {

  struct ArchiveJobQueueRow {

  public:

    // Template member function to get the value of a column by index
    template<typename T>
    T getColumn(ArchColIdx column_index) const {
      return std::any_cast<T>(row[static_cast<size_t>(column_index)]);
    }

    // Template member function to set the value of a column by index
    template<typename T>
    void setColumn(ArchColIdx column_index, const T& value) {
      row[static_cast<size_t>(column_index)] = value;
    }

    const std::vector<std::any>& row;
    ArchiveJobStatus status = ArchiveJobStatus::AJS_ToTransferForUser;
    bool is_repack = false;
    common::dataStructures::ArchiveFile archiveFile;

  ArchiveJobQueueRow()
  {
       archiveFile.reconciliationTime = 0;
       archiveFile.archiveFileID = 0;
       archiveFile.fileSize = 0;
       archiveFile.diskFileInfo.owner_uid = 0;
       archiveFile.diskFileInfo.gid = 0;
       archiveFile.creationTime = 0;
  }
  // Constructor to initialize the row with column values
  ArchiveJobQueueRow(const std::vector<std::any>& data) : row(data) {}

  /**
   * Constructor from row
   *
   * @param row  A single row from the current row of the rset
   */
  explicit ArchiveJobQueueRow(const rdbms::Rset &rset) {
    *this = rset;
  }

  ArchiveJobQueueRow& operator=(const rdbms::Rset &rset) {
    for (const auto& key : ArchColIdx) {
      std::any value;
      try {
        value = rset.columnAny(TO_STRING(key));
        if (value.has_value()) {
          if (value.type() == typeid(int)) {
            int intValue = std::any_cast<int>(value);
            std::cout << "Retrieved int value: " << intValue << std::endl;
          } else if (value.type() == typeid(double)) {
            double doubleValue = std::any_cast<double>(value);
            std::cout << "Retrieved double value: " << doubleValue << std::endl;
          } else if (value.type() == typeid(std::string)) {
            std::string stringValue = std::any_cast<std::string>(value);
            std::cout << "Retrieved string value: " << stringValue << std::endl;
          } else {
            std::cout << "Retrieved value has an unsupported type" << std::endl;
          }
        } else {
          std::cout << "Retrieved value is null" << std::endl;
        }
      } catch (exception::Exception &ex)

      }
      row.setColumn(key, rset.columnAny());
    }

    jobId                     = rset.columnUint64("JOB_ID");
    mountId                   = rset.columnUint64("MOUNT_ID");
    status                    = from_string<ArchiveJobStatus>(
                                rset.columnString("STATUS") );
    tapePool                  = rset.columnString("TAPE_POOL");
    mountPolicy               = rset.columnString("MOUNT_POLICY");
    priority                  = rset.columnUint16("PRIORITY");
    minArchiveRequestAge      = rset.columnUint32("MIN_ARCHIVE_REQUEST_AGE");
    archiveFile.archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
    archiveFile.fileSize      = rset.columnUint64("SIZE_IN_BYTES");
    copyNb                    = rset.columnUint16("COPY_NB");
    startTime                 = rset.columnUint64("START_TIME");
    archiveFile.checksumBlob.deserialize(
                                rset.columnBlob("CHECKSUMBLOB") );
    archiveFile.creationTime  = rset.columnUint64("CREATION_TIME");
    archiveFile.diskInstance  = rset.columnString("DISK_INSTANCE");
    archiveFile.diskFileId    = rset.columnString("DISK_FILE_ID");
    archiveFile.diskFileInfo.owner_uid = rset.columnUint32("DISK_FILE_OWNER_UID");
    archiveFile.diskFileInfo.gid       = rset.columnUint32("DISK_FILE_GID");
    archiveFile.diskFileInfo.path      = rset.columnString("DISK_FILE_PATH");
    archiveReportUrl          = rset.columnString("ARCHIVE_REPORT_URL");
    archiveErrorReportUrl     = rset.columnString("ARCHIVE_ERROR_REPORT_URL");
    requesterName             = rset.columnString("REQUESTER_NAME");
    requesterGroup            = rset.columnString("REQUESTER_GROUP");
    srcUrl                    = rset.columnString("SRC_URL");
    archiveFile.storageClass  = rset.columnString("STORAGE_CLASS");
    retriesWithinMount        = rset.columnUint16("RETRIES_WITHIN_MOUNT");
    totalRetries              = rset.columnUint16("TOTAL_RETRIES");
    lastMountWithFailure      = rset.columnUint32("LAST_MOUNT_WITH_FAILURE");
    maxTotalRetries           = rset.columnUint16("MAX_TOTAL_RETRIES");
    return *this;
  }

  void insert(Transaction &txn) const {
    // does not set mountId or jobId
    const char *const sql =
      "INSERT INTO ARCHIVE_JOB_QUEUE("
        "STATUS,"
        "TAPE_POOL,"
        "MOUNT_POLICY,"
        "PRIORITY,"
        "MIN_ARCHIVE_REQUEST_AGE,"
        "ARCHIVE_FILE_ID,"
        "SIZE_IN_BYTES,"
        "COPY_NB,"
        "START_TIME,"
        "CHECKSUMBLOB,"
        "CREATION_TIME,"
        "DISK_INSTANCE,"
        "DISK_FILE_ID,"
        "DISK_FILE_OWNER_UID,"
        "DISK_FILE_GID,"
        "DISK_FILE_PATH,"
        "ARCHIVE_REPORT_URL,"
        "ARCHIVE_ERROR_REPORT_URL,"
        "REQUESTER_NAME,"
        "REQUESTER_GROUP,"
        "SRC_URL,"
        "STORAGE_CLASS,"
        "RETRIES_WITHIN_MOUNT,"
        "TOTAL_RETRIES,"
        "LAST_MOUNT_WITH_FAILURE,"
        "MAX_TOTAL_RETRIES) VALUES ("
        ":STATUS,"
        ":TAPE_POOL,"
        ":MOUNT_POLICY,"
        ":PRIORITY,"
        ":MIN_ARCHIVE_REQUEST_AGE,"
        ":ARCHIVE_FILE_ID,"
        ":SIZE_IN_BYTES,"
        ":COPY_NB,"
        ":START_TIME,"
        ":CHECKSUMBLOB,"
        ":CREATION_TIME,"
        ":DISK_INSTANCE,"
        ":DISK_FILE_ID,"
        ":DISK_FILE_OWNER_UID,"
        ":DISK_FILE_GID,"
        ":DISK_FILE_PATH,"
        ":ARCHIVE_REPORT_URL,"
        ":ARCHIVE_ERROR_REPORT_URL,"
        ":REQUESTER_NAME,"
        ":REQUESTER_GROUP,"
        ":SRC_URL,"
        ":STORAGE_CLASS,"
        ":RETRIES_WITHIN_MOUNT,"
        ":TOTAL_RETRIES,"
        ":LAST_MOUNT_WITH_FAILURE,"
        ":MAX_TOTAL_RETRIES)";

    auto stmt = txn.conn().createStmt(sql);
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindString(":TAPE_POOL", tapePool);
    stmt.bindString(":MOUNT_POLICY", mountPolicy);
    stmt.bindUint16(":PRIORITY", priority);
    stmt.bindUint32(":MIN_ARCHIVE_REQUEST_AGE", minArchiveRequestAge);
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFile.archiveFileID);
    stmt.bindUint64(":SIZE_IN_BYTES", archiveFile.fileSize);
    stmt.bindUint16(":COPY_NB", copyNb);
    stmt.bindUint64(":START_TIME", startTime);
    stmt.bindBlob(":CHECKSUMBLOB", archiveFile.checksumBlob.serialize());
    stmt.bindUint64(":CREATION_TIME", archiveFile.creationTime);
    stmt.bindString(":DISK_INSTANCE", archiveFile.diskInstance);
    stmt.bindString(":DISK_FILE_ID", archiveFile.diskFileId);
    stmt.bindUint32(":DISK_FILE_OWNER_UID", archiveFile.diskFileInfo.owner_uid);
    stmt.bindUint32(":DISK_FILE_GID", archiveFile.diskFileInfo.gid);
    stmt.bindString(":DISK_FILE_PATH", archiveFile.diskFileInfo.path);
    stmt.bindString(":ARCHIVE_REPORT_URL", archiveReportUrl);
    stmt.bindString(":ARCHIVE_ERROR_REPORT_URL", archiveErrorReportUrl);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.bindString(":REQUESTER_GROUP", requesterGroup);
    stmt.bindString(":SRC_URL", srcUrl);
    stmt.bindString(":STORAGE_CLASS", archiveFile.storageClass);
    stmt.bindUint16(":RETRIES_WITHIN_MOUNT", retriesWithinMount);
    stmt.bindUint16(":TOTAL_RETRIES", totalRetries);
    stmt.bindUint32(":LAST_MOUNT_WITH_FAILURE", lastMountWithFailure);
    stmt.bindUint16(":MAX_TOTAL_RETRIES", maxTotalRetries);

    stmt.executeNonQuery();
  }

  void addParamsToLogContext(log::ScopedParamContainer& params) const {
    // does not set jobId
    params.add("mountId", mountId);
    params.add("status", to_string(status));
    params.add("tapePool", tapePool);
    params.add("mountPolicy", mountPolicy);
    params.add("priority", priority);
    params.add("minArchiveRequestAge", minArchiveRequestAge);
    params.add("archiveFileId", archiveFile.archiveFileID);
    params.add("sizeInBytes", archiveFile.fileSize);
    params.add("copyNb", copyNb);
    params.add("startTime", startTime);
    params.add("checksumBlob", archiveFile.checksumBlob);
    params.add("creationTime", archiveFile.creationTime);
    params.add("diskInstance", archiveFile.diskInstance);
    params.add("diskFileId", archiveFile.diskFileId);
    params.add("diskFileOwnerUid", archiveFile.diskFileInfo.owner_uid);
    params.add("diskFileGid", archiveFile.diskFileInfo.gid);
    params.add("diskFilePath", archiveFile.diskFileInfo.path);
    params.add("archiveReportUrl", archiveReportUrl);
    params.add("archiveErrorReportUrl", archiveErrorReportUrl);
    params.add("requesterName", requesterName);
    params.add("requesterGroup", requesterGroup);
    params.add("srcUrl", srcUrl);
    params.add("storageClass", archiveFile.storageClass);
    params.add("retriesWithinMount", retriesWithinMount);
    params.add("totalRetries", totalRetries);
    params.add("lastMountWithFailure", lastMountWithFailure);
    params.add("maxTotalRetries", maxTotalRetries);
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
  static rdbms::Rset select(Transaction &txn, ArchiveJobStatus status, const std::string& tapepool, uint64_t limit) {

    const char *const sql =
    "SELECT "
      "JOB_ID AS JOB_ID,"
      "MOUNT_ID AS MOUNT_ID,"
      "STATUS AS STATUS,"
      "TAPE_POOL AS TAPE_POOL,"
      "MOUNT_POLICY AS MOUNT_POLICY,"
      "PRIORITY AS PRIORITY,"
      "MIN_ARCHIVE_REQUEST_AGE AS MIN_ARCHIVE_REQUEST_AGE,"
      "ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
      "SIZE_IN_BYTES AS SIZE_IN_BYTES,"
      "COPY_NB AS COPY_NB,"
      "START_TIME AS START_TIME,"
      "CHECKSUMBLOB AS CHECKSUMBLOB,"
      "CREATION_TIME AS CREATION_TIME,"
      "DISK_INSTANCE AS DISK_INSTANCE,"
      "DISK_FILE_ID AS DISK_FILE_ID,"
      "DISK_FILE_OWNER_UID AS DISK_FILE_OWNER_UID,"
      "DISK_FILE_GID AS DISK_FILE_GID,"
      "DISK_FILE_PATH AS DISK_FILE_PATH,"
      "ARCHIVE_REPORT_URL AS ARCHIVE_REPORT_URL,"
      "ARCHIVE_ERROR_REPORT_URL AS ARCHIVE_ERROR_REPORT_URL,"
      "REQUESTER_NAME AS REQUESTER_NAME,"
      "REQUESTER_GROUP AS REQUESTER_GROUP,"
      "SRC_URL AS SRC_URL,"
      "STORAGE_CLASS AS STORAGE_CLASS,"
      "RETRIES_WITHIN_MOUNT AS RETRIES_WITHIN_MOUNT,"
      "TOTAL_RETRIES AS TOTAL_RETRIES,"
      "LAST_MOUNT_WITH_FAILURE AS LAST_MOUNT_WITH_FAILURE,"
      "MAX_TOTAL_RETRIES AS MAX_TOTAL_RETRIES "
    "FROM ARCHIVE_JOB_QUEUE "
    "WHERE "
      "TAPE_POOL = :TAPE_POOL "
      "AND STATUS = :STATUS "
      "AND MOUNT_ID IS NULL "
    "ORDER BY PRIORITY DESC, JOB_ID "
      "LIMIT :LIMIT";

    auto stmt = txn.conn().createStmt(sql);
    stmt.bindString(":TAPE_POOL", tapepool);
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindUint64(":LIMIT", limit);

    return stmt.executeQuery();
  }

  /**
   * Select owned jobs from the queue
   *
   * @param txn        Transaction to use for this query
   * @param status     Archive Job Status to select on
   * @param tapepool   Tapepool to select on
   * @param mount_id   Mount id which owns this job
   * @param limit      Maximum number of rows to return
   *
   * @return  result set
   */
  static rdbms::Rset select(Transaction &txn, ArchiveJobStatus status, const std::string& tapepool, uint64_t limit, uint64_t mount_id) {
    const char *const sql =
    "SELECT "
      "JOB_ID AS JOB_ID,"
      "MOUNT_ID AS MOUNT_ID,"
      "STATUS AS STATUS,"
      "TAPE_POOL AS TAPE_POOL,"
      "MOUNT_POLICY AS MOUNT_POLICY,"
      "PRIORITY AS PRIORITY,"
      "MIN_ARCHIVE_REQUEST_AGE AS MIN_ARCHIVE_REQUEST_AGE,"
      "ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
      "SIZE_IN_BYTES AS SIZE_IN_BYTES,"
      "COPY_NB AS COPY_NB,"
      "START_TIME AS START_TIME,"
      "CHECKSUMBLOB AS CHECKSUMBLOB,"
      "CREATION_TIME AS CREATION_TIME,"
      "DISK_INSTANCE AS DISK_INSTANCE,"
      "DISK_FILE_ID AS DISK_FILE_ID,"
      "DISK_FILE_OWNER_UID AS DISK_FILE_OWNER_UID,"
      "DISK_FILE_GID AS DISK_FILE_GID,"
      "DISK_FILE_PATH AS DISK_FILE_PATH,"
      "ARCHIVE_REPORT_URL AS ARCHIVE_REPORT_URL,"
      "ARCHIVE_ERROR_REPORT_URL AS ARCHIVE_ERROR_REPORT_URL,"
      "REQUESTER_NAME AS REQUESTER_NAME,"
      "REQUESTER_GROUP AS REQUESTER_GROUP,"
      "SRC_URL AS SRC_URL,"
      "STORAGE_CLASS AS STORAGE_CLASS,"
      "RETRIES_WITHIN_MOUNT AS RETRIES_WITHIN_MOUNT,"
      "TOTAL_RETRIES AS TOTAL_RETRIES,"
      "LAST_MOUNT_WITH_FAILURE AS LAST_MOUNT_WITH_FAILURE,"
      "MAX_TOTAL_RETRIES AS MAX_TOTAL_RETRIES "
    "FROM ARCHIVE_JOB_QUEUE "
    "WHERE "
      "TAPE_POOL = :TAPE_POOL "
      "AND STATUS = :STATUS "
      "AND MOUNT_ID = :MOUNT_ID "
    "ORDER BY PRIORITY DESC, JOB_ID "
      "LIMIT :LIMIT";

    auto stmt = txn.conn().createStmt(sql);
    stmt.bindString(":TAPE_POOL", tapepool);
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindUint64(":MOUNT_ID", mount_id);
    stmt.bindUint32(":LIMIT", limit);

    return stmt.executeQuery();
  }

  /**
   * Select not owned jobs from the queue ordered by tapepool
   *
   * @param txn        Transaction to use for this query
   * @param status     Archive Job Status to select on
   * @param limit      Maximum number of rows to return
   *
   * @return  result set
   */
  static rdbms::Rset select(Transaction &txn, ArchiveJobStatus status, uint64_t limit) {
    const char *const sql =
            "SELECT "
            "JOB_ID AS JOB_ID,"
            "MOUNT_ID AS MOUNT_ID,"
            "STATUS AS STATUS,"
            "TAPE_POOL AS TAPE_POOL,"
            "MOUNT_POLICY AS MOUNT_POLICY,"
            "PRIORITY AS PRIORITY,"
            "MIN_ARCHIVE_REQUEST_AGE AS MIN_ARCHIVE_REQUEST_AGE,"
            "ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
            "SIZE_IN_BYTES AS SIZE_IN_BYTES,"
            "COPY_NB AS COPY_NB,"
            "START_TIME AS START_TIME,"
            "CHECKSUMBLOB AS CHECKSUMBLOB,"
            "CREATION_TIME AS CREATION_TIME,"
            "DISK_INSTANCE AS DISK_INSTANCE,"
            "DISK_FILE_ID AS DISK_FILE_ID,"
            "DISK_FILE_OWNER_UID AS DISK_FILE_OWNER_UID,"
            "DISK_FILE_GID AS DISK_FILE_GID,"
            "DISK_FILE_PATH AS DISK_FILE_PATH,"
            "ARCHIVE_REPORT_URL AS ARCHIVE_REPORT_URL,"
            "ARCHIVE_ERROR_REPORT_URL AS ARCHIVE_ERROR_REPORT_URL,"
            "REQUESTER_NAME AS REQUESTER_NAME,"
            "REQUESTER_GROUP AS REQUESTER_GROUP,"
            "SRC_URL AS SRC_URL,"
            "STORAGE_CLASS AS STORAGE_CLASS,"
            "RETRIES_WITHIN_MOUNT AS RETRIES_WITHIN_MOUNT,"
            "TOTAL_RETRIES AS TOTAL_RETRIES,"
            "LAST_MOUNT_WITH_FAILURE AS LAST_MOUNT_WITH_FAILURE,"
            "MAX_TOTAL_RETRIES AS MAX_TOTAL_RETRIES "
            "FROM ARCHIVE_JOB_QUEUE "
            "WHERE MOUNT_ID IS NULL "
            "AND STATUS = :STATUS "
            "ORDER BY PRIORITY DESC, TAPE_POOL "
            "LIMIT :LIMIT";

    auto stmt = txn.conn().createStmt(sql);
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindUint32(":LIMIT", limit);

    return stmt.executeQuery();
  }

  /**
   * Assign a mount ID to the specified rows
   *
   * @param txn        Transaction to use for this query
   * @param rowList    List of table rows to claim for the new owner
   * @param mountId    Mount ID to assign
   */
  static void updateMountId(Transaction &txn, const std::list<ArchiveJobQueueRow>& rowList, uint64_t mountId);
};

} // namespace cta::postgresscheddb::sql
