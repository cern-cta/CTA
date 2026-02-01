/**
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/checksum/ChecksumBlob.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/log/LogContext.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/NullDbValue.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/rdbms/postgres/Enums.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"

#include <stdexcept>
#include <vector>

namespace cta::schedulerdb::postgres {

struct ArchiveQueueJob {
  int copyNb;
  ArchiveJobStatus status;
  std::string tapepool;
  int totalRetries;
  int retriesWithinMount;
  int lastMountWithFailure;
  int maxRetriesWithinMount;
  int maxTotalRetries;
  int totalReportRetries;
  int maxReportRetries;
  int archiveRequestId;
};

struct ArchiveJobQueueRow {
  uint64_t jobId = 0;
  uint64_t reqId = 0;
  uint64_t repackRequestId = 0;
  uint32_t reqJobCount = 1;
  std::optional<std::uint64_t> mountId = std::nullopt;
  ArchiveJobStatus status = ArchiveJobStatus::AJS_ToTransferForUser;
  std::string tapePool = "";
  std::string mountPolicy = "";
  uint32_t priority = 0;
  uint64_t minArchiveRequestAge = 0;
  uint8_t copyNb = 0;
  time_t startTime = 0;  //!< Time the job was inserted into the queue
  time_t creationTime = 0;
  std::string archiveReportURL = "";
  std::string archiveErrorReportURL = "";
  std::string requesterName = "";
  std::string requesterGroup = "";
  std::string srcUrl = "";
  uint32_t retriesWithinMount = 0;
  uint32_t totalRetries = 0;
  uint64_t lastMountWithFailure = 0;
  uint32_t maxTotalRetries = 0;
  uint32_t maxRetriesWithinMount = 0;
  uint32_t maxReportRetries = 0;
  uint32_t totalReportRetries = 0;
  std::optional<std::string> failureLogs = std::nullopt;
  std::optional<std::string> reportFailureLogs = std::nullopt;
  bool is_repack = false;
  bool isReporting = false;
  uint64_t repackId = 0;
  std::string repackFilebufUrl = "";
  uint64_t repackFseq = 0;
  std::string repackDestVid = "";
  std::string vid = "";
  std::string drive = "";
  std::string host = "";
  std::string mount_type = "";
  std::string logical_library = "";
  uint64_t archiveFileID = 0;
  std::string diskFileId = "";
  std::string diskInstance = "";
  uint64_t fileSize = 0;
  std::string storageClass = "";
  std::string diskFileInfoPath = "";
  uint32_t diskFileInfoOwnerUid = 0;
  uint32_t diskFileInfoGid = 0;
  checksum::ChecksumBlob checksumBlob;

private:
  void reserveStringFields() {
    tapePool.reserve(64);
    mountPolicy.reserve(64);
    archiveReportURL.reserve(2048);
    archiveErrorReportURL.reserve(2048);
    requesterName.reserve(64);
    requesterGroup.reserve(64);
    srcUrl.reserve(2048);
    repackFilebufUrl.reserve(2048);
    repackDestVid.reserve(64);
    vid.reserve(64);
    drive.reserve(64);
    host.reserve(64);
    mount_type.reserve(64);
    logical_library.reserve(64);
    diskFileId.reserve(128);
    diskInstance.reserve(128);
    storageClass.reserve(128);
    diskFileInfoPath.reserve(2048);
  }

public:
  ArchiveJobQueueRow() { reserveStringFields(); }

  /**
     * Constructor from row
     *
     * @param row  A single row from the current row of the rset
     */
  explicit ArchiveJobQueueRow(const rdbms::Rset& rset) {
    reserveStringFields();
    *this = rset;
  }

  // Reset function
  void reset() {
    jobId = 0;
    reqId = 0;
    repackRequestId = 0;
    reqJobCount = 1;
    mountId.reset();  // Resetting optional value
    status = ArchiveJobStatus::AJS_ToTransferForUser;
    tapePool.clear();
    mountPolicy.clear();
    priority = 0;
    minArchiveRequestAge = 0;
    copyNb = 0;
    startTime = 0;
    archiveReportURL.clear();
    archiveErrorReportURL.clear();
    requesterName.clear();
    requesterGroup.clear();
    srcUrl.clear();
    retriesWithinMount = 0;
    totalRetries = 0;
    lastMountWithFailure = 0;
    maxTotalRetries = 0;
    maxRetriesWithinMount = 0;
    maxReportRetries = 0;
    totalReportRetries = 0;
    failureLogs.reset();        // Resetting optional value
    reportFailureLogs.reset();  // Resetting optional value
    is_repack = false;
    isReporting = false;
    repackId = 0;
    repackFilebufUrl.clear();
    repackFseq = 0;
    repackDestVid.clear();
    vid.clear();
    drive.clear();
    host.clear();
    mount_type.clear();
    logical_library.clear();
    archiveFileID = 0;
    creationTime = 0;
    diskFileId.clear();
    diskInstance.clear();
    fileSize = 0;
    storageClass.clear();
    diskFileInfoPath.clear();
    diskFileInfoOwnerUid = 0;
    diskFileInfoGid = 0;
    checksumBlob.clear();
  }

  ArchiveJobQueueRow& operator=(const rdbms::Rset& rset) {
    jobId = rset.columnUint64NoOpt("JOB_ID");
    if (rset.columnExists("REPACK_REQUEST_ID")) {
      repackRequestId = rset.columnUint64NoOpt("REPACK_REQUEST_ID");
    }
    reqId = rset.columnUint64NoOpt("ARCHIVE_REQUEST_ID");
    reqJobCount = rset.columnUint32NoOpt("REQUEST_JOB_COUNT");
    mountId = rset.columnOptionalUint64("MOUNT_ID");
    status = from_string<ArchiveJobStatus>(rset.columnStringNoOpt("STATUS"));
    tapePool = rset.columnStringNoOpt("TAPE_POOL");
    mountPolicy = rset.columnStringNoOpt("MOUNT_POLICY");
    priority = rset.columnUint16NoOpt("PRIORITY");
    minArchiveRequestAge = rset.columnUint32NoOpt("MIN_ARCHIVE_REQUEST_AGE");
    archiveFileID = rset.columnUint64NoOpt("ARCHIVE_FILE_ID");
    fileSize = rset.columnUint64NoOpt("SIZE_IN_BYTES");
    copyNb = rset.columnUint16NoOpt("COPY_NB");
    startTime = rset.columnUint64NoOpt("START_TIME");
    auto blob_view = rset.columnBlobView("CHECKSUMBLOB");
    checksumBlob.deserialize(blob_view->data(), blob_view->size());
    creationTime = rset.columnUint64NoOpt("CREATION_TIME");
    diskInstance = rset.columnStringNoOpt("DISK_INSTANCE");
    diskFileId = rset.columnStringNoOpt("DISK_FILE_ID");
    diskFileInfoOwnerUid = rset.columnUint32NoOpt("DISK_FILE_OWNER_UID");
    diskFileInfoGid = rset.columnUint32NoOpt("DISK_FILE_GID");
    diskFileInfoPath = rset.columnStringNoOpt("DISK_FILE_PATH");
    archiveReportURL = rset.columnStringNoOpt("ARCHIVE_REPORT_URL");
    archiveErrorReportURL = rset.columnStringNoOpt("ARCHIVE_ERROR_REPORT_URL");
    requesterName = rset.columnStringNoOpt("REQUESTER_NAME");
    requesterGroup = rset.columnStringNoOpt("REQUESTER_GROUP");
    srcUrl = rset.columnStringNoOpt("SRC_URL");
    storageClass = rset.columnStringNoOpt("STORAGE_CLASS");
    isReporting = rset.columnBoolNoOpt("IS_REPORTING");
    vid = rset.columnStringNoOpt("VID");
    drive = rset.columnStringNoOpt("DRIVE");
    host = rset.columnStringNoOpt("HOST");
    mount_type = rset.columnStringNoOpt("MOUNT_TYPE");
    logical_library = rset.columnStringNoOpt("LOGICAL_LIBRARY");
    failureLogs = std::move(rset.columnOptionalString("FAILURE_LOG"));
    reportFailureLogs = std::move(rset.columnOptionalString("REPORT_FAILURE_LOG"));
    lastMountWithFailure = rset.columnUint32NoOpt("LAST_MOUNT_WITH_FAILURE");
    retriesWithinMount = rset.columnUint16NoOpt("RETRIES_WITHIN_MOUNT");
    maxRetriesWithinMount = rset.columnUint16NoOpt("MAX_RETRIES_WITHIN_MOUNT");
    totalRetries = rset.columnUint16NoOpt("TOTAL_RETRIES");
    maxReportRetries = rset.columnUint16NoOpt("MAX_REPORT_RETRIES");
    maxTotalRetries = rset.columnUint16NoOpt("MAX_TOTAL_RETRIES");
    totalReportRetries = rset.columnUint16NoOpt("TOTAL_REPORT_RETRIES");
    return *this;
  }

  void insert(rdbms::Conn& conn) const {
    // does not set mountId or jobId
    const char* const sql = R"SQL(
      INSERT INTO ARCHIVE_PENDING_QUEUE(
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
        MAX_REPORT_RETRIES)
    VALUES (
         nextval('archive_request_id_seq'),
        :REQUEST_JOB_COUNT,
        :STATUS,
        :TAPE_POOL,
        :MOUNT_POLICY,
        :PRIORITY,
        :MIN_ARCHIVE_REQUEST_AGE,
        :ARCHIVE_FILE_ID,
        :SIZE_IN_BYTES,
        :COPY_NB,
        :START_TIME,
        :CHECKSUMBLOB,
        :CREATION_TIME,
        :DISK_INSTANCE,
        :DISK_FILE_ID,
        :DISK_FILE_OWNER_UID,
        :DISK_FILE_GID,
        :DISK_FILE_PATH,
        :ARCHIVE_REPORT_URL,
        :ARCHIVE_ERROR_REPORT_URL,
        :REQUESTER_NAME,
        :REQUESTER_GROUP,
        :SRC_URL,
        :STORAGE_CLASS,
        :RETRIES_WITHIN_MOUNT,
        :MAX_RETRIES_WITHIN_MOUNT,
        :TOTAL_RETRIES::SMALLINT,
        :LAST_MOUNT_WITH_FAILURE,
        :MAX_TOTAL_RETRIES,
        :TOTAL_REPORT_RETRIES,
        :MAX_REPORT_RETRIES
     )
    )SQL";
    auto stmt = conn.createStmt(sql);
    //stmt.bindUint64(":ARCHIVE_REQUEST_ID", reqId);
    stmt.bindUint32(":REQUEST_JOB_COUNT", reqJobCount);
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindString(":TAPE_POOL", tapePool);
    stmt.bindString(":MOUNT_POLICY", mountPolicy);
    stmt.bindUint16(":PRIORITY", priority);
    stmt.bindUint32(":MIN_ARCHIVE_REQUEST_AGE", minArchiveRequestAge);
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileID);
    stmt.bindUint64(":SIZE_IN_BYTES", fileSize);
    stmt.bindUint16(":COPY_NB", copyNb);
    stmt.bindUint64(":START_TIME", startTime);
    stmt.bindBlob(":CHECKSUMBLOB", checksumBlob.serialize());
    stmt.bindUint64(":CREATION_TIME", creationTime);
    stmt.bindString(":DISK_INSTANCE", diskInstance);
    stmt.bindString(":DISK_FILE_ID", diskFileId);
    stmt.bindUint32(":DISK_FILE_OWNER_UID", diskFileInfoOwnerUid);
    stmt.bindUint32(":DISK_FILE_GID", diskFileInfoGid);
    stmt.bindString(":DISK_FILE_PATH", diskFileInfoPath);
    stmt.bindString(":ARCHIVE_REPORT_URL", archiveReportURL);
    stmt.bindString(":ARCHIVE_ERROR_REPORT_URL", archiveErrorReportURL);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.bindString(":REQUESTER_GROUP", requesterGroup);
    stmt.bindString(":SRC_URL", srcUrl);
    stmt.bindString(":STORAGE_CLASS", storageClass);
    stmt.bindUint16(":RETRIES_WITHIN_MOUNT", retriesWithinMount);
    stmt.bindUint16(":MAX_RETRIES_WITHIN_MOUNT", maxRetriesWithinMount);
    stmt.bindUint16(":TOTAL_RETRIES", totalRetries);
    stmt.bindUint32(":LAST_MOUNT_WITH_FAILURE", lastMountWithFailure);
    stmt.bindUint16(":MAX_TOTAL_RETRIES", maxTotalRetries);
    stmt.bindUint16(":TOTAL_REPORT_RETRIES", totalReportRetries);
    stmt.bindUint16(":MAX_REPORT_RETRIES", maxReportRetries);
    conn.setDbQuerySummary("insert archive");
    stmt.executeNonQuery();
  }

  static void
  insertBatch(rdbms::Conn& conn, const std::vector<std::unique_ptr<ArchiveJobQueueRow>>& rows, bool isRepack) {
    if (rows.empty()) {
      return;
    }

    std::string prefix = isRepack ? "REPACK_" : "";
    std::string sql = "INSERT INTO " + prefix + "ARCHIVE_PENDING_QUEUE ( ";
    if (isRepack) {
      sql += "REPACK_REQUEST_ID,";
    }
    sql += R"SQL(
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
      MAX_REPORT_RETRIES)
    SELECT
    )SQL";
    if (isRepack) {
      sql += "v.repack_request_id,";
    }
    sql += R"SQL(
       req.archive_request_id,
       v.request_job_count,
       v.status,
       v.tape_pool,
       v.mount_policy,
       v.priority,
       v.min_archive_request_age,
       v.archive_file_id,
       v.size_in_bytes,
       v.copy_nb,
       v.start_time,
       v.checksumblob,
       v.creation_time,
       v.disk_instance,
       v.disk_file_id,
       v.disk_file_owner_uid,
       v.disk_file_gid,
       v.disk_file_path,
       v.archive_report_url,
       v.archive_error_report_url,
       v.requester_name,
       v.requester_group,
       v.src_url,
       v.storage_class,
       v.retries_within_mount,
       v.max_retries_within_mount,
       v.total_retries,
       v.last_mount_with_failure,
       v.max_total_retries,
       v.total_report_retries,
       v.max_report_retries
  FROM
    (SELECT nextval('archive_request_id_seq') AS archive_request_id) req
  CROSS JOIN
    (
VALUES )SQL";
    // the cross join is required to get the archive request ID from the sequence and
    // for multi-copy inserts to have same archive request ID
    // Generate VALUES placeholders for each row
    for (size_t i = 0; i < rows.size(); ++i) {
      std::string idx = std::to_string(i);
      sql += "(";
      if (isRepack) {
        sql += ":REPACK_REQUEST_ID" + idx + "::BIGINT,";
      }
      sql += ":REQUEST_JOB_COUNT" + idx
             + "::SMALLINT,"
               ":STATUS"
             + idx
             + "::ARCHIVE_JOB_STATUS,"
               ":TAPE_POOL"
             + idx
             + "::VARCHAR,"
               ":MOUNT_POLICY"
             + idx
             + "::VARCHAR,"
               ":PRIORITY"
             + idx
             + "::SMALLINT,"
               ":MIN_ARCHIVE_REQUEST_AGE"
             + idx
             + "::INTEGER,"
               ":ARCHIVE_FILE_ID"
             + idx
             + "::BIGINT,"
               ":SIZE_IN_BYTES"
             + idx
             + "::BIGINT,"
               ":COPY_NB"
             + idx
             + "::NUMERIC,"
               ":START_TIME"
             + idx
             + "::BIGINT,"
               ":CHECKSUMBLOB"
             + idx
             + "::BYTEA,"
               ":CREATION_TIME"
             + idx
             + "::BIGINT,"
               ":DISK_INSTANCE"
             + idx
             + "::VARCHAR,"
               ":DISK_FILE_ID"
             + idx
             + "::VARCHAR,"
               ":DISK_FILE_OWNER_UID"
             + idx
             + "::INTEGER,"
               ":DISK_FILE_GID"
             + idx
             + "::INTEGER,"
               ":DISK_FILE_PATH"
             + idx
             + "::VARCHAR,"
               ":ARCHIVE_REPORT_URL"
             + idx
             + "::VARCHAR,"
               ":ARCHIVE_ERROR_REPORT_URL"
             + idx
             + "::VARCHAR,"
               ":REQUESTER_NAME"
             + idx
             + "::VARCHAR,"
               ":REQUESTER_GROUP"
             + idx
             + "::VARCHAR,"
               ":SRC_URL"
             + idx
             + "::VARCHAR,"
               ":STORAGE_CLASS"
             + idx
             + "::VARCHAR,"
               ":RETRIES_WITHIN_MOUNT"
             + idx
             + "::SMALLINT,"
               ":MAX_RETRIES_WITHIN_MOUNT"
             + idx
             + "::SMALLINT,"
               ":TOTAL_RETRIES"
             + idx
             + "::SMALLINT,"
               ":LAST_MOUNT_WITH_FAILURE"
             + idx
             + "::BIGINT,"
               ":MAX_TOTAL_RETRIES"
             + idx
             + "::SMALLINT,"
               ":TOTAL_REPORT_RETRIES"
             + idx
             + "::SMALLINT,"
               ":MAX_REPORT_RETRIES"
             + idx + "::SMALLINT)";
      if (i < rows.size() - 1) {
        sql += ",";
      }
    }
    sql += R"SQL(
       ) AS v (
    )SQL";
    if (isRepack) {
      sql += "repack_request_id,";
    }
    sql += R"SQL(
       request_job_count,
       status,
       tape_pool,
       mount_policy,
       priority,
       min_archive_request_age,
       archive_file_id,
       size_in_bytes,
       copy_nb,
       start_time,
       checksumblob,
       creation_time,
       disk_instance,
       disk_file_id,
       disk_file_owner_uid,
       disk_file_gid,
       disk_file_path,
       archive_report_url,
       archive_error_report_url,
       requester_name,
       requester_group,
       src_url,
       storage_class,
       retries_within_mount,
       max_retries_within_mount,
       total_retries,
       last_mount_with_failure,
       max_total_retries,
       total_report_retries,
       max_report_retries
     )

)SQL";

    auto stmt = conn.createStmt(sql);
    // Bind values for each row with distinct names
    for (size_t i = 0; i < rows.size(); ++i) {
      const auto& row = *rows[i];
      std::string idx = std::to_string(i);
      if (isRepack) {
        stmt.bindUint64(":REPACK_REQUEST_ID" + idx, row.repackRequestId);
      }
      //stmt.bindUint64(":ARCHIVE_REQUEST_ID" + idx, row.reqId);
      stmt.bindUint32(":REQUEST_JOB_COUNT" + idx, row.reqJobCount);
      stmt.bindString(":STATUS" + idx, to_string(row.status));
      stmt.bindString(":TAPE_POOL" + idx, row.tapePool);
      stmt.bindString(":MOUNT_POLICY" + idx, row.mountPolicy);
      stmt.bindUint16(":PRIORITY" + idx, row.priority);
      stmt.bindUint32(":MIN_ARCHIVE_REQUEST_AGE" + idx, row.minArchiveRequestAge);
      stmt.bindUint64(":ARCHIVE_FILE_ID" + idx, row.archiveFileID);
      stmt.bindUint64(":SIZE_IN_BYTES" + idx, row.fileSize);
      stmt.bindUint16(":COPY_NB" + idx, row.copyNb);
      stmt.bindUint64(":START_TIME" + idx, row.startTime);
      stmt.bindBlob(":CHECKSUMBLOB" + idx, row.checksumBlob.serialize());
      stmt.bindUint64(":CREATION_TIME" + idx, row.creationTime);
      stmt.bindString(":DISK_INSTANCE" + idx, row.diskInstance);
      stmt.bindString(":DISK_FILE_ID" + idx, row.diskFileId);
      stmt.bindUint32(":DISK_FILE_OWNER_UID" + idx, row.diskFileInfoOwnerUid);
      stmt.bindUint32(":DISK_FILE_GID" + idx, row.diskFileInfoGid);
      stmt.bindString(":DISK_FILE_PATH" + idx, row.diskFileInfoPath);
      stmt.bindString(":ARCHIVE_REPORT_URL" + idx, row.archiveReportURL);
      stmt.bindString(":ARCHIVE_ERROR_REPORT_URL" + idx, row.archiveErrorReportURL);
      stmt.bindString(":REQUESTER_NAME" + idx, row.requesterName);
      stmt.bindString(":REQUESTER_GROUP" + idx, row.requesterGroup);
      stmt.bindString(":SRC_URL" + idx, row.srcUrl);
      stmt.bindString(":STORAGE_CLASS" + idx, row.storageClass);
      stmt.bindUint16(":RETRIES_WITHIN_MOUNT" + idx, row.retriesWithinMount);
      stmt.bindUint16(":MAX_RETRIES_WITHIN_MOUNT" + idx, row.maxRetriesWithinMount);
      stmt.bindUint16(":TOTAL_RETRIES" + idx, row.totalRetries);
      stmt.bindUint32(":LAST_MOUNT_WITH_FAILURE" + idx, row.lastMountWithFailure);
      stmt.bindUint16(":MAX_TOTAL_RETRIES" + idx, row.maxTotalRetries);
      stmt.bindUint16(":TOTAL_REPORT_RETRIES" + idx, row.totalReportRetries);
      stmt.bindUint16(":MAX_REPORT_RETRIES" + idx, row.maxReportRetries);
    }
    conn.setDbQuerySummary("insert archive");
    stmt.executeNonQuery();
  }

  void addParamsToLogContext(log::ScopedParamContainer& params) const {
    // does not set jobId
    params.add("mountId", mountId.has_value() ? std::to_string(mountId.value()) : "no value");
    params.add("jobID", std::to_string(jobId));
    params.add("reqId", std::to_string(reqId));
    params.add("repackRequestId", std::to_string(repackRequestId));
    params.add("reqJobCount", std::to_string(reqJobCount));
    params.add("status", to_string(status));
    params.add("tapePool", tapePool);
    params.add("mountPolicy", mountPolicy);
    params.add("priority", priority);
    params.add("minArchiveRequestAge", minArchiveRequestAge);
    params.add("archiveFileId", archiveFileID);
    params.add("sizeInBytes", fileSize);
    params.add("copyNb", copyNb);
    params.add("startTime", startTime);
    params.add("checksumBlob", checksumBlob);
    params.add("creationTime", creationTime);
    params.add("diskInstance", diskInstance);
    params.add("diskFileId", diskFileId);
    params.add("diskFileOwnerUid", diskFileInfoOwnerUid);
    params.add("diskFileGid", diskFileInfoGid);
    params.add("diskFilePath", diskFileInfoPath);
    params.add("archiveReportURL", archiveReportURL);
    params.add("archiveErrorReportURL", archiveErrorReportURL);
    params.add("requesterName", requesterName);
    params.add("requesterGroup", requesterGroup);
    params.add("srcUrl", srcUrl);
    params.add("storageClass", storageClass);
    params.add("retriesWithinMount", retriesWithinMount);
    params.add("totalRetries", totalRetries);
    params.add("lastMountWithFailure", lastMountWithFailure);
    params.add("maxTotalRetries", maxTotalRetries);
    params.add("maxReportRetries", maxReportRetries);
  }

  // This is used for opportunistic batching during queueing user requests, repack requests are not handled by this method
  static uint64_t insertRequestBatch(rdbms::Conn& conn,
                                     const std::vector<std::unique_ptr<ArchiveJobQueueRow>>& rows,
                                     const std::vector<uint32_t>& groupIds) {
    if (rows.empty()) {
      return 0;
    }
    if (rows.size() != groupIds.size()) {
      throw std::logic_error("insertRequestBatch: groupIds size mismatch");
    }
    std::string sql = R"SQL(
    WITH input_rows (
      request_group_id,
      request_job_count,
      status,
      tape_pool,
      mount_policy,
      priority,
      min_archive_request_age,
      archive_file_id,
      size_in_bytes,
      copy_nb,
      start_time,
      checksumblob,
      creation_time,
      disk_instance,
      disk_file_id,
      disk_file_owner_uid,
      disk_file_gid,
      disk_file_path,
      archive_report_url,
      archive_error_report_url,
      requester_name,
      requester_group,
      src_url,
      storage_class,
      retries_within_mount,
      max_retries_within_mount,
      total_retries,
      last_mount_with_failure,
      max_total_retries,
      total_report_retries,
      max_report_retries
    ) AS (
      VALUES
    )SQL";
    for (size_t i = 0; i < rows.size(); ++i) {
      const std::string idx = std::to_string(i);
      sql += "("
             ":REQUEST_GROUP"
             + idx
             + "::INTEGER,"
               ":REQUEST_JOB_COUNT"
             + idx
             + "::SMALLINT,"
               ":STATUS"
             + idx
             + "::ARCHIVE_JOB_STATUS,"
               ":TAPE_POOL"
             + idx
             + "::VARCHAR,"
               ":MOUNT_POLICY"
             + idx
             + "::VARCHAR,"
               ":PRIORITY"
             + idx
             + "::SMALLINT,"
               ":MIN_ARCHIVE_REQUEST_AGE"
             + idx
             + "::INTEGER,"
               ":ARCHIVE_FILE_ID"
             + idx
             + "::BIGINT,"
               ":SIZE_IN_BYTES"
             + idx
             + "::BIGINT,"
               ":COPY_NB"
             + idx
             + "::SMALLINT,"
               ":START_TIME"
             + idx
             + "::BIGINT,"
               ":CHECKSUMBLOB"
             + idx
             + "::BYTEA,"
               ":CREATION_TIME"
             + idx
             + "::BIGINT,"
               ":DISK_INSTANCE"
             + idx
             + "::VARCHAR,"
               ":DISK_FILE_ID"
             + idx
             + "::VARCHAR,"
               ":DISK_FILE_OWNER_UID"
             + idx
             + "::INTEGER,"
               ":DISK_FILE_GID"
             + idx
             + "::INTEGER,"
               ":DISK_FILE_PATH"
             + idx
             + "::VARCHAR,"
               ":ARCHIVE_REPORT_URL"
             + idx
             + "::VARCHAR,"
               ":ARCHIVE_ERROR_REPORT_URL"
             + idx
             + "::VARCHAR,"
               ":REQUESTER_NAME"
             + idx
             + "::VARCHAR,"
               ":REQUESTER_GROUP"
             + idx
             + "::VARCHAR,"
               ":SRC_URL"
             + idx
             + "::VARCHAR,"
               ":STORAGE_CLASS"
             + idx
             + "::VARCHAR,"
               ":RETRIES_WITHIN_MOUNT"
             + idx
             + "::SMALLINT,"
               ":MAX_RETRIES_WITHIN_MOUNT"
             + idx
             + "::SMALLINT,"
               ":TOTAL_RETRIES"
             + idx
             + "::SMALLINT,"
               ":LAST_MOUNT_WITH_FAILURE"
             + idx
             + "::BIGINT,"
               ":MAX_TOTAL_RETRIES"
             + idx
             + "::SMALLINT,"
               ":TOTAL_REPORT_RETRIES"
             + idx
             + "::SMALLINT,"
               ":MAX_REPORT_RETRIES"
             + idx
             + "::SMALLINT"
               ")";
      if (i + 1 < rows.size()) {
        sql += ",";
      }
    }
    sql += R"SQL(
    ),
      request_ids AS (
        SELECT
          request_group_id,
        nextval('archive_request_id_seq') AS archive_request_id
      FROM (
        SELECT DISTINCT request_group_id FROM input_rows ORDER BY request_group_id ASC
           ) g
      )
    )SQL";
    sql += R"SQL( INSERT INTO ARCHIVE_PENDING_QUEUE (
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
      MAX_REPORT_RETRIES)
    SELECT
    )SQL";
    sql += R"SQL(
       req.archive_request_id,
       v.request_job_count,
       v.status,
       v.tape_pool,
       v.mount_policy,
       v.priority,
       v.min_archive_request_age,
       v.archive_file_id,
       v.size_in_bytes,
       v.copy_nb,
       v.start_time,
       v.checksumblob,
       v.creation_time,
       v.disk_instance,
       v.disk_file_id,
       v.disk_file_owner_uid,
       v.disk_file_gid,
       v.disk_file_path,
       v.archive_report_url,
       v.archive_error_report_url,
       v.requester_name,
       v.requester_group,
       v.src_url,
       v.storage_class,
       v.retries_within_mount,
       v.max_retries_within_mount,
       v.total_retries,
       v.last_mount_with_failure,
       v.max_total_retries,
       v.total_report_retries,
       v.max_report_retries
  FROM input_rows v JOIN request_ids req ON v.request_group_id = req.request_group_id
    )SQL";
    auto stmt = conn.createStmt(sql);
    // Bind values for each row with distinct names
    for (size_t i = 0; i < rows.size(); ++i) {
      const auto& row = *rows[i];
      std::string idx = std::to_string(i);
      //stmt.bindUint64(":ARCHIVE_REQUEST_ID" + idx, row.reqId);
      stmt.bindUint32(":REQUEST_GROUP" + idx, groupIds[i]);
      stmt.bindUint32(":REQUEST_JOB_COUNT" + idx, row.reqJobCount);
      stmt.bindString(":STATUS" + idx, to_string(row.status));
      stmt.bindString(":TAPE_POOL" + idx, row.tapePool);
      stmt.bindString(":MOUNT_POLICY" + idx, row.mountPolicy);
      stmt.bindUint16(":PRIORITY" + idx, row.priority);
      stmt.bindUint32(":MIN_ARCHIVE_REQUEST_AGE" + idx, row.minArchiveRequestAge);
      stmt.bindUint64(":ARCHIVE_FILE_ID" + idx, row.archiveFileID);
      stmt.bindUint64(":SIZE_IN_BYTES" + idx, row.fileSize);
      stmt.bindUint16(":COPY_NB" + idx, row.copyNb);
      stmt.bindUint64(":START_TIME" + idx, row.startTime);
      stmt.bindBlob(":CHECKSUMBLOB" + idx, row.checksumBlob.serialize());
      stmt.bindUint64(":CREATION_TIME" + idx, row.creationTime);
      stmt.bindString(":DISK_INSTANCE" + idx, row.diskInstance);
      stmt.bindString(":DISK_FILE_ID" + idx, row.diskFileId);
      stmt.bindUint32(":DISK_FILE_OWNER_UID" + idx, row.diskFileInfoOwnerUid);
      stmt.bindUint32(":DISK_FILE_GID" + idx, row.diskFileInfoGid);
      stmt.bindString(":DISK_FILE_PATH" + idx, row.diskFileInfoPath);
      stmt.bindString(":ARCHIVE_REPORT_URL" + idx, row.archiveReportURL);
      stmt.bindString(":ARCHIVE_ERROR_REPORT_URL" + idx, row.archiveErrorReportURL);
      stmt.bindString(":REQUESTER_NAME" + idx, row.requesterName);
      stmt.bindString(":REQUESTER_GROUP" + idx, row.requesterGroup);
      stmt.bindString(":SRC_URL" + idx, row.srcUrl);
      stmt.bindString(":STORAGE_CLASS" + idx, row.storageClass);
      stmt.bindUint16(":RETRIES_WITHIN_MOUNT" + idx, row.retriesWithinMount);
      stmt.bindUint16(":MAX_RETRIES_WITHIN_MOUNT" + idx, row.maxRetriesWithinMount);
      stmt.bindUint16(":TOTAL_RETRIES" + idx, row.totalRetries);
      stmt.bindUint32(":LAST_MOUNT_WITH_FAILURE" + idx, row.lastMountWithFailure);
      stmt.bindUint16(":MAX_TOTAL_RETRIES" + idx, row.maxTotalRetries);
      stmt.bindUint16(":TOTAL_REPORT_RETRIES" + idx, row.totalReportRetries);
      stmt.bindUint16(":MAX_REPORT_RETRIES" + idx, row.maxReportRetries);
    }
    conn.setDbQuerySummary("insert archive");
    stmt.executeNonQuery();
    return stmt.getNbAffectedRows();
  }

  /**
   * When CTA received the deleteArchive request from the disk buffer,
   * the following method ensures removal from the pending queue
   *
   * @param txn           Transaction handling the connection to the backend database
   * @param diskInstance  Name of the disk instance where the archive request was issued from
   * @param archiveFileID The archive file ID assigned originally
   *
   * @return  The number of affected jobs
   */
  static uint64_t cancelArchiveJob(Transaction& txn, const std::string& diskInstance, uint64_t archiveFileID);

  /**
     * Select any jobs with specified status(es) from the report,
     * flag them as being reported and return the job IDs
     *
     * @param txn       Transaction handlign the connection to the backend database
     * @param statusList List of Archive Job Status to select on
     * @param limit      Maximum number of rows to return
     *
     * @return  result set of job IDs
     */
  static rdbms::Rset
  flagReportingJobsByStatus(Transaction& txn, std::list<ArchiveJobStatus> statusList, uint64_t limit);

  /**
   * Assign a mount ID and VID to a selection of rows
   * which will be moved from the ARCHIVE_PENDING_QUEUE table
   * to the ARCHIVE_ACTIVE_QUEUE table in the DB
   *
   *
   * @param txn        Transaction to use for this query
   * @param newStatus  Archive Job Status to select on
   * @param mountInfo  mountInfo object
   * @param maxBytesRequested  the maximum cumulative size of the files in the bunch requested
   * @param limit      Maximum number of rows to return
   *
   * @return  result set containing job IDs of the rows which were updated
   */
  static std::pair<rdbms::Rset, uint64_t>
  moveJobsToDbActiveQueue(Transaction& txn,
                          ArchiveJobStatus newStatus,
                          const SchedulerDatabase::ArchiveMount::MountInfo& mountInfo,
                          uint64_t maxBytesRequested,
                          uint64_t limit,
                          bool isRepack);

  /**
   * Update job status for jobs from single-copy user request
   *
   * @param txn        Transaction to use for this query
   * @param newStatus  Archive Job Status to select on
   * @param jobIDs     List of jobID strings to select
   * @return           Number of updated rows
   */
  static uint64_t updateJobStatus(Transaction& txn, ArchiveJobStatus newStatus, const std::vector<std::string>& jobIDs);

  /**
   * Update job status for repack jobs to success
   *
   * @param txn        Transaction to use for this query
   * @param jobIDs     List of jobID strings to select
   * @return           Number of updated rows
   */
  static uint64_t updateRepackJobSuccess(Transaction& txn, const std::vector<std::string>& jobIDs);
  /**
   * Update successful job status for jobs from multi-copy user request
   *
   * @param txn                       Transaction to use for this query
   * @param jobIDs                    List of jobIDs
   * @return                          Number of updated rows
   */
  static uint64_t updateMultiCopyJobSuccess(Transaction& txn, const std::vector<std::string>& jobIDs);

  /**
   * Update failed job status
   *
   * @param txn                  Transaction to use for this query
   * @param newStatus            Archive Job Status to select on
   * @return                     Number of updated rows
   */
  uint64_t updateFailedJobStatus(Transaction& txn, bool isRepack);

  /**
   * Move a failed job from ARCHIVE_ACTIVE_QUEUE
   * to ARCHIVE_PENDING_QUEUE so that it can be picked up
   * by a different drive process again.
   * This method updates also the retry statistics
   *
   * @param txn                  Transaction to use for this query
   * @param newStatus            Archive Job Status to select on
   * @param keepMountId          true or false
   * @return                     Number of updated rows
   */
  uint64_t requeueFailedJob(Transaction& txn,
                            ArchiveJobStatus newStatus,
                            bool keepMountId,
                            bool isRepack,
                            std::optional<std::list<std::string>> jobIDs = std::nullopt);

  /**
   * Move from ARCHIVE_ACTIVE_QUEUE to ARCHIVE_PENDING_QUEUE
   * a batch of jobs so that they can be requeued to drive queues later
   * This methos is static and does not udate any retry statistics
   * It is used for batch of jobs not processed, returning from the task queue
   * (e.g. in case of a full tape)
   *
   * @param txn                  Transaction to use for this query
   * @param newStatus            Archive Job Status to select on
   * @param keepMountId          true or false
   * @return                     Number of updated rows
   */
  static uint64_t
  requeueJobBatch(Transaction& txn, ArchiveJobStatus newStatus, const std::list<std::string>& jobIDs, bool isRepack);

  /**
   * Update job status when job report failed
   *
   * @param txn                  Transaction to use for this query
   * @param newStatus            Archive Job Status to select on
   * @return                     Number of updated rows
   */
  uint64_t updateJobStatusForFailedReport(Transaction& txn, ArchiveJobStatus newStatus);

  /**
   * Move the job row to the ARCHIVE FAILED JOB TABLE
   *
   * @param txn                  Transaction to use for this query
   * @return nrows               The number of rows moved.
   */
  uint64_t moveJobToFailedQueueTable(Transaction& txn);

  /**
   * Move the job rows to the ARCHIVE FAILED JOB TABLE
   * (unused at the moment but shall be used for
   * batched failed reports to disk)
   *
   * @param txn                  Transaction to use for this query
   * @param jobIDs               List of job IDs as strings for the query
   * @return nrows               The number of rows moved.
   */
  static uint64_t moveJobBatchToFailedQueueTable(Transaction& txn, const std::vector<std::string>& jobIDs);

  static rdbms::Rset moveFailedRepackJobBatchToFailedQueueTable(Transaction& txn, uint64_t limit);
  /**
   * Increment Archive Request ID and return the new value
   *
   * @param conn  DB connection to use for this query
   *
   * @return     Archive Request ID
   */
  static uint64_t getNextArchiveRequestID(rdbms::Conn& conn);

  /**
   * Appends the provided failure reason, along with timestamp and hostname, to the job's failure log.
   *
   * @param reason        The textual explanation for the failure.
   * @param is_report_log If true report failure log will be appended instead of job failure log.
   */
  void updateJobRowFailureLog(const std::string& reason, bool is_report_log = false);

  /**
   * Updates the retry counters for the current mount and globally.
   * Increments the number of retries and updates the last failed mount accordingly.
   */
  void updateRetryCounts(uint64_t mountId);

  /**
   * @brief Updates or inserts the last fetch timestamp for a mount in the archive job queue.
   *
   * This function ensures that the MOUNT_QUEUE_LAST_FETCH table has the most recent
   * timestamp for a given mount and queue type (MOUNT_ID, QUEUE_TYPE) is a unique identifier in the table.
   * It inserts a new row if none exists or updates the LAST_UPDATE_TIME for an existing row,
   *  but only if the current stored timestamp is at least 5 seconds older than the new timestamp
   *  (write optimisation).
   *
   * The queue type is determined based on the `isRepack` and `isActive` flags:
   * - If `isRepack` is true: "REPACK_ARCHIVE"
   * - If `isRepack` is false: "ARCHIVE"
   * - In addition "_ACTIVE" is appended to queue type string
   * - if `isActive` is true, otherwise "_PENDING" is appended
   *
   * @param txn      Reference to the active database transaction.
   * @param mountId  ID of the mount whose last fetch timestamp is being updated.
   * @param isActive Indicates if the active queue or pending queue types are supposed to be updated.
   * @param isRepack Indicates if the queue type is for repack or user archive.
   *
   * @return The number of rows affected by the insert or update operation.
   */
  static uint64_t updateMountQueueLastFetch(Transaction& txn, uint64_t mountId, bool isActive, bool isRepack);

  static rdbms::Rset getNextSuccessfulArchiveRepackReportBatch(Transaction& txn, const size_t limit);
  static rdbms::Rset deleteSuccessfulRepackArchiveJobBatch(Transaction& txn, std::vector<std::string>& jobIDs);
};
};  // namespace cta::schedulerdb::postgres
