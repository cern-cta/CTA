/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/checksum/ChecksumBlob.hpp"
#include "common/log/LogContext.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/NullDbValue.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/rdbms/postgres/Enums.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"

#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace cta::schedulerdb::postgres {

struct RetrieveJobQueueRow {
  uint64_t jobId = 0;
  uint64_t retrieveRequestId = 0;
  uint64_t repackRequestId = 0;
  uint32_t reqJobCount = 1;
  std::optional<std::uint64_t> mountId = std::nullopt;
  RetrieveJobStatus status = RetrieveJobStatus::RJS_ToTransfer;
  std::string tapePool = "";
  std::string mountPolicy = "";
  uint32_t priority = 0;
  uint32_t minRetrieveRequestAge = 0;
  uint8_t copyNb = 0;
  uint64_t fSeq = 0;
  uint64_t blockId = 0;
  std::string alternateFSeq = "";
  std::string alternateBlockId = "";
  std::string alternateCopyNbs = "";
  std::optional<std::string> rearchiveCopyNbs = std::nullopt;
  std::optional<std::string> rearchiveTapePools = std::nullopt;
  time_t startTime = 0;  //!< Time the job was inserted into the queue
  time_t creationTime = 0;
  std::string retrieveReportURL = "";
  std::string retrieveErrorReportURL = "";
  std::string requesterName = "";
  std::string requesterGroup = "";
  std::string dstURL = "";
  uint32_t retriesWithinMount = 0;
  uint32_t totalRetries = 0;
  uint64_t lastMountWithFailure = 0;
  uint32_t maxTotalRetries = 0;
  uint32_t maxRetriesWithinMount = 0;
  uint32_t maxReportRetries = 0;
  uint32_t totalReportRetries = 0;
  std::optional<std::string> failureLogs = std::nullopt;
  std::optional<std::string> reportFailureLogs = std::nullopt;
  bool isVerifyOnly = false;
  bool isReporting = false;
  std::string vid = "";
  std::string alternateVids = "";
  std::string drive = "";
  std::string host = "";
  std::string logical_library = "";
  std::optional<std::string> activity = std::nullopt;
  std::string srrUsername = "";
  std::string srrHost = "";
  uint64_t srrTime = 0;
  std::string srrMountPolicy = "";
  std::string srrActivity = "";
  uint64_t archiveFileID = 0;
  std::string diskFileId = "";
  std::string diskInstance = "";
  uint64_t fileSize = 0;
  std::string storageClass = "";
  std::string diskFileInfoPath = "";
  uint32_t diskFileInfoOwnerUid = 0;
  uint32_t diskFileInfoGid = 0;

  checksum::ChecksumBlob checksumBlob;

  time_t lifecycleTimings_creation_time = 0;
  time_t lifecycleTimings_first_selected_time = 0;
  time_t lifecycleTimings_completed_time = 0;
  std::optional<std::string> diskSystemName = std::nullopt;

private:
  void reserveStringFields() {
    diskFileId.reserve(128);
    diskInstance.reserve(128);
    storageClass.reserve(128);
    diskFileInfoPath.reserve(2048);
    tapePool.reserve(64);
    mountPolicy.reserve(64);
    retrieveReportURL.reserve(2048);
    retrieveErrorReportURL.reserve(2048);
    requesterName.reserve(64);
    requesterGroup.reserve(64);
    dstURL.reserve(2048);
    vid.reserve(64);
    drive.reserve(128);
    logical_library.reserve(128);
    host.reserve(64);
    srrUsername.reserve(128);
    srrHost.reserve(64);
    srrMountPolicy.reserve(64);
    srrActivity.reserve(64);
    alternateVids.reserve(128);
    alternateCopyNbs.reserve(20);
    alternateFSeq.reserve(256);
    alternateBlockId.reserve(256);
  }

public:
  RetrieveJobQueueRow() { reserveStringFields(); }

  /**
  * Constructor from row
  *
  * @param row  A single row from the current row of the rset
  */
  explicit RetrieveJobQueueRow(const rdbms::Rset& rset) {
    reserveStringFields();
    *this = rset;
  }

  void reset() {
    jobId = 0;
    retrieveRequestId = 0;
    repackRequestId = 0;
    reqJobCount = 1;
    mountId = std::nullopt;
    status = RetrieveJobStatus::RJS_ToTransfer;
    tapePool.clear();
    mountPolicy.clear();
    priority = 0;
    minRetrieveRequestAge = 0;
    copyNb = 0;
    fSeq = 0;
    blockId = 0;
    alternateCopyNbs.clear();
    rearchiveCopyNbs = std::nullopt;
    rearchiveTapePools = std::nullopt;
    alternateFSeq.clear();
    alternateBlockId.clear();
    startTime = 0;
    retrieveReportURL.clear();
    retrieveErrorReportURL.clear();
    requesterName.clear();
    requesterGroup.clear();
    dstURL.clear();
    retriesWithinMount = 0;
    totalRetries = 0;
    lastMountWithFailure = 0;
    maxTotalRetries = 0;
    maxRetriesWithinMount = 0;
    maxReportRetries = 0;
    totalReportRetries = 0;
    failureLogs = std::nullopt;
    reportFailureLogs = std::nullopt;
    isVerifyOnly = false;
    isReporting = false;
    vid.clear();
    alternateVids.clear();
    drive.clear();
    host.clear();
    logical_library.clear();
    srrUsername.clear();
    srrHost.clear();
    srrTime = 0;
    srrMountPolicy.clear();
    srrActivity.clear();
    diskSystemName = std::nullopt;
    activity = std::nullopt;
    archiveFileID = 0;
    diskFileId = "";
    diskInstance = "";
    fileSize = 0;
    storageClass = "";
    diskFileInfoPath = "";
    diskFileInfoOwnerUid = 0;
    diskFileInfoGid = 0;
    lifecycleTimings_creation_time = 0;
    lifecycleTimings_first_selected_time = 0;
    lifecycleTimings_completed_time = 0;
    checksumBlob.clear();
  }

  RetrieveJobQueueRow& operator=(const rdbms::Rset& rset) {
    jobId = rset.columnUint64NoOpt("JOB_ID");
    retrieveRequestId = rset.columnUint64NoOpt("RETRIEVE_REQUEST_ID");
    if (rset.columnExists("REPACK_REQUEST_ID")) {
      repackRequestId = rset.columnUint64NoOpt("REPACK_REQUEST_ID");
    }
    if (rset.columnExists("REPACK_REARCHIVE_COPY_NBS")) {
      rearchiveCopyNbs = rset.columnOptionalString("REPACK_REARCHIVE_COPY_NBS");
    }
    if (rset.columnExists("REPACK_REARCHIVE_TAPE_POOLS")) {
      rearchiveTapePools = rset.columnOptionalString("REPACK_REARCHIVE_TAPE_POOLS");
    }
    reqJobCount = rset.columnUint32NoOpt("REQUEST_JOB_COUNT");
    mountId = rset.columnOptionalUint64("MOUNT_ID");
    status = from_string<RetrieveJobStatus>(rset.columnStringNoOpt("STATUS"));
    tapePool = rset.columnStringNoOpt("TAPE_POOL");
    mountPolicy = rset.columnStringNoOpt("MOUNT_POLICY");
    priority = rset.columnUint32NoOpt("PRIORITY");
    minRetrieveRequestAge = rset.columnUint32NoOpt("MIN_RETRIEVE_REQUEST_AGE");
    copyNb = rset.columnUint8NoOpt("COPY_NB");
    fSeq = rset.columnUint64NoOpt("FSEQ");
    blockId = rset.columnUint64NoOpt("BLOCK_ID");
    alternateFSeq = rset.columnStringNoOpt("ALTERNATE_FSEQS");
    alternateBlockId = rset.columnStringNoOpt("ALTERNATE_BLOCK_IDS");
    startTime = rset.columnUint64NoOpt("START_TIME");
    retrieveReportURL = rset.columnStringNoOpt("RETRIEVE_REPORT_URL");
    // The following is necessary value translation as the current rdbms API
    // does not allow to bind and save an empty string [""], only nullptr or value are allowed.
    // The disk reporter factory requires at minimum empty string or a "null:" as a string,
    // since that is being used in unit tests
    // We therefore translate this to an empty string.
    // In addition, NULL is throwing an error in columnString() recall, so we can not use NULL either.
    // For this particular field we do need [""] as the start DiskReporterFactory::createDiskReporter() later depends on it.
    if (retrieveReportURL == "NOT_PROVIDED") {
      retrieveReportURL = "";
    }
    retrieveErrorReportURL = rset.columnStringNoOpt("RETRIEVE_ERROR_REPORT_URL");
    if (retrieveErrorReportURL == "NOT_PROVIDED") {
      retrieveErrorReportURL = "";
    }
    requesterName = rset.columnStringNoOpt("REQUESTER_NAME");
    requesterGroup = rset.columnStringNoOpt("REQUESTER_GROUP");
    dstURL = rset.columnStringNoOpt("DST_URL");
    retriesWithinMount = rset.columnUint32NoOpt("RETRIES_WITHIN_MOUNT");
    totalRetries = rset.columnUint32NoOpt("TOTAL_RETRIES");
    lastMountWithFailure = rset.columnUint64NoOpt("LAST_MOUNT_WITH_FAILURE");
    maxTotalRetries = rset.columnUint32NoOpt("MAX_TOTAL_RETRIES");
    maxRetriesWithinMount = rset.columnUint32NoOpt("MAX_RETRIES_WITHIN_MOUNT");
    maxReportRetries = rset.columnUint32NoOpt("MAX_REPORT_RETRIES");
    totalReportRetries = rset.columnUint32NoOpt("TOTAL_REPORT_RETRIES");
    failureLogs = rset.columnOptionalString("FAILURE_LOG");
    reportFailureLogs = rset.columnOptionalString("REPORT_FAILURE_LOG");
    isVerifyOnly = rset.columnBoolNoOpt("IS_VERIFY_ONLY");
    isReporting = rset.columnBoolNoOpt("IS_REPORTING");
    vid = rset.columnStringNoOpt("VID");
    alternateVids = rset.columnStringNoOpt("ALTERNATE_VIDS");
    alternateCopyNbs = rset.columnStringNoOpt("ALTERNATE_COPY_NBS");
    drive = rset.columnStringNoOpt("DRIVE");
    host = rset.columnStringNoOpt("HOST");
    logical_library = rset.columnStringNoOpt("LOGICAL_LIBRARY");
    activity = rset.columnOptionalString("ACTIVITY");
    srrUsername = rset.columnStringNoOpt("SRR_USERNAME");
    srrHost = rset.columnStringNoOpt("SRR_HOST");
    srrTime = rset.columnUint64NoOpt("SRR_TIME");
    srrMountPolicy = rset.columnStringNoOpt("SRR_MOUNT_POLICY");
    srrActivity = rset.columnStringNoOpt("SRR_ACTIVITY");

    lifecycleTimings_creation_time = rset.columnUint64NoOpt("LIFECYCLE_CREATION_TIME");
    lifecycleTimings_first_selected_time = rset.columnUint64NoOpt("LIFECYCLE_FIRST_SELECTED_TIME");
    lifecycleTimings_completed_time = rset.columnUint64NoOpt("LIFECYCLE_COMPLETED_TIME");

    diskSystemName = rset.columnOptionalString("DISK_SYSTEM_NAME");

    archiveFileID = rset.columnUint64NoOpt("ARCHIVE_FILE_ID");
    fileSize = rset.columnUint64NoOpt("SIZE_IN_BYTES");
    auto blob_view = rset.columnBlobView("CHECKSUMBLOB");
    checksumBlob.deserialize(blob_view->data(), blob_view->size());
    creationTime = rset.columnUint64NoOpt("CREATION_TIME");
    diskInstance = rset.columnStringNoOpt("DISK_INSTANCE");
    diskFileId = rset.columnStringNoOpt("DISK_FILE_ID");
    diskFileInfoOwnerUid = rset.columnUint32NoOpt("DISK_FILE_OWNER_UID");
    diskFileInfoGid = rset.columnUint32NoOpt("DISK_FILE_GID");
    diskFileInfoPath = rset.columnStringNoOpt("DISK_FILE_PATH");
    storageClass = rset.columnStringNoOpt("STORAGE_CLASS");

    fSeq = rset.columnUint64NoOpt("FSEQ");
    blockId = rset.columnUint64NoOpt("BLOCK_ID");
    alternateFSeq = rset.columnStringNoOpt("ALTERNATE_FSEQS");
    alternateBlockId = rset.columnStringNoOpt("ALTERNATE_BLOCK_IDS");

    return *this;
  }

  void insert(rdbms::Conn& conn) const {
    // used only for queueRetrieve(), repack uses insertBatch()
    std::string sql = R"(INSERT INTO
       RETRIEVE_PENDING_QUEUE (
           RETRIEVE_REQUEST_ID,
           REQUEST_JOB_COUNT,
           STATUS,
           CREATION_TIME,
           STORAGE_CLASS,
           SIZE_IN_BYTES,
           ARCHIVE_FILE_ID,
           CHECKSUMBLOB,
           FSEQ,
           BLOCK_ID,
           DISK_INSTANCE,
           DISK_FILE_PATH,
           DISK_FILE_ID,
           DISK_FILE_GID,
           DISK_FILE_OWNER_UID,
           MOUNT_POLICY,
           VID,
           ALTERNATE_VIDS,
           PRIORITY,
           MIN_RETRIEVE_REQUEST_AGE,
           COPY_NB,
           ALTERNATE_COPY_NBS,
           ALTERNATE_FSEQS,
           ALTERNATE_BLOCK_IDS,
           START_TIME,
           RETRIEVE_ERROR_REPORT_URL,
           REQUESTER_NAME,
           REQUESTER_GROUP,
           DST_URL,
           RETRIES_WITHIN_MOUNT,
           TOTAL_RETRIES,
           LAST_MOUNT_WITH_FAILURE,
           MAX_TOTAL_RETRIES,
           MAX_RETRIES_WITHIN_MOUNT,
           MAX_REPORT_RETRIES,
           TOTAL_REPORT_RETRIES,
           IS_VERIFY_ONLY,
           IS_REPORTING,
           SRR_USERNAME,
           SRR_HOST,
           SRR_TIME,
           SRR_MOUNT_POLICY,
           LIFECYCLE_CREATION_TIME,
           LIFECYCLE_FIRST_SELECTED_TIME,
           LIFECYCLE_COMPLETED_TIME,
           RETRIEVE_REPORT_URL
   )";
    if (diskSystemName.has_value()) {
      sql += R"(,DISK_SYSTEM_NAME )";
    }
    if (activity.has_value()) {
      sql += R"(,ACTIVITY )";
    }
    if (!srrActivity.empty()) {
      sql += R"(,SRR_ACTIVITY )";
    }
    sql += R"(
       ) VALUES (
           :RETRIEVE_REQUEST_ID,
           :REQUEST_JOB_COUNT,
           :STATUS,
           :CREATION_TIME,
           :STORAGE_CLASS,
           :SIZE_IN_BYTES,
           :ARCHIVE_FILE_ID,
           :CHECKSUMBLOB,
           :FSEQ,
           :BLOCK_ID,
           :DISK_INSTANCE,
           :DISK_FILE_PATH,
           :DISK_FILE_ID,
           :DISK_FILE_GID,
           :DISK_FILE_OWNER_UID,
           :MOUNT_POLICY,
           :VID,
           :ALTERNATE_VIDS,
           :PRIORITY,
           :MIN_RETRIEVE_REQUEST_AGE,
           :COPY_NB,
           :ALTERNATE_COPY_NBS,
           :ALTERNATE_FSEQS,
           :ALTERNATE_BLOCK_IDS,
           :START_TIME,
           :RETRIEVE_ERROR_REPORT_URL,
           :REQUESTER_NAME,
           :REQUESTER_GROUP,
           :DST_URL,
           :RETRIES_WITHIN_MOUNT,
           :TOTAL_RETRIES,
           :LAST_MOUNT_WITH_FAILURE,
           :MAX_TOTAL_RETRIES,
           :MAX_RETRIES_WITHIN_MOUNT,
           :MAX_REPORT_RETRIES,
           :TOTAL_REPORT_RETRIES,
           :IS_VERIFY_ONLY,
           :IS_REPORTING,
           :SRR_USERNAME,
           :SRR_HOST,
           :SRR_TIME,
           :SRR_MOUNT_POLICY,
           :LIFECYCLE_CREATION_TIME,
           :LIFECYCLE_FIRST_SELECTED_TIME,
           :LIFECYCLE_COMPLETED_TIME,
           :RETRIEVE_REPORT_URL
   )";
    if (diskSystemName.has_value()) {
      sql += R"(,:DISK_SYSTEM_NAME )";
    }
    if (activity.has_value()) {
      sql += R"(,:ACTIVITY )";
    }
    if (!srrActivity.empty()) {
      sql += R"(,:SRR_ACTIVITY )";
    }
    sql += R"(    ) )";

    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":RETRIEVE_REQUEST_ID", retrieveRequestId);
    stmt.bindUint32(":REQUEST_JOB_COUNT", reqJobCount);
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindUint64(":CREATION_TIME", creationTime);
    stmt.bindString(":STORAGE_CLASS", storageClass);
    stmt.bindUint64(":SIZE_IN_BYTES", fileSize);
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileID);
    stmt.bindBlob(":CHECKSUMBLOB", checksumBlob.serialize());
    stmt.bindUint64(":FSEQ", fSeq);
    stmt.bindUint64(":BLOCK_ID", blockId);
    stmt.bindString(":DISK_INSTANCE", diskInstance);
    stmt.bindString(":DISK_FILE_PATH", diskFileInfoPath);
    stmt.bindString(":DISK_FILE_ID", diskFileId);
    stmt.bindUint32(":DISK_FILE_GID", diskFileInfoGid);
    stmt.bindUint32(":DISK_FILE_OWNER_UID", diskFileInfoOwnerUid);
    stmt.bindString(":MOUNT_POLICY", mountPolicy);
    stmt.bindUint32(":PRIORITY", priority);
    stmt.bindUint32(":MIN_RETRIEVE_REQUEST_AGE", minRetrieveRequestAge);
    stmt.bindUint8(":COPY_NB", copyNb);
    stmt.bindString(":ALTERNATE_COPY_NBS", alternateCopyNbs);
    stmt.bindString(":ALTERNATE_FSEQS", alternateFSeq);
    stmt.bindString(":ALTERNATE_BLOCK_IDS", alternateBlockId);
    stmt.bindUint64(":START_TIME", startTime);
    if (!retrieveErrorReportURL.empty()) {
      stmt.bindString(":RETRIEVE_ERROR_REPORT_URL", retrieveErrorReportURL);
    } else {
      stmt.bindString(":RETRIEVE_ERROR_REPORT_URL", "NOT_PROVIDED");
    }
    // stmt.bindString(":RETRIEVE_ERROR_REPORT_URL", retrieveErrorReportURL); // this is empty for the tests and throws an exception
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.bindString(":REQUESTER_GROUP", requesterGroup);
    stmt.bindString(":DST_URL", dstURL);
    stmt.bindUint32(":RETRIES_WITHIN_MOUNT", retriesWithinMount);
    stmt.bindUint32(":TOTAL_RETRIES", totalRetries);
    stmt.bindUint64(":LAST_MOUNT_WITH_FAILURE", lastMountWithFailure);
    stmt.bindUint32(":MAX_TOTAL_RETRIES", maxTotalRetries);
    stmt.bindUint32(":MAX_RETRIES_WITHIN_MOUNT", maxRetriesWithinMount);
    stmt.bindUint32(":MAX_REPORT_RETRIES", maxReportRetries);
    stmt.bindUint32(":TOTAL_REPORT_RETRIES", totalReportRetries);
    stmt.bindBool(":IS_VERIFY_ONLY", isVerifyOnly);
    stmt.bindBool(":IS_REPORTING", isReporting);
    stmt.bindString(":VID", vid);
    stmt.bindString(":ALTERNATE_VIDS", alternateVids);
    stmt.bindString(":SRR_USERNAME", srrUsername); // this also seems to be empty and causing a problem
    stmt.bindString(":SRR_HOST", srrHost); // this would be a problem too I suppose
    stmt.bindUint64(":SRR_TIME", srrTime);
    stmt.bindString(":SRR_MOUNT_POLICY", srrMountPolicy);
    stmt.bindUint64(":LIFECYCLE_CREATION_TIME", lifecycleTimings_creation_time);
    stmt.bindUint64(":LIFECYCLE_FIRST_SELECTED_TIME", lifecycleTimings_first_selected_time);
    stmt.bindUint64(":LIFECYCLE_COMPLETED_TIME", lifecycleTimings_completed_time);
    if (diskSystemName.has_value()) {
      stmt.bindString(":DISK_SYSTEM_NAME", diskSystemName.value());
    }
    if (!retrieveReportURL.empty()) {
      stmt.bindString(":RETRIEVE_REPORT_URL", retrieveReportURL);
    } else {
      // Requires an empty string in order to still have a null reporter created by
      // DiskReporterFactory::createDiskReporter() later ! Since empty string is not
      // permitted in the current implementation of bindString and columnString, we pass a dummy value
      stmt.bindString(":RETRIEVE_REPORT_URL", "NOT_PROVIDED");
    }
    if (!retrieveErrorReportURL.empty()) {
      stmt.bindString(":RETRIEVE_ERROR_REPORT_URL", retrieveErrorReportURL);
    } else {
      stmt.bindString(":RETRIEVE_ERROR_REPORT_URL", "NOT_PROVIDED");
    }
    if (activity.has_value()) {
      stmt.bindString(":ACTIVITY", activity.value());
    }
    if (!srrActivity.empty()) {
      stmt.bindString(":SRR_ACTIVITY", srrActivity);
    }
    stmt.executeNonQuery();
  }

  void addParamsToLogContext(log::ScopedParamContainer& params) const {
    params.add("retrieveRequestId", retrieveRequestId);
    params.add("repackRequestId", repackRequestId);
    params.add("mountId", mountId);
    params.add("status", to_string(status));
    params.add("vid", vid);
    params.add("alternateVids", alternateVids);
    params.add("alternateCopyNbs", alternateCopyNbs);
    params.add("rearchiveCopyNbs", rearchiveCopyNbs);
    params.add("rearchiveTapePools", rearchiveTapePools);
    params.add("alternateFSeqs", alternateFSeq);
    params.add("alternateBlockIds", alternateBlockId);
    params.add("copyNb", copyNb);
    params.add("fSeq", fSeq);
    params.add("blockId", blockId);
    params.add("activity", activity.value_or(""));
    params.add("priority", priority);
    params.add("retMinReqAge", minRetrieveRequestAge);
    params.add("startTime", startTime);
    // retrieveRequest fields
    params.add("requester.name", requesterName);
    params.add("requester.group", requesterGroup);
    params.add("dstURL", dstURL);
    params.add("creationlog.ssername", srrUsername);
    params.add("creationlog.host", srrHost);
    params.add("creationlog.time", srrTime);
    params.add("retrieveReportURL", retrieveReportURL);
    params.add("retrieveErrorReportURL", retrieveErrorReportURL);
    params.add("isVerifyOnly", isVerifyOnly);
    params.add("retrieveRequest.mountPolicy", srrMountPolicy);
    params.add("retrieveRequest.activity", srrActivity);
    // archiveFile fields
    params.add("archiveFileID", archiveFileID);
    params.add("diskFileInfoOwnerUid", diskFileInfoOwnerUid);
    params.add("diskFileInfoGid", diskFileInfoGid);
    params.add("diskFileInfoPath", diskFileInfoPath);
    params.add("mountPolicyName", mountPolicy);
    params.add("fileSize", fileSize);
    params.add("diskFileId", diskFileId);
    params.add("diskInstance", diskInstance);
    params.add("checksumBlob", checksumBlob);
    params.add("creationTime", creationTime);
    params.add("storageClass", storageClass);

    params.add("retrieveErrorReportURL", retrieveErrorReportURL);
    params.add("failureLogs", failureLogs.value_or(""));

    /* Columns to be replaced by other DB columns than protobuf filled columns
     * params.add("retrieveJobsProtoBuf", retrieveJobsProtoBuf);
     * params.add("repackInfoProtoBuf", repackInfoProtoBuf);
     */
    params.add("lifecycleTimings.creation_time", lifecycleTimings_creation_time);
    params.add("lifecycleTimings.first_selected_time", lifecycleTimings_first_selected_time);
    params.add("lifecycleTimings.completed_time", lifecycleTimings_completed_time);
    params.add("diskSystemName", diskSystemName.value_or(""));
  }

  static void
  insertBatch(rdbms::Conn& conn, const std::vector<std::unique_ptr<RetrieveJobQueueRow>>& rows, bool isRepack) {
    if (rows.empty()) {
      return;
    }

    // detect optional columns
    bool hasDiskSystemName = false;
    bool hasActivity = false;
    bool hasSrrActivity = false;
    for (const auto& row : rows) {
      if (row->diskSystemName.has_value()) {
        hasDiskSystemName = true;
      }
      if (row->activity.has_value()) {
        hasActivity = true;
      }
      if (!row->srrActivity.empty()) {
        hasSrrActivity = true;
      }
    }

    std::string prefix = isRepack ? "REPACK_" : "";
    std::string sql = "INSERT INTO " + prefix + "RETRIEVE_PENDING_QUEUE ( ";
    if (isRepack) {
      sql += " REPACK_REQUEST_ID,";
      sql += " REPACK_REARCHIVE_COPY_NBS,";
      sql += " REPACK_REARCHIVE_TAPE_POOLS,";
    }
    sql += R"SQL(
      RETRIEVE_REQUEST_ID,
      REQUEST_JOB_COUNT,
      STATUS,
      CREATION_TIME,
      STORAGE_CLASS,
      SIZE_IN_BYTES,
      ARCHIVE_FILE_ID,
      CHECKSUMBLOB,
      FSEQ,
      BLOCK_ID,
      DISK_INSTANCE,
      DISK_FILE_PATH,
      RETRIEVE_ERROR_REPORT_URL,
      REQUESTER_NAME,
      REQUESTER_GROUP,
      SRR_USERNAME,
      SRR_HOST,
      SRR_TIME,
      SRR_MOUNT_POLICY,
      DISK_FILE_ID,
      DISK_FILE_GID,
      DISK_FILE_OWNER_UID,
      MOUNT_POLICY,
      VID,
      ALTERNATE_VIDS,
      PRIORITY,
      MIN_RETRIEVE_REQUEST_AGE,
      COPY_NB,
      ALTERNATE_COPY_NBS,
      ALTERNATE_FSEQS,
      ALTERNATE_BLOCK_IDS,
      START_TIME,
      DST_URL,
      RETRIES_WITHIN_MOUNT,
      TOTAL_RETRIES,
      LAST_MOUNT_WITH_FAILURE,
      MAX_TOTAL_RETRIES,
      MAX_RETRIES_WITHIN_MOUNT,
      MAX_REPORT_RETRIES,
      TOTAL_REPORT_RETRIES,
      IS_VERIFY_ONLY,
      IS_REPORTING,
      LIFECYCLE_CREATION_TIME,
      LIFECYCLE_FIRST_SELECTED_TIME,
      LIFECYCLE_COMPLETED_TIME,
      RETRIEVE_REPORT_URL)SQL";

    if (hasDiskSystemName) {
      sql += ",DISK_SYSTEM_NAME";
    }
    if (hasActivity) {
      sql += ",ACTIVITY";
    }
    if (hasSrrActivity) {
      sql += ",SRR_ACTIVITY";
    }

    sql += ") VALUES ";

    // Generate VALUES placeholders for each row
    for (size_t i = 0; i < rows.size(); ++i) {
      sql += "(";
      if (isRepack) {
        sql += ":REPACK_REQUEST_ID" + std::to_string(i) + ",";
        sql += ":REPACK_REARCHIVE_COPY_NBS" + std::to_string(i) + ",";
        sql += ":REPACK_REARCHIVE_TAPE_POOLS" + std::to_string(i) + ",";
      }
      sql += ":RETRIEVE_REQUEST_ID" + std::to_string(i) + ",";
      sql += ":REQUEST_JOB_COUNT" + std::to_string(i) + ",";
      sql += ":STATUS" + std::to_string(i) + ",";
      sql += ":CREATION_TIME" + std::to_string(i) + ",";
      sql += ":STORAGE_CLASS" + std::to_string(i) + ",";
      sql += ":SIZE_IN_BYTES" + std::to_string(i) + ",";
      sql += ":ARCHIVE_FILE_ID" + std::to_string(i) + ",";
      sql += ":CHECKSUMBLOB" + std::to_string(i) + ",";
      sql += ":FSEQ" + std::to_string(i) + ",";
      sql += ":BLOCK_ID" + std::to_string(i) + ",";
      sql += ":DISK_INSTANCE" + std::to_string(i) + ",";
      sql += ":DISK_FILE_PATH" + std::to_string(i) + ",";
      sql += ":RETRIEVE_ERROR_REPORT_URL" + std::to_string(i) + ",";
      sql += ":REQUESTER_NAME" + std::to_string(i) + ",";
      sql += ":REQUESTER_GROUP" + std::to_string(i) + ",";
      sql += ":SRR_USERNAME" + std::to_string(i) + ",";
      sql += ":SRR_HOST" + std::to_string(i) + ",";
      sql += ":SRR_TIME" + std::to_string(i) + ",";
      sql += ":SRR_MOUNT_POLICY" + std::to_string(i) + ",";
      sql += ":DISK_FILE_ID" + std::to_string(i) + ",";
      sql += ":DISK_FILE_GID" + std::to_string(i) + ",";
      sql += ":DISK_FILE_OWNER_UID" + std::to_string(i) + ",";
      sql += ":MOUNT_POLICY" + std::to_string(i) + ",";
      sql += ":VID" + std::to_string(i) + ",";
      sql += ":ALTERNATE_VIDS" + std::to_string(i) + ",";
      sql += ":PRIORITY" + std::to_string(i) + ",";
      sql += ":MIN_RETRIEVE_REQUEST_AGE" + std::to_string(i) + ",";
      sql += ":COPY_NB" + std::to_string(i) + ",";
      sql += ":ALTERNATE_COPY_NBS" + std::to_string(i) + ",";
      sql += ":ALTERNATE_FSEQS" + std::to_string(i) + ",";
      sql += ":ALTERNATE_BLOCK_IDS" + std::to_string(i) + ",";
      sql += ":START_TIME" + std::to_string(i) + ",";
      sql += ":DST_URL" + std::to_string(i) + ",";
      sql += ":RETRIES_WITHIN_MOUNT" + std::to_string(i) + ",";
      sql += ":TOTAL_RETRIES" + std::to_string(i) + ",";
      sql += ":LAST_MOUNT_WITH_FAILURE" + std::to_string(i) + ",";
      sql += ":MAX_TOTAL_RETRIES" + std::to_string(i) + ",";
      sql += ":MAX_RETRIES_WITHIN_MOUNT" + std::to_string(i) + ",";
      sql += ":MAX_REPORT_RETRIES" + std::to_string(i) + ",";
      sql += ":TOTAL_REPORT_RETRIES" + std::to_string(i) + ",";
      sql += ":IS_VERIFY_ONLY" + std::to_string(i) + ",";
      sql += ":IS_REPORTING" + std::to_string(i) + ",";
      sql += ":LIFECYCLE_CREATION_TIME" + std::to_string(i) + ",";
      sql += ":LIFECYCLE_FIRST_SELECTED_TIME" + std::to_string(i) + ",";
      sql += ":LIFECYCLE_COMPLETED_TIME" + std::to_string(i) + ",";
      sql += ":RETRIEVE_REPORT_URL" + std::to_string(i);
      if (hasDiskSystemName) {
        sql += ",:DISK_SYSTEM_NAME" + std::to_string(i);
      }
      if (hasActivity) {
        sql += ",:ACTIVITY" + std::to_string(i);
      }
      if (hasSrrActivity) {
        sql += ",:SRR_ACTIVITY" + std::to_string(i);
      }
      sql += ")";
      if (i < rows.size() - 1) {
        sql += ",";
      }
    }

    auto stmt = conn.createStmt(sql);

    // Bind values for each row with distinct names
    for (size_t i = 0; i < rows.size(); ++i) {
      const auto& row = *rows[i];
      if (isRepack) {
        stmt.bindUint64(":REPACK_REQUEST_ID" + std::to_string(i), row.repackRequestId);
        stmt.bindString(":REPACK_REARCHIVE_COPY_NBS" + std::to_string(i), row.rearchiveCopyNbs);
        stmt.bindString(":REPACK_REARCHIVE_TAPE_POOLS" + std::to_string(i), row.rearchiveTapePools);
      }
      stmt.bindUint64(":RETRIEVE_REQUEST_ID" + std::to_string(i), row.retrieveRequestId);
      stmt.bindUint32(":REQUEST_JOB_COUNT" + std::to_string(i), row.reqJobCount);
      stmt.bindString(":STATUS" + std::to_string(i), to_string(row.status));
      stmt.bindUint64(":CREATION_TIME" + std::to_string(i), row.creationTime);
      stmt.bindString(":STORAGE_CLASS" + std::to_string(i), row.storageClass);
      stmt.bindUint64(":SIZE_IN_BYTES" + std::to_string(i), row.fileSize);
      stmt.bindUint64(":ARCHIVE_FILE_ID" + std::to_string(i), row.archiveFileID);
      stmt.bindBlob(":CHECKSUMBLOB" + std::to_string(i), row.checksumBlob.serialize());
      stmt.bindUint64(":FSEQ" + std::to_string(i), row.fSeq);
      stmt.bindUint64(":BLOCK_ID" + std::to_string(i), row.blockId);
      stmt.bindString(":DISK_INSTANCE" + std::to_string(i), row.diskInstance);
      // for Repack the following columns are not populated
      stmt.bindString(":DISK_FILE_PATH" + std::to_string(i),
                      !row.diskFileInfoPath.empty() ? row.diskFileInfoPath : "NOT_PROVIDED");
      stmt.bindString(":RETRIEVE_ERROR_REPORT_URL" + std::to_string(i),
                      !row.retrieveErrorReportURL.empty() ? row.retrieveErrorReportURL : "NOT_PROVIDED");
      stmt.bindString(":REQUESTER_NAME" + std::to_string(i),
                      !row.requesterName.empty() ? row.requesterName : "NOT_PROVIDED");
      stmt.bindString(":REQUESTER_GROUP" + std::to_string(i),
                      !row.requesterGroup.empty() ? row.requesterGroup : "NOT_PROVIDED");
      stmt.bindString(":SRR_USERNAME" + std::to_string(i), !row.srrUsername.empty() ? row.srrUsername : "NOT_PROVIDED");
      stmt.bindString(":SRR_HOST" + std::to_string(i), !row.srrHost.empty() ? row.srrHost : "NOT_PROVIDED");
      stmt.bindUint64(":SRR_TIME" + std::to_string(i),
                      row.srrTime != 0 ? row.srrTime : 0);  // keep 0 as sentinel for "NOT_PROVIDED"
      stmt.bindString(":SRR_MOUNT_POLICY" + std::to_string(i),
                      !row.srrMountPolicy.empty() ? row.srrMountPolicy : "NOT_PROVIDED");
      stmt.bindString(":RETRIEVE_REPORT_URL" + std::to_string(i),
                      !row.retrieveReportURL.empty() ? row.retrieveReportURL : "NOT_PROVIDED");
      stmt.bindString(":DISK_FILE_ID" + std::to_string(i), row.diskFileId);
      stmt.bindUint32(":DISK_FILE_GID" + std::to_string(i), row.diskFileInfoGid);
      stmt.bindUint32(":DISK_FILE_OWNER_UID" + std::to_string(i), row.diskFileInfoOwnerUid);
      stmt.bindString(":MOUNT_POLICY" + std::to_string(i), row.mountPolicy);
      stmt.bindString(":VID" + std::to_string(i), row.vid);
      stmt.bindString(":ALTERNATE_VIDS" + std::to_string(i), row.alternateVids);
      stmt.bindUint32(":PRIORITY" + std::to_string(i), row.priority);
      stmt.bindUint32(":MIN_RETRIEVE_REQUEST_AGE" + std::to_string(i), row.minRetrieveRequestAge);
      stmt.bindUint8(":COPY_NB" + std::to_string(i), row.copyNb);
      stmt.bindString(":ALTERNATE_COPY_NBS" + std::to_string(i), row.alternateCopyNbs);
      stmt.bindString(":ALTERNATE_FSEQS" + std::to_string(i), row.alternateFSeq);
      stmt.bindString(":ALTERNATE_BLOCK_IDS" + std::to_string(i), row.alternateBlockId);
      stmt.bindUint64(":START_TIME" + std::to_string(i), row.startTime);
      stmt.bindString(":DST_URL" + std::to_string(i), row.dstURL);
      stmt.bindUint32(":RETRIES_WITHIN_MOUNT" + std::to_string(i), row.retriesWithinMount);
      stmt.bindUint32(":TOTAL_RETRIES" + std::to_string(i), row.totalRetries);
      stmt.bindUint64(":LAST_MOUNT_WITH_FAILURE" + std::to_string(i), row.lastMountWithFailure);
      stmt.bindUint32(":MAX_TOTAL_RETRIES" + std::to_string(i), row.maxTotalRetries);
      stmt.bindUint32(":MAX_RETRIES_WITHIN_MOUNT" + std::to_string(i), row.maxRetriesWithinMount);
      stmt.bindUint32(":MAX_REPORT_RETRIES" + std::to_string(i), row.maxReportRetries);
      stmt.bindUint32(":TOTAL_REPORT_RETRIES" + std::to_string(i), row.totalReportRetries);
      stmt.bindBool(":IS_VERIFY_ONLY" + std::to_string(i), row.isVerifyOnly);
      stmt.bindBool(":IS_REPORTING" + std::to_string(i), row.isReporting);
      stmt.bindUint64(":LIFECYCLE_CREATION_TIME" + std::to_string(i), row.lifecycleTimings_creation_time);
      stmt.bindUint64(":LIFECYCLE_FIRST_SELECTED_TIME" + std::to_string(i), row.lifecycleTimings_first_selected_time);
      stmt.bindUint64(":LIFECYCLE_COMPLETED_TIME" + std::to_string(i), row.lifecycleTimings_completed_time);

      if (hasDiskSystemName) {
        stmt.bindString(":DISK_SYSTEM_NAME" + std::to_string(i), row.diskSystemName.value());
      }
      if (hasActivity) {
        stmt.bindString(":ACTIVITY" + std::to_string(i), row.activity.value());
      }
      if (hasSrrActivity) {
        stmt.bindString(":SRR_ACTIVITY" + std::to_string(i), row.srrActivity);
      }
    }

    stmt.executeNonQuery();
  }

  /**
  * When CTA received the PREPARE_ABORT request from the disk buffer,
  * the following method ensures removal from the pending queue
  *
  * @param txn           Transaction handling the connection to the backend database
  * @param archiveFileID The retrieve file ID assigned originally
  *
  * @return  The number of affected jobs
  */
  static uint64_t cancelRetrieveJob(Transaction& txn, uint64_t archiveFileID);

  /**
    * Select any jobs with specified status(es) from the report,
    * flag them as being reported and return the job IDs
    *
    * @param txn       Transaction handlign the connection to the backend database
    * @param statusList List of Retrieve Job Status to select on
    * @param limit      Maximum number of rows to return
    *
    * @return  result set of job IDs
    */
  static rdbms::Rset flagReportingJobsByStatus(Transaction& txn,
                                               std::list<RetrieveJobStatus> statusList,
                                               uint64_t gc_delay,
                                               uint64_t limit);

  /**
  * Assign a mount ID and VID to a selection of rows
  * which will be moved from the RETRIEVE_PENDING_QUEUE table
  * to the RETRIEVE_ACTIVE_QUEUE table in the DB
  *
  *
  * @param txn        Transaction to use for this query
  * @param newStatus  Retrieve Job Status to select on
  * @param mountInfo  mountInfo object
  * @param noSpaceDiskSystemNames list of diskSystemNames where there is no space left for more retrieves
  * @param maxBytesRequested  the maximum cumulative size of the files in the bunch requested
  * @param limit      Maximum number of rows to return
  *
  * @return  result set containing job IDs of the rows which were updated
  */
  static std::pair<rdbms::Rset, uint64_t>
  moveJobsToDbActiveQueue(Transaction& txn,
                          RetrieveJobStatus newStatus,
                          const SchedulerDatabase::RetrieveMount::MountInfo& mountInfo,
                          std::vector<std::string>& noSpaceDiskSystemNames,
                          uint64_t maxBytesRequested,
                          uint64_t limit,
                          bool isRepack);

  /**
  * Update job status
  *
  * @param txn        Transaction to use for this query
  * @param newStatus  Retrieve Job Status to select on
  * @param jobIDs     List of jobID strings to select
  * @return           Number of updated rows
  */
  static uint64_t
  updateJobStatus(Transaction& txn, RetrieveJobStatus newStatus, const std::vector<std::string>& jobIDs, bool isRepack);

  /**
  * Update failed job status
  *
  * @param txn                  Transaction to use for this query
  * @param newStatus            Retrieve Job Status to select on
  * @return                     Number of updated rows
  */
  uint64_t updateFailedJobStatus(Transaction& txn, bool isRepack);

  /**
  * Move a failed job from RETRIEVE_ACTIVE_QUEUE
  * to RETRIEVE_PENDING_QUEUE so that it can be picked up
  * by a different drive process again.
  * This method updates also the retry statistics
  *
  * @param txn                  Transaction to use for this query
  * @param newStatus            Retrieve Job Status to select on
  * @param keepMountId          true or false
  * @return                     Number of updated rows
  */
  uint64_t requeueFailedJob(Transaction& txn,
                            RetrieveJobStatus newStatus,
                            bool keepMountId,
                            bool isRepack,
                            std::optional<std::list<std::string>> jobIDs = std::nullopt);

  /**
  * @brief This method will check if RETRIEVE_ERROR_REPORT_URL column is not NULL and not empty
  * if the URL exists, it will move all the jobs of this VID from RETRIEVE_PENDING_QUEUE
  * to RETRIEVE_ACTIVE_QUEUE and set the status to RJS_ToReportToUserForFailure
  * In case the RETRIEVE_ERROR_REPORT_URL does not exist, it will directly delete these job
  * rows assuming reporting is not required
  *
  * @param txn                  Transaction to use for this query
  * @param vid                  tape VID to requeue
  * @return                     Number of updated rows
  */
  static uint64_t handlePendingRetrieveJobsAfterTapeStateChange(Transaction& txn, std::string vid);

  /**
  * Move from RETRIEVE_ACTIVE_QUEUE to RETRIEVE_PENDING_QUEUE
  * a batch of jobs so that they can be requeued to drive queues later
  * This methos is static and does not udate any retry statistics
  * It is used for batch of jobs not processed, returning from the task queue
  * (e.g. in case of a full tape)
  *
  * @param txn                  Transaction to use for this query
  * @param newStatus            Retrieve Job Status to select on
  * @param keepMountId          true or false
  * @return                     Number of updated rows
  */
  static uint64_t
  requeueJobBatch(Transaction& txn, RetrieveJobStatus newStatus, const std::list<std::string>& jobIDs, bool isRepack);

  /**
   * @brief Atomically moves a batch of retrieve jobs from the active queue to the archive queue.
   *
   * This method selects up to @p limit jobs from the REPACK_RETRIEVE_ACTIVE_QUEUE,
   * and moves them into REPACK_ARCHIVE_PENDING_QUEUE.
   * All additional copies are created from the ALTERNATE_COPYNBS column on the DB side
   * (as filled when queueing the original retrieve request for repack)
   *
   * @param txn       Active database transaction handle.
   * @param limit     Maximum number of jobs to move in this batch.
   *
   * @return The rset containing statistics summary info
   *
   * @throws std::runtime_error if the database query fails.
   */
  static rdbms::Rset transformJobBatchToArchive(Transaction& txn, const size_t limit);

  /**
  * Update job status when job report failed
  *
  * @param txn                  Transaction to use for this query
  * @param newStatus            Retrieve Job Status to select on
  * @return                     Number of updated rows
  */
  uint64_t updateJobStatusForFailedReport(Transaction& txn, RetrieveJobStatus newStatus);

  /**
  * Move the job row to the ARCHIVE FAILED JOB TABLE
  *
  * @param txn                  Transaction to use for this query
  * @return nrows               The number of rows moved.
  */
  uint64_t moveJobToFailedQueueTable(Transaction& txn);

  /**
  * Move the job rows to the ARCHIVE FAILED JOB TABLE (static alternate for multiple jobs)
  *
  * @param txn                  Transaction to use for this query
  * @param jobIDs               The jobIDs to be moved
  * @return nrows               The number of rows moved
  */
  static uint64_t
  moveJobBatchToFailedQueueTable(Transaction& txn, const std::vector<std::string>& jobIDs, bool isRepack);

  /**
  * Move the failed job rows to the ARCHIVE FAILED JOB TABLE for REPACK
  *
  * @param txn                  Transaction to use for this query
  * @param limit                The number of rows moved.
  * @return Rset                Rset with statistics about the failed retrieves
  */
  static rdbms::Rset moveFailedRepackJobBatchToFailedQueueTable(Transaction& txn, uint64_t limit);

  /**
  * Increment Retrieve Request ID and return the new value
  *
  * @param conn  DB connection to use for this query
  *
  * @return     Retrieve Request ID
  */
  static uint64_t getNextRetrieveRequestID(rdbms::Conn& conn);

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
};
};  // namespace cta::schedulerdb::postgres
