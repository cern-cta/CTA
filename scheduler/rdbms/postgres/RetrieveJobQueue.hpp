/**
* @project        The CERN Tape Retrieve (CTA)
* @copyright      Copyright © 2021-2023 CERN
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
#include "common/checksum/ChecksumBlob.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"
#include "scheduler/rdbms/postgres/Enums.hpp"
#include "rdbms/NullDbValue.hpp"
#include "rdbms/Conn.hpp"

#include <string>
#include <set>
#include <unordered_map>
#include <vector>

namespace cta::schedulerdb::postgres {

struct RetrieveJobQueueRow {
  uint64_t jobId = 0;
  uint64_t retrieveRequestId = 0;
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
  time_t startTime = 0;  //!< Time the job was inserted into the queue
  time_t creationTime = 0;
  std::string retrieveReportURL = "NOT_DEFINED";
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

  RetrieveJobQueueRow() {
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

  /**
  * Constructor from row
  *
  * @param row  A single row from the current row of the rset
  */
  explicit RetrieveJobQueueRow(const rdbms::Rset& rset) {
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
    *this = rset;
  }

  void reset() {
    jobId = 0;
    retrieveRequestId = 0;
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
    retrieveErrorReportURL = rset.columnStringNoOpt("RETRIEVE_ERROR_REPORT_URL");
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
    // does not set mountId and the following
    std::string sql = R"(
       INSERT INTO RETRIEVE_PENDING_QUEUE (
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
    stmt.bindString(":RETRIEVE_ERROR_REPORT_URL", retrieveErrorReportURL);
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
    // stmt.bindString(":DRIVE", drive);
    // stmt.bindString(":HOST", host);
    // stmt.bindString(":LOGICAL_LIBRARY", logical_library);
    stmt.bindString(":SRR_USERNAME", srrUsername);
    stmt.bindString(":SRR_HOST", srrHost);
    stmt.bindUint64(":SRR_TIME", srrTime);
    stmt.bindString(":SRR_MOUNT_POLICY", srrMountPolicy);
    stmt.bindUint64(":LIFECYCLE_CREATION_TIME", lifecycleTimings_creation_time);
    stmt.bindUint64(":LIFECYCLE_FIRST_SELECTED_TIME", lifecycleTimings_first_selected_time);
    stmt.bindUint64(":LIFECYCLE_COMPLETED_TIME", lifecycleTimings_completed_time);
    if (diskSystemName.has_value()) {
      stmt.bindString(":DISK_SYSTEM_NAME", diskSystemName.value());
    }
    //stmt.bindBool(":IS_FAILED", isFailed);
    if (!retrieveReportURL.empty()) {
      stmt.bindString(":RETRIEVE_REPORT_URL", retrieveReportURL);
    } else {
      stmt.bindString(":RETRIEVE_REPORT_URL", "NOT_PROVIDED");
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
    params.add("retrieveReqId", retrieveRequestId);
    params.add("mountId", mountId);
    params.add("status", to_string(status));
    params.add("vid", vid);
    params.add("alternateVids", alternateVids);
    params.add("alternateCopyNbs", alternateCopyNbs);
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
    //params.add("isRepack", isRepack);
    //params.add("repackReqId", repackReqId);

    /* Columns to be replaced by other DB columns than protobuf filled columns
    * params.add("retrieveJobsProtoBuf", retrieveJobsProtoBuf);
    * params.add("repackInfoProtoBuf", repackInfoProtoBuf);
    */
    params.add("lifecycleTimings.creation_time", lifecycleTimings_creation_time);
    params.add("lifecycleTimings.first_selected_time", lifecycleTimings_first_selected_time);
    params.add("lifecycleTimings.completed_time", lifecycleTimings_completed_time);
    params.add("diskSystemName", diskSystemName.value_or(""));
    //params.add("isFailed", isFailed);
  }

  /**
  * When CTA received the deleteRetrieve request from the disk buffer,
  * this ensures removal from the queue
  *
  * @param txn           Transaction handling the connection to the backend database
  * @param diskInstance  Name of the disk instance where the retrieve request was issued from
  * @param archiveFileID The retrieve file ID assigned originally
  *
  * @return  The number of affected jobs
  */
  static uint64_t cancelRetrieveJob(Transaction& txn, const std::string& diskInstance, uint64_t archiveFileID);
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
  * which will be moved from Insert queue
  * to Job queue table in the DB
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
  moveJobsToDbQueue(Transaction& txn,
                    RetrieveJobStatus newStatus,
                    const SchedulerDatabase::RetrieveMount::MountInfo& mountInfo,
                    std::vector<std::string>& noSpaceDiskSystemNames,
                    uint64_t maxBytesRequested,
                    uint64_t limit);
  /**
  * Update job status
  *
  * @param txn        Transaction to use for this query
  * @param newStatus  Retrieve Job Status to select on
  * @param jobIDs     List of jobID strings to select
  * @return           Number of updated rows
  */
  static uint64_t updateJobStatus(Transaction& txn, RetrieveJobStatus newStatus, const std::vector<std::string>& jobIDs);

  /**
  * Update failed job status
  *
  * @param txn                  Transaction to use for this query
  * @param newStatus            Retrieve Job Status to select on
  * @return                     Number of updated rows
  */
  uint64_t updateFailedJobStatus(Transaction& txn, RetrieveJobStatus newStatus);

  /**
  * Move from ARCHIVE_ACTIVE_QUEUE to ARCHIVE_PENDING_QUEUE
  * a failed job so that it can be to drive queues requeued.
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
                            std::optional<std::list<std::string>> jobIDs = std::nullopt);

  /**
  * Move from ARCHIVE_ACTIVE_QUEUE to ARCHIVE_PENDING_QUEUE
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
  static uint64_t requeueJobBatch(Transaction& txn, RetrieveJobStatus newStatus, const std::list<std::string>& jobIDs);

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
  * @return nrows               The number of rows moved.
  */
  static uint64_t moveJobBatchToFailedQueueTable(Transaction& txn, const std::vector<std::string>& jobIDs);

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
