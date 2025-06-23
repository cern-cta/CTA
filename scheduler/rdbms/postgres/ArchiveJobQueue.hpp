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
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"
#include "scheduler/rdbms/postgres/Enums.hpp"
#include "rdbms/NullDbValue.hpp"
#include "rdbms/Conn.hpp"

#include <vector>

namespace cta::schedulerdb::postgres {

struct ArchiveJobQueueRow {
  uint64_t jobId = 0;
  uint64_t reqId = 0;
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

  //common::dataStructures::ArchiveFile archiveFile;

  uint64_t archiveFileID = 0;
  std::string diskFileId = "";
  std::string diskInstance = "";
  uint64_t fileSize = 0;
  std::string storageClass = "";
  std::string diskFileInfoPath = "";
  uint32_t diskFileInfoOwnerUid = 0;
  uint32_t diskFileInfoGid = 0;
  checksum::ChecksumBlob checksumBlob;

  ArchiveJobQueueRow() {
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

  /**
     * Constructor from row
     *
     * @param row  A single row from the current row of the rset
     */
  explicit ArchiveJobQueueRow(const rdbms::Rset& rset) {
    *this = rset;
  }

  // Reset function
  void reset() {
    jobId = 0;
    reqId = 0;
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
        MAX_REPORT_RETRIES) VALUES (
        :ARCHIVE_REQUEST_ID,
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
        :TOTAL_RETRIES,
        :LAST_MOUNT_WITH_FAILURE,
        :MAX_TOTAL_RETRIES,
        :TOTAL_REPORT_RETRIES,
        :MAX_REPORT_RETRIES)
    )SQL";

    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":ARCHIVE_REQUEST_ID", reqId);
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
    stmt.executeNonQuery();
  }

  void addParamsToLogContext(log::ScopedParamContainer& params) const {
    // does not set jobId
    params.add("mountId", mountId.has_value() ? std::to_string(mountId.value()) : "no value");
    params.add("jobID", std::to_string(jobId));
    params.add("reqId", std::to_string(reqId));
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

  /**
   * Select any jobs from the queue by job ID
   *
   * @param conn       Connection to the DB backend
   * @param jobIDs     List of jobID strings to select
   * @return  result set
   */
  static rdbms::Rset selectJobsByJobID(rdbms::Conn& conn, const std::list<std::string>& jobIDs) {
    if (jobIDs.empty()) {
      rdbms::Rset ret;
      return ret;
    }
    std::string sqlpart;
    for (const auto& piece : jobIDs) {
      sqlpart += piece + ",";
    }
    if (!sqlpart.empty()) {
      sqlpart.pop_back();
    }
    std::string sql = R"SQL(
      SELECT 
        JOB_ID AS JOB_ID,
        ARCHIVE_REQUEST_ID AS ARCHIVE_REQUEST_ID,
        REQUEST_JOB_COUNT AS REQUEST_JOB_COUNT,
        MOUNT_ID AS MOUNT_ID,
        VID AS VID,
        STATUS AS STATUS,
        TAPE_POOL AS TAPE_POOL,
        MOUNT_POLICY AS MOUNT_POLICY,
        PRIORITY AS PRIORITY,
        MIN_ARCHIVE_REQUEST_AGE AS MIN_ARCHIVE_REQUEST_AGE,
        ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,
        SIZE_IN_BYTES AS SIZE_IN_BYTES,
        COPY_NB AS COPY_NB,
        START_TIME AS START_TIME,
        CHECKSUMBLOB AS CHECKSUMBLOB,
        CREATION_TIME AS CREATION_TIME,
        DISK_INSTANCE AS DISK_INSTANCE,
        DISK_FILE_ID AS DISK_FILE_ID,
        DISK_FILE_OWNER_UID AS DISK_FILE_OWNER_UID,
        DISK_FILE_GID AS DISK_FILE_GID,
        DISK_FILE_PATH AS DISK_FILE_PATH,
        ARCHIVE_REPORT_URL AS ARCHIVE_REPORT_URL,
        ARCHIVE_ERROR_REPORT_URL AS ARCHIVE_ERROR_REPORT_URL,
        REQUESTER_NAME AS REQUESTER_NAME,
        REQUESTER_GROUP AS REQUESTER_GROUP,
        SRC_URL AS SRC_URL,
        STORAGE_CLASS AS STORAGE_CLASS,
        RETRIES_WITHIN_MOUNT AS RETRIES_WITHIN_MOUNT,
        MAX_RETRIES_WITHIN_MOUNT AS MAX_RETRIES_WITHIN_MOUNT,
        TOTAL_RETRIES AS TOTAL_RETRIES,
        TOTAL_REPORT_RETRIES AS TOTAL_REPORT_RETRIES,
        FAILURE_LOG AS FAILURE_LOG,
        REPORT_FAILURE_LOG AS REPORT_FAILURE_LOG,
        LAST_MOUNT_WITH_FAILURE  AS LAST_MOUNT_WITH_FAILURE,
        MAX_TOTAL_RETRIES AS MAX_TOTAL_RETRIES,
        MAX_REPORT_RETRIES AS MAX_REPORT_RETRIES,
        IS_REPORTING AS IS_REPORTING,
        DRIVE AS DRIVE,
        HOST AS HOST,
        MOUNT_TYPE AS MOUNT_TYPE,
        LOGICAL_LIBRARY AS LOGICAL_LIBRARY
      FROM ARCHIVE_ACTIVE_QUEUE
      WHERE 
        JOB_ID IN (
    )SQL" + sqlpart + R"SQL(
      ) ORDER BY PRIORITY DESC, JOB_ID
    )SQL";
    auto stmt = conn.createStmt(sql);
    return stmt.executeQuery();
  }

  /**
   * When CTA received the deleteArchive request from the disk buffer,
   * this ensures removal from the queue
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
  static rdbms::Rset flagReportingJobsByStatus(Transaction& txn,
                                               std::list<ArchiveJobStatus> statusList,
                                               uint64_t gc_delay,
                                               uint64_t limit);

  /**
   * Assign a mount ID and VID to a selection of rows
   * which will be moved from Insert queue
   * to Job queue table in the DB
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
  static std::pair<rdbms::Rset, uint64_t> moveJobsToDbQueue(Transaction& txn,
                                                            ArchiveJobStatus newStatus,
                                                            const SchedulerDatabase::ArchiveMount::MountInfo& mountInfo,
                                                            uint64_t maxBytesRequested,
                                                            uint64_t limit);

  /**
   * Update job status
   *
   * @param txn        Transaction to use for this query
   * @param newStatus  Archive Job Status to select on
   * @param jobIDs     List of jobID strings to select
   * @return           Number of updated rows
   */
  static uint64_t updateJobStatus(Transaction& txn, ArchiveJobStatus newStatus, const std::vector<std::string>& jobIDs);

  /**
   * Update failed job status
   *
   * @param txn                  Transaction to use for this query
   * @param newStatus            Archive Job Status to select on
   * @return                     Number of updated rows
   */
  uint64_t updateFailedJobStatus(Transaction& txn, ArchiveJobStatus newStatus);

  /**
   * Move from ARCHIVE_ACTIVE_QUEUE to ARCHIVE_PENDING_QUEUE
   * a failed job so that it can be to drive queues requeued.
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
  static uint64_t requeueJobBatch(Transaction& txn,
                                  ArchiveJobStatus newStatus,
                                  const std::list<std::string>& jobIDs);

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
   * Move the job rows to the ARCHIVE FAILED JOB TABLE (static alternative for multiple jobs)
   *
   * @param txn                  Transaction to use for this query
   * @return nrows               The number of rows moved.
   */
  static uint64_t moveJobBatchToFailedQueueTable(Transaction& txn, const std::vector<std::string>& jobIDs);

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
};
};  // namespace cta::schedulerdb::postgres
