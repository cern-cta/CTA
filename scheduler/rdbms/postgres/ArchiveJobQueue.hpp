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
    std::optional <std::uint64_t> mountId = std::nullopt;
    ArchiveJobStatus status = ArchiveJobStatus::AJS_ToTransferForUser;
    std::string tapePool = "";
    std::string mountPolicy = "";
    uint32_t priority = 0;
    uint64_t minArchiveRequestAge = 0;
    uint8_t copyNb = 0;
    time_t startTime = 0;                       //!< Time the job was inserted into the queue
    std::string archiveReportUrl = "";
    std::string archiveErrorReportUrl = "";
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
    std::optional <std::string> failureLogs = std::nullopt;
    std::optional <std::string> reportFailureLogs = std::nullopt;
    bool is_repack = false;
    bool is_reporting = false;
    bool in_drive_queue = false;
    uint64_t repackId = 0;
    std::string repackFilebufUrl = "";
    uint64_t repackFseq = 0;
    std::string repackDestVid = "";
    std::string vid = "";
    std::string drive = "";
    std::string host = "";
    std::string mount_type = "";
    std::string logical_library = "";


    common::dataStructures::ArchiveFile archiveFile;

    ArchiveJobQueueRow() {
      archiveFile.reconciliationTime = 0;
      archiveFile.archiveFileID = 0;
      archiveFile.fileSize = 0;
      archiveFile.diskFileInfo.owner_uid = 0;
      archiveFile.diskFileInfo.gid = 0;
      archiveFile.creationTime = 0;
    }


    /**
     * Constructor from row
     *
     * @param row  A single row from the current row of the rset
     */
    explicit ArchiveJobQueueRow(const rdbms::Rset &rset) {
      *this = rset;
    }

    ArchiveJobQueueRow &operator=(const rdbms::Rset &rset) {
      jobId = rset.columnUint64("JOB_ID");
      reqId = rset.columnUint64("ARCHIVE_REQUEST_ID");
      reqJobCount = rset.columnUint32("REQUEST_JOB_COUNT");
      mountId = rset.columnOptionalUint64("MOUNT_ID");
      status = from_string<ArchiveJobStatus>(
              rset.columnString("STATUS"));
      tapePool = rset.columnString("TAPE_POOL");
      mountPolicy = rset.columnString("MOUNT_POLICY");
      priority = rset.columnUint16("PRIORITY");
      minArchiveRequestAge = rset.columnUint32("MIN_ARCHIVE_REQUEST_AGE");
      archiveFile.archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
      archiveFile.fileSize = rset.columnUint64("SIZE_IN_BYTES");
      copyNb = rset.columnUint16("COPY_NB");
      startTime = rset.columnUint64("START_TIME");
      archiveFile.checksumBlob.deserialize(
              rset.columnBlob("CHECKSUMBLOB"));
      archiveFile.creationTime = rset.columnUint64("CREATION_TIME");
      archiveFile.diskInstance = rset.columnString("DISK_INSTANCE");
      archiveFile.diskFileId = rset.columnString("DISK_FILE_ID");
      archiveFile.diskFileInfo.owner_uid = rset.columnUint32("DISK_FILE_OWNER_UID");
      archiveFile.diskFileInfo.gid = rset.columnUint32("DISK_FILE_GID");
      archiveFile.diskFileInfo.path = rset.columnString("DISK_FILE_PATH");
      archiveReportUrl = rset.columnString("ARCHIVE_REPORT_URL");
      archiveErrorReportUrl = rset.columnString("ARCHIVE_ERROR_REPORT_URL");
      requesterName = rset.columnString("REQUESTER_NAME");
      requesterGroup = rset.columnString("REQUESTER_GROUP");
      srcUrl = rset.columnString("SRC_URL");
      archiveFile.storageClass = rset.columnString("STORAGE_CLASS");
      is_reporting = rset.columnBool("IS_REPORTING");
      in_drive_queue = rset.columnBool("IN_DRIVE_QUEUE");
      vid = rset.columnString("VID");
      drive = rset.columnString("DRIVE");
      host = rset.columnString("HOST");
      mount_type = rset.columnString("MOUNT_TYPE");
      logical_library = rset.columnString("LOGICAL_LIBRARY");
      failureLogs = rset.columnOptionalString("FAILURE_LOG");
      reportFailureLogs = rset.columnOptionalString("REPORT_FAILURE_LOG");
      lastMountWithFailure = rset.columnUint32("LAST_MOUNT_WITH_FAILURE");
      retriesWithinMount = rset.columnUint16("RETRIES_WITHIN_MOUNT");
      maxRetriesWithinMount = rset.columnUint16("MAX_RETRIES_WITHIN_MOUNT");
      totalRetries = rset.columnUint16("TOTAL_RETRIES");
      maxTotalRetries = rset.columnUint16("MAX_TOTAL_RETRIES");
      totalReportRetries = rset.columnUint16("TOTAL_REPORT_RETRIES");
      return *this;
    }

    void insert(Transaction &txn) const {

      // does not set mountId or jobId
      const char *const sql = R"SQL(
      INSERT INTO ARCHIVE_JOB_QUEUE(
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

      auto stmt = txn.conn().createStmt(sql);
      stmt.bindUint64(":ARCHIVE_REQUEST_ID", reqId);
      stmt.bindUint32(":REQUEST_JOB_COUNT", reqJobCount);
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
      stmt.bindUint16(":MAX_RETRIES_WITHIN_MOUNT", maxRetriesWithinMount);
      stmt.bindUint16(":TOTAL_RETRIES", totalRetries);
      stmt.bindUint32(":LAST_MOUNT_WITH_FAILURE", lastMountWithFailure);
      stmt.bindUint16(":MAX_TOTAL_RETRIES", maxTotalRetries);
      stmt.bindUint16(":TOTAL_REPORT_RETRIES", totalReportRetries);
      stmt.bindUint16(":MAX_REPORT_RETRIES", maxReportRetries);
      stmt.executeNonQuery();

    }

    void addParamsToLogContext(log::ScopedParamContainer &params) const {
      // does not set jobId
      params.add("mountId", mountId.has_value() ? std::to_string(mountId.value()) : "no value");
      params.add("jobID",  std::to_string(jobId));
      params.add("reqId",  std::to_string(reqId));
      params.add("reqJobCount",  std::to_string(reqJobCount));
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
     * Select any jobs from the queue by job ID
     *
     * @param conn       Connection to the DB backend
     * @param jobIDs     List of jobID strings to select
     * @return  result set
     */
    static rdbms::Rset selectJobsByJobID(rdbms::Conn &conn, const std::list <std::string> &jobIDs) {
      if (jobIDs.empty()) {
        rdbms::Rset ret;
        return ret;
      }
      std::string sqlpart;
      for (const auto &piece: jobIDs) sqlpart += piece + ",";
      if (!sqlpart.empty()) { sqlpart.pop_back(); }
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
        IS_REPORTING AS IS_REPORTING,
        IN_DRIVE_QUEUE AS IN_DRIVE_QUEUE,
        DRIVE AS DRIVE,
        HOST AS HOST,
        MOUNT_TYPE AS MOUNT_TYPE,
        LOGICAL_LIBRARY AS LOGICAL_LIBRARY
      FROM ARCHIVE_JOB_QUEUE 
      WHERE 
        JOB_ID IN (
    )SQL" + sqlpart + R"SQL(
      ) ORDER BY PRIORITY DESC, JOB_ID
    )SQL";
      auto stmt = conn.createStmt(sql);
      return stmt.executeQuery();
    }

    /**
     * Select any jobs with specified status(es) from the report,
     * flag them as being reported and return the job IDs
     *
     * @param conn       Connection to the backend database
     * @param statusList List of Archive Job Status to select on
     * @param limit      Maximum number of rows to return
     *
     * @return  result set of job IDs
     */
    static rdbms::Rset
    flagReportingJobsByStatus(Transaction &txn, std::list <ArchiveJobStatus> statusList, uint64_t limit);

    /**
     * Assign a mount ID and VID to a selection of rows
     *
     * @param txn        Transaction to use for this query
     * @param status     Archive Job Status to select on
     * @param mountInfo  mountInfo object
     * @param limit      Maximum number of rows to return
     * @param gc_delay   delay for garbage collection of jobs which were not processed
     *                   untill a final state by the mount where they started processing
     *                   defalut is 3 days
     *
     * @return  result set containing job IDs of the rows which were updated
     */
    static rdbms::Rset updateMountInfo(Transaction &txn, ArchiveJobStatus status,
                                       const SchedulerDatabase::ArchiveMount::MountInfo &mountInfo, uint64_t limit, uint64_t gc_delay = 259200);

    /**
     * Update job status
     *
     * @param txn        Transaction to use for this query
     * @param status     Archive Job Status to select on
     * @param jobIDs     List of jobID strings to select
     */
    static void updateJobStatus(Transaction &txn, ArchiveJobStatus status, const std::vector <std::string> &jobIDs);

    /**
     * Update failed job status
     *
     * @param txn                  Transaction to use for this query
     * @param status               Archive Job Status to select on
     * @param jobID                jobID to select the job for update
     */
    void
    updateFailedJobStatus(Transaction &txn, ArchiveJobStatus status, std::optional <uint64_t> mountId = std::nullopt);

    /**
     * Update job status when job report failed
     *
     * @param txn                  Transaction to use for this query
     * @param status               Archive Job Status to select on
     */
    void updateJobStatusForFailedReport(Transaction &txn, ArchiveJobStatus status);

    /**
     * Copy the job row to the ARCHIVE FAILED JOB TABLE
     *
     * @param txn                  Transaction to use for this query
     */
    void copyToFailedJobTable(Transaction &txn);

    /**
     * Copy the job row to the ARCHIVE FAILED JOB TABLE (static alternative)
     *
     * @param txn                  Transaction to use for this query
     */
    static void copyToFailedJobTable(Transaction &txn, const std::vector<std::string>& jobIDs);

    /**
     * Increment Archive Request ID and return the new value
     *
     * @param txn  Transaction to use for this query
     *
     * @return     Archive Request ID
     */
    static uint64_t getNextArchiveRequestID(Transaction &txn);
  };
}; // namespace cta::schedulerdb::postgres
