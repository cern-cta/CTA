/**
 * @project        The CERN Tape Archive (CTA)
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
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/checksum/ChecksumBlob.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"
#include "scheduler/rdbms/postgres/Enums.hpp"

#include <string>
#include <set>

namespace cta::schedulerdb::postgres {

struct RetrieveJobQueueRow {

  uint64_t jobId = 0;
  uint64_t retrieveReqId = 0;
  uint64_t mountId = 0;
  RetrieveJobStatus status = RetrieveJobStatus::RJS_ToTransfer;
  std::string vid;
  std::optional<std::string> activity;
  uint32_t actCopyNb = 0;
  uint64_t priority = 0;
  time_t retMinReqAge = 0;
  time_t startTime = 0;
  std::string mountPolicyName;
  std::string failureReportUrl;
  std::string failureReportLog;
  bool isRepack = false;
  uint64_t repackReqId = 0;
  bool isFailed = false;

  std::string retrieveJobsProtoBuf;
  std::string repackInfoProtoBuf;

  cta::common::dataStructures::ArchiveFile archiveFile;
  cta::common::dataStructures::RetrieveRequest retrieveRequest;
  std::optional<std::string> diskSystemName;

  RetrieveJobQueueRow() = default;

  /**
   * Constructor from row
   *
   * @param row  A single row from the current row of the rset
   */
  explicit RetrieveJobQueueRow(const rdbms::Rset &rset) {
    *this = rset;
  }

  RetrieveJobQueueRow& operator=(const rdbms::Rset &rset) {
    jobId                     = rset.columnUint64("JOB_ID");
    retrieveReqId             = rset.columnUint64("RETRIEVE_REQID");
    mountId                   = rset.columnUint64("MOUNT_ID");
    status                    = from_string<RetrieveJobStatus>(
                                rset.columnString("STATUS") );
    vid                       = rset.columnString("VID");
    activity                  = rset.columnOptionalString("ACTIVITY");
    actCopyNb                 = rset.columnUint16("ACTIVE_COPY_NB");
    priority                  = rset.columnUint16("PRIORITY");
    retMinReqAge              = rset.columnUint64("RETRIEVE_MIN_REQ_AGE");
    startTime                 = rset.columnUint64("STARTTIME");
    retrieveJobsProtoBuf      = rset.columnBlob("RETRIEVEJOB_PB");
    retrieveRequest.requester.name            = rset.columnString("REQUESTER_NAME");
    retrieveRequest.requester.group           = rset.columnString("REQUESTER_GROUP");
    archiveFile.archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
    retrieveRequest.archiveFileID = archiveFile.archiveFileID;
    retrieveRequest.dstURL                    = rset.columnString("DST_URL");
    archiveFile.diskFileInfo.owner_uid = rset.columnUint32("DISK_FILE_OWNER_UID");
    archiveFile.diskFileInfo.gid       = rset.columnUint32("DISK_FILE_GID");
    archiveFile.diskFileInfo.path      = rset.columnString("DISK_FILE_PATH");
    retrieveRequest.diskFileInfo = archiveFile.diskFileInfo;
    retrieveRequest.creationLog.username = rset.columnString("SRR_USERNAME");
    retrieveRequest.creationLog.host     = rset.columnString("SRR_HOST");
    retrieveRequest.creationLog.time     = rset.columnUint64("SRR_TIME");
    retrieveRequest.errorReportURL      = rset.columnString("RETRIEVE_ERROR_REPORT_URL");
    retrieveRequest.isVerifyOnly        = rset.columnBool("IS_VERIFY");
    mountPolicyName                     = rset.columnString("MOUNT_POLICY");
    retrieveRequest.mountPolicy        = rset.columnString("SRR_MOUNT_POLICY");
    retrieveRequest.activity           = rset.columnOptionalString("SRR_ACTIVITY");
    archiveFile.fileSize               = rset.columnUint64("SIZE_IN_BYTES");
    archiveFile.diskFileId             = rset.columnString("DISK_FILE_ID");
    archiveFile.diskInstance           = rset.columnString("DISK_INSTANCE");
    archiveFile.checksumBlob.deserialize(
                                         rset.columnBlob("CHECKSUMBLOB") );
    archiveFile.creationTime           = rset.columnUint64("CREATION_TIME");
    archiveFile.reconciliationTime     = 0;
    archiveFile.storageClass           = rset.columnString("STORAGE_CLASS");
    failureReportUrl                   = rset.columnString("FAILURE_REPORT_URL");
    failureReportLog                   = rset.columnString("FAILURE_REPORT_LOG");
    isRepack                           = rset.columnBool("IS_REPACK");
    repackReqId                        = rset.columnUint64("REPACK_REQID");
    repackInfoProtoBuf                 = rset.columnBlob("RR_REPACKINFO_PB");
    retrieveRequest.lifecycleTimings.creation_time            = rset.columnUint64("LT_CREATE");
    retrieveRequest.lifecycleTimings.first_selected_time       = rset.columnUint64("LT_FIRST_SELECTED");
    retrieveRequest.lifecycleTimings.completed_time          = rset.columnUint64("LT_COMPLETED");
    diskSystemName                     = rset.columnString("DISK_SYSTEM_NAME");
    isFailed                           = rset.columnBool("IS_FAILED");

    return *this;
  }

  void insert(Transaction &txn) const {
    // does not set mountId or jobId
    const char *const sql =
      "INSERT INTO RETRIEVE_JOB_QUEUE("
        "RETRIEVE_REQID,"
        "STATUS,"
        "VID,"
        "ACTIVITY,"
        "ACTIVE_COPY_NB,"
        "PRIORITY,"
        "RETRIEVE_MIN_REQ_AGE,"
        "STARTTIME,"
        "RETRIEVEJOB_PB,"
        "REQUESTER_NAME,"
        "REQUESTER_GROUP,"
        "ARCHIVE_FILE_ID,"
        "DST_URL,"
        "DISK_FILE_OWNER_UID,"
        "DISK_FILE_GID,"
        "DISK_FILE_PATH,"
        "SRR_USERNAME,"
        "SRR_HOST,"
        "SRR_TIME,"
        "RETRIEVE_ERROR_REPORT_URL,"
        "IS_VERIFY,"
        "MOUNT_POLICY,"
        "SRR_MOUNT_POLICY,"
        "SRR_ACTIVITY,"
        "SIZE_IN_BYTES,"
        "DISK_FILE_ID,"
        "DISK_INSTANCE,"
        "CHECKSUMBLOB,"
        "CREATION_TIME,"
        "STORAGE_CLASS,"
        "FAILURE_REPORT_URL,"
        "FAILURE_REPORT_LOG,"
        "IS_REPACK,"
        "REPACK_REQID,"
        "RR_REPACKINFO_PB,"
        "LT_CREATE,"
        "LT_FIRST_SELECTED,"
        "LT_COMPLETED,"
        "DISK_SYSTEM_NAME,"
        "IS_FAILED) VALUES ("
        ":RETRIEVE_REQID,"
        ":STATUS,"
        ":VID,"
        ":ACTIVITY,"
        ":ACTIVE_COPY_NB,"
        ":PRIORITY,"
        ":RETRIEVE_MIN_REQ_AGE,"
        ":STARTTIME,"
        ":RETRIEVEJOB_PB,"
        ":REQUESTER_NAME,"
        ":REQUESTER_GROUP,"
        ":ARCHIVE_FILE_ID,"
        ":DST_URL,"
        ":DISK_FILE_OWNER_UID,"
        ":DISK_FILE_GID,"
        ":DISK_FILE_PATH,"
        ":SRR_USERNAME,"
        ":SRR_HOST,"
        ":SRR_TIME,"
        ":RETRIEVE_ERROR_REPORT_URL,"
        ":IS_VERIFY,"
        ":MOUNT_POLICY,"
        ":SRR_MOUNT_POLICY,"
        ":SRR_ACTIVITY,"
        ":SIZE_IN_BYTES,"
        ":DISK_FILE_ID,"
        ":DISK_INSTANCE,"
        ":CHECKSUMBLOB,"
        ":CREATION_TIME,"
        ":STORAGE_CLASS,"
        ":FAILURE_REPORT_URL,"
        ":FAILURE_REPORT_LOG,"
        ":IS_REPACK,"
        ":REPACK_REQID,"
        ":RR_REPACKINFO_PB,"
        ":LT_CREATE,"
        ":LT_FIRST_SELECTED,"
        ":LT_COMPLETED,"
        ":DISK_SYSTEM_NAME,"
        ":IS_FAILED)";

    auto stmt = txn.conn().createStmt(sql);
    stmt.bindUint64(":RETRIEVE_REQID", retrieveReqId);
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindString(":VID", vid);
    stmt.bindString(":ACTVITIY", activity);
    stmt.bindUint16(":ACTIVE_COPY_NB", actCopyNb);
    stmt.bindUint16(":PRIORITY", priority);
    stmt.bindUint64(":RETRIEVE_MIN_REQ_AGE", retMinReqAge);
    stmt.bindUint64(":STARTTIME", startTime);
    stmt.bindBlob(":RETRIEVEJOB_PB", retrieveJobsProtoBuf);
    stmt.bindString(":REQUESTER_NAME", retrieveRequest.requester.name);
    stmt.bindString(":REQUESTER_GROUP", retrieveRequest.requester.group);
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFile.archiveFileID);
    stmt.bindString(":DST_URL", retrieveRequest.dstURL);
    stmt.bindUint32(":DISK_FILE_OWNER_UID", archiveFile.diskFileInfo.owner_uid);
    stmt.bindUint32(":DISK_FILE_GID", archiveFile.diskFileInfo.gid);
    stmt.bindString(":DISK_FILE_PATH", archiveFile.diskFileInfo.path);
    stmt.bindString(":SRR_USERNAME", retrieveRequest.creationLog.username);
    stmt.bindString(":SRR_HOST", retrieveRequest.creationLog.host);
    stmt.bindUint64(":SRR_TIME", retrieveRequest.creationLog.time);
    stmt.bindString(":RETRIEVE_ERROR_REPORT_URL", retrieveRequest.errorReportURL);
    stmt.bindBool(":IS_VERIFY", retrieveRequest.isVerifyOnly);
    stmt.bindString(":MOUNT_POLICY", mountPolicyName);
    stmt.bindString(":SRR_MOUNT_POLICY", retrieveRequest.mountPolicy);
    stmt.bindString(":SRR_ACTIVITY", retrieveRequest.activity);
    stmt.bindUint64(":SIZE_IN_BYTES", archiveFile.fileSize);
    stmt.bindString(":DISK_FILE_ID", archiveFile.diskFileId);
    stmt.bindString(":DISK_INSTANCE", archiveFile.diskInstance);
    stmt.bindBlob(":CHECKSUMBLOB", archiveFile.checksumBlob.serialize());
    stmt.bindUint64(":CREATION_TIME", archiveFile.creationTime);
    stmt.bindString(":STORAGE_CLASS", archiveFile.storageClass);
    stmt.bindString(":FAILURE_REPORT_URL", failureReportUrl);
    stmt.bindString(":FAILURE_REPORT_LOG", failureReportLog);
    stmt.bindBool(":IS_REPACK", isRepack);
    stmt.bindUint64(":REPACK_REQID", repackReqId);
    stmt.bindBlob(":RR_REPACKINFO_PB", repackInfoProtoBuf);
    stmt.bindUint64(":LT_CREATE", retrieveRequest.lifecycleTimings.creation_time);
    stmt.bindUint64(":LT_FIRST_SELECTED", retrieveRequest.lifecycleTimings.first_selected_time);
    stmt.bindUint64(":LT_COMPLETED", retrieveRequest.lifecycleTimings.completed_time);
    stmt.bindString(":DISK_SYSTEM_NAME", diskSystemName);
    stmt.bindBool(":IS_FAILED", isFailed);

    stmt.executeNonQuery();
  }

  void addParamsToLogContext(log::ScopedParamContainer& params) const {
    params.add("retrieveReqId", retrieveReqId);
    params.add("mountId", mountId);
    params.add("status", to_string(status));
    params.add("vid", vid);
    params.add("actCopyNb", actCopyNb);
    params.add("activity", activity.value());
    params.add("priority", priority);
    params.add("retMinReqAge", retMinReqAge);
    params.add("startTime", startTime);
    params.add("requester.name", retrieveRequest.requester.name);
    params.add("requester.group", retrieveRequest.requester.group);
    params.add("archiveFileID", archiveFile.archiveFileID);
    params.add("dstURL", retrieveRequest.dstURL);
    params.add("diskFileInfo.owner_uid", archiveFile.diskFileInfo.owner_uid);
    params.add("diskFileInfo.gid", archiveFile.diskFileInfo.gid);
    params.add("diskFileInfo.path", archiveFile.diskFileInfo.path);
    params.add("creationlog.ssername", retrieveRequest.creationLog.username);
    params.add("creationlog.host", retrieveRequest.creationLog.host);
    params.add("creationlog.time", retrieveRequest.creationLog.time);
    params.add("errorReportURL", retrieveRequest.errorReportURL);
    params.add("isVerifyOnly", retrieveRequest.isVerifyOnly);
    params.add("mountPolicyName", mountPolicyName);
    params.add("retrieveRequest.mountPolicy", retrieveRequest.mountPolicy.value());
    params.add("retrieveRequest.activity", retrieveRequest.activity.value());
    params.add("fileSize", archiveFile.fileSize);
    params.add("diskFileId", archiveFile.diskFileId);
    params.add("diskInstance", archiveFile.diskInstance);
    params.add("checksumBlob", archiveFile.checksumBlob);
    params.add("creationTime", archiveFile.creationTime);
    params.add("storageClass", archiveFile.storageClass);
    params.add("failureReportUrl", failureReportUrl);
    params.add("failureReportLog", failureReportLog);
    params.add("isRepack", isRepack);
    params.add("repackReqId", repackReqId);

    /* Columns to be replaced by other DB columns than protobuf filled columns
     * params.add("retrieveJobsProtoBuf", retrieveJobsProtoBuf);
     * params.add("repackInfoProtoBuf", repackInfoProtoBuf);
     */
    params.add("lifecycleTimings.creation_time", retrieveRequest.lifecycleTimings.creation_time);
    params.add("lifecycleTimings.first_selected_time", retrieveRequest.lifecycleTimings.first_selected_time);
    params.add("lifecycleTimings.completed_time", retrieveRequest.lifecycleTimings.completed_time);
    params.add("diskSystemName", diskSystemName.value());
    params.add("isFailed", isFailed);
  }


  static rdbms::Rset select(Transaction &txn, RetrieveJobStatus status, const std::string &vid, uint32_t limit) {

    const char *const sql =
      "SELECT "
        "JOB_ID AS JOB_ID,"
        "RETRIEVE_REQID AS RETRIEVE_REQID,"
        "MOUNT_ID AS MOUNT_ID,"
        "STATUS AS STATUS,"
        "VID AS VID,"
        "ACTIVE_COPY_NB AS ACTIVE_COPY_NB,"
        "ACTIVITY AS ACTIVITY,"
        "PRIORITY AS PRIORITY,"
        "RETRIEVE_MIN_REQ_AGE AS RETRIEVE_MIN_REQ_AGE,"
        "STARTTIME AS STARTTIME,"
        "RETRIEVEJOB_PB AS RETRIEVEJOB_PB,"
        "REQUESTER_NAME AS REQUESTER_NAME,"
        "REQUESTER_GROUP AS REQUESTER_GROUP,"
        "ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "DST_URL AS DST_URL,"
        "DISK_FILE_OWNER_UID AS DISK_FILE_OWNER_UID,"
        "DISK_FILE_GID AS DISK_FILE_GID,"
        "DISK_FILE_PATH AS DISK_FILE_PATH,"
        "SRR_USERNAME AS SRR_USERNAME,"
        "SRR_HOST AS SRR_HOST,"
        "SRR_TIME AS SRR_TIME,"
        "RETRIEVE_ERROR_REPORT_URL AS RETRIEVE_ERROR_REPORT_URL,"
        "IS_VERIFY AS IS_VERIFY,"
        "MOUNT_POLICY AS MOUNT_POLICY,"
        "SRR_MOUNT_POLICY AS SRR_MOUNT_POLICY,"
        "SRR_ACTIVITY AS SRR_ACTIVITY,"
        "SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "DISK_FILE_ID AS DISK_FILE_ID,"
        "DISK_INSTANCE AS DISK_INSTANCE,"
        "CHECKSUMBLOB AS CHECKSUMBLOB,"
        "CREATION_TIME AS CREATION_TIME,"
        "STORAGE_CLASS AS STORAGE_CLASS,"
        "FAILURE_REPORT_URL AS FAILURE_REPORT_URL,"
        "FAILURE_REPORT_LOG AS FAILURE_REPORT_LOG,"
        "IS_REPACK AS IS_REPACK,"
        "REPACK_REQID AS REPACK_REQID,"
        "RR_REPACKINFO_PB AS RR_REPACKINFO_PB,"
        "LT_CREATE AS LT_CREATE,"
        "LT_FIRST_SELECTED AS LT_FIRST_SELECTED,"
        "LT_COMPLETED AS LT_COMPLETED,"
        "DISK_SYSTEM_NAME AS DISK_SYSTEM_NAME,"
        "IS_FAILED AS IS_FAILED "
      "FROM RETRIEVE_JOB_QUEUE "
      "WHERE "
        "VID = :VID "
        "AND STATUS = :STATUS "
        "AND MOUNT_ID IS NULL "
    "ORDER BY PRIORITY DESC, JOB_ID "
      "LIMIT :LIMIT";

    auto stmt = txn.conn().createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.bindString(":STATUS", to_string(status));
    stmt.bindUint32(":LIMIT", limit);

    return stmt.executeQuery();
  }

  /**
   * Select not owned jobs from the queue ordered by priority and job_id
   *
   * @param txn        Transaction to use for this query
   * @param status     Archive Job Status to select on
   * @param limit      Maximum number of rows to return
   *
   * @return  result set
   */
  static rdbms::Rset select(Transaction &txn, RetrieveJobStatus status, uint64_t limit) {
    const char *const sql =
            "SELECT "
            "JOB_ID AS JOB_ID,"
            "RETRIEVE_REQID AS RETRIEVE_REQID,"
            "MOUNT_ID AS MOUNT_ID,"
            "STATUS AS STATUS,"
            "VID AS VID,"
            "ACTIVE_COPY_NB AS ACTIVE_COPY_NB,"
            "ACTIVITY AS ACTIVITY,"
            "PRIORITY AS PRIORITY,"
            "RETRIEVE_MIN_REQ_AGE AS RETRIEVE_MIN_REQ_AGE,"
            "STARTTIME AS STARTTIME,"
            "RETRIEVEJOB_PB AS RETRIEVEJOB_PB,"
            "REQUESTER_NAME AS REQUESTER_NAME,"
            "REQUESTER_GROUP AS REQUESTER_GROUP,"
            "ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
            "DST_URL AS DST_URL,"
            "DISK_FILE_OWNER_UID AS DISK_FILE_OWNER_UID,"
            "DISK_FILE_GID AS DISK_FILE_GID,"
            "DISK_FILE_PATH AS DISK_FILE_PATH,"
            "SRR_USERNAME AS SRR_USERNAME,"
            "SRR_HOST AS SRR_HOST,"
            "SRR_TIME AS SRR_TIME,"
            "RETRIEVE_ERROR_REPORT_URL AS RETRIEVE_ERROR_REPORT_URL,"
            "IS_VERIFY AS IS_VERIFY,"
            "MOUNT_POLICY AS MOUNT_POLICY,"
            "SRR_MOUNT_POLICY AS SRR_MOUNT_POLICY,"
            "SRR_ACTIVITY AS SRR_ACTIVITY,"
            "SIZE_IN_BYTES AS SIZE_IN_BYTES,"
            "DISK_FILE_ID AS DISK_FILE_ID,"
            "DISK_INSTANCE AS DISK_INSTANCE,"
            "CHECKSUMBLOB AS CHECKSUMBLOB,"
            "CREATION_TIME AS CREATION_TIME,"
            "STORAGE_CLASS AS STORAGE_CLASS,"
            "FAILURE_REPORT_URL AS FAILURE_REPORT_URL,"
            "FAILURE_REPORT_LOG AS FAILURE_REPORT_LOG,"
            "IS_REPACK AS IS_REPACK,"
            "REPACK_REQID AS REPACK_REQID,"
            "RR_REPACKINFO_PB AS RR_REPACKINFO_PB,"
            "LT_CREATE AS LT_CREATE,"
            "LT_FIRST_SELECTED AS LT_FIRST_SELECTED,"
            "LT_COMPLETED AS LT_COMPLETED,"
            "DISK_SYSTEM_NAME AS DISK_SYSTEM_NAME,"
            "IS_FAILED AS IS_FAILED "
            "FROM RETRIEVE_JOB_QUEUE "
            "WHERE "
            "STATUS = :STATUS "
            "AND MOUNT_ID IS NULL "
            "ORDER BY PRIORITY DESC, JOB_ID "
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
  static void updateMountID(Transaction &txn, const std::list<RetrieveJobQueueRow>& rowList, uint64_t mountId);
};

} // namespace cta::schedulerdb::postgres
