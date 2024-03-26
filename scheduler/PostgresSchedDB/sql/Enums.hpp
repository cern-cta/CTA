/**
 * @project        The CERN Tape Archive (CTA)
 * @description    Defines enumerated types used in the PostgreSQL schema, with mappings
 *                 to/from C++ enum classes
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

#include "scheduler/PostgresSchedDB/sql/EnumToString.hpp"

// Enums should inherit from uint8_t. A corresponding array of string values should be provided. The
// macro TO_STRING uses the enum class and the string array to define cta::to_string(T) and
// cta::from_string<T>(string). The string array is also used to define the valid values in the
// enumerated type in the PostgreSQL schema.

namespace cta::postgresscheddb {

// ============================== Archive Job Status ===========================

enum class ArchiveJobStatus : uint8_t {
  AJS_ToTransferForUser,
  AJS_ToReportToUserForTransfer,
  AJS_Complete,
  AJS_ToReportToUserForFailure,
  AJS_Failed,
  AJS_Abandoned,
  AJS_ToTransferForRepack,
  AJS_ToReportToRepackForFailure
};

constexpr std::array<const char*, 8> const StringsArchiveJobStatus = {
  "AJS_ToTransferForUser",
  "AJS_ToReportToUserForTransfer",
  "AJS_Complete",
  "AJS_ToReportToUserForFailure",
  "AJS_Failed",
  "AJS_Abandoned",
  "AJS_ToTransferForRepack",
  "AJS_ToReportToRepackForFailure"
};

// ================================ Job Queue Type =============================

//enum class JobQueueType : uint8_t {
//  JobsToTransferForUser, 
//  FailedJobs, 
//  JobsToReportToUser, 
//  JobsToReportToRepackForSuccess, 
//  JobsToReportToRepackForFailure, 
//  JobsToTransferForRepack 
//};

//constexpr const char* const StringsJobQueueType[] = {
//  "JobsToTransfer", 
//  "FailedJobs", 
//  "JobsToReportToUser", 
//  "JobsToReportToRepackForSuccess", 
//  "JobsToReportToRepackForFailure", 
//  "JobsToTransferForRepack"
//};

// ================================ Retrieve Job Status ========================

enum class RetrieveJobStatus : uint8_t {
  RJS_ToTransfer,
  RJS_ToReportToUserForFailure,
  RJS_Failed,
  RJS_ToReportToRepackForSuccess,
  RJS_ToReportToRepackForFailure
};

constexpr std::array<const char*, 5> const StringsRetrieveJobStatus = {
  "RJS_ToTransfer"
  "RJS_ToReportToUserForFailure",
  "RJS_Failed",
  "RJS_ToReportToRepackForSuccess",
  "RJS_ToReportToRepackForFailure"
};

// ============================== Repack Job Status ===========================

enum class RepackJobStatus : uint8_t {
  RRS_Pending,
  RRS_ToExpand,
  RRS_Starting,
  RRS_Running,
  RRS_Complete,
  RRS_Failed
};

constexpr std::array<const char*, 6> const StringsRepackJobStatus = {
  "RRS_Pending",
  "RRS_ToExpand",
  "RRS_Starting",
  "RRS_Running",
  "RRS_Complete",
  "RRS_Failed"
};

// ============================== Archive Queue Job Column Indices  ===========================

enum class ArchColIdx {
  JOB_ID,
  ARCHIVE_REQID,
  STATUS,
  CREATION_TIME,
  MOUNT_POLICY,
  VID,
  MOUNT_ID,
  START_TIME,
  PRIORITY,
  STORAGE_CLASS,
  MIN_ARCHIVE_REQUEST_AGE,
  COPY_NB,
  SIZE_IN_BYTES,
  ARCHIVE_FILE_ID,
  CHECKSUMBLOB,
  REQUESTER_NAME,
  REQUESTER_GROUP,
  SRC_URL,
  DISK_INSTANCE,
  DISK_FILE_PATH,
  DISK_FILE_ID,
  DISK_FILE_GID,
  DISK_FILE_OWNER_UID,
  REPACK_REQID,
  IS_REPACK,
  ARCHIVE_ERROR_REPORT_URL,
  ARCHIVE_REPORT_URL,
  FAILURE_REPORT_LOG,
  FAILURE_LOG,
  REPACK_DEST_VID,
  IS_REPORTDECIDED,
  TOTAL_RETRIES,
  MAX_TOTAL_RETRIES,
  RETRIES_WITHIN_MOUNT,
  MAX_RETRIES_WITHIN_MOUNT,
  LAST_MOUNT_WITH_FAILURE,
  TOTAL_REPORT_RETRIES,
  MAX_REPORT_RETRIES,
  TAPE_POOL,
  REPACK_FILEBUF_URL,
  REPACK_FSEQ
};

constexpr std::array<const char*, 41> const StringsArchColIdx = {
          "JOB_ID",
          "ARCHIVE_REQID",
          "STATUS",
          "CREATION_TIME",
          "MOUNT_POLICY",
          "VID",
          "MOUNT_ID",
          "START_TIME",
          "PRIORITY",
          "STORAGE_CLASS",
          "MIN_ARCHIVE_REQUEST_AGE",
          "COPY_NB",
          "SIZE_IN_BYTES",
          "ARCHIVE_FILE_ID",
          "CHECKSUMBLOB",
          "REQUESTER_NAME",
          "REQUESTER_GROUP",
          "SRC_URL",
          "DISK_INSTANCE",
          "DISK_FILE_PATH",
          "DISK_FILE_ID",
          "DISK_FILE_GID",
          "DISK_FILE_OWNER_UID",
          "REPACK_REQID",
          "IS_REPACK",
          "ARCHIVE_ERROR_REPORT_URL",
          "ARCHIVE_REPORT_URL",
          "FAILURE_REPORT_LOG",
          "FAILURE_LOG",
          "REPACK_DEST_VID",
          "IS_REPORTDECIDED",
          "TOTAL_RETRIES",
          "MAX_TOTAL_RETRIES",
          "RETRIES_WITHIN_MOUNT",
          "MAX_RETRIES_WITHIN_MOUNT",
          "LAST_MOUNT_WITH_FAILURE",
          "TOTAL_REPORT_RETRIES",
          "MAX_REPORT_RETRIES",
          "TAPE_POOL",
          "REPACK_FILEBUF_URL",
          "REPACK_FSEQ"
};

} // namespace cta::postgresscheddb


// Define to_string and from_string in cta namespace
namespace cta {
TO_STRING(ArchiveJobStatus)
// TO_STRING(JobQueueType)
TO_STRING(RetrieveJobStatus)
TO_STRING(RepackJobStatus)
TO_STRING(ArchColIdx)

} // namespace cta
