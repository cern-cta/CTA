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

namespace cta {
namespace postgresscheddb {

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

constexpr const char* const StringsArchiveJobStatus[] = {
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

constexpr const char* const StringsRetrieveJobStatus[] = {
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

constexpr const char* const StringsRepackJobStatus[] = {
  "RRS_Pending",
  "RRS_ToExpand",
  "RRS_Starting",
  "RRS_Running",
  "RRS_Complete",
  "RRS_Failed"
};


} // namespace postgresscheddb

// Define to_string and from_string in cta namespace

TO_STRING(ArchiveJobStatus)
// TO_STRING(JobQueueType)
TO_STRING(RetrieveJobStatus)
TO_STRING(RepackJobStatus)

} // namespace cta
