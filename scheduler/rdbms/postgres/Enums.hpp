/**
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "scheduler/rdbms/postgres/EnumToString.hpp"

// Defines enumerated types used in the PostgreSQL schema, with mappings to/from C++ enum classes

// Enums should inherit from uint8_t. A corresponding array of string values should be provided. The
// macro TO_STRING uses the enum class and the string array to define cta::to_string(T) and
// cta::from_string<T>(string). The string array is also used to define the valid values in the
// enumerated type in the PostgreSQL schema.

namespace cta::schedulerdb {

// ============================== Archive Job Status ===========================

enum class ArchiveJobStatus : uint8_t {
  AJS_ToTransferForUser,
  AJS_ToReportToUserForSuccess,
  AJS_Complete,
  AJS_ToReportToUserForFailure,
  AJS_Failed,
  AJS_Abandoned,
  AJS_ToTransferForRepack,
  AJS_ToReportToRepackForFailure,
  ReadyForDeletion
};

constexpr const std::array<const char*, 9> StringsArchiveJobStatus = {"AJS_ToTransferForUser",
                                                                      "AJS_ToReportToUserForSuccess",
                                                                      "AJS_Complete",
                                                                      "AJS_ToReportToUserForFailure",
                                                                      "AJS_Failed",
                                                                      "AJS_Abandoned",
                                                                      "AJS_ToTransferForRepack",
                                                                      "AJS_ToReportToRepackForFailure",
                                                                      "ReadyForDeletion"};

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
  RJS_ToReportToUserForSuccess,
  RJS_ToReportToUserForFailure,
  RJS_Failed,
  RJS_Complete,
  RJS_ToReportToRepackForSuccess,
  RJS_ToReportToRepackForFailure,
  ReadyForDeletion
};

constexpr const std::array<const char*, 8> StringsRetrieveJobStatus = {"RJS_ToTransfer",
                                                                       "RJS_ToReportToUserForSuccess",
                                                                       "RJS_ToReportToUserForFailure",
                                                                       "RJS_Failed",
                                                                       "RJS_Complete",
                                                                       "RJS_ToReportToRepackForSuccess",
                                                                       "RJS_ToReportToRepackForFailure",
                                                                       "ReadyForDeletion"};

// ============================== Repack Job Status ===========================

enum class RepackJobStatus : uint8_t { RRS_Pending, RRS_ToExpand, RRS_Starting, RRS_Running, RRS_Complete, RRS_Failed };

constexpr const std::array<const char*, 6> StringsRepackJobStatus =
  {"RRS_Pending", "RRS_ToExpand", "RRS_Starting", "RRS_Running", "RRS_Complete", "RRS_Failed"};
}  // namespace cta::schedulerdb

// Define to_string and from_string in cta namespace
namespace cta {
TO_STRING(ArchiveJobStatus)
// TO_STRING(JobQueueType)
TO_STRING(RetrieveJobStatus)
TO_STRING(RepackJobStatus)

}  // namespace cta
