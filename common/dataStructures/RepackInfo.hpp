/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "EntryLog.hpp"
#include "RepackQueueType.hpp"

#include <list>
#include <optional>
#include <string>

namespace cta::common::dataStructures {

/**
 * Repack information for a given tape
 */
struct RepackInfo {
  struct RepackDestinationInfo {
    std::string vid;
    uint64_t files = 0;
    uint64_t bytes = 0;
    using List = std::list<RepackDestinationInfo>;
  };

  uint64_t repackReqId = 0;
  std::string vid;
  std::string repackBufferBaseURL;
  enum class Type { MoveAndAddCopies, AddCopiesOnly, MoveOnly, Undefined } type = Type::Undefined;
  enum class Status {
    // Values matching the cta.proto values
    Pending = 1,
    ToExpand = 2,
    Starting = 3,
    Running = 4,
    Complete = 5,
    Failed = 6,
    Undefined = 999
  } status = Status::Undefined;
  uint64_t totalFilesOnTapeAtStart = 0;
  uint64_t totalBytesOnTapeAtStart = 0;
  bool allFilesSelectedAtStart = false;
  uint64_t totalFilesToArchive = 0;
  uint64_t totalBytesToArchive = 0;
  uint64_t totalFilesToRetrieve = 0;
  uint64_t totalBytesToRetrieve = 0;
  uint64_t failedFilesToArchive = 0;
  uint64_t failedBytesToArchive = 0;
  uint64_t failedFilesToRetrieve = 0;
  uint64_t failedBytesToRetrieve = 0;
  uint64_t lastExpandedFseq = 0;
  uint64_t userProvidedFiles = 0;
  uint64_t retrievedFiles = 0;
  uint64_t retrievedBytes = 0;
  uint64_t archivedFiles = 0;
  uint64_t archivedBytes = 0;
  uint64_t maxFilesToSelect = 0;
  uint64_t failedToCreateArchiveReq = 0;
  std::string storageClass;
  bool isExpandFinished = false;
  bool isExpandStarted = false;
  bool noRecall = false;
  EntryLog creationLog;
  time_t repackFinishedTime = 0;
  RepackDestinationInfo::List destinationInfos;
  std::string mountPolicy;

  RepackQueueType getQueueType() const;

};  // struct RepackInfo

std::string toString(RepackInfo::Type type);
std::string toString(RepackInfo::Status status);

}  // namespace cta::common::dataStructures
