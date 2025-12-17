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
  enum class Type { MoveAndAddCopies, AddCopiesOnly, MoveOnly, Undefined } type;
  enum class Status {
    // Values matching the cta.proto values
    Pending = 1,
    ToExpand = 2,
    Starting = 3,
    Running = 4,
    Complete = 5,
    Failed = 6,
    Undefined = 999
  } status;
  uint64_t totalFilesOnTapeAtStart;
  uint64_t totalBytesOnTapeAtStart;
  bool allFilesSelectedAtStart;
  uint64_t totalFilesToArchive;
  uint64_t totalBytesToArchive;
  uint64_t totalFilesToRetrieve;
  uint64_t totalBytesToRetrieve;
  uint64_t failedFilesToArchive;
  uint64_t failedBytesToArchive;
  uint64_t failedFilesToRetrieve;
  uint64_t failedBytesToRetrieve;
  uint64_t lastExpandedFseq;
  uint64_t userProvidedFiles;
  uint64_t retrievedFiles;
  uint64_t retrievedBytes;
  uint64_t archivedFiles;
  uint64_t archivedBytes;
  uint64_t maxFilesToSelect;
  bool isExpandFinished;
  bool isExpandStarted;
  bool noRecall;
  EntryLog creationLog;
  time_t repackFinishedTime = 0;
  RepackDestinationInfo::List destinationInfos;
  std::string mountPolicy;

  RepackQueueType getQueueType();
};  // struct RepackInfo

std::string toString(RepackInfo::Type type);
std::string toString(RepackInfo::Status status);

}  // namespace cta::common::dataStructures
