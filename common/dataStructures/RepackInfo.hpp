/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <list>
#include <optional>
#include <string>

#include "RepackQueueType.hpp"
#include "EntryLog.hpp"

namespace cta::common::dataStructures {

/**
 * This is the repack information for a given tape
 */
struct RepackInfo {

  struct RepackDestinationInfo {
    std::string vid;
    uint64_t files = 0;
    uint64_t bytes = 0;
    typedef std::list<RepackDestinationInfo> List;
  };

  std::string vid;
  std::string repackBufferBaseURL;
  enum class Type {
    MoveAndAddCopies,
    AddCopiesOnly,
    MoveOnly,
    Undefined
  } type;
  enum class Status {
    // Those values are matching the cta.proto values
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
  bool noRecall;
  common::dataStructures::EntryLog creationLog;
  time_t repackFinishedTime = 0;
  RepackDestinationInfo::List destinationInfos;
//  std::string tag;
//  uint64_t totalFiles;
//  uint64_t totalSize;
//  uint64_t filesToRetrieve;
//  uint64_t filesToArchive;
//  uint64_t filesFailed;
//  uint64_t filesArchived;
//  RepackType repackType;
//  std::string repackStatus;
//  std::map<uint64_t,std::string> errors;
//  EntryLog creationLog;
RepackQueueType getQueueType();
}; // struct RepackInfo

std::string toString(RepackInfo::Type type);
std::string toString(RepackInfo::Status status);

} // namespace cta::common::dataStructures
