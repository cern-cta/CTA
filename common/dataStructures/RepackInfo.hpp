/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include <string>
#include <list>
#include "RepackQueueType.hpp"
#include "EntryLog.hpp"
#include "common/optional.hpp"

namespace cta {
namespace common {
namespace dataStructures {

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
  bool isExpandFinished;
  bool forceDisabledTape;
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

} // namespace dataStructures
} // namespace common
} // namespace cta
