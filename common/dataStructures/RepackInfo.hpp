/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <string>

namespace cta {
namespace common {
namespace dataStructures {

/**
 * This is the repack information for a given tape 
 */
struct RepackInfo {

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
    Aborting = 5,
    Aborted = 6,
    Complete = 7,
    Failed = 8,
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

}; // struct RepackInfo

std::string toString(RepackInfo::Type type);
std::string toString(RepackInfo::Status status);

} // namespace dataStructures
} // namespace common
} // namespace cta
