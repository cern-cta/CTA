/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
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

#include "TapeItemWritten.hpp"
#include "TapeFileWritten.hpp"

namespace cta { namespace catalogue {

  struct InsertFileRecycleLog {
    std::string vid;
    uint64_t fseq;
    uint64_t blockId;
    uint64_t logicalSizeInBytes;
    uint64_t copyNb;
    time_t tapeFileCreationTime;
    uint64_t archiveFileId;
    std::string diskInstanceName;
    uint64_t diskFileId;
    uint64_t diskFileIdWhenDeleted;
    uint64_t diskFileUid;
    uint64_t diskFileGid;
    uint64_t sizeInBytes;
    checksum::ChecksumBlob checksumBlob;
    uint32_t checksumAdler32;
    uint64_t storageClassId;
    time_t archiveFileCreationTime;
    time_t reconciliationTime;
    std::string collocationHint;
    std::string diskFilePath;
    std::string reasonLog;
    time_t recycleLogTime;
  };
  
}}

