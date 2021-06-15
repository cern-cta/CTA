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
#include "common/checksum/ChecksumBlob.hpp"
#include "common/optional.hpp"

namespace cta{
namespace common{
namespace dataStructures{

struct  FileRecycleLog {
  std::string vid;
  uint64_t fSeq;
  uint64_t blockId;
  /**
   * The TAPE_FILE.LOGICAL_SIZE_IN_BYTES column contains the same information as the ARCHIVE_FILE.
   * https://gitlab.cern.ch/cta/CTA/-/issues/412
   * Therefore, we don't have to save it in the FileRecycleLog
   */
  /*uint64_t logicalSizeInBytes;*/
  uint8_t copyNb;
  time_t tapeFileCreationTime;
  uint64_t archiveFileId;
  std::string diskInstanceName;
  std::string diskFileId;
  std::string diskFileIdWhenDeleted;
  uint64_t diskFileUid;
  uint64_t diskFileGid;
  uint64_t sizeInBytes;
  cta::checksum::ChecksumBlob checksumBlob;
  std::string storageClassName;
  time_t archiveFileCreationTime;
  time_t reconciliationTime;
  cta::optional<std::string> collocationHint;
  cta::optional<std::string> diskFilePath;
  std::string reasonLog;
  time_t recycleLogTime;
};

}}}

