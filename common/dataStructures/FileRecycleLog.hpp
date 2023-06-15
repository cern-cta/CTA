/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <optional>
#include <string>

#include "common/checksum/ChecksumBlob.hpp"

namespace cta {
namespace common {
namespace dataStructures {

struct FileRecycleLog {
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
  std::optional<std::string> collocationHint;
  std::optional<std::string> diskFilePath;
  std::string reasonLog;
  time_t recycleLogTime;
};

}  // namespace dataStructures
}  // namespace common
}  // namespace cta
