/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/checksum/ChecksumBlob.hpp"

#include <optional>
#include <string>

namespace cta::common::dataStructures {

struct FileRecycleLog {
  std::string vid;
  uint64_t fSeq;
  uint64_t blockId;
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
  std::string vo;
  time_t archiveFileCreationTime;
  time_t reconciliationTime;
  std::optional<std::string> collocationHint;
  std::optional<std::string> diskFilePath;
  std::string reasonLog;
  time_t recycleLogTime;
};

}  // namespace cta::common::dataStructures
