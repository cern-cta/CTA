/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/checksum/ChecksumBlob.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"

#include <list>
#include <map>
#include <optional>
#include <stdint.h>
#include <string>

namespace cta::common::dataStructures {

/**
 * This is a request to delete an existing archive file or to cancel an ongoing
 * archival.
 */
struct DeleteArchiveRequest {
  DeleteArchiveRequest();

  bool operator==(const DeleteArchiveRequest& rhs) const;

  bool operator!=(const DeleteArchiveRequest& rhs) const;

  RequesterIdentity requester;
  uint64_t archiveFileID = 0;
  std::optional<std::string> address;
  std::string diskFilePath;
  std::string diskFileId;
  std::string diskInstance;
  time_t recycleTime;
  //In the case the ArchiveFile does not exist yet, it will not be set
  std::optional<ArchiveFile> archiveFile;
  std::optional<uint64_t> diskFileSize;
  std::optional<checksum::ChecksumBlob> checksumBlob;
};  // struct DeleteArchiveRequest

std::ostream& operator<<(std::ostream& os, const DeleteArchiveRequest& obj);

}  // namespace cta::common::dataStructures
