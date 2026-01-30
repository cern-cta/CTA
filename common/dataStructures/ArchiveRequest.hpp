/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/checksum/ChecksumBlob.hpp"
#include "common/dataStructures/DiskFileInfo.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"

#include <future>
#include <list>
#include <map>
#include <stdint.h>
#include <string>

namespace cta::common::dataStructures {

/**
 * This struct holds all the command line parameters of a CTA archive command
 */
struct ArchiveRequest {
  ArchiveRequest();

  bool operator==(const ArchiveRequest& rhs) const;

  bool operator!=(const ArchiveRequest& rhs) const;

  RequesterIdentity requester;
  std::string diskFileID;

  std::string srcURL;
  uint64_t fileSize = 0;
  checksum::ChecksumBlob checksumBlob;
  std::string storageClass;
  DiskFileInfo diskFileInfo;
  std::string archiveReportURL;
  std::string archiveErrorReportURL;
  EntryLog creationLog;

};  // struct ArchiveRequest

struct ArchiveInsertQueueItem {
  uint64_t archiveFileId;
  std::string instanceName;
  cta::common::dataStructures::ArchiveRequest request;
  std::map<uint32_t, std::string> copyToPoolMap;
  common::dataStructures::MountPolicy mountPolicy;

  std::promise<std::string> promise;
};

std::ostream& operator<<(std::ostream& os, const ArchiveRequest& obj);

}  // namespace cta::common::dataStructures
