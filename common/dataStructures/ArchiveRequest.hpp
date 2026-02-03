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

struct ArchiveInsertQueueCriteria {
  std::map<uint32_t, std::string> copyToPoolMap;
  common::dataStructures::MountPolicy mountPolicy;
};

struct ArchiveInsertQueueCriteriaKey {
  std::string_view instanceName;
  std::string_view storageClass;
  std::string_view requesterName;
  std::string_view requesterGroup;

  bool operator==(const ArchiveInsertQueueCriteriaKey& other) const {
    return instanceName == other.instanceName && storageClass == other.storageClass
           && requesterName == other.requesterName && requesterGroup == other.requesterGroup;
  }
};

// Hash function for unordered_map
struct ArchiveInsertQueueCriteriaKeyHash {
  std::size_t operator()(const ArchiveInsertQueueCriteriaKey& k) const {
    std::size_t h = std::hash<std::string_view> {}(k.instanceName);
    h ^= std::hash<std::string_view> {}(k.storageClass) + 0x9e3779b9 + (h << 6) + (h >> 2);
    h ^= std::hash<std::string_view> {}(k.requesterName) + 0x9e3779b9 + (h << 6) + (h >> 2);
    h ^= std::hash<std::string_view> {}(k.requesterGroup) + 0x9e3779b9 + (h << 6) + (h >> 2);
    return h;
  }
};

std::ostream& operator<<(std::ostream& os, const ArchiveRequest& obj);

}  // namespace cta::common::dataStructures
