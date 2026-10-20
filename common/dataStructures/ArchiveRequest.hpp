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

/**
 * One archive request in flight through the opportunistic batching pipeline.
 *
 * Lifecycle: a caller thread constructs the item, resolves the catalogue criteria
 * (stage 1) on its own thread, then hands it to OpportunisticQueueBatcher via
 * enqueueAndWait().  The leader thread collects a batch and calls
 * FrontendBatchingLayer::resolveArchiveBatch() (stage 2), which performs a single
 * bulk DB insert and settles every promise.  The caller thread wakes up, reads the
 * result from the future, and returns.
 */
struct ArchiveInsertQueueItem {
  /** CTA archive file ID, allocated by the catalogue before this item is created. */
  uint64_t archiveFileId;

  /** Disk-system instance name (e.g. EOS instance) the file originates from. */
  std::string instanceName;

  /** Full set of file-level metadata supplied by the client (storage class, checksums,
   *  source URL, report URLs, requester identity, …). */
  cta::common::dataStructures::ArchiveRequest request;

  /** Map from tape-copy number to tape-pool name, resolved from the catalogue in
   *  stage 1.  Each entry produces one scheduler-DB archive job row in stage 2.
   *  Size == number of tape copies required by the storage class. */
  std::map<uint32_t, std::string> copyToPoolMap;

  /** Mount policy resolved from the catalogue in stage 1; governs scheduling
   *  priority and age-based mount triggers for this request. */
  common::dataStructures::MountPolicy mountPolicy;

  /** Settled by the leader thread in stage 2 with the request's scheduler-DB
   *  address on success, or with an exception on failure.  The caller thread
   *  blocks on the corresponding future until the batch is committed. */
  std::promise<std::string> promise;

  /** True once promise.set_value() has been called (i.e. the DB insert succeeded).
   *  Left false when promise.set_exception() is used instead.  copyToPoolMap alone
   *  cannot distinguish the two states: a stage-2 failure still has it populated
   *  from the earlier stage-1 lookup. */
  bool queued = false;
};

struct ArchiveInsertQueueCriteria {
  std::map<uint32_t, std::string> copyToPoolMap;
  common::dataStructures::MountPolicy mountPolicy;
};

// Owns its strings rather than viewing them (deliberately not string_view): this key is used as an
// std::unordered_map key in a cache that outlives any single batch (Scheduler::
// m_archiveInsertQueueCriteriaCache), while the strings a string_view here would naturally be built
// from (an ArchiveInsertQueueItem's own instanceName/storageClass/requester fields) only live as
// long as that one batch. A string_view key would dangle the moment that batch is destroyed,
// corrupting every lookup and insertion into the cache after it -- silently, since freed memory
// often isn't overwritten immediately, making it look like it works until it doesn't.
struct ArchiveInsertQueueCriteriaKey {
  std::string instanceName;
  std::string storageClass;
  std::string requesterName;
  std::string requesterGroup;

  bool operator==(const ArchiveInsertQueueCriteriaKey& other) const {
    return instanceName == other.instanceName && storageClass == other.storageClass
           && requesterName == other.requesterName && requesterGroup == other.requesterGroup;
  }
};

// Hash function for unordered_map
struct ArchiveInsertQueueCriteriaKeyHash {
  std::size_t operator()(const ArchiveInsertQueueCriteriaKey& k) const {
    std::size_t h = std::hash<std::string> {}(k.instanceName);
    h ^= std::hash<std::string> {}(k.storageClass) + 0x9e3779b9 + (h << 6) + (h >> 2);
    h ^= std::hash<std::string> {}(k.requesterName) + 0x9e3779b9 + (h << 6) + (h >> 2);
    h ^= std::hash<std::string> {}(k.requesterGroup) + 0x9e3779b9 + (h << 6) + (h >> 2);
    return h;
  }
};

std::ostream& operator<<(std::ostream& os, const ArchiveRequest& obj);

}  // namespace cta::common::dataStructures
