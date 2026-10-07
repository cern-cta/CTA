/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#ifdef CTA_PGSCHED

#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "disk/DiskSystem.hpp"
#include "scheduler/OpportunisticQueueBatcher.hpp"

#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace cta {

namespace catalogue {
class Catalogue;
}

namespace log {
class LogContext;
}
class Scheduler;
class SchedulerDatabase;

/**
 * Opportunistic-batching front-end for archive and retrieve queueing.
 *
 * Sits in front of a Scheduler and intercepts queueArchiveWithGivenId() and
 * queueRetrieve(): when batching is enabled, concurrent callers are coalesced
 * into a single bulk DB insert per window; when disabled, calls pass straight
 * through to the underlying Scheduler. All other Scheduler operations are
 * unaffected and called directly on the Scheduler.
 *
 * Only meaningful for CTA_PGSCHED builds: the bulk-insert DB methods this
 * relies on do not exist for the objectstore scheduler.
 */
class FrontendBatchingLayer {
public:
  FrontendBatchingLayer(Scheduler& scheduler,
                        bool enableOpportunisticBatching,
                        uint64_t opportunisticBatchingWindowMs,
                        uint64_t opportunisticBatchingMaxBatchSize);

  /**
   * Queue an archive request, batching it with concurrent callers when
   * opportunistic batching is enabled. Falls through to Scheduler::queueArchiveWithGivenId()
   * when batching is disabled.
   */
  std::string queueArchiveWithGivenId(uint64_t archiveFileId,
                                      const std::string& instanceName,
                                      const cta::common::dataStructures::ArchiveRequest& request,
                                      log::LogContext& lc);

  /**
   * Queue a retrieve request, batching it with concurrent callers when
   * opportunistic batching is enabled. Falls through to Scheduler::queueRetrieve()
   * when batching is disabled.
   */
  std::string queueRetrieve(const std::string& instanceName,
                            cta::common::dataStructures::RetrieveRequest& request,
                            log::LogContext& lc);

private:
  Scheduler& m_scheduler;
  catalogue::Catalogue& m_catalogue;
  SchedulerDatabase& m_db;

  const bool m_enableOpportunisticBatching;

  // Resolves the promise for every item in the batch via one bulk DB insert, whatever the internal
  // outcome — this is the fast part, run before followers waiting on the batch are released. Stage 1
  // (per-item catalogue lookup) no longer happens here: it runs on each caller's own thread, in
  // resolveArchiveInsertCriteria() below, before the item is even enqueued -- see that method's own
  // comment for why. Every item that reaches this function already carries a resolved
  // copyToPoolMap/mountPolicy.
  void resolveArchiveBatch(std::vector<cta::common::dataStructures::ArchiveInsertQueueItem>& batch,
                           log::LogContext& lc);
  // Per-item audit log for the items resolveArchiveBatch() queued successfully — the slow part
  // (synchronous log writes), run only after followers have already been released.
  void logQueuedArchiveItems(std::vector<cta::common::dataStructures::ArchiveInsertQueueItem>& batch,
                             log::LogContext& lc);

  // Stage 1 of opportunistic archive queueing: resolves (with caching) the copyToPoolMap/mountPolicy
  // for one request. Called from queueArchiveWithGivenId() on the caller's own thread, before the
  // item is enqueued with the batcher -- not from resolveArchiveBatch() -- so that concurrent
  // callers' catalogue lookups run in parallel with each other instead of being serialized, one at a
  // time, inside the single leader thread's critical round-latency window. A request whose lookup
  // fails throws directly from here and is never enqueued at all, which is what gives it the same
  // per-request isolation the old in-batch try/catch used to provide, without needing one any more.
  cta::common::dataStructures::ArchiveInsertQueueCriteria
  resolveArchiveInsertCriteria(const std::string& instanceName,
                               const std::string& storageClass,
                               const cta::common::dataStructures::RequesterIdentity& requester,
                               log::LogContext& lc);

  /**
   * Maximum time the leader waits for concurrent requests to join its batch before processing it,
   * and the maximum number of requests it will wait to accumulate: whichever limit is hit first
   * ends the wait. A longer window/larger cap means fewer, bigger DB round trips (more efficient
   * under load) at the cost of more added latency per request; a shorter window/smaller cap means
   * less added latency but less batching benefit.
   */
  const std::chrono::milliseconds m_opportunisticBatchingWindow;
  const size_t m_opportunisticBatchingMaxBatchSize;
  std::unique_ptr<OpportunisticQueueBatcher<cta::common::dataStructures::ArchiveInsertQueueItem, std::string>>
    m_archiveBatcher;

  // Pairs a cached criteria lookup with when it was fetched, so a hit can be judged stale (see
  // m_archiveInsertQueueCriteriaCacheTtl below) instead of being trusted forever -- a storage
  // class's routing or mount policy can be changed by an admin at any time, and without this the
  // cache would keep serving whatever was true the first time a given (instance, storage class,
  // requester) combination was seen, for as long as the process runs (or until the size-based clear
  // below happens to evict it, which may be never in a low-cardinality deployment).
  struct CachedArchiveInsertQueueCriteria {
    cta::common::dataStructures::ArchiveInsertQueueCriteria criteria;
    std::chrono::steady_clock::time_point cachedAt;
  };

  // Guards m_archiveInsertQueueCriteriaCache: accessed from every caller's own thread inside
  // resolveArchiveInsertCriteria() (stage 1 runs before enqueueing, in parallel across concurrent
  // callers), rather than only ever from a single leader thread at a time.
  std::mutex m_archiveInsertQueueCriteriaCacheMutex;
  std::unordered_map<cta::common::dataStructures::ArchiveInsertQueueCriteriaKey,
                     CachedArchiveInsertQueueCriteria,
                     cta::common::dataStructures::ArchiveInsertQueueCriteriaKeyHash>
    m_archiveInsertQueueCriteriaCache;
  size_t m_archiveInsertQueueCriteriaCacheMaxSize = 1000;
  // Single-flight coalescing for cold/stale keys: several concurrent callers can miss on the exact
  // same key at once (e.g. the first requests for a storage class after startup, or right after its
  // TTL expires under load). Without this, every one of them would independently issue the same
  // catalogue call. The first to miss registers a shared_future here and becomes the "fetcher"; every
  // other caller that misses on the same key while it's present just waits on it and reuses the
  // result (or rethrows the same exception, if the fetch failed) instead of repeating the lookup.
  std::unordered_map<cta::common::dataStructures::ArchiveInsertQueueCriteriaKey,
                     std::shared_future<cta::common::dataStructures::ArchiveInsertQueueCriteria>,
                     cta::common::dataStructures::ArchiveInsertQueueCriteriaKeyHash>
    m_archiveInsertQueueCriteriaInFlight;
  // Deliberately short: this cache only exists to spare the catalogue a lookup for the (common)
  // case of several concurrent requests in the same opportunistic-batching window sharing a storage
  // class, not to be a long-lived cache -- a stale hit just costs one avoidable catalogue call, so
  // there is little to gain from a longer TTL, while a shorter one bounds how long an admin's
  // routing/mount-policy change takes to be picked up.
  static constexpr std::chrono::seconds m_archiveInsertQueueCriteriaCacheTtl {30};

  // Resolves the promise for every item in the batch via one bulk DB insert (which also selects the
  // vid to read each item from), whatever the internal outcome — this is the fast part, run before
  // followers waiting on the batch are released. Stage 1 (catalogue lookup and disk-system-name
  // resolution) no longer happens here: it runs on each caller's own thread, in
  // resolveRetrieveInsertCriteria() below, before the item is even enqueued.
  void resolveRetrieveBatch(std::vector<cta::common::dataStructures::RetrieveInsertQueueItem>& batch,
                            log::LogContext& lc);
  // Per-item audit log for the items resolveRetrieveBatch() queued successfully — the slow part
  // (synchronous log writes), run only after followers have already been released.
  void logQueuedRetrieveItems(std::vector<cta::common::dataStructures::RetrieveInsertQueueItem>& batch,
                              log::LogContext& lc);

  // Stage 1 of opportunistic retrieve queueing: catalogue lookup and disk-system-name resolution for
  // one request. Called from queueRetrieve() on the caller's own thread, before the item is
  // enqueued with the batcher -- not from resolveRetrieveBatch() -- so that concurrent callers'
  // catalogue lookups run in parallel with each other instead of being serialized, one at a time,
  // inside the single leader thread's critical round-latency window. A request whose lookup fails
  // throws directly from here and is never enqueued at all, which is what gives it the same
  // per-request isolation the old in-batch try/catch used to provide, without needing one any more.
  cta::common::dataStructures::RetrieveFileQueueCriteria
  resolveRetrieveInsertCriteria(const std::string& instanceName,
                                const cta::common::dataStructures::RetrieveRequest& request,
                                std::optional<std::string>& diskSystemName,
                                log::LogContext& lc);

  // Returns the (TTL-cached, single-flight coalesced) disk system list used by
  // resolveRetrieveInsertCriteria() to resolve a request's dstURL to a disk system name. Unlike
  // m_archiveInsertQueueCriteriaCache, this isn't keyed at all -- there is exactly one disk system
  // list, shared by every retrieve request regardless of instance/requester/file -- so every caller
  // either hits the same cached value or coalesces onto the same single in-flight fetch.
  disk::DiskSystemList getCachedDiskSystemList();
  std::mutex m_diskSystemListCacheMutex;
  std::optional<disk::DiskSystemList> m_diskSystemListCache;
  std::chrono::steady_clock::time_point m_diskSystemListCachedAt;
  std::optional<std::shared_future<disk::DiskSystemList>> m_diskSystemListInFlight;
  static constexpr std::chrono::seconds m_diskSystemListCacheTtl {30};

  // Uses the same m_opportunisticBatchingWindow/m_opportunisticBatchingMaxBatchSize as
  // m_archiveBatcher above: one config, both workflows. Unlike archive, there is no per-item criteria
  // cache here — archive's is keyed on storage class, shared by many requests; retrieve criteria are
  // keyed on archiveFileID, unique per request, so nothing would ever be reused from it. The disk
  // system list is the one piece of retrieve's stage 1 that is shared across requests -- see
  // getCachedDiskSystemList() above.
  std::unique_ptr<OpportunisticQueueBatcher<cta::common::dataStructures::RetrieveInsertQueueItem, std::string>>
    m_retrieveBatcher;
};

}  // namespace cta

#endif  // CTA_PGSCHED