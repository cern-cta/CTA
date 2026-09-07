/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/SchedulerInstruments.hpp"
#include "common/utils/Timer.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/OpportunisticQueueBatcher.hpp"
#include "scheduler/Scheduler.hpp"

#include <future>
#include <mutex>
#include <opentelemetry/context/runtime_context.h>
#include <sstream>

namespace cta {

//------------------------------------------------------------------------------
// resolveArchiveInsertCriteria
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveInsertQueueCriteria
Scheduler::resolveArchiveInsertCriteria(const std::string& instanceName,
                                        const std::string& storageClass,
                                        const cta::common::dataStructures::RequesterIdentity& requester,
                                        log::LogContext& lc) {
  cta::common::dataStructures::ArchiveInsertQueueCriteriaKey k {instanceName,
                                                                storageClass,
                                                                requester.name,
                                                                requester.group};
  const auto now = std::chrono::steady_clock::now();

  // Single-flight coalescing: several concurrent callers can miss on the exact same key at once
  // (e.g. the first requests for a storage class, or right after its TTL expires, under load).
  // Whichever caller finds no cache hit AND no fetch already in flight for this key becomes the
  // fetcher and registers fetchPromise's shared_future for everyone else to find; any other caller
  // that misses on the same key while that's present just waits on it below instead of repeating the
  // catalogue call.
  std::promise<cta::common::dataStructures::ArchiveInsertQueueCriteria> fetchPromise;
  std::shared_future<cta::common::dataStructures::ArchiveInsertQueueCriteria> waitOnFuture;
  bool isFetcher = false;
  {
    std::lock_guard<std::mutex> cacheLock(m_archiveInsertQueueCriteriaCacheMutex);
    auto it = m_archiveInsertQueueCriteriaCache.find(k);
    if (it != m_archiveInsertQueueCriteriaCache.end() && (now - it->second.cachedAt) < m_archiveInsertQueueCriteriaCacheTtl) {
      return it->second.criteria;
    }

    auto inFlightIt = m_archiveInsertQueueCriteriaInFlight.find(k);
    if (inFlightIt != m_archiveInsertQueueCriteriaInFlight.end()) {
      waitOnFuture = inFlightIt->second;
    } else {
      isFetcher = true;
      waitOnFuture = fetchPromise.get_future().share();
      m_archiveInsertQueueCriteriaInFlight.emplace(k, waitOnFuture);
    }
  }

  if (!isFetcher) {
    // Rethrows here too if the fetcher's own lookup failed -- the same error every one of these
    // waiters would have gotten from its own lookup anyway, since they all share the same key.
    return waitOnFuture.get();
  }

  // Deliberately called outside the lock: this is the (relatively slow) catalogue round trip, and
  // holding the mutex across it would serialize every OTHER key's lookups behind this one, right
  // back into the one-at-a-time pattern this whole move out of resolveArchiveBatch() was meant to
  // avoid. Callers waiting on THIS key are parked on waitOnFuture above, not on the mutex.
  try {
    auto queueCriteria =
      m_catalogue.ArchiveFile()->getArchiveFileQueueCriteria(instanceName, storageClass, requester);
    cta::common::dataStructures::ArchiveInsertQueueCriteria criteria {std::move(queueCriteria.copyToPoolMap),
                                                                      std::move(queueCriteria.mountPolicy)};
    {
      std::lock_guard<std::mutex> cacheLock(m_archiveInsertQueueCriteriaCacheMutex);
      auto it = m_archiveInsertQueueCriteriaCache.find(k);
      if (it != m_archiveInsertQueueCriteriaCache.end()) {
        // Refreshed in place rather than counted as new growth, so a small set of hot keys cycling
        // past the TTL can't by itself trigger the size-based clear below.
        it->second = {criteria, now};
      } else {
        m_archiveInsertQueueCriteriaCache.emplace(k, CachedArchiveInsertQueueCriteria {criteria, now});
        if (m_archiveInsertQueueCriteriaCache.size() > m_archiveInsertQueueCriteriaCacheMaxSize) {
          m_archiveInsertQueueCriteriaCache.clear();
        }
      }
      m_archiveInsertQueueCriteriaInFlight.erase(k);
    }
    fetchPromise.set_value(criteria);
    return criteria;
  } catch (...) {
    {
      std::lock_guard<std::mutex> cacheLock(m_archiveInsertQueueCriteriaCacheMutex);
      m_archiveInsertQueueCriteriaInFlight.erase(k);
    }
    // Propagates to every waiter parked on waitOnFuture.get() above, then rethrown here for this
    // (the fetcher's) own caller too.
    fetchPromise.set_exception(std::current_exception());
    throw;
  }
}

//------------------------------------------------------------------------------
// resolveArchiveBatch
//------------------------------------------------------------------------------
void Scheduler::resolveArchiveBatch(std::vector<cta::common::dataStructures::ArchiveInsertQueueItem>& batch,
                                    log::LogContext& lc) {
  cta::utils::Timer batchTimer;
  uint64_t successfulJobs = 0;
  uint64_t failedJobs = 0;

  // Every item reaching this point already carries a resolved copyToPoolMap/mountPolicy: stage 1
  // (the catalogue lookup) now runs in resolveArchiveInsertCriteria(), on each caller's own thread,
  // before the item is even enqueued with the batcher -- see that method's own comment for why. A
  // request whose lookup failed never reached here at all, having already thrown directly from
  // queueArchiveWithGivenId(). So this is stage 2 only: one bulk insert for the whole batch.
  static const char* const failMsg = "In Scheduler::resolveArchiveBatch(): bulk archive insert failed, failing this batch";
  auto jobsPerItem = [](const cta::common::dataStructures::ArchiveInsertQueueItem& item) -> uint64_t {
    return item.copyToPoolMap.size();
  };
  size_t successfulItems = 0;
  try {
    auto archiveReqAddrVector = m_db.queueArchive(batch, lc);

    if (archiveReqAddrVector.size() != batch.size()) {
      throw exception::Exception("queueArchive returned size " + std::to_string(archiveReqAddrVector.size())
                                 + " but batch size is " + std::to_string(batch.size()));
    }

    for (size_t i = 0; i < batch.size(); ++i) {
      batch[i].promise.set_value(archiveReqAddrVector[i]);
      batch[i].queued = true;
      successfulJobs += batch[i].copyToPoolMap.size();
      ++successfulItems;
    }
  } catch (const std::exception& e) {
    cta::failWholeBatch(batch, lc, e.what(), failMsg, failedJobs, jobsPerItem);
  } catch (...) {
    cta::failWholeBatch(batch, lc, std::string("unknown exception"), failMsg, failedJobs, jobsPerItem);
  }

  // Duration covers this batch's bulk insert only now that stage 1 has moved out — the actual DB
  // wall time of queueing this batch — timed here rather than by the caller in
  // queueArchiveWithGivenId(), whose own elapsed time also includes the opportunistic-batching
  // wait/sleep, which is about the batching mechanism, not the queueing work itself. Job count only
  // includes items that actually got queued, not merely items which reached this function, so a
  // stage-2 failure (which fails every item in batch) doesn't inflate the reported throughput.
  auto batchTimeMSecs = batchTimer.msecs();
  cta::telemetry::metrics::ctaSchedulerOperationDuration->Record(
    batchTimeMSecs,
    {
      {cta::semconv::attr::kSchedulerOperationName,     cta::semconv::attr::SchedulerOperationNameValues::kEnqueue},
      {cta::semconv::attr::kSchedulerOperationWorkflow,
       cta::semconv::attr::SchedulerOperationWorkflowValues::kArchive                                             }
  },
    opentelemetry::context::RuntimeContext::GetCurrent());
  cta::telemetry::metrics::ctaSchedulerOperationJobCount->Add(
    successfulJobs,
    {
      {cta::semconv::attr::kSchedulerOperationName,     cta::semconv::attr::SchedulerOperationNameValues::kEnqueue},
      {cta::semconv::attr::kSchedulerOperationWorkflow,
       cta::semconv::attr::SchedulerOperationWorkflowValues::kArchive                                             }
  },
    opentelemetry::context::RuntimeContext::GetCurrent());
  if (failedJobs > 0) {
    // Same counter as the success case above, tagged with kErrorType, following the convention
    // already used in Scheduler::reportArchiveJobsBatch() rather than a separate metric.
    cta::telemetry::metrics::ctaSchedulerOperationJobCount->Add(
      failedJobs,
      {
        {cta::semconv::attr::kSchedulerOperationName,     cta::semconv::attr::SchedulerOperationNameValues::kEnqueue},
        {cta::semconv::attr::kSchedulerOperationWorkflow,
         cta::semconv::attr::SchedulerOperationWorkflowValues::kArchive                                             },
        {cta::semconv::attr::kErrorType,                  cta::semconv::attr::ErrorTypeValues::kException           }
    },
      opentelemetry::context::RuntimeContext::GetCurrent());
  }

  log::ScopedParamContainer(lc)
    .add("batchSize", batch.size())
    .add("successfulItems", successfulItems)
    .add("failedItems", batch.size() - successfulItems)
    .add("successfulJobs", successfulJobs)
    .add("failedJobs", failedJobs)
    .log(log::INFO, "In Scheduler::resolveArchiveBatch(): processed a batch of archive requests.");
}

//------------------------------------------------------------------------------
// logQueuedArchiveItems
//------------------------------------------------------------------------------
void Scheduler::logQueuedArchiveItems(std::vector<cta::common::dataStructures::ArchiveInsertQueueItem>& batch,
                                      log::LogContext& lc) {
  // Per-item audit log, mirroring the file-by-file path's own "Queued archive request" INFO line
  // (same fields), run only for items resolveArchiveBatch() actually queued, after followers have
  // already been released and don't wait on it. catalogueTime/schedulerDbTime don't apply here
  // (that work is shared across the whole batch, not attributable to one item), so batchSize is
  // logged in their place instead.
  using utils::midEllipsis;
  for (const auto& item : batch) {
    if (!item.queued) {
      continue;
    }
    log::ScopedParamContainer spc(lc);
    spc.add("instanceName", item.instanceName)
      .add("storageClass", item.request.storageClass)
      .add("diskFileID", item.request.diskFileID)
      .add("fileSize", item.request.fileSize)
      .add("fileId", item.archiveFileId);
    for (const auto& [copyNum, tapePool] : item.copyToPoolMap) {
      std::stringstream tp;
      tp << "tapePool" << copyNum;
      spc.add(tp.str(), tapePool);
    }
    spc.add("policyName", item.mountPolicy.name)
      .add("policyArchiveMinAge", item.mountPolicy.archiveMinRequestAge)
      .add("policyArchivePriority", item.mountPolicy.archivePriority)
      .add("diskFilePath", item.request.diskFileInfo.path)
      .add("diskFileOwnerUid", item.request.diskFileInfo.owner_uid)
      .add("diskFileGid", item.request.diskFileInfo.gid)
      .add("archiveReportURL", midEllipsis(item.request.archiveReportURL, 50, 15))
      .add("archiveErrorReportURL", midEllipsis(item.request.archiveErrorReportURL, 50, 15))
      .add("creationHost", item.request.creationLog.host)
      .add("creationTime", item.request.creationLog.time)
      .add("creationUser", item.request.creationLog.username)
      .add("requesterName", item.request.requester.name)
      .add("requesterGroup", item.request.requester.group)
      .add("srcURL", midEllipsis(item.request.srcURL, 50, 15))
      .add("batchSize", batch.size());
    item.request.checksumBlob.addFirstChecksumToLog(spc);
    lc.log(log::INFO, "In Scheduler::logQueuedArchiveItems(): Queued archive request");
  }
}

}  // namespace cta
