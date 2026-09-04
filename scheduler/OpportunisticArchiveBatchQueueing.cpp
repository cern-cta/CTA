/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/Catalogue.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/SchedulerInstruments.hpp"
#include "common/utils/Timer.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/OpportunisticQueueBatcher.hpp"
#include "scheduler/Scheduler.hpp"

#include <opentelemetry/context/runtime_context.h>
#include <sstream>

namespace cta {

//------------------------------------------------------------------------------
// resolveArchiveBatch
//------------------------------------------------------------------------------
void Scheduler::resolveArchiveBatch(std::vector<cta::common::dataStructures::ArchiveInsertQueueItem>& batch,
                                    log::LogContext& lc) {
  cta::utils::Timer batchTimer;
  uint64_t successfulJobs = 0;
  uint64_t failedJobs = 0;

  auto logFailedItem = [&lc](const cta::common::dataStructures::ArchiveInsertQueueItem& item,
                             const std::string& exceptionMessage,
                             const char* logMsg) {
    log::ScopedParamContainer(lc)
      .add("instanceName", item.instanceName)
      .add("storageClass", item.request.storageClass)
      .add("diskFileID", item.request.diskFileID)
      .add("fileId", item.archiveFileId)
      .add("requesterName", item.request.requester.name)
      .add("requesterGroup", item.request.requester.group)
      .add("exceptionMessage", exceptionMessage)
      .log(log::WARNING, logMsg);
  };

  // Stage 1: catalogue lookup, isolated per item. An item that fails here (e.g. an unknown
  // storage class) gets its own exception on its own promise and is left in place in batch (not
  // touched again). validItems is moved out of batch, by item, only for the items which passed
  // this stage; stage1Indices tracks each validItems[i]'s original index in batch, so those items
  // can be moved back into batch afterwards, once stage 2 has resolved them, since batch (not
  // validItems, which only lives for this function) is what logQueuedArchiveItems() logs from.
  std::vector<cta::common::dataStructures::ArchiveInsertQueueItem> validItems;
  std::vector<size_t> stage1Indices;
  validItems.reserve(batch.size());
  stage1Indices.reserve(batch.size());
  for (size_t i = 0; i < batch.size(); ++i) {
    auto& item = batch[i];
    try {
      cta::common::dataStructures::ArchiveInsertQueueCriteriaKey k {item.instanceName,
                                                                    item.request.storageClass,
                                                                    item.request.requester.name,
                                                                    item.request.requester.group};
      auto it = m_archiveInsertQueueCriteriaCache.find(k);
      if (it != m_archiveInsertQueueCriteriaCache.end()) {
        item.copyToPoolMap = it->second.copyToPoolMap;
        item.mountPolicy = it->second.mountPolicy;
      } else {
        auto queueCriteria = m_catalogue.ArchiveFile()->getArchiveFileQueueCriteria(item.instanceName,
                                                                                    item.request.storageClass,
                                                                                    item.request.requester);
        item.copyToPoolMap = queueCriteria.copyToPoolMap;
        item.mountPolicy = queueCriteria.mountPolicy;
        m_archiveInsertQueueCriteriaCache[k].copyToPoolMap = queueCriteria.copyToPoolMap;
        m_archiveInsertQueueCriteriaCache[k].mountPolicy = queueCriteria.mountPolicy;
        if (m_archiveInsertQueueCriteriaCache.size() > m_archiveInsertQueueCriteriaCacheMaxSize) {
          m_archiveInsertQueueCriteriaCache.clear();
        }
      }
      stage1Indices.push_back(i);
      validItems.push_back(std::move(item));
    } catch (const std::exception& ex) {
      // Preserve the original exception (e.g. a UserError for an unknown storage class) instead
      // of flattening it into a generic error, so the caller sees the same error it would have
      // gotten via the file-by-file path.
      item.promise.set_exception(std::current_exception());
      // Job count for this item is unknown (it never got a copyToPoolMap), so it counts as 1
      // failed request standing in for however many jobs it would have produced.
      failedJobs += 1;
      logFailedItem(item,
                    ex.what(),
                    "In Scheduler::resolveArchiveBatch(): failed to resolve archive queue criteria for request");
    } catch (...) {
      item.promise.set_exception(std::current_exception());
      failedJobs += 1;
      logFailedItem(item,
                    "unknown exception",
                    "In Scheduler::resolveArchiveBatch(): failed to resolve archive queue criteria for request");
    }
  }

  size_t successfulItems = 0;
  if (!validItems.empty()) {
    // Stage 2: bulk insert of the items which passed stage 1. This is one SQL statement in one
    // transaction, so a single bad row fails all of validItems here, not just itself — but unlike
    // stage 1, this is not retried item by item. Stage 1 already isolated the kind of per-item data
    // problem worth isolating (an unknown storage class, a bad requester), so what's left to make
    // the bulk insert itself fail is mostly systemic (lost connection, deadlock, timeout) and would
    // very likely fail every one of the N individual retries identically, while every follower in
    // this batch sits blocked waiting for them all to run out. A genuine one-off case (e.g. a
    // duplicate archiveFileId from a client retry) is recoverable the normal way: the caller gets
    // an error and retries, rather than paying an unbounded-latency tail for every unrelated
    // request in the batch.
    static const char* const failMsg =
      "In Scheduler::resolveArchiveBatch(): bulk archive insert failed, failing this batch";
    auto jobsPerItem = [](const cta::common::dataStructures::ArchiveInsertQueueItem& item) {
      return item.copyToPoolMap.size();
    };
    try {
      auto archiveReqAddrVector = m_db.queueArchive(validItems, lc);

      if (archiveReqAddrVector.size() != validItems.size()) {
        throw exception::Exception("queueArchive returned size " + std::to_string(archiveReqAddrVector.size())
                                   + " but batch size is " + std::to_string(validItems.size()));
      }

      for (size_t i = 0; i < validItems.size(); ++i) {
        validItems[i].promise.set_value(archiveReqAddrVector[i]);
        validItems[i].queued = true;
        successfulJobs += validItems[i].copyToPoolMap.size();
        ++successfulItems;
      }
    } catch (const std::exception& e) {
      cta::failWholeBatch(validItems, lc, e.what(), failMsg, failedJobs, jobsPerItem);
    } catch (...) {
      cta::failWholeBatch(validItems, lc, std::string("unknown exception"), failMsg, failedJobs, jobsPerItem);
    }

    // Move every stage-1-passed item back into its original slot in batch, now carrying its
    // resolved queued/copyToPoolMap/mountPolicy state, so logQueuedArchiveItems() (which only sees
    // batch, not this function's local validItems) can log it afterwards.
    for (size_t i = 0; i < validItems.size(); ++i) {
      batch[stage1Indices[i]] = std::move(validItems[i]);
    }
  }

  // Duration covers this whole batch operation (stage 1 catalogue lookups plus the stage 2 DB
  // insert(s)) — the actual wall time of queueing this batch — timed here rather than by the caller
  // in queueArchiveWithGivenId(), whose own elapsed time also includes the opportunistic-batching
  // wait/sleep, which is about the batching mechanism, not the queueing work itself. Job count only
  // includes items that actually got queued, not merely items which passed stage 1, so a stage-2
  // failure (which fails every item in validItems) doesn't inflate the reported throughput.
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