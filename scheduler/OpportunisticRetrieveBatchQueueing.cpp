/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/Catalogue.hpp"
#include "common/exception/UserError.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/SchedulerInstruments.hpp"
#include "common/utils/Timer.hpp"
#include "scheduler/OpportunisticQueueBatcher.hpp"
#include "scheduler/Scheduler.hpp"

#include <future>
#include <mutex>
#include <opentelemetry/context/runtime_context.h>

namespace cta {

//------------------------------------------------------------------------------
// resolveRetrieveInsertCriteria
//------------------------------------------------------------------------------
cta::common::dataStructures::RetrieveFileQueueCriteria
Scheduler::resolveRetrieveInsertCriteria(const std::string& instanceName,
                                         const cta::common::dataStructures::RetrieveRequest& request,
                                         std::optional<std::string>& diskSystemName,
                                         log::LogContext& lc) {
  auto criteria = m_catalogue.TapeFile()->prepareToRetrieveFile(instanceName,
                                                                request.archiveFileID,
                                                                request.requester,
                                                                request.activity,
                                                                lc,
                                                                request.mountPolicy);
  criteria.archiveFile.diskFileInfo = request.diskFileInfo;

  // By default, the scheduler makes its decision based on all available vids. But if a vid is
  // specified in the protobuf, ignore all the others.
  if (request.vid) {
    criteria.archiveFile.tapeFiles.removeAllVidsExcept(*request.vid);
    if (criteria.archiveFile.tapeFiles.empty()) {
      exception::UserError ex;
      ex.getMessage() << "In Scheduler::resolveRetrieveInsertCriteria(): VID " << *request.vid
                      << " does not contain a tape copy of file with archive file ID " << request.archiveFileID;
      throw ex;
    }
  }

  auto diskSystemList = getCachedDiskSystemList();
  try {
    diskSystemName = diskSystemList.getDSName(request.dstURL);
  } catch (std::out_of_range&) {
    // If there is no match the function throws an out of range exception. Not a real error:
    // it just means this request's destination does not match any declared disk system.
  }

  return criteria;
}

//------------------------------------------------------------------------------
// getCachedDiskSystemList
//------------------------------------------------------------------------------
disk::DiskSystemList Scheduler::getCachedDiskSystemList() {
  const auto now = std::chrono::steady_clock::now();

  // Same single-flight coalescing pattern as resolveArchiveInsertCriteria()'s cache, just simpler:
  // there is only one value here, not one per key, so a plain optional<shared_future<>> stands in
  // for what was a map keyed by ArchiveInsertQueueCriteriaKey there.
  std::promise<disk::DiskSystemList> fetchPromise;
  std::shared_future<disk::DiskSystemList> waitOnFuture;
  bool isFetcher = false;
  {
    std::lock_guard<std::mutex> cacheLock(m_diskSystemListCacheMutex);
    if (m_diskSystemListCache.has_value() && (now - m_diskSystemListCachedAt) < m_diskSystemListCacheTtl) {
      return *m_diskSystemListCache;
    }
    if (m_diskSystemListInFlight.has_value()) {
      waitOnFuture = *m_diskSystemListInFlight;
    } else {
      isFetcher = true;
      waitOnFuture = fetchPromise.get_future().share();
      m_diskSystemListInFlight = waitOnFuture;
    }
  }

  if (!isFetcher) {
    return waitOnFuture.get();
  }

  try {
    auto list = m_catalogue.DiskSystem()->getAllDiskSystems();
    {
      std::lock_guard<std::mutex> cacheLock(m_diskSystemListCacheMutex);
      m_diskSystemListCache.emplace(list);
      m_diskSystemListCachedAt = now;
      m_diskSystemListInFlight.reset();
    }
    fetchPromise.set_value(list);
    return list;
  } catch (...) {
    {
      std::lock_guard<std::mutex> cacheLock(m_diskSystemListCacheMutex);
      m_diskSystemListInFlight.reset();
    }
    fetchPromise.set_exception(std::current_exception());
    throw;
  }
}

//------------------------------------------------------------------------------
// resolveRetrieveBatch
//------------------------------------------------------------------------------
void Scheduler::resolveRetrieveBatch(std::vector<cta::common::dataStructures::RetrieveInsertQueueItem>& batch,
                                     log::LogContext& lc) {
  cta::utils::Timer batchTimer;
  uint64_t failedItems = 0;

  // Every item reaching this point already carries resolved criteria/diskSystemName: stage 1 (the
  // catalogue lookup and disk-system-name resolution) now runs in resolveRetrieveInsertCriteria(),
  // on each caller's own thread, before the item is even enqueued -- see that method's own comment
  // for why. A request whose lookup failed never reached here at all, having already thrown directly
  // from Scheduler::queueRetrieve(). So this is stage 2 only: one bulk insert for the whole batch.
  size_t successfulItems = 0;
  if (!batch.empty()) {
    static const char* const failMsg =
      "In Scheduler::resolveRetrieveBatch(): bulk retrieve insert failed, failing this batch";
    // A retrieve request is always exactly one job (one copy read), unlike archive requests, which
    // can fan out into several — so failure here is always counted as 1 per item.
    auto oneJobPerItem = [](const cta::common::dataStructures::RetrieveInsertQueueItem&) -> uint64_t { return 1; };
    try {
      auto requestIds = m_db.queueRetrieve(batch, lc);

      if (requestIds.size() != batch.size()) {
        throw exception::Exception("queueRetrieve returned size " + std::to_string(requestIds.size())
                                   + " but batch size is " + std::to_string(batch.size()));
      }

      for (size_t i = 0; i < batch.size(); ++i) {
        batch[i].promise.set_value(requestIds[i]);
        batch[i].queued = true;
        ++successfulItems;
      }
    } catch (const std::exception& e) {
      cta::failWholeBatch(batch, lc, e.what(), failMsg, failedItems, oneJobPerItem);
    } catch (...) {
      cta::failWholeBatch(batch, lc, std::string("unknown exception"), failMsg, failedItems, oneJobPerItem);
    }
  }

  auto batchTimeMSecs = batchTimer.msecs();
  cta::telemetry::metrics::ctaSchedulerOperationDuration->Record(
    batchTimeMSecs,
    {
      {cta::semconv::attr::kSchedulerOperationName,     cta::semconv::attr::SchedulerOperationNameValues::kEnqueue},
      {cta::semconv::attr::kSchedulerOperationWorkflow,
       cta::semconv::attr::SchedulerOperationWorkflowValues::kRetrieve                                            }
  },
    opentelemetry::context::RuntimeContext::GetCurrent());
  cta::telemetry::metrics::ctaSchedulerOperationJobCount->Add(
    successfulItems,
    {
      {cta::semconv::attr::kSchedulerOperationName,     cta::semconv::attr::SchedulerOperationNameValues::kEnqueue},
      {cta::semconv::attr::kSchedulerOperationWorkflow,
       cta::semconv::attr::SchedulerOperationWorkflowValues::kRetrieve                                            }
  },
    opentelemetry::context::RuntimeContext::GetCurrent());
  if (failedItems > 0) {
    cta::telemetry::metrics::ctaSchedulerOperationJobCount->Add(
      failedItems,
      {
        {cta::semconv::attr::kSchedulerOperationName,     cta::semconv::attr::SchedulerOperationNameValues::kEnqueue},
        {cta::semconv::attr::kSchedulerOperationWorkflow,
         cta::semconv::attr::SchedulerOperationWorkflowValues::kRetrieve                                            },
        {cta::semconv::attr::kErrorType,                  cta::semconv::attr::ErrorTypeValues::kException           }
    },
      opentelemetry::context::RuntimeContext::GetCurrent());
  }

  log::ScopedParamContainer(lc)
    .add("batchSize", batch.size())
    .add("successfulItems", successfulItems)
    .add("failedItems", batch.size() - successfulItems)
    .log(log::INFO, "In Scheduler::resolveRetrieveBatch(): processed a batch of retrieve requests.");
}

//------------------------------------------------------------------------------
// logQueuedRetrieveItems
//------------------------------------------------------------------------------
void Scheduler::logQueuedRetrieveItems(std::vector<cta::common::dataStructures::RetrieveInsertQueueItem>& batch,
                                       log::LogContext& lc) {
  // Per-item audit log, mirroring the file-by-file path's own "Queued retrieve request" INFO line
  // (same fields), run only for items resolveRetrieveBatch() actually queued, after followers have
  // already been released and don't wait on it. catalogueTime/schedulerDbTime don't apply here (that
  // work is shared across the whole batch, not attributable to one item), so batchSize is logged in
  // their place instead.
  for (const auto& item : batch) {
    if (!item.queued) {
      continue;
    }
    log::ScopedParamContainer spc(lc);
    spc.add("fileId", item.request.archiveFileID)
      .add("instanceName", item.instanceName)
      .add("diskSystemName", item.diskSystemName.value_or(""))
      .add("diskFilePath", item.request.diskFileInfo.path)
      .add("diskFileOwnerUid", item.request.diskFileInfo.owner_uid)
      .add("diskFileGid", item.request.diskFileInfo.gid)
      .add("dstURL", item.request.dstURL)
      .add("errorReportURL", item.request.errorReportURL)
      .add("creationHost", item.request.creationLog.host)
      .add("creationTime", item.request.creationLog.time)
      .add("creationUser", item.request.creationLog.username)
      .add("requesterName", item.request.requester.name)
      .add("requesterGroup", item.request.requester.group)
      .add("criteriaArchiveFileId", item.criteria.archiveFile.archiveFileID)
      .add("criteriaCreationTime", item.criteria.archiveFile.creationTime)
      .add("criteriaDiskFileId", item.criteria.archiveFile.diskFileId)
      .add("criteriaDiskFileOwnerUid", item.criteria.archiveFile.diskFileInfo.owner_uid)
      .add("criteriaDiskInstance", item.criteria.archiveFile.diskInstance)
      .add("criteriaFileSize", item.criteria.archiveFile.fileSize)
      .add("reconciliationTime", item.criteria.archiveFile.reconciliationTime)
      .add("storageClass", item.criteria.archiveFile.storageClass);
    item.criteria.archiveFile.checksumBlob.addFirstChecksumToLog(spc);

    if (!item.criteria.archiveFile.tapeFiles.empty()) {
      const auto& tapeFile = item.criteria.archiveFile.tapeFiles.front();
      spc.add("fSeq", tapeFile.fSeq)
        .add("vid", tapeFile.vid)
        .add("blockId", tapeFile.blockId)
        .add("fileSize", tapeFile.fileSize)
        .add("copyNb", tapeFile.copyNb)
        .add("creationTime", tapeFile.creationTime);
    }

    spc.add("selectedVid", item.selectedVid)
      .add("verifyOnly", item.request.isVerifyOnly)
      .add("policyName", item.criteria.mountPolicy.name)
      .add("policyMinAge", item.criteria.mountPolicy.retrieveMinRequestAge)
      .add("policyPriority", item.criteria.mountPolicy.retrievePriority)
      .add("batchSize", batch.size());
    if (item.request.activity) {
      spc.add("activity", item.request.activity.value());
    }
    lc.log(log::INFO, "In Scheduler::logQueuedRetrieveItems(): Queued retrieve request");
  }
}

}  // namespace cta
