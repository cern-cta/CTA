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

#include <opentelemetry/context/runtime_context.h>

namespace cta {

//------------------------------------------------------------------------------
// resolveRetrieveBatch
//------------------------------------------------------------------------------
void Scheduler::resolveRetrieveBatch(std::vector<cta::common::dataStructures::RetrieveInsertQueueItem>& batch,
                                     log::LogContext& lc) {
  cta::utils::Timer batchTimer;
  uint64_t failedItems = 0;

  auto logFailedItem = [&lc](const cta::common::dataStructures::RetrieveInsertQueueItem& item,
                             const std::string& exceptionMessage,
                             const char* logMsg) {
    log::ScopedParamContainer(lc)
      .add("instanceName", item.instanceName)
      .add("fileId", item.request.archiveFileID)
      .add("requesterName", item.request.requester.name)
      .add("requesterGroup", item.request.requester.group)
      .add("exceptionMessage", exceptionMessage)
      .log(log::WARNING, logMsg);
  };

  // Stage 1: catalogue lookup and disk-system-name resolution, isolated per item — same pattern as
  // resolveArchiveBatch()'s stage 1. diskSystemList is fetched once for the whole batch rather than
  // once per item, since it does not depend on any per-item state.
  auto diskSystemList = m_catalogue.DiskSystem()->getAllDiskSystems();
  std::vector<cta::common::dataStructures::RetrieveInsertQueueItem> validItems;
  std::vector<size_t> stage1Indices;
  validItems.reserve(batch.size());
  stage1Indices.reserve(batch.size());
  for (size_t i = 0; i < batch.size(); ++i) {
    auto& item = batch[i];
    try {
      item.criteria = m_catalogue.TapeFile()->prepareToRetrieveFile(item.instanceName,
                                                                    item.request.archiveFileID,
                                                                    item.request.requester,
                                                                    item.request.activity,
                                                                    lc,
                                                                    item.request.mountPolicy);
      item.criteria.archiveFile.diskFileInfo = item.request.diskFileInfo;

      // By default, the scheduler makes its decision based on all available vids. But if a vid is
      // specified in the protobuf, ignore all the others.
      if (item.request.vid) {
        item.criteria.archiveFile.tapeFiles.removeAllVidsExcept(*item.request.vid);
        if (item.criteria.archiveFile.tapeFiles.empty()) {
          exception::UserError ex;
          ex.getMessage() << "In Scheduler::resolveRetrieveBatch(): VID " << *item.request.vid
                          << " does not contain a tape copy of file with archive file ID "
                          << item.request.archiveFileID;
          throw ex;
        }
      }

      try {
        item.diskSystemName = diskSystemList.getDSName(item.request.dstURL);
      } catch (std::out_of_range&) {
        // If there is no match the function throws an out of range exception. Not a real error:
        // it just means this request's destination does not match any declared disk system.
      }

      stage1Indices.push_back(i);
      validItems.push_back(std::move(item));
    } catch (const std::exception& ex) {
      item.promise.set_exception(std::current_exception());
      failedItems += 1;
      logFailedItem(item,
                    ex.what(),
                    "In Scheduler::resolveRetrieveBatch(): failed to resolve retrieve queue criteria for request");
    } catch (...) {
      item.promise.set_exception(std::current_exception());
      failedItems += 1;
      logFailedItem(item,
                    "unknown exception",
                    "In Scheduler::resolveRetrieveBatch(): failed to resolve retrieve queue criteria for request");
    }
  }

  size_t successfulItems = 0;
  if (!validItems.empty()) {
    // Stage 2: bulk insert of the items which passed stage 1, which also selects the vid to read
    // each one from (see RelationalDB::queueRetrieve()'s bulk overload). Same all-or-nothing policy
    // as resolveArchiveBatch()'s stage 2, and for the same reason: stage 1 already isolated the
    // per-item problems worth isolating, so what's left to make this fail is mostly systemic.
    static const char* const failMsg =
      "In Scheduler::resolveRetrieveBatch(): bulk retrieve insert failed, failing this batch";
    // A retrieve request is always exactly one job (one copy read), unlike archive requests, which
    // can fan out into several — so failure here is always counted as 1 per item.
    auto oneJobPerItem = [](const cta::common::dataStructures::RetrieveInsertQueueItem&) -> uint64_t { return 1; };
    try {
      auto requestIds = m_db.queueRetrieve(validItems, lc);

      if (requestIds.size() != validItems.size()) {
        throw exception::Exception("queueRetrieve returned size " + std::to_string(requestIds.size())
                                   + " but batch size is " + std::to_string(validItems.size()));
      }

      for (size_t i = 0; i < validItems.size(); ++i) {
        validItems[i].promise.set_value(requestIds[i]);
        validItems[i].queued = true;
        ++successfulItems;
      }
    } catch (const std::exception& e) {
      cta::failWholeBatch(validItems, lc, e.what(), failMsg, failedItems, oneJobPerItem);
    } catch (...) {
      cta::failWholeBatch(validItems, lc, std::string("unknown exception"), failMsg, failedItems, oneJobPerItem);
    }

    // Move every stage-1-passed item back into its original slot in batch, now carrying its
    // resolved queued/criteria/selectedVid state, so logQueuedRetrieveItems() (which only sees
    // batch, not this function's local validItems) can log it afterwards.
    for (size_t i = 0; i < validItems.size(); ++i) {
      batch[stage1Indices[i]] = std::move(validItems[i]);
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