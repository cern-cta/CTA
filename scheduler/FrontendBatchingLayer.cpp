/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/FrontendBatchingLayer.hpp"

#include "common/exception/UserError.hpp"
#include "scheduler/Scheduler.hpp"

namespace cta {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
FrontendBatchingLayer::FrontendBatchingLayer(Scheduler& scheduler,
                                             const bool enableOpportunisticBatching,
                                             const uint64_t opportunisticBatchingWindowMs,
                                             const uint64_t opportunisticBatchingMaxBatchSize)
    : m_scheduler(scheduler),
      m_catalogue(scheduler.getCatalogue()),
      m_db(scheduler.getDb()),
      m_enableOpportunisticBatching(enableOpportunisticBatching),
      m_opportunisticBatchingWindow(opportunisticBatchingWindowMs),
      m_opportunisticBatchingMaxBatchSize(opportunisticBatchingMaxBatchSize) {
  m_archiveBatcher =
    std::make_unique<OpportunisticQueueBatcher<common::dataStructures::ArchiveInsertQueueItem, std::string>>(
      m_opportunisticBatchingWindow,
      m_opportunisticBatchingMaxBatchSize,
      [this](std::vector<common::dataStructures::ArchiveInsertQueueItem>& batch, log::LogContext& lc) {
        resolveArchiveBatch(batch, lc);
      },
      [this](std::vector<common::dataStructures::ArchiveInsertQueueItem>& batch, log::LogContext& lc) {
        logQueuedArchiveItems(batch, lc);
      });
  m_retrieveBatcher =
    std::make_unique<OpportunisticQueueBatcher<common::dataStructures::RetrieveInsertQueueItem, std::string>>(
      m_opportunisticBatchingWindow,
      m_opportunisticBatchingMaxBatchSize,
      [this](std::vector<common::dataStructures::RetrieveInsertQueueItem>& batch, log::LogContext& lc) {
        resolveRetrieveBatch(batch, lc);
      },
      [this](std::vector<common::dataStructures::RetrieveInsertQueueItem>& batch, log::LogContext& lc) {
        logQueuedRetrieveItems(batch, lc);
      });
}

//------------------------------------------------------------------------------
// queueArchiveWithGivenId
//------------------------------------------------------------------------------
std::string FrontendBatchingLayer::queueArchiveWithGivenId(const uint64_t archiveFileId,
                                                           const std::string& instanceName,
                                                           const cta::common::dataStructures::ArchiveRequest& request,
                                                           log::LogContext& lc) {
  if (!request.fileSize) {
    throw cta::exception::UserError(
      std::string(
        "In FrontendBatchingLayer::queueArchiveWithGivenId(): Rejecting archive request for zero-length file: ")
      + request.diskFileInfo.path);
  }

  if (m_enableOpportunisticBatching) {
    // Stage 1 (catalogue lookup) runs here, on this caller's own thread, before the item is
    // enqueued — not inside resolveArchiveBatch() on the single leader thread — so concurrent
    // callers' catalogue lookups run in parallel with each other instead of being serialized one at
    // a time inside the batch's critical round-latency window. Throws directly (never enqueuing)
    // on failure, same observable per-request isolation as before, just resolved earlier.
    auto criteria = resolveArchiveInsertCriteria(instanceName, request.storageClass, request.requester, lc);

    // m_archiveBatcher handles the leader/follower coordination, the window+cap wait, and releasing
    // followers as soon as resolveArchiveBatch() has settled every promise in the batch — before the
    // slower logQueuedArchiveItems() runs, so no follower waits on it. See OpportunisticQueueBatcher.hpp.
    return m_archiveBatcher->enqueueAndWait(
      cta::common::dataStructures::ArchiveInsertQueueItem {.archiveFileId = archiveFileId,
                                                           .instanceName = instanceName,
                                                           .request = request,
                                                           .copyToPoolMap = std::move(criteria.copyToPoolMap),
                                                           .mountPolicy = std::move(criteria.mountPolicy),
                                                           .promise = std::promise<std::string>()},
      lc);
  }

  return m_scheduler.queueArchiveWithGivenId(archiveFileId, instanceName, request, lc);
}

//------------------------------------------------------------------------------
// queueRetrieve
//------------------------------------------------------------------------------
std::string FrontendBatchingLayer::queueRetrieve(const std::string& instanceName,
                                                 cta::common::dataStructures::RetrieveRequest& request,
                                                 log::LogContext& lc) {
  if (m_enableOpportunisticBatching) {
    // Stage 1 (catalogue lookup and disk-system-name resolution) runs here, on this caller's own
    // thread, before the item is enqueued — not inside resolveRetrieveBatch() on the single leader
    // thread — so concurrent callers' catalogue lookups run in parallel with each other instead of
    // being serialized one at a time inside the batch's critical round-latency window. Throws
    // directly (never enqueuing) on failure, same observable per-request isolation as before.
    std::optional<std::string> diskSystemName;
    auto criteria = resolveRetrieveInsertCriteria(instanceName, request, diskSystemName, lc);

    // m_retrieveBatcher handles the leader/follower coordination, the window+cap wait, and releasing
    // followers as soon as resolveRetrieveBatch() has settled every promise in the batch — before the
    // slower logQueuedRetrieveItems() runs, so no follower waits on it. See OpportunisticQueueBatcher.hpp.
    // Unlike queueArchiveWithGivenId(), request is copied into the item rather than referenced: the
    // only caller-visible mutation the file-by-file path below makes to it (appendFileSizeToDstURL())
    // is consumed entirely inside the DB insert, not read back by the caller afterwards, so a copy
    // costs nothing observable.
    return m_retrieveBatcher->enqueueAndWait(
      cta::common::dataStructures::RetrieveInsertQueueItem {.instanceName = instanceName,
                                                            .request = request,
                                                            .criteria = std::move(criteria),
                                                            .diskSystemName = std::move(diskSystemName),
                                                            .promise = std::promise<std::string>()},
      lc);
  }

  return m_scheduler.queueRetrieve(instanceName, request, lc);
}

}  // namespace cta