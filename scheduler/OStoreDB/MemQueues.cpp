/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MemQueues.hpp"
#include "OStoreDB.hpp"
#include "objectstore/Helpers.hpp"
#include "common/dataStructures/RetrieveJobToAdd.hpp"

namespace cta::ostoredb {

template<>
void MemQueue<objectstore::ArchiveRequest, objectstore::ArchiveQueue>::specializedAddJobsToQueueAndCommit(
  std::list<MemQueue<objectstore::ArchiveRequest, objectstore::ArchiveQueue>::JobAndRequest> & jobsToAdd,
  objectstore::ArchiveQueue& queue, objectstore::AgentReference & agentReference, log::LogContext & logContext) {
  std::list<objectstore::ArchiveQueue::JobToAdd> jtal;
  auto queueAddress = queue.getAddressIfSet();
  for (auto & j: jobsToAdd) {
    jtal.push_back({j.job, j.request.getAddressIfSet(), j.request.getArchiveFile().archiveFileID, j.request.getArchiveFile().fileSize,
      j.request.getMountPolicy(), j.request.getEntryLog().time});
    // We pre-mark (in memory) request as being owned by the queue.
    // The actual commit of the request will happen after the queue's,
    // so the back reference will be valid.
    j.job.owner = queueAddress;
    j.request.setJobOwner(j.job.copyNb, j.job.owner);
  }
  queue.addJobsAndCommit(jtal, agentReference, logContext);
}

template<>
void MemQueue<objectstore::RetrieveRequest, objectstore::RetrieveQueue>::specializedAddJobsToQueueAndCommit(
  std::list<MemQueue<objectstore::RetrieveRequest, objectstore::RetrieveQueue>::JobAndRequest> & jobsToAdd,
  objectstore::RetrieveQueue &queue, objectstore::AgentReference & agentReference, log::LogContext & logContext) {
  std::list<common::dataStructures::RetrieveJobToAdd> jtal;
  auto queueAddress = queue.getAddressIfSet();
  for (auto & jta: jobsToAdd) {
    // We need to find the job corresponding to the copyNb
    auto & job = jta.job;
    auto & request = jta.request;
    for (auto & j: request.getArchiveFile().tapeFiles) {
      if (j.copyNb == job.copyNb) {
        auto criteria = request.getRetrieveFileQueueCriteria();
        jtal.emplace_back(j.copyNb, j.fSeq, request.getAddressIfSet(), criteria.archiveFile.fileSize,
            criteria.mountPolicy, request.getEntryLog().time, request.getActivity(), request.getDiskSystemName());
        request.setActiveCopyNumber(j.copyNb);
        request.setOwner(queueAddress);
        goto jobAdded;
      }
    }
  jobAdded:;
  }
  queue.addJobsAndCommit(jtal, agentReference, logContext);
}

template<>
void MemQueue<objectstore::ArchiveRequest,objectstore::ArchiveQueue>::specializedUpdateCachedQueueStats(objectstore::ArchiveQueue &queue, log::LogContext &lc) {}

template<>
void MemQueue<objectstore::RetrieveRequest,objectstore::RetrieveQueue>::specializedUpdateCachedQueueStats(objectstore::RetrieveQueue &queue, log::LogContext &lc) {
  auto summary = queue.getJobsSummary();
  objectstore::Helpers::updateRetrieveQueueStatisticsCache(queue.getVid(), summary.jobs, summary.bytes, summary.priority, lc);
}

} // namespace cta::ostoredb
