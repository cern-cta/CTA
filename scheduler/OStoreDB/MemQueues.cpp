/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "MemQueues.hpp"
#include "OStoreDB.hpp"
#include "objectstore/Helpers.hpp"

namespace cta { namespace ostoredb {

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
  std::list<objectstore::RetrieveQueue::JobToAdd> jtal;
  auto queueAddress = queue.getAddressIfSet();
  for (auto & jta: jobsToAdd) {
    // We need to find the job corresponding to the copyNb
    auto & job = jta.job;
    auto & request = jta.request;
    for (auto & j: request.getArchiveFile().tapeFiles) {
      if (j.copyNb == job.copyNb) {
        auto criteria = request.getRetrieveFileQueueCriteria();
        jtal.push_back({j.copyNb, j.fSeq, request.getAddressIfSet(), criteria.archiveFile.fileSize,
            criteria.mountPolicy, request.getEntryLog().time, request.getActivity(), request.getDiskSystemName()});
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
void MemQueue<objectstore::ArchiveRequest,objectstore::ArchiveQueue>::specializedUpdateCachedQueueStats(objectstore::ArchiveQueue &queue) {}

template<>
void MemQueue<objectstore::RetrieveRequest,objectstore::RetrieveQueue>::specializedUpdateCachedQueueStats(objectstore::RetrieveQueue &queue) {
  auto summary = queue.getJobsSummary();
  objectstore::Helpers::updateRetrieveQueueStatisticsCache(queue.getVid(), summary.jobs, summary.bytes, summary.priority);
}

}} // namespac ecta::ostoredb
