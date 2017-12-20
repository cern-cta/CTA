/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/helgrind_annotator.hpp"
#include "MemQueues.hpp"
#include "OStoreDB.hpp"
#include "objectstore/Helpers.hpp"

namespace cta { namespace ostoredb {

template<>
void MemQueue<objectstore::ArchiveRequest, objectstore::ArchiveQueue>::specializedAddJobsToQueue(
  std::list<MemQueue<objectstore::ArchiveRequest, objectstore::ArchiveQueue>::JobAndRequest> & jobsToAdd,
  objectstore::ArchiveQueue& queue) {
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
  queue.addJobs(jtal);
}

template<>
void MemQueue<objectstore::RetrieveRequest, objectstore::RetrieveQueue>::specializedAddJobsToQueue(
  std::list<MemQueue<objectstore::RetrieveRequest, objectstore::RetrieveQueue>::JobAndRequest> & jobsToAdd,
  objectstore::RetrieveQueue &queue) {
  for (auto & jta: jobsToAdd) {
    // We need to find corresponding to the copyNb
    auto & job = jta.job;
    auto & request = jta.request;
    for (auto & j: request.getArchiveFile().tapeFiles) {
      if (j.second.copyNb == job.copyNb) {
        auto criteria = request.getRetrieveFileQueueCriteria();
        queue.addJob(j.second.copyNb, j.second.fSeq, request.getAddressIfSet(), criteria.archiveFile.fileSize, 
            criteria.mountPolicy, request.getEntryLog().time);
        request.setActiveCopyNumber(j.second.copyNb);
        request.setOwner(queue.getAddressIfSet());
        goto jobAdded;
      }
    }
  jobAdded:;
  }
}

template<>
void MemQueue<objectstore::ArchiveRequest,objectstore::ArchiveQueue>::specializedUpdateCachedQueueStats(objectstore::ArchiveQueue &queue) {}

template<>
void MemQueue<objectstore::RetrieveRequest,objectstore::RetrieveQueue>::specializedUpdateCachedQueueStats(objectstore::RetrieveQueue &queue) {
  auto summary = queue.getJobsSummary();
  objectstore::Helpers::updateRetrieveQueueStatisticsCache(queue.getVid(), summary.files, summary.bytes, summary.priority);
}

}} // namespac ecta::ostoredb
