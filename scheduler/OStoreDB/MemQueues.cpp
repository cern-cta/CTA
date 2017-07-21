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
void MemQueue<objectstore::ArchiveRequest, objectstore::ArchiveQueue>::specializedAddJobToQueue(
  objectstore::ArchiveRequest::JobDump& job, objectstore::ArchiveRequest& request, objectstore::ArchiveQueue& queue) {
  auto af = request.getArchiveFile();
  queue.addJob(job, request.getAddressIfSet(), af.archiveFileID,
      af.fileSize, request.getMountPolicy(), request.getEntryLog().time);
  // Back reference the queue in the job and archive request
  job.owner = queue.getAddressIfSet();
  request.setJobOwner(job.copyNb, job.owner);
}

template<>
void MemQueue<objectstore::RetrieveRequest, objectstore::RetrieveQueue>::specializedAddJobToQueue(
  objectstore::RetrieveRequest::JobDump& job, objectstore::RetrieveRequest& request, objectstore::RetrieveQueue& queue) {
  // We need to find corresponding to the copyNb
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

}} // namespac ecta::ostoredb
