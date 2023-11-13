
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

#include "ArchiveQueueShard.hpp"
#include "GenericObject.hpp"
#include <google/protobuf/util/json_util.h>



namespace cta::objectstore {

ArchiveQueueShard::ArchiveQueueShard(Backend& os):
  ObjectOps<serializers::ArchiveQueueShard, serializers::ArchiveQueueShard_t>(os) { }

ArchiveQueueShard::ArchiveQueueShard(const std::string& address, Backend& os):
  ObjectOps<serializers::ArchiveQueueShard, serializers::ArchiveQueueShard_t>(os, address) { }

ArchiveQueueShard::ArchiveQueueShard(GenericObject& go):
  ObjectOps<serializers::ArchiveQueueShard, serializers::ArchiveQueueShard_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

void ArchiveQueueShard::rebuild() {
  checkPayloadWritable();
  uint64_t totalSize=0;
  for (auto j: m_payload.archivejobs()) {
    totalSize += j.size();
  }
  m_payload.set_archivejobstotalsize(totalSize);
}

std::string ArchiveQueueShard::dump() {
  checkPayloadReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}

void ArchiveQueueShard::garbageCollect(const std::string& presumedOwner, AgentReference& agentReference, log::LogContext& lc, cta::catalogue::Catalogue& catalogue) {
  throw exception::Exception("In ArchiveQueueShard::garbageCollect(): garbage collection should not be necessary for this type of object.");
}

ArchiveQueue::CandidateJobList ArchiveQueueShard::getCandidateJobList(uint64_t maxBytes, uint64_t maxFiles, const std::set<std::string> &archiveRequestsToSkip) {
  checkPayloadReadable();
  ArchiveQueue::CandidateJobList ret;
  ret.remainingBytesAfterCandidates = m_payload.archivejobstotalsize();
  ret.remainingFilesAfterCandidates = m_payload.archivejobs_size();
  for (auto & j: m_payload.archivejobs()) {
    if (!archiveRequestsToSkip.count(j.address())) {
      ret.candidates.push_back({j.size(), j.address(), (uint16_t)j.copynb()});
      ret.candidateBytes += j.size();
      ret.candidateFiles ++;
    }
    ret.remainingBytesAfterCandidates -= j.size();
    ret.remainingFilesAfterCandidates--;
    if (ret.candidateBytes >= maxBytes || ret.candidateFiles >= maxFiles) break;
  }
  return ret;
}

auto ArchiveQueueShard::removeJobs(const std::list<std::string>& jobsToRemove) -> RemovalResult {
  checkPayloadWritable();
  RemovalResult ret;
  uint64_t totalSize = m_payload.archivejobstotalsize();
  auto * jl=m_payload.mutable_archivejobs();
  for (auto &rrt: jobsToRemove) {
    bool found = false;
    do {
      found = false;
      // Push the found entry all the way to the end.
      for (size_t i=0; i<(size_t)jl->size(); i++) {
        if (jl->Get(i).address() == rrt) {
          found = true;
          const auto & j = jl->Get(i);
          ret.removedJobs.emplace_back(JobInfo());
          ret.removedJobs.back().address = j.address();
          ret.removedJobs.back().copyNb = j.copynb();
          ret.removedJobs.back().minArchiveRequestAge = j.minarchiverequestage();
          ret.removedJobs.back().priority = j.priority();
          ret.removedJobs.back().size = j.size();
          ret.removedJobs.back().startTime = j.starttime();
          ret.removedJobs.back().mountPolicyName = j.mountpolicyname();
          ret.bytesRemoved += j.size();
          totalSize -= j.size();
          ret.jobsRemoved++;
          m_payload.set_archivejobstotalsize(m_payload.archivejobstotalsize() - j.size());
          while (i+1 < (size_t)jl->size()) {
            jl->SwapElements(i, i+1);
            i++;
          }
          break;
        }
      }
      // and remove it
      if (found)
        jl->RemoveLast();
    } while (found);
  }
  ret.bytesAfter = totalSize;
  ret.jobsAfter = m_payload.archivejobs_size();
  return ret;
}

void ArchiveQueueShard::initialize(const std::string& owner) {
  ObjectOps<serializers::ArchiveQueueShard, serializers::ArchiveQueueShard_t>::initialize();
  setOwner(owner);
  setBackupOwner(owner);
  m_payload.set_archivejobstotalsize(0);
  m_payloadInterpreted=true;
}

auto ArchiveQueueShard::dumpJobs() -> std::list<JobInfo> {
  checkPayloadReadable();
  std::list<JobInfo> ret;
  for (auto &j: m_payload.archivejobs()) {
    ret.emplace_back(JobInfo{j.size(), j.address(), (uint16_t)j.copynb(), j.priority(),
        j.minarchiverequestage(), (time_t)j.starttime(),j.mountpolicyname()});
  }
  return ret;
}

auto ArchiveQueueShard::getJobsSummary() -> JobsSummary {
  checkPayloadReadable();
  JobsSummary ret;
  ret.bytes = m_payload.archivejobstotalsize();
  ret.jobs = m_payload.archivejobs_size();
  return ret;
}

uint64_t ArchiveQueueShard::addJob(ArchiveQueue::JobToAdd& jobToAdd) {
  checkPayloadWritable();
  auto * j = m_payload.add_archivejobs();
  j->set_address(jobToAdd.archiveRequestAddress);
  j->set_size(jobToAdd.fileSize);
  j->set_fileid(jobToAdd.archiveFileId);
  j->set_copynb(jobToAdd.job.copyNb);
  j->set_priority(jobToAdd.policy.archivePriority);
  j->set_minarchiverequestage(jobToAdd.policy.archiveMinRequestAge);
  j->set_starttime(jobToAdd.startTime);
  j->set_mountpolicyname(jobToAdd.policy.name);
  m_payload.set_archivejobstotalsize(m_payload.archivejobstotalsize()+jobToAdd.fileSize);
  return m_payload.archivejobs_size();
}

void ArchiveQueueShard::initialize() {
  throw std::runtime_error("initialize() is not supported for ArchiveQueueShard");
}

} // namespace cta::objectstore
