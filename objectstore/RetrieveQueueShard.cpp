
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

#include "RetrieveQueueShard.hpp"
#include "GenericObject.hpp"
#include <google/protobuf/util/json_util.h>



namespace cta { namespace objectstore {

RetrieveQueueShard::RetrieveQueueShard(Backend& os):
  ObjectOps<serializers::RetrieveQueueShard, serializers::RetrieveQueueShard_t>(os) { }

RetrieveQueueShard::RetrieveQueueShard(const std::string& address, Backend& os):
  ObjectOps<serializers::RetrieveQueueShard, serializers::RetrieveQueueShard_t>(os, address) { }

RetrieveQueueShard::RetrieveQueueShard(GenericObject& go):
  ObjectOps<serializers::RetrieveQueueShard, serializers::RetrieveQueueShard_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

void RetrieveQueueShard::rebuild() {
  checkPayloadWritable();
  uint64_t totalSize=0;
  for (auto j: m_payload.retrievejobs()) {
    totalSize += j.size();
  }
  m_payload.set_retrievejobstotalsize(totalSize);
}

std::string RetrieveQueueShard::dump() {  
  checkPayloadReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}

void RetrieveQueueShard::garbageCollect(const std::string& presumedOwner, AgentReference& agentReference, log::LogContext& lc, cta::catalogue::Catalogue& catalogue) {
  throw exception::Exception("In RetrieveQueueShard::garbageCollect(): garbage collection should not be necessary for this type of object.");
}

RetrieveQueue::CandidateJobList RetrieveQueueShard::getCandidateJobList(uint64_t maxBytes, uint64_t maxFiles, std::set<std::string> retrieveRequestsToSkip) {
  checkPayloadReadable();
  RetrieveQueue::CandidateJobList ret;
  ret.remainingBytesAfterCandidates = m_payload.retrievejobstotalsize();
  ret.remainingFilesAfterCandidates = m_payload.retrievejobs_size();
  for (auto & j: m_payload.retrievejobs()) {
    if (!retrieveRequestsToSkip.count(j.address())) {
      ret.candidates.push_back({j.address(), (uint16_t)j.copynb(), j.size()});
      ret.candidateBytes += j.size();
      ret.candidateFiles ++;
    }
    ret.remainingBytesAfterCandidates -= j.size();
    ret.remainingFilesAfterCandidates--;
    if (ret.candidateBytes >= maxBytes || ret.candidateFiles >= maxFiles) break;
  }
  return ret;
}

auto RetrieveQueueShard::removeJobs(const std::list<std::string>& jobsToRemove) -> RemovalResult {
  checkPayloadWritable();
  RemovalResult ret;
  uint64_t totalSize = m_payload.retrievejobstotalsize();
  auto * jl=m_payload.mutable_retrievejobs();
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
          ret.removedJobs.back().maxDrivesAllowed = j.maxdrivesallowed();
          ret.removedJobs.back().minRetrieveRequestAge = j.minretrieverequestage();
          ret.removedJobs.back().priority = j.priority();
          ret.removedJobs.back().size = j.size();
          ret.removedJobs.back().startTime = j.starttime();
          ret.bytesRemoved += j.size();
          totalSize -= j.size();
          ret.jobsRemoved++;
          m_payload.set_retrievejobstotalsize(m_payload.retrievejobstotalsize() - j.size());
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
  ret.jobsAfter = m_payload.retrievejobs_size();
  return ret;
}

void RetrieveQueueShard::initialize(const std::string& owner) {
  ObjectOps<serializers::RetrieveQueueShard, serializers::RetrieveQueueShard_t>::initialize();
  setOwner(owner);
  setBackupOwner(owner);
  m_payload.set_retrievejobstotalsize(0);
  m_payloadInterpreted=true;
}

auto RetrieveQueueShard::dumpJobs() -> std::list<JobInfo> { 
  checkPayloadReadable();
  std::list<JobInfo> ret;
  for (auto &j: m_payload.retrievejobs()) {
    ret.emplace_back(JobInfo{j.size(), j.address(), (uint16_t)j.copynb(), j.priority(), 
        j.minretrieverequestage(), j.maxdrivesallowed(), (time_t)j.starttime(), j.fseq()});
  }
  return ret;
}

std::list<RetrieveQueue::JobToAdd> RetrieveQueueShard::dumpJobsToAdd() {
  checkPayloadReadable();
  std::list<RetrieveQueue::JobToAdd> ret;
  for (auto &j: m_payload.retrievejobs()) {
    ret.emplace_back(RetrieveQueue::JobToAdd());
    ret.back().copyNb = j.copynb();
    ret.back().fSeq = j.fseq();
    ret.back().fileSize = j.size();
    ret.back().policy.retrieveMinRequestAge = j.minretrieverequestage();
    ret.back().policy.maxDrivesAllowed = j.maxdrivesallowed();
    ret.back().policy.retrievePriority = j.priority();
    ret.back().startTime = j.starttime();
    ret.back().retrieveRequestAddress = j.address();
  }
  return ret;
}


auto RetrieveQueueShard::getJobsSummary() -> JobsSummary {
  checkPayloadReadable();
  JobsSummary ret;
  ret.bytes = m_payload.retrievejobstotalsize();
  ret.jobs = m_payload.retrievejobs_size();
  ret.minFseq = m_payload.retrievejobs(0).fseq();
  ret.maxFseq = m_payload.retrievejobs(m_payload.retrievejobs_size()-1).fseq();
  return ret;
}

uint64_t RetrieveQueueShard::addJob(RetrieveQueue::JobToAdd& jobToAdd) {
  checkPayloadWritable();
  auto * j = m_payload.add_retrievejobs();
  j->set_address(jobToAdd.retrieveRequestAddress);
  j->set_size(jobToAdd.fileSize);
  j->set_copynb(jobToAdd.copyNb);
  j->set_fseq(jobToAdd.fSeq);
  j->set_starttime(jobToAdd.startTime);
  j->set_maxdrivesallowed(jobToAdd.policy.maxDrivesAllowed);
  j->set_priority(jobToAdd.policy.retrievePriority);
  j->set_minretrieverequestage(jobToAdd.policy.retrieveMinRequestAge);
  j->set_starttime(jobToAdd.startTime);
  m_payload.set_retrievejobstotalsize(m_payload.retrievejobstotalsize()+jobToAdd.fileSize);
  return m_payload.retrievejobs_size();
}




}}