
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
  if (ret.minFseq > ret.maxFseq)
    throw cta::exception::Exception("In RetrieveQueueShard::getJobsSummary(): wrong shard ordering.");
  return ret;
}

void RetrieveQueueShard::addJobsBatch(JobsToAddSet& jobsToAdd) {
  checkPayloadWritable();
  // Protect following algorithms against zero-sized array.
  if (jobsToAdd.empty()) return; 
  // Decide on the best algorithm. In place insertion implies 3*k*(N/2) copies
  // as we move the inserted job in place by swaps from the end (N/2 on average).
  // In practice, we can reduce the value of N by finding the last index imin 
  // at which fseq is smaller than the lowest fseq of the data to be inserted.
  // We then know that the first element to insert take exactly Np = N-imin-1 swaps
  // to move in place.
  // If we make no assumption for the other jobs to insert, they will each require 
  // Np/2 swaps to move  in place on average.
  // The cost can then reduced to Np + (k-1)Np/2 = 3*(k+1)Np/2
  // On the other hand, insertion through memory implies 2N+3k copies, N to copy
  // to a set, k copies to create the second set, k copies again during merge
  // and then N+k copies to copy the merged set into payload.
  // Find Np by bisection.
  size_t N=m_payload.retrievejobs_size();
  size_t Np=N;
  auto fSeqLimit = jobsToAdd.begin()->fSeq;
  if (0==N) Np=0;
  else if (1==N) Np=m_payload.retrievejobs(0).fseq() <= fSeqLimit? 0: 1;
  else {
    size_t iminmin=0;
    size_t iminmax=N-1;
    size_t imin=N/2;
    uint64_t iminfseq = ~0;
    uint64_t imin1fseq = ~0;
    while (true) {
      iminfseq = m_payload.retrievejobs(imin).fseq();
      if (iminfseq > fSeqLimit) {
        iminmax=imin;
        imin=(iminmin+iminmax)/2;
        // Did we reach the beginning of the array?
        if (!imin) {
          Np=m_payload.retrievejobs(0).fseq() <= fSeqLimit? N-1: N;
          break;
        }
      } else {
        // Before comparing, make sure we are not reaching the end of the set.
        if (imin >= N-1) {
          Np=m_payload.retrievejobs(N-1).fseq() <= fSeqLimit? 0: 1;
          break;
        }
        imin1fseq = m_payload.retrievejobs(imin+1).fseq();
        if (imin1fseq > fSeqLimit) {
          // We found the right imin.
          Np=N-imin-1;
          break;
        } else if (imin+1 == N-1) {
          Np=0;
          break;
        } else {
          iminmin=imin;
          imin=(iminmin+iminmax)/2;
        }
      }
    }
  }
  size_t k=jobsToAdd.size();
  if (3*(k+1)*Np/2 > 2*N+3*k)
    addJobsThroughCopy(jobsToAdd);
  else
    addJobsInPlace(jobsToAdd);
}

void RetrieveQueueShard::addJobsInPlace(JobsToAddSet& jobsToAdd) {
  for (auto &j: jobsToAdd) addJob(j);
}

void RetrieveQueueShard::addJob(const RetrieveQueue::JobToAdd& jobToAdd) {
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
  m_payload.set_retrievejobstotalsize(m_payload.retrievejobstotalsize()+jobToAdd.fileSize);
  // Sort the shard
  size_t jobIndex = m_payload.retrievejobs_size() - 1;
  while (jobIndex > 0 && m_payload.retrievejobs(jobIndex).fseq() < m_payload.retrievejobs(jobIndex-1).fseq()) {
    m_payload.mutable_retrievejobs()->SwapElements(jobIndex-1, jobIndex);
    jobIndex--;
  }
}

void RetrieveQueueShard::addJobsThroughCopy(JobsToAddSet& jobsToAdd) {
  checkPayloadWritable();
  SerializedJobsToAddSet jobsSet;
  SerializedJobsToAddSet serializedJobsToAdd;
  SerializedJobsToAddSet::iterator i = jobsSet.begin();
  // Copy the request pointers in memory (in an ordered multi set)
  for (auto &j: m_payload.retrievejobs())
    // As the queue is already sorted, we hit at the right
    // location (in-order insertion).
    i = jobsSet.insert(i, j);
  // Create a serialized version of the jobs to add.
  i = serializedJobsToAdd.begin();
  uint64_t totalSize = m_payload.retrievejobstotalsize();
  for (auto &jobToAdd: jobsToAdd) {
    serializers::RetrieveJobPointer rjp;
    rjp.set_address(jobToAdd.retrieveRequestAddress);
    rjp.set_size(jobToAdd.fileSize);
    rjp.set_copynb(jobToAdd.copyNb);
    rjp.set_fseq(jobToAdd.fSeq);
    rjp.set_starttime(jobToAdd.startTime);
    rjp.set_maxdrivesallowed(jobToAdd.policy.maxDrivesAllowed);
    rjp.set_priority(jobToAdd.policy.retrievePriority);
    rjp.set_minretrieverequestage(jobToAdd.policy.retrieveMinRequestAge);
    i = serializedJobsToAdd.insert(i, rjp);
    totalSize+=jobToAdd.fileSize;
  }
  // Let STL do the heavy lifting of in-order insertion.
  jobsSet.insert(serializedJobsToAdd.begin(), serializedJobsToAdd.end());
  // Recreate the shard from the in-memory image (it's already sorted).
  m_payload.mutable_retrievejobs()->Clear();
  for (auto &j: jobsSet)
    *m_payload.add_retrievejobs() = j;
  m_payload.set_retrievejobstotalsize(totalSize);
}





}}