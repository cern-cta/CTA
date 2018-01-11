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

#include "ArchiveQueue.hpp"
#include "GenericObject.hpp"
#include "ProtocolBuffersAlgorithms.hpp"
#include "EntryLogSerDeser.hpp"
#include "RootEntry.hpp"
#include "ValueCountMap.hpp"
#include <google/protobuf/util/json_util.h>

namespace cta { namespace objectstore { 

ArchiveQueue::ArchiveQueue(const std::string& address, Backend& os):
  ObjectOps<serializers::ArchiveQueue, serializers::ArchiveQueue_t>(os, address) { }

ArchiveQueue::ArchiveQueue(Backend& os):
  ObjectOps<serializers::ArchiveQueue, serializers::ArchiveQueue_t>(os) { }

ArchiveQueue::ArchiveQueue(GenericObject& go):
  ObjectOps<serializers::ArchiveQueue, serializers::ArchiveQueue_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

std::string ArchiveQueue::dump() {  
  checkPayloadReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}

void ArchiveQueue::initialize(const std::string& name) {
  // Setup underlying object
  ObjectOps<serializers::ArchiveQueue, serializers::ArchiveQueue_t>::initialize();
  // Setup the object so it's valid
  m_payload.set_tapepool(name);
  // set the archive jobs counter to zero
  m_payload.set_archivejobstotalsize(0);
  m_payload.set_oldestjobcreationtime(0);
  // set the initial summary map rebuild count to zero
  m_payload.set_mapsrebuildcount(0);
  // This object is good to go (to storage)
  m_payloadInterpreted = true;
}

void ArchiveQueue::commit() {
  // Before calling ObjectOps::commit, check that we have coherent queue summaries
  ValueCountMap maxDriveAllowedMap(m_payload.mutable_maxdrivesallowedmap());
  ValueCountMap priorityMap(m_payload.mutable_prioritymap());
  ValueCountMap minArchiveRequestAgeMap(m_payload.mutable_minarchiverequestagemap());
  if (maxDriveAllowedMap.total() != (uint64_t)m_payload.pendingarchivejobs_size() ||
      priorityMap.total() != (uint64_t)m_payload.pendingarchivejobs_size() ||
      minArchiveRequestAgeMap.total() != (uint64_t)m_payload.pendingarchivejobs_size()) {
    // The maps counts are off: recompute them.
    maxDriveAllowedMap.clear();
    priorityMap.clear();
    minArchiveRequestAgeMap.clear();
    for (size_t i=0; i<(size_t)m_payload.pendingarchivejobs_size(); i++) {
      maxDriveAllowedMap.incCount(m_payload.pendingarchivejobs(i).maxdrivesallowed());
      priorityMap.incCount(m_payload.pendingarchivejobs(i).priority());
      minArchiveRequestAgeMap.incCount(m_payload.pendingarchivejobs(i).priority());
    }
    m_payload.set_mapsrebuildcount(m_payload.mapsrebuildcount()+1);
  }
  ObjectOps<serializers::ArchiveQueue, serializers::ArchiveQueue_t>::commit();
}


bool ArchiveQueue::isEmpty() {
  checkPayloadReadable();
  // Check we have no archive jobs pending
  if (m_payload.pendingarchivejobs_size() 
      || m_payload.orphanedarchivejobsnscreation_size()
      || m_payload.orphanedarchivejobsnsdeletion_size())
    return false;
  // If we made it to here, it seems the pool is indeed empty.
  return true;
}

void ArchiveQueue::garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) {
  checkPayloadWritable();
  // If the agent is not anymore the owner of the object, then only the very
  // last operation of the tape pool creation failed. We have nothing to do.
  if (presumedOwner != m_header.owner())
    return;
  // If the owner is still the agent, there are 2 possibilities
  // 1) The tape pool is referenced in the root entry, and then nothing is needed
  // besides setting the tape pool's owner to the root entry's address in 
  // order to enable its usage. Before that, it was considered as a dangling
  // pointer.
  {
    RootEntry re(m_objectStore);
    ScopedSharedLock rel (re);
    re.fetch();
    auto tpd=re.dumpArchiveQueues();
    for (auto tp=tpd.begin(); tp!=tpd.end(); tp++) {
      if (tp->address == getAddressIfSet()) {
        setOwner(re.getAddressIfSet());
        commit();
        return;
      }
    }
  }
  // 2) The tape pool is not referenced by the root entry. It is then effectively
  // not accessible and should be discarded.
  if (!isEmpty()) {
    throw (NotEmpty("Trying to garbage collect a non-empty ArchiveQueue: internal error"));
  }
  remove();
  log::ScopedParamContainer params(lc);
  params.add("archiveQueueObject", getAddressIfSet());
  lc.log(log::INFO, "In ArchiveQueue::garbageCollect(): Garbage collected and removed archive queue object.");
}

void ArchiveQueue::setTapePool(const std::string& name) {
  checkPayloadWritable();
  m_payload.set_tapepool(name);
}

std::string ArchiveQueue::getTapePool() {
  checkPayloadReadable();
  return m_payload.tapepool();
}

void ArchiveQueue::addJobsAndCommit(std::list<JobToAdd> & jobsToAdd) {
  checkPayloadWritable();
  for (auto & jta: jobsToAdd) {
    // Keep track of the mounting criteria
    ValueCountMap maxDriveAllowedMap(m_payload.mutable_maxdrivesallowedmap());
    maxDriveAllowedMap.incCount(jta.policy.maxDrivesAllowed);
    ValueCountMap priorityMap(m_payload.mutable_prioritymap());
    priorityMap.incCount(jta.policy.archivePriority);
    ValueCountMap minArchiveRequestAgeMap(m_payload.mutable_minarchiverequestagemap());
    minArchiveRequestAgeMap.incCount(jta.policy.archiveMinRequestAge);
    if (m_payload.pendingarchivejobs_size()) {
      if ((uint64_t)jta.startTime < m_payload.oldestjobcreationtime())
        m_payload.set_oldestjobcreationtime(jta.startTime);
      m_payload.set_archivejobstotalsize(m_payload.archivejobstotalsize() + jta.fileSize);
    } else {
      m_payload.set_archivejobstotalsize(jta.fileSize);
      m_payload.set_oldestjobcreationtime(jta.startTime);
    }
    auto * j = m_payload.add_pendingarchivejobs();
    j->set_address(jta.archiveRequestAddress);
    j->set_size(jta.fileSize);
    j->set_fileid(jta.archiveFileId);
    j->set_copynb(jta.job.copyNb);
    j->set_maxdrivesallowed(jta.policy.maxDrivesAllowed);
    j->set_priority(jta.policy.archivePriority);
    j->set_minarchiverequestage(jta.policy.archiveMinRequestAge);
  }
  commit();
}

auto ArchiveQueue::getJobsSummary() -> JobsSummary {
  checkPayloadReadable();
  JobsSummary ret;
  ret.files = m_payload.pendingarchivejobs_size();
  ret.bytes = m_payload.archivejobstotalsize();
  ret.oldestJobStartTime = m_payload.oldestjobcreationtime();
  if (ret.files) {
    ValueCountMap maxDriveAllowedMap(m_payload.mutable_maxdrivesallowedmap());
    ret.maxDrivesAllowed = maxDriveAllowedMap.maxValue();
    ValueCountMap priorityMap(m_payload.mutable_prioritymap());
    ret.priority = priorityMap.maxValue();
    ValueCountMap minArchiveRequestAgeMap(m_payload.mutable_minarchiverequestagemap());
    ret.minArchiveRequestAge = minArchiveRequestAgeMap.minValue();
  } else {
    ret.maxDrivesAllowed = 0;
    ret.priority = 0;
    ret.minArchiveRequestAge = 0;
  }
  return ret;
}

ArchiveQueue::AdditionSummary ArchiveQueue::addJobsIfNecessaryAndCommit(std::list<JobToAdd>& jobsToAdd) {
  checkPayloadWritable();
  ValueCountMap maxDriveAllowedMap(m_payload.mutable_maxdrivesallowedmap());
  ValueCountMap priorityMap(m_payload.mutable_prioritymap());
  ValueCountMap minArchiveRequestAgeMap(m_payload.mutable_minarchiverequestagemap());
  AdditionSummary ret;
  for (auto & jta: jobsToAdd) {
    auto & jl=m_payload.pendingarchivejobs();
    for (auto j=jl.begin(); j!= jl.end(); j++) {
      if (j->address() == jta.archiveRequestAddress)
        goto skipInsertion;
    }
    {
      // Keep track of the mounting criteria
      maxDriveAllowedMap.incCount(jta.policy.maxDrivesAllowed);
      priorityMap.incCount(jta.policy.archivePriority);
      minArchiveRequestAgeMap.incCount(jta.policy.archiveMinRequestAge);
      if (m_payload.pendingarchivejobs_size()) {
        if ((uint64_t)jta.startTime < m_payload.oldestjobcreationtime())
          m_payload.set_oldestjobcreationtime(jta.startTime);
        m_payload.set_archivejobstotalsize(m_payload.archivejobstotalsize() + jta.fileSize);
      } else {
        m_payload.set_archivejobstotalsize(jta.fileSize);
        m_payload.set_oldestjobcreationtime(jta.startTime);
      }
      auto * j = m_payload.add_pendingarchivejobs();
      j->set_address(jta.archiveRequestAddress);
      j->set_size(jta.fileSize);
      j->set_fileid(jta.archiveFileId);
      j->set_copynb(jta.job.copyNb);
      j->set_maxdrivesallowed(jta.policy.maxDrivesAllowed);
      j->set_priority(jta.policy.archivePriority);
      j->set_minarchiverequestage(jta.policy.archiveMinRequestAge);
      // Keep track of this addition.
      ret.files++;
      ret.bytes+=jta.fileSize;
    }
    skipInsertion:;
  }
  if (ret.files) commit();
  return ret;
}

void ArchiveQueue::removeJobsAndCommit(const std::list<std::string>& requestsToRemove) {
  checkPayloadWritable();
  ValueCountMap maxDriveAllowedMap(m_payload.mutable_maxdrivesallowedmap());
  ValueCountMap priorityMap(m_payload.mutable_prioritymap());
  ValueCountMap minArchiveRequestAgeMap(m_payload.mutable_minarchiverequestagemap());
  auto * jl=m_payload.mutable_pendingarchivejobs();
  bool jobRemoved=false;
  for (auto &rrt: requestsToRemove) {
    bool found = false;
    do {
      found = false;
      // Push the found entry all the way to the end.
      for (size_t i=0; i<(size_t)jl->size(); i++) {
        if (jl->Get(i).address() == rrt) {
          found = jobRemoved = true;
          maxDriveAllowedMap.decCount(jl->Get(i).maxdrivesallowed());
          priorityMap.decCount(jl->Get(i).priority());
          minArchiveRequestAgeMap.decCount(jl->Get(i).minarchiverequestage());
          m_payload.set_archivejobstotalsize(m_payload.archivejobstotalsize() - jl->Get(i).size());
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
  if (jobRemoved) commit();
}

auto ArchiveQueue::dumpJobs() -> std::list<JobDump> {
  checkPayloadReadable();
  std::list<JobDump> ret;
  auto & jl=m_payload.pendingarchivejobs();
  for (auto j=jl.begin(); j!=jl.end(); j++) {
    ret.push_back(JobDump());
    JobDump & jd = ret.back();
    jd.address = j->address();
    jd.size = j->size();
    jd.copyNb = j->copynb();
  }
  return ret;
}

auto ArchiveQueue::getCandidateList(uint64_t maxBytes, uint64_t maxFiles, std::set<std::string> archiveRequestsToSkip) -> CandidateJobList {
  checkPayloadReadable();
  CandidateJobList ret;
  ret.remainingBytesAfterCandidates = m_payload.archivejobstotalsize();
  ret.remainingFilesAfterCandidates = m_payload.pendingarchivejobs_size();
  for (auto & j: m_payload.pendingarchivejobs()) {
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

}} // namespace cta::objectstore
