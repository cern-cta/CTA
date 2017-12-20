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

#include "RetrieveQueue.hpp"
#include "GenericObject.hpp"
#include "EntryLogSerDeser.hpp"
#include "ValueCountMap.hpp"
#include <google/protobuf/util/json_util.h>

cta::objectstore::RetrieveQueue::RetrieveQueue(const std::string& address, Backend& os):
  ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t>(os, address) { }

cta::objectstore::RetrieveQueue::RetrieveQueue(GenericObject& go):
  ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t>(go.objectStore()){
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

cta::objectstore::RetrieveQueue::RetrieveQueue(Backend& os):
  ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t>(os) { }

void cta::objectstore::RetrieveQueue::initialize(const std::string &vid) {
  ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t>::initialize();
  // Set the reguired fields
  m_payload.set_oldestjobcreationtime(0);
  m_payload.set_retrievejobstotalsize(0);
  m_payload.set_vid(vid);
  m_payload.set_mapsrebuildcount(0);
  m_payloadInterpreted = true;
}

void cta::objectstore::RetrieveQueue::commit() {
  // Before calling ObjectOps::commit, check that we have coherent queue summaries
  ValueCountMap maxDriveAllowedMap(m_payload.mutable_maxdrivesallowedmap());
  ValueCountMap priorityMap(m_payload.mutable_prioritymap());
  ValueCountMap minRetrieveRequestAgeMap(m_payload.mutable_minretrieverequestagemap());
  if (maxDriveAllowedMap.total() != (uint64_t)m_payload.retrievejobs_size() ||
      priorityMap.total() != (uint64_t)m_payload.retrievejobs_size() ||
      minRetrieveRequestAgeMap.total() != (uint64_t)m_payload.retrievejobs_size()) {
    // The maps counts are off: recompute them.
    maxDriveAllowedMap.clear();
    priorityMap.clear();
    minRetrieveRequestAgeMap.clear();
    for (size_t i=0; i<(size_t)m_payload.retrievejobs_size(); i++) {
      maxDriveAllowedMap.incCount(m_payload.retrievejobs(i).maxdrivesallowed());
      priorityMap.incCount(m_payload.retrievejobs(i).priority());
      minRetrieveRequestAgeMap.incCount(m_payload.retrievejobs(i).minretrieverequestage());
    }
    m_payload.set_mapsrebuildcount(m_payload.mapsrebuildcount()+1);
  }
  ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t>::commit();
}

bool cta::objectstore::RetrieveQueue::isEmpty() {
  checkPayloadReadable();
  return !m_payload.retrievejobs_size();
}

void cta::objectstore::RetrieveQueue::removeIfEmpty(log::LogContext & lc) {
  checkPayloadWritable();
  if (!isEmpty()) {
    throw NotEmpty("In RetrieveQueue::removeIfEmpty: trying to remove an tape with retrieves queued");
  }
  remove();
  log::ScopedParamContainer params(lc);
  params.add("retrieveQueueObject", getAddressIfSet());
  lc.log(log::INFO, "In RetrieveQueue::removeIfEmpty(): removed the queue.");
}

std::string cta::objectstore::RetrieveQueue::getVid() {
  checkPayloadReadable();
  return m_payload.vid();
}

std::string cta::objectstore::RetrieveQueue::dump() {  
  checkPayloadReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}

void cta::objectstore::RetrieveQueue::addJobsAndCommit(std::list<cta::objectstore::RetrieveQueue::JobToAdd> & jobsToAdd) {
  checkPayloadWritable();
  // Keep track of the mounting criteria
  ValueCountMap maxDriveAllowedMap(m_payload.mutable_maxdrivesallowedmap());
  ValueCountMap priorityMap(m_payload.mutable_prioritymap());
  ValueCountMap minRetrieveRequestAgeMap(m_payload.mutable_minretrieverequestagemap());
  for (auto & jta: jobsToAdd) {
    maxDriveAllowedMap.incCount(jta.policy.maxDrivesAllowed);
    priorityMap.incCount(jta.policy.retrievePriority);
    minRetrieveRequestAgeMap.incCount(jta.policy.retrieveMinRequestAge);
    if (m_payload.retrievejobs_size()) {
      if (m_payload.oldestjobcreationtime() > (uint64_t)jta.startTime) {
        m_payload.set_oldestjobcreationtime(jta.startTime);
      }
      m_payload.set_retrievejobstotalsize(m_payload.retrievejobstotalsize() + jta.size);
    } else {
      m_payload.set_oldestjobcreationtime(jta.startTime);
      m_payload.set_retrievejobstotalsize(jta.size);
    }
    auto * j = m_payload.add_retrievejobs();
    j->set_address(jta.retrieveRequestAddress);
    j->set_size(jta.size);
    j->set_copynb(jta.copyNb);
    j->set_fseq(jta.fSeq);
    j->set_priority(jta.policy.retrievePriority);
    j->set_minretrieverequestage(jta.policy.retrieveMinRequestAge);
    j->set_maxdrivesallowed(jta.policy.maxDrivesAllowed);
    // move the the new job in the right spot on the queue.
    // i points to the newly added job all the time.
    size_t i=m_payload.retrievejobs_size() - 1;
    while (i > 0 && m_payload.retrievejobs(i).fseq() < m_payload.retrievejobs(i - 1).fseq()) {
      m_payload.mutable_retrievejobs()->SwapElements(i-1, i);
      i--;
    }
  }
  commit();
}

bool cta::objectstore::RetrieveQueue::addJobIfNecessary(uint64_t copyNb, uint64_t fSeq,
  const std::string & retrieveRequestAddress, uint64_t size, 
  const cta::common::dataStructures::MountPolicy & policy, time_t startTime) {
  checkPayloadWritable();
  // Check if the job is present and skip insertion if so
  for (auto &j: m_payload.retrievejobs()) {
    if (j.address() == retrieveRequestAddress)
      return false;
  }
  // Keep track of the mounting criteria
  ValueCountMap maxDriveAllowedMap(m_payload.mutable_maxdrivesallowedmap());
  maxDriveAllowedMap.incCount(policy.maxDrivesAllowed);
  ValueCountMap priorityMap(m_payload.mutable_prioritymap());
  priorityMap.incCount(policy.retrievePriority);
  ValueCountMap minRetrieveRequestAgeMap(m_payload.mutable_minretrieverequestagemap());
  minRetrieveRequestAgeMap.incCount(policy.retrieveMinRequestAge);
  if (m_payload.retrievejobs_size()) {
    if (m_payload.oldestjobcreationtime() > (uint64_t)startTime) {
      m_payload.set_oldestjobcreationtime(startTime);
    }
    m_payload.set_retrievejobstotalsize(m_payload.retrievejobstotalsize() + size);
  } else {
    m_payload.set_oldestjobcreationtime(startTime);
    m_payload.set_retrievejobstotalsize(size);
  }
  auto * j = m_payload.add_retrievejobs();
  j->set_address(retrieveRequestAddress);
  j->set_size(size);
  j->set_copynb(copyNb);
  j->set_fseq(fSeq);
  j->set_priority(policy.retrievePriority);
  j->set_minretrieverequestage(policy.retrieveMinRequestAge);
  j->set_maxdrivesallowed(policy.maxDrivesAllowed);
  // move the the new job in the right spot on the queue.
  // i points to the newly added job all the time.
  size_t i=m_payload.retrievejobs_size() - 1;
  while (i > 0 && m_payload.retrievejobs(i).fseq() < m_payload.retrievejobs(i - 1).fseq()) {
    m_payload.mutable_retrievejobs()->SwapElements(i-1, i);
    i--;
  }
  return true;
}

cta::objectstore::RetrieveQueue::JobsSummary cta::objectstore::RetrieveQueue::getJobsSummary() {
  checkPayloadReadable();
  JobsSummary ret;
  ret.bytes = m_payload.retrievejobstotalsize();
  ret.files = m_payload.retrievejobs_size();
  ret.oldestJobStartTime = m_payload.oldestjobcreationtime();
  if (ret.files) {
    ValueCountMap maxDriveAllowedMap(m_payload.mutable_maxdrivesallowedmap());
    ret.maxDrivesAllowed = maxDriveAllowedMap.maxValue();
    ValueCountMap priorityMap(m_payload.mutable_prioritymap());
    ret.priority = priorityMap.maxValue();
    ValueCountMap minArchiveRequestAgeMap(m_payload.mutable_minretrieverequestagemap());
    ret.minArchiveRequestAge = minArchiveRequestAgeMap.minValue();
  } else {
    ret.maxDrivesAllowed = 0;
    ret.priority = 0;
    ret.minArchiveRequestAge = 0;
  }
  return ret;
}

auto cta::objectstore::RetrieveQueue::dumpAndFetchRetrieveRequests() 
  -> std::list<RetrieveRequestDump> {
  checkPayloadReadable();
  std::list<RetrieveRequestDump> ret;
  auto & rjl = m_payload.retrievejobs();
  for (auto rj=rjl.begin(); rj!=rjl.end(); rj++) {
    try {
      cta::objectstore::RetrieveRequest retrieveRequest(rj->address(),m_objectStore);
      objectstore::ScopedSharedLock rtfrl(retrieveRequest);
      retrieveRequest.fetch();
      ret.push_back(RetrieveRequestDump());
      auto & retReq = ret.back();
      retReq.retrieveRequest = retrieveRequest.getSchedulerRequest();
      retReq.criteria = retrieveRequest.getRetrieveFileQueueCriteria();
      retReq.activeCopyNb = retrieveRequest.getActiveCopyNumber();
    } catch (cta::exception::Exception &) {}
  }
  return ret;
}

auto cta::objectstore::RetrieveQueue::dumpJobs() -> std::list<JobDump> {
  checkPayloadReadable();
  std::list<JobDump> ret;
  auto & rjl = m_payload.retrievejobs();
  for (auto rj=rjl.begin(); rj!=rjl.end(); rj++) {
    ret.push_back(JobDump());
    auto & b=ret.back();
    b.copyNb = rj->copynb();
    b.address = rj->address();
    b.size = rj->size();
  }
  return ret;
}

void cta::objectstore::RetrieveQueue::removeJob(const std::string& retrieveToFileAddress) {
  checkPayloadWritable();
  auto * jl = m_payload.mutable_retrievejobs();
  bool found=false;
  do {
    found=false;
    // Push the found entry all the way to the end.
    for (size_t i=0; i<(size_t)jl->size(); i++) {
      if (jl->Get(i).address() == retrieveToFileAddress) {
        found = true;
        // Keep track of the mounting criteria
        ValueCountMap maxDriveAllowedMap(m_payload.mutable_maxdrivesallowedmap());
        maxDriveAllowedMap.decCount(jl->Get(i).maxdrivesallowed());
        ValueCountMap priorityMap(m_payload.mutable_prioritymap());
        priorityMap.decCount(jl->Get(i).priority());
        ValueCountMap minRetrieveRequestAgeMap(m_payload.mutable_minretrieverequestagemap());
        minRetrieveRequestAgeMap.decCount(jl->Get(i).minretrieverequestage());
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

void cta::objectstore::RetrieveQueue::garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) {
  throw cta::exception::Exception("In RetrieveQueue::garbageCollect(): not implemented");
}
