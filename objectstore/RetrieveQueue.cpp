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
#include "EntryLog.hpp"
#include <json-c/json.h>

cta::objectstore::RetrieveQueue::RetrieveQueue(const std::string& address, Backend& os):
  ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t>(os, address) { }

cta::objectstore::RetrieveQueue::RetrieveQueue(GenericObject& go):
  ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t>(go.objectStore()){
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

void cta::objectstore::RetrieveQueue::initialize(const std::string &name, 
    const std::string &logicallibrary, const cta::common::dataStructures::EntryLog & entryLog) {
  ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t>::initialize();
  // Set the reguired fields
  m_payload.set_oldestjobtime(0);
  m_payload.set_retrievejobstotalsize(0);
  m_payloadInterpreted = true;
}


bool cta::objectstore::RetrieveQueue::isEmpty() {
  checkPayloadReadable();
  return !m_payload.retrievejobs_size();
}

void cta::objectstore::RetrieveQueue::removeIfEmpty() {
  checkPayloadWritable();
  if (!isEmpty()) {
    throw NotEmpty("In TapeQueue::removeIfEmpty: trying to remove an tape with retrieves queued");
  }
  remove();
}

std::string cta::objectstore::RetrieveQueue::getVid() {
  checkPayloadReadable();
  return m_payload.vid();
}

std::string cta::objectstore::RetrieveQueue::dump() {  
  checkPayloadReadable();
  std::stringstream ret;
  ret << "TapePool" << std::endl;
  struct json_object * jo = json_object_new_object();
  
  json_object_object_add(jo, "vid", json_object_new_string(m_payload.vid().c_str()));
  json_object_object_add(jo, "retrievejobstotalsize", json_object_new_int64(m_payload.retrievejobstotalsize()));
  json_object_object_add(jo, "oldestjobtime", json_object_new_int64(m_payload.oldestjobtime()));
  
  {
    json_object * array = json_object_new_array();
    for (auto i = m_payload.retrievejobs().begin(); i!=m_payload.retrievejobs().end(); i++) {
      json_object * rjobs = json_object_new_object();
      json_object_object_add(rjobs, "size", json_object_new_int64(i->size()));
      json_object_object_add(rjobs, "address", json_object_new_string(i->address().c_str()));
      json_object_object_add(rjobs, "copynb", json_object_new_int(i->copynb()));
      json_object_array_add(array, rjobs);
    }
    json_object_object_add(jo, "retrievejobs", array);
  }
  
  ret << json_object_to_json_string_ext(jo, JSON_C_TO_STRING_PRETTY) << std::endl;
  json_object_put(jo);
  return ret.str();
}

void cta::objectstore::RetrieveQueue::addJob(const RetrieveToFileRequest::JobDump& job,
  const std::string & retrieveToFileAddress, uint64_t size, uint64_t priority,
  time_t startTime) {
  checkPayloadWritable();
  // Manage the cumulative properties
  if (m_payload.retrievejobs_size()) {
    if (m_payload.oldestjobtime() > (uint64_t)startTime) {
      m_payload.set_oldestjobtime(startTime);
    }
    m_payload.set_retrievejobstotalsize(m_payload.retrievejobstotalsize() + size);
  } else {
    m_payload.set_oldestjobtime(startTime);
    m_payload.set_retrievejobstotalsize(size);
  }
  auto * j = m_payload.add_retrievejobs();
  j->set_address(retrieveToFileAddress);
  j->set_size(size);
  j->set_copynb(job.copyNb);
}

cta::objectstore::RetrieveQueue::JobsSummary cta::objectstore::RetrieveQueue::getJobsSummary() {
  checkPayloadReadable();
  JobsSummary ret;
  ret.bytes = m_payload.retrievejobstotalsize();
  ret.files = m_payload.retrievejobs_size();
  ret.oldestJobStartTime = m_payload.oldestjobtime();
  return ret;
}

auto cta::objectstore::RetrieveQueue::dumpAndFetchRetrieveRequests() 
  -> std::list<RetrieveRequestDump> {
  checkPayloadReadable();
  std::list<RetrieveRequestDump> ret;
  auto & rjl = m_payload.retrievejobs();
  for (auto rj=rjl.begin(); rj!=rjl.end(); rj++) {
    try {
      cta::objectstore::RetrieveToFileRequest rtfr(rj->address(),m_objectStore);
      objectstore::ScopedSharedLock rtfrl(rtfr);
      rtfr.fetch();
      ret.push_back(RetrieveRequestDump());
      auto & retReq = ret.back();
      retReq.archiveFile = rtfr.getArchiveFile();
      retReq.remoteFile = rtfr.getRemoteFile();
      retReq.entryLog = rtfr.getEntryLog();
      // Find the copy number from the list of jobs
      retReq.activeCopyNb = rj->copynb();
      auto jl = rtfr.dumpJobs();
      for (auto j=jl.begin(); j!= jl.end(); j++) {
        retReq.tapeCopies.push_back(TapeFileLocation());
        auto & retJob = retReq.tapeCopies.back();
        retJob.blockId = j->blockid;
        retJob.copyNb = j->copyNb;
        retJob.fSeq = j->fseq;
        retJob.vid = j->tape;
      }
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

void cta::objectstore::RetrieveQueue::removeJob(const std::string& retriveToFileAddress) {
  checkPayloadWritable();
  auto * jl = m_payload.mutable_retrievejobs();
  bool found=false;
  do {
    found=false;
    found = false;
    // Push the found entry all the way to the end.
    for (size_t i=0; i<(size_t)jl->size(); i++) {
      if (jl->Get(i).address() == retriveToFileAddress) {
        found = true;
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











