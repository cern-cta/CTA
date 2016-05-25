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
#include "EntryLog.hpp"
#include "RootEntry.hpp"
#include <json-c/json.h>

cta::objectstore::ArchiveQueue::ArchiveQueue(const std::string& address, Backend& os):
  ObjectOps<serializers::ArchiveQueue, serializers::ArchiveQueue_t>(os, address) { }

cta::objectstore::ArchiveQueue::ArchiveQueue(GenericObject& go):
  ObjectOps<serializers::ArchiveQueue, serializers::ArchiveQueue_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

std::string cta::objectstore::ArchiveQueue::dump() {  
  checkPayloadReadable();
  std::stringstream ret;
  ret << "ArchiveQueue" << std::endl;
  struct json_object * jo = json_object_new_object();
  
  json_object_object_add(jo, "name", json_object_new_string(m_payload.tapepoolname().c_str()));
  json_object_object_add(jo, "ArchiveJobsTotalSize", json_object_new_int64(m_payload.archivejobstotalsize()));
  json_object_object_add(jo, "oldestJobCreationTime", json_object_new_int64(m_payload.oldestjobcreationtime()));
  {
    json_object * array = json_object_new_array();
    for (auto i = m_payload.pendingarchivejobs().begin(); i!=m_payload.pendingarchivejobs().end(); i++) {
      json_object * jot = json_object_new_object();
      json_object_object_add(jot, "fileid", json_object_new_int64(i->fileid()));
      json_object_object_add(jot, "address", json_object_new_string(i->address().c_str()));
      json_object_object_add(jot, "copynb", json_object_new_int(i->copynb()));
      json_object_object_add(jot, "size", json_object_new_int64(i->size()));
      json_object_array_add(array, jot);
    }
    json_object_object_add(jo, "pendingarchivejobs", array);
  }
  {
    json_object * array = json_object_new_array();
    for (auto i = m_payload.orphanedarchivejobsnscreation().begin(); i!=m_payload.orphanedarchivejobsnscreation().end(); i++) {
      json_object * jot = json_object_new_object();
      json_object_object_add(jot, "fileid", json_object_new_int64(i->fileid()));
      json_object_object_add(jot, "address", json_object_new_string(i->address().c_str()));
      json_object_object_add(jot, "copynb", json_object_new_int(i->copynb()));
      json_object_object_add(jot, "size", json_object_new_int64(i->size()));
      json_object_array_add(array, jot);
    }
    json_object_object_add(jo, "orphanedarchivejobsnscreation", array);
  }
  {
    json_object * array = json_object_new_array();
    for (auto i = m_payload.orphanedarchivejobsnsdeletion().begin(); i!=m_payload.orphanedarchivejobsnsdeletion().end(); i++) {
      json_object * jot = json_object_new_object();
      json_object_object_add(jot, "fileid", json_object_new_int64(i->fileid()));
      json_object_object_add(jot, "address", json_object_new_string(i->address().c_str()));
      json_object_object_add(jot, "copynb", json_object_new_int(i->copynb()));
      json_object_object_add(jot, "size", json_object_new_int64(i->size()));
      json_object_array_add(array, jot);
    }
    json_object_object_add(jo, "orphanedarchivejobsnsdeletion", array);
  }
  ret << json_object_to_json_string_ext(jo, JSON_C_TO_STRING_PRETTY) << std::endl;
  json_object_put(jo);
  return ret.str();
}

void cta::objectstore::ArchiveQueue::initialize(const std::string& name) {
  // Setup underlying object
  ObjectOps<serializers::ArchiveQueue, serializers::ArchiveQueue_t>::initialize();
  // Setup the object so it's valid
  m_payload.set_tapepoolname(name);
  // set the archive jobs counter to zero
  m_payload.set_archivejobstotalsize(0);
  m_payload.set_oldestjobcreationtime(0);
  // This object is good to go (to storage)
  m_payloadInterpreted = true;
}

bool cta::objectstore::ArchiveQueue::isEmpty() {
  checkPayloadReadable();
  // Check we have no archive jobs pending
  if (m_payload.pendingarchivejobs_size() 
      || m_payload.orphanedarchivejobsnscreation_size()
      || m_payload.orphanedarchivejobsnsdeletion_size())
    return false;
  // If we made it to here, it seems the pool is indeed empty.
  return true;
}

void cta::objectstore::ArchiveQueue::garbageCollect(const std::string &presumedOwner) {
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
}

void cta::objectstore::ArchiveQueue::setName(const std::string& name) {
  checkPayloadWritable();
  m_payload.set_tapepoolname(name);
}

std::string cta::objectstore::ArchiveQueue::getName() {
  checkPayloadReadable();
  return m_payload.tapepoolname();
}

void cta::objectstore::ArchiveQueue::addJob(const ArchiveRequest::JobDump& job,
  const std::string & archiveToFileAddress, uint64_t fileid,
  uint64_t size, uint64_t priority, time_t startTime) {
  checkPayloadWritable();
  // The tape pool gets the highest priority of its jobs
  if (m_payload.pendingarchivejobs_size()) {
    if ((uint64_t)startTime < m_payload.oldestjobcreationtime())
      m_payload.set_oldestjobcreationtime(startTime);
    m_payload.set_archivejobstotalsize(m_payload.archivejobstotalsize() + size);
  } else {
    m_payload.set_archivejobstotalsize(size);
    m_payload.set_oldestjobcreationtime(startTime);
  }
  auto * j = m_payload.add_pendingarchivejobs();
  j->set_address(archiveToFileAddress);
  j->set_size(size);
  j->set_fileid(fileid);
  j->set_copynb(job.copyNb);
}

auto cta::objectstore::ArchiveQueue::getJobsSummary() -> JobsSummary {
  checkPayloadReadable();
  JobsSummary ret;
  ret.files = m_payload.pendingarchivejobs_size();
  ret.bytes = m_payload.archivejobstotalsize();
  ret.oldestJobStartTime = m_payload.oldestjobcreationtime();
  return ret;
}

bool cta::objectstore::ArchiveQueue::addJobIfNecessary(
  const ArchiveRequest::JobDump& job, 
  const std::string& archiveToFileAddress,
  uint64_t fileid, uint64_t size) {
  checkPayloadWritable();
  auto & jl=m_payload.pendingarchivejobs();
  for (auto j=jl.begin(); j!= jl.end(); j++) {
    if (j->address() == archiveToFileAddress)
      return false;
  }
  auto * j = m_payload.add_pendingarchivejobs();
  j->set_address(archiveToFileAddress);
  j->set_size(size);
  j->set_fileid(fileid);
  j->set_copynb(job.copyNb);
  return true;
}

void cta::objectstore::ArchiveQueue::removeJob(const std::string& archiveToFileAddress) {
  checkPayloadWritable();
  auto * jl=m_payload.mutable_pendingarchivejobs();
  bool found = false;
  do {
    found = false;
    // Push the found entry all the way to the end.
    for (size_t i=0; i<(size_t)jl->size(); i++) {
      if (jl->Get(i).address() == archiveToFileAddress) {
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

auto cta::objectstore::ArchiveQueue::dumpJobs() -> std::list<JobDump> {
  checkPayloadReadable();
  std::list<JobDump> ret;
  auto & jl=m_payload.pendingarchivejobs();
  for (auto j=jl.begin(); j!=jl.end(); j++) {
    ret.push_back(JobDump());
    ret.back().address = j->address();
    ret.back().size = j->size();
    ret.back().copyNb = j->copynb();
  }
  return ret;
}

bool cta::objectstore::ArchiveQueue::addOrphanedJobPendingNsCreation(
  const ArchiveRequest::JobDump& job, 
  const std::string& archiveToFileAddress, 
  uint64_t fileid,
  uint64_t size) {
  checkPayloadWritable();
  auto & jl=m_payload.orphanedarchivejobsnscreation();
  for (auto j=jl.begin(); j!= jl.end(); j++) {
    if (j->address() == archiveToFileAddress)
      return false;
  }
  auto * j = m_payload.add_orphanedarchivejobsnscreation();
  j->set_address(archiveToFileAddress);
  j->set_size(size);
  j->set_fileid(fileid);
  j->set_copynb(job.copyNb);
  return true;
}

bool cta::objectstore::ArchiveQueue::addOrphanedJobPendingNsDeletion(
  const ArchiveRequest::JobDump& job, 
  const std::string& archiveToFileAddress, 
  uint64_t fileid, uint64_t size) {
  checkPayloadWritable();
  auto & jl=m_payload.orphanedarchivejobsnsdeletion();
  for (auto j=jl.begin(); j!= jl.end(); j++) {
    if (j->address() == archiveToFileAddress)
      return false;
  }
  auto * j = m_payload.add_orphanedarchivejobsnsdeletion();
  j->set_address(archiveToFileAddress);
  j->set_size(size);
  j->set_fileid(fileid);
  return true;
}