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

#include "TapePool.hpp"
#include "GenericObject.hpp"
#include "ProtocolBuffersAlgorithms.hpp"
#include "CreationLog.hpp"
#include "Tape.hpp"
#include "RootEntry.hpp"
#include <json-c/json.h>

cta::objectstore::TapePool::TapePool(const std::string& address, Backend& os):
  ObjectOps<serializers::TapePool, serializers::TapePool_t>(os, address) { }

cta::objectstore::TapePool::TapePool(GenericObject& go):
  ObjectOps<serializers::TapePool, serializers::TapePool_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

std::string cta::objectstore::TapePool::dump() {  
  checkPayloadReadable();
  std::stringstream ret;
  ret << "TapePool" << std::endl;
  struct json_object * jo = json_object_new_object();
  
  json_object_object_add(jo, "name", json_object_new_string(m_payload.name().c_str()));
  json_object_object_add(jo, "ArchiveJobsTotalSize", json_object_new_int64(m_payload.archivejobstotalsize()));
  json_object_object_add(jo, "oldestJobCreationTime", json_object_new_int64(m_payload.oldestjobcreationtime()));
  json_object_object_add(jo, "archivemountcriteria - maxFilesBeforeMount", json_object_new_int64(m_payload.archivemountcriteria().maxfilesbeforemount()));
  json_object_object_add(jo, "archivemountcriteria - maxBytesBeforeMount", json_object_new_int64(m_payload.archivemountcriteria().maxbytesbeforemount()));
  json_object_object_add(jo, "archivemountcriteria - maxSecondsBeforeMount", json_object_new_int64(m_payload.archivemountcriteria().maxsecondsbeforemount()));
  json_object_object_add(jo, "archivemountcriteria - quota", json_object_new_int(m_payload.archivemountcriteria().quota()));
  json_object_object_add(jo, "retievemountcriteria - maxFilesBeforeMount", json_object_new_int64(m_payload.retievemountcriteria().maxfilesbeforemount()));
  json_object_object_add(jo, "retievemountcriteria - maxBytesBeforeMount", json_object_new_int64(m_payload.retievemountcriteria().maxbytesbeforemount()));
  json_object_object_add(jo, "retievemountcriteria - maxSecondsBeforeMount", json_object_new_int64(m_payload.retievemountcriteria().maxsecondsbeforemount()));
  json_object_object_add(jo, "retievemountcriteria - quota", json_object_new_int(m_payload.retievemountcriteria().quota()));
  json_object_object_add(jo, "priority", json_object_new_int64(m_payload.priority()));
  json_object_object_add(jo, "maxretriespermount", json_object_new_int(m_payload.maxretriespermount()));
  json_object_object_add(jo, "maxtotalretries", json_object_new_int(m_payload.maxtotalretries()));
  {
    json_object * array = json_object_new_array();
    for (auto i = m_payload.tapes().begin(); i!=m_payload.tapes().end(); i++) {
      json_object * jot = json_object_new_object();
      json_object_object_add(jot, "vid", json_object_new_string(i->vid().c_str()));
      json_object_object_add(jot, "address", json_object_new_string(i->address().c_str()));
      json_object_object_add(jot, "capacity", json_object_new_int64(i->capacity()));
      json_object_object_add(jot, "library", json_object_new_string(i->library().c_str()));
      json_object_array_add(array, jot);
    }
    json_object_object_add(jo, "tapes", array);
  }
  {
    json_object * array = json_object_new_array();
    for (auto i = m_payload.pendingarchivejobs().begin(); i!=m_payload.pendingarchivejobs().end(); i++) {
      json_object * jot = json_object_new_object();
      json_object_object_add(jot, "address", json_object_new_string(i->address().c_str()));
      json_object_object_add(jot, "copynb", json_object_new_int(i->copynb()));
      json_object_object_add(jot, "path", json_object_new_string(i->path().c_str()));
      json_object_object_add(jot, "size", json_object_new_int64(i->size()));
      json_object_array_add(array, jot);
    }
    json_object_object_add(jo, "pendingarchivejobs", array);
  }
  {
    json_object * array = json_object_new_array();
    for (auto i = m_payload.orphanedarchivejobsnscreation().begin(); i!=m_payload.orphanedarchivejobsnscreation().end(); i++) {
      json_object * jot = json_object_new_object();
      json_object_object_add(jot, "address", json_object_new_string(i->address().c_str()));
      json_object_object_add(jot, "copynb", json_object_new_int(i->copynb()));
      json_object_object_add(jot, "path", json_object_new_string(i->path().c_str()));
      json_object_object_add(jot, "size", json_object_new_int64(i->size()));
      json_object_array_add(array, jot);
    }
    json_object_object_add(jo, "orphanedarchivejobsnscreation", array);
  }
  {
    json_object * array = json_object_new_array();
    for (auto i = m_payload.orphanedarchivejobsnsdeletion().begin(); i!=m_payload.orphanedarchivejobsnsdeletion().end(); i++) {
      json_object * jot = json_object_new_object();
      json_object_object_add(jot, "address", json_object_new_string(i->address().c_str()));
      json_object_object_add(jot, "copynb", json_object_new_int(i->copynb()));
      json_object_object_add(jot, "path", json_object_new_string(i->path().c_str()));
      json_object_object_add(jot, "size", json_object_new_int64(i->size()));
      json_object_array_add(array, jot);
    }
    json_object_object_add(jo, "orphanedarchivejobsnsdeletion", array);
  }
  ret << json_object_to_json_string_ext(jo, JSON_C_TO_STRING_PRETTY) << std::endl;
  json_object_put(jo);
  return ret.str();
}

void cta::objectstore::TapePool::initialize(const std::string& name) {
  // Setup underlying object
  ObjectOps<serializers::TapePool, serializers::TapePool_t>::initialize();
  // Setup the object so it's valid
  m_payload.set_name(name);
  // set the archive jobs counter to zero
  m_payload.set_archivejobstotalsize(0);
  // Set initial value for priority. This value does not matter as long as
  // no job is queued. First job queueing will reset it. Same goes for oldest
  // job creation time.
  m_payload.set_priority(0);
  m_payload.set_oldestjobcreationtime(0);
  // Set the default values for the mount quotas and criteria.
  // The defaults mount criteria are a safely high numbers.
  m_payload.mutable_archivemountcriteria()->set_maxbytesbeforemount(10L*1000*1000*1000*1000);
  m_payload.mutable_archivemountcriteria()->set_maxfilesbeforemount(100000);
  m_payload.mutable_archivemountcriteria()->set_maxsecondsbeforemount(7*24*60*60);
  m_payload.mutable_retievemountcriteria()->set_maxbytesbeforemount(10L*1000*1000*1000*1000);
  m_payload.mutable_retievemountcriteria()->set_maxfilesbeforemount(100000);
  m_payload.mutable_retievemountcriteria()->set_maxsecondsbeforemount(7*24*60*60);
  // Default quotas are zero.
  m_payload.mutable_archivemountcriteria()->set_quota(0);
  m_payload.mutable_retievemountcriteria()->set_quota(0);
  // default the retries to 0.
  m_payload.set_maxretriespermount(0);
  m_payload.set_maxtotalretries(0);
  // This object is good to go (to storage)
  m_payloadInterpreted = true;
}

namespace {
  bool operator == (const std::string & vid,
    const cta::objectstore::serializers::TapePointer &t) {
    return vid==t.vid();
  }
}

std::string cta::objectstore::TapePool::addOrGetTapeAndCommit(const std::string& vid, 
  const std::string& logicalLibraryName, const uint64_t capacityInBytes, 
  Agent& agent, const cta::CreationLog& creationLog) {
  checkPayloadWritable();
  // Check the tape already exists
  try {
    return serializers::findElement(m_payload.tapes(), vid).address();
  } catch (serializers::NotFound &) {}
  // Insert the tape, then its pointer, with agent intent log update
  // first generate the intent. We expect the agent to be passed locked.
  std::string tapeAddress = agent.nextId(std::string("tape_") + vid + "_");
  // TODO Do we expect the agent to be passed locked or not: to be clarified.
  ScopedExclusiveLock agl(agent);
  agent.fetch();
  agent.addToOwnership(tapeAddress);
  agent.commit();
  // The create the tape object
  Tape t(tapeAddress, ObjectOps<serializers::TapePool, serializers::TapePool_t>::m_objectStore);
  t.initialize(vid, logicalLibraryName, creationLog);
  t.setOwner(agent.getAddressIfSet());
  t.setBackupOwner(getAddressIfSet());
  t.insert();
  ScopedExclusiveLock tl(t);
  // Now reference the tape in the pool
  auto * pt = m_payload.mutable_tapes()->Add();
  pt->set_address(tapeAddress);
  pt->set_capacity(capacityInBytes);
  pt->set_library(logicalLibraryName);
  pt->set_vid(vid);
  objectstore::CreationLog oslog(creationLog);
  oslog.serialize(*pt->mutable_log());
  commit();
  // Switch the tape ownership
  t.setOwner(getAddressIfSet());
  t.commit();
  // Clean up the agent. We're done.
  agent.removeFromOwnership(tapeAddress);
  agent.commit();
  return tapeAddress;
}

void cta::objectstore::TapePool::removeTapeAndCommit(const std::string& vid) {
  checkPayloadWritable();
  try {
    // Find the tape
    auto tp = serializers::findElement(m_payload.tapes(), vid);
    // Open the tape object
    Tape t(tp.address(), m_objectStore);
    ScopedExclusiveLock tl(t);
    t.fetch();
    // Verify this is the tape we're looking for.
    if (t.getVid() != vid) {
      std::stringstream err;
      err << "Unexpected tape VID found in object pointed to for tape: "
          << vid << " found: " << t.getVid();
      throw WrongTape(err.str());
    }
    if(!t.isEmpty()) {
      throw exception::Exception("Cannot delete the tape: it has queued retrieve jobs");
    }
    // We can now delete the tape
    t.remove();
    // And remove it from our entry
    serializers::removeOccurences(m_payload.mutable_tapes(), vid);
    // We commit for safety and symmetry with the add operation
    commit();
  } catch (serializers::NotFound &) {
    // No such tape. Nothing to to.
    throw NoSuchTape("In TapePool::removeTapeAndCommit: trying to remove non-existing tape");
  }
}

auto cta::objectstore::TapePool::dumpTapes() -> std::list<TapeBasicDump>{
  checkPayloadReadable();
  std::list<TapeBasicDump> ret;
  auto & tl = m_payload.tapes();
  for (auto tp=tl.begin(); tp!=tl.end(); tp++) {
    ret.push_back(TapeBasicDump());
    ret.back().address = tp->address();
    ret.back().vid = tp->vid();
    ret.back().capacityInBytes = tp->capacity();
    ret.back().logicalLibraryName = tp->library();
    ret.back().log.deserialize(tp->log());
  }
  return ret;
}

auto cta::objectstore::TapePool::dumpTapesAndFetchStatus() -> std::list<TapeDump>{
  checkPayloadReadable();
  std::list<TapeDump> ret;
  auto & tl = m_payload.tapes();
  for (auto tp=tl.begin(); tp!=tl.end(); tp++) {
    cta::Tape::Status stat;
    try {
      objectstore::Tape t(tp->address(), m_objectStore);
      objectstore::ScopedSharedLock tlock(t);
      t.fetch();
      stat.archived = t.isArchived();
      stat.busy = t.isBusy();
      stat.disabled = t.isDisabled();
      stat.full = t.isFull();
      stat.readonly = t.isReadOnly();
    } catch (cta::exception::Exception & ex) {
      continue;
    }
    ret.push_back(TapeDump());
    ret.back().address = tp->address();
    ret.back().vid = tp->vid();
    ret.back().capacityInBytes = tp->capacity();
    ret.back().logicalLibraryName = tp->library();
    ret.back().log.deserialize(tp->log());
    ret.back().status = stat;
  }
  return ret;
}

std::string cta::objectstore::TapePool::getTapeAddress(const std::string& vid) {
  checkPayloadReadable();
  return serializers::findElement(m_payload.tapes(), vid).address();
}

bool cta::objectstore::TapePool::isEmpty() {
  checkPayloadReadable();
  // Check we have no tapes in pool
  if (m_payload.tapes_size())
    return false;
  // Check we have no archive jobs pending
  if (m_payload.pendingarchivejobs_size() 
      || m_payload.orphanedarchivejobsnscreation_size()
      || m_payload.orphanedarchivejobsnsdeletion_size())
    return false;
  // If we made it to here, it seems the pool is indeed empty.
  return true;
}

void cta::objectstore::TapePool::garbageCollect(const std::string &presumedOwner) {
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
    auto tpd=re.dumpTapePools();
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
    throw (NotEmpty("Trying to garbage collect a non-empty TapePool: internal error"));
  }
  remove();
}

void cta::objectstore::TapePool::setName(const std::string& name) {
  checkPayloadWritable();
  m_payload.set_name(name);
}

std::string cta::objectstore::TapePool::getName() {
  checkPayloadReadable();
  return m_payload.name();
}

void cta::objectstore::TapePool::addJob(const ArchiveToFileRequest::JobDump& job,
  const std::string & archiveToFileAddress, const std::string & path,
  uint64_t size, uint64_t priority, time_t startTime) {
  checkPayloadWritable();
  // The tape pool gets the highest priority of its jobs
  if (m_payload.pendingarchivejobs_size()) {
    if (priority > m_payload.priority())
      m_payload.set_priority(priority);
    if ((uint64_t)startTime < m_payload.oldestjobcreationtime())
      m_payload.set_oldestjobcreationtime(startTime);
    m_payload.set_archivejobstotalsize(m_payload.archivejobstotalsize() + size);
  } else {
    m_payload.set_priority(priority);
    m_payload.set_archivejobstotalsize(size);
    m_payload.set_oldestjobcreationtime(startTime);
  }
  auto * j = m_payload.add_pendingarchivejobs();
  j->set_address(archiveToFileAddress);
  j->set_size(size);
  j->set_path(path);
  j->set_copynb(job.copyNb);
}

void cta::objectstore::TapePool::addJob(const ArchiveRequest::JobDump& job,
  const std::string & archiveToFileAddress, const std::string & path,
  uint64_t size, uint64_t priority, time_t startTime) {
  checkPayloadWritable();
  // The tape pool gets the highest priority of its jobs
  if (m_payload.pendingarchivejobs_size()) {
    if (priority > m_payload.priority())
      m_payload.set_priority(priority);
    if ((uint64_t)startTime < m_payload.oldestjobcreationtime())
      m_payload.set_oldestjobcreationtime(startTime);
    m_payload.set_archivejobstotalsize(m_payload.archivejobstotalsize() + size);
  } else {
    m_payload.set_priority(priority);
    m_payload.set_archivejobstotalsize(size);
    m_payload.set_oldestjobcreationtime(startTime);
  }
  auto * j = m_payload.add_pendingarchivejobs();
  j->set_address(archiveToFileAddress);
  j->set_size(size);
  j->set_path(path);
  j->set_copynb(job.copyNb);
}

auto cta::objectstore::TapePool::getJobsSummary() -> JobsSummary {
  checkPayloadReadable();
  JobsSummary ret;
  ret.files = m_payload.pendingarchivejobs_size();
  ret.bytes = m_payload.archivejobstotalsize();
  ret.priority = m_payload.priority();
  ret.oldestJobStartTime = m_payload.oldestjobcreationtime();
  return ret;
}

bool cta::objectstore::TapePool::addJobIfNecessary(
  const ArchiveToFileRequest::JobDump& job, 
  const std::string& archiveToFileAddress,
  const std::string & path, uint64_t size) {
  checkPayloadWritable();
  auto & jl=m_payload.pendingarchivejobs();
  for (auto j=jl.begin(); j!= jl.end(); j++) {
    if (j->address() == archiveToFileAddress)
      return false;
  }
  auto * j = m_payload.add_pendingarchivejobs();
  j->set_address(archiveToFileAddress);
  j->set_size(size);
  j->set_path(path);
  j->set_copynb(job.copyNb);
  return true;
}

bool cta::objectstore::TapePool::addJobIfNecessary(
  const ArchiveRequest::JobDump& job, 
  const std::string& archiveToFileAddress,
  const std::string & path, uint64_t size) {
  checkPayloadWritable();
  auto & jl=m_payload.pendingarchivejobs();
  for (auto j=jl.begin(); j!= jl.end(); j++) {
    if (j->address() == archiveToFileAddress)
      return false;
  }
  auto * j = m_payload.add_pendingarchivejobs();
  j->set_address(archiveToFileAddress);
  j->set_size(size);
  j->set_path(path);
  j->set_copynb(job.copyNb);
  return true;
}

void cta::objectstore::TapePool::removeJob(const std::string& archiveToFileAddress) {
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

auto cta::objectstore::TapePool::dumpJobs() -> std::list<JobDump> {
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

bool cta::objectstore::TapePool::addOrphanedJobPendingNsCreation(
  const ArchiveToFileRequest::JobDump& job, 
  const std::string& archiveToFileAddress, 
  const std::string & path,
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
  j->set_path(path);
  j->set_copynb(job.copyNb);
  return true;
}

bool cta::objectstore::TapePool::addOrphanedJobPendingNsDeletion(
  const ArchiveToFileRequest::JobDump& job, 
  const std::string& archiveToFileAddress, 
  const std::string & path, uint64_t size) {
  checkPayloadWritable();
  auto & jl=m_payload.orphanedarchivejobsnsdeletion();
  for (auto j=jl.begin(); j!= jl.end(); j++) {
    if (j->address() == archiveToFileAddress)
      return false;
  }
  auto * j = m_payload.add_orphanedarchivejobsnsdeletion();
  j->set_address(archiveToFileAddress);
  j->set_size(size);
  j->set_path(path);
  return true;
}

bool cta::objectstore::TapePool::addOrphanedJobPendingNsCreation(
  const ArchiveRequest::JobDump& job, 
  const std::string& archiveToFileAddress, 
  const std::string & path,
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
  j->set_path(path);
  j->set_copynb(job.copyNb);
  return true;
}

bool cta::objectstore::TapePool::addOrphanedJobPendingNsDeletion(
  const ArchiveRequest::JobDump& job, 
  const std::string& archiveToFileAddress, 
  const std::string & path, uint64_t size) {
  checkPayloadWritable();
  auto & jl=m_payload.orphanedarchivejobsnsdeletion();
  for (auto j=jl.begin(); j!= jl.end(); j++) {
    if (j->address() == archiveToFileAddress)
      return false;
  }
  auto * j = m_payload.add_orphanedarchivejobsnsdeletion();
  j->set_address(archiveToFileAddress);
  j->set_size(size);
  j->set_path(path);
  return true;
}

cta::MountCriteriaByDirection
  cta::objectstore::TapePool::getMountCriteriaByDirection() {
  MountCriteriaByDirection ret;
  checkPayloadReadable();
  ret.archive.maxAge = m_payload.archivemountcriteria().maxsecondsbeforemount();
  ret.archive.maxBytesQueued = m_payload.archivemountcriteria().maxbytesbeforemount();
  ret.archive.maxFilesQueued = m_payload.archivemountcriteria().maxfilesbeforemount();
  ret.archive.quota = m_payload.archivemountcriteria().quota();
  ret.retrieve.maxAge = m_payload.retievemountcriteria().maxsecondsbeforemount();
  ret.retrieve.maxBytesQueued = m_payload.retievemountcriteria().maxbytesbeforemount();
  ret.retrieve.maxFilesQueued = m_payload.retievemountcriteria().maxfilesbeforemount();
  ret.retrieve.quota = m_payload.retievemountcriteria().quota();
  return ret;
}

void cta::objectstore::TapePool::setMountCriteriaByDirection(
  const MountCriteriaByDirection& mountCriteria) {
  checkPayloadWritable();
  m_payload.mutable_archivemountcriteria()->set_maxbytesbeforemount(mountCriteria.archive.maxBytesQueued);
  m_payload.mutable_archivemountcriteria()->set_maxfilesbeforemount(mountCriteria.archive.maxFilesQueued);
  m_payload.mutable_archivemountcriteria()->set_maxsecondsbeforemount(mountCriteria.archive.maxAge);
  m_payload.mutable_archivemountcriteria()->set_quota(mountCriteria.archive.quota);
  m_payload.mutable_retievemountcriteria()->set_maxbytesbeforemount(mountCriteria.retrieve.maxBytesQueued);
  m_payload.mutable_retievemountcriteria()->set_maxfilesbeforemount(mountCriteria.retrieve.maxFilesQueued);
  m_payload.mutable_retievemountcriteria()->set_maxsecondsbeforemount(mountCriteria.retrieve.maxAge);
  m_payload.mutable_retievemountcriteria()->set_quota(mountCriteria.retrieve.quota);
}


void cta::objectstore::TapePool::setMaxRetriesWithinMount(uint16_t maxRetriesPerMount) {
  checkPayloadWritable();
  m_payload.set_maxretriespermount(maxRetriesPerMount);
}

uint16_t cta::objectstore::TapePool::getMaxRetriesWithinMount() {
  checkPayloadReadable();
  return m_payload.maxretriespermount();
}

void  cta::objectstore::TapePool::setMaxTotalRetries(uint16_t maxTotalRetries) {
  checkPayloadWritable();
  m_payload.set_maxtotalretries(maxTotalRetries);
}

uint16_t cta::objectstore::TapePool::getMaxTotalRetries() {
  checkPayloadWritable();
  return m_payload.maxtotalretries();
}

