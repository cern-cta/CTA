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

cta::objectstore::TapePool::TapePool(const std::string& address, Backend& os):
  ObjectOps<serializers::TapePool>(os, address) { }

cta::objectstore::TapePool::TapePool(GenericObject& go):
  ObjectOps<serializers::TapePool>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}


void cta::objectstore::TapePool::initialize(const std::string& name) {
  // Setup underlying object
  ObjectOps<serializers::TapePool>::initialize();
  // Setup the object so it's valid
  m_payload.set_name(name);
  // set the archival jobs counter to zero
  m_payload.set_archivejobstotalsize(0);
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
  Tape t(tapeAddress, ObjectOps<serializers::TapePool>::m_objectStore);
  t.initialize(vid);
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

auto cta::objectstore::TapePool::dumpTapes() -> std::list<TapeDump>{
  checkPayloadReadable();
  std::list<TapeDump> ret;
  auto & tl = m_payload.tapes();
  for (auto t=tl.begin(); t!=tl.end(); t++) {
    ret.push_back(TapeDump());
    ret.back().address = t->address();
    ret.back().vid = t->vid();
    ret.back().capacityInBytes = t->capacity();
    ret.back().logicalLibraryName = t->library();
    ret.back().log.deserialize(t->log());
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
  // Check we have no archival jobs pending
  if (m_payload.archivaljobs_size())
    return false;
  // If we made it to here, it seems the pool is indeed empty.
  return true;
}

void cta::objectstore::TapePool::garbageCollect() {
  checkPayloadWritable();
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
  const std::string & archiveToFileAddress, uint64_t size) {
  checkPayloadWritable();
  auto * j = m_payload.add_archivaljobs();
  j->set_address(archiveToFileAddress);
  j->set_size(size);
}

