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

#include "GenericObject.hpp"
#include "AgentRegister.hpp"
#include "Agent.hpp"
#include "ArchiveRequest.hpp"
#include "DriveRegister.hpp"
#include "RootEntry.hpp"
#include "SchedulerGlobalLock.hpp"
#include "ArchiveQueue.hpp"
#include "RetrieveQueue.hpp"
#include "DriveRegister.hpp"
#include <stdexcept>
#include <google/protobuf/util/json_util.h>

namespace cta {  namespace objectstore {

void GenericObject::fetch() {
  // Check that the object is locked, one way or another
  if(!m_locksCount)
    throw NotLocked("In ObjectOps::fetch(): object not locked");
  m_existingObject = true;
  // Get the header from the object store. We don't care for the type
  m_header.ParseFromString(m_objectStore.read(getAddressIfSet()));
  m_headerInterpreted = true;
}

serializers::ObjectType GenericObject::type() {
  checkHeaderReadable();
  return m_header.type();
}

void GenericObject::commit() {
  checkHeaderWritable();
  m_objectStore.atomicOverwrite(getAddressIfSet(), m_header.SerializeAsString());
}

void GenericObject::insert() {
  throw ForbiddenOperation("In GenericObject::insert: this operation is not possible");
}

void GenericObject::initialize() {
  throw ForbiddenOperation("In GenericObject::initialize: this operation is not possible");
}

void GenericObject::transplantHeader(ObjectOpsBase& destination) {
  destination.m_header = m_header;
  destination.m_existingObject = m_existingObject;
  destination.m_headerInterpreted = m_headerInterpreted;
  destination.m_name = m_name;
  destination.m_nameSet = m_nameSet;
  destination.m_payloadInterpreted = false;
}

Backend& GenericObject::objectStore() {
  return m_objectStore;
}

namespace {
  using cta::objectstore::GenericObject;
  using cta::objectstore::ScopedExclusiveLock;
  template <class C>
  void garbageCollectWithType(GenericObject * gop, ScopedExclusiveLock& lock,
      const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) {
    C typedObject(*gop);
    lock.transfer(typedObject);
    typedObject.garbageCollect(presumedOwner, agentReference, lc, catalogue);
    // Release the lock now as if we let the caller do, it will point
    // to the then-removed typedObject.
    lock.release();
  }
}

void GenericObject::garbageCollect(const std::string& presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) {
  throw ForbiddenOperation("In GenericObject::garbageCollect(): GenericObject cannot be garbage collected");
}

void GenericObject::garbageCollectDispatcher(ScopedExclusiveLock& lock, 
    const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) {
  checkHeaderWritable();
  switch(m_header.type()) {
    case serializers::AgentRegister_t:
      garbageCollectWithType<AgentRegister>(this, lock, presumedOwner, agentReference, lc, catalogue);
      break;
    case serializers::Agent_t:
      garbageCollectWithType<Agent>(this, lock, presumedOwner, agentReference, lc, catalogue);
      break;
    case serializers::DriveRegister_t:
      garbageCollectWithType<DriveRegister>(this, lock, presumedOwner, agentReference, lc, catalogue);
      break;
    case serializers::SchedulerGlobalLock_t:
      garbageCollectWithType<SchedulerGlobalLock>(this, lock, presumedOwner, agentReference, lc, catalogue);
      break;
    case serializers::ArchiveRequest_t:
      garbageCollectWithType<ArchiveRequest>(this, lock, presumedOwner, agentReference, lc, catalogue);
      break;
    case serializers::RetrieveRequest_t:
      garbageCollectWithType<RetrieveRequest>(this, lock, presumedOwner, agentReference, lc, catalogue);
      break;
    case serializers::ArchiveQueue_t:
      garbageCollectWithType<ArchiveQueue>(this, lock, presumedOwner, agentReference, lc, catalogue);
      break;
    case serializers::RetrieveQueue_t:
      garbageCollectWithType<RetrieveQueue>(this, lock, presumedOwner, agentReference, lc, catalogue);
      break;
    default: {
      std::stringstream err;
      err << "In GenericObject::garbageCollect, unsupported type: "
          << m_header.type();
      throw UnsupportedType(err.str());
    }
  }
}

namespace {
  using cta::objectstore::GenericObject;
  using cta::objectstore::ScopedExclusiveLock;
  template <class C>
  std::string dumpWithType(GenericObject * gop, ScopedSharedLock& lock) {
    C typedObject(*gop);
    lock.transfer(typedObject);
    std::string ret = typedObject.dump();
    // Release the lock now as if we let the caller do, it will point
    // to the then-removed typedObject.
    lock.release();
    return ret;
  }
}

std::string GenericObject::dump(ScopedSharedLock& lock) {
  checkHeaderReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  std::string bodyDump;
  google::protobuf::util::MessageToJsonString(m_header, &headerDump, options);
  switch(m_header.type()) {
    case serializers::RootEntry_t:
      bodyDump = dumpWithType<RootEntry>(this, lock);
      break;
    case serializers::AgentRegister_t:
      bodyDump = dumpWithType<AgentRegister>(this, lock);
      break;
    case serializers::Agent_t:
      bodyDump = dumpWithType<Agent>(this, lock);
      break;
    case serializers::DriveRegister_t:
      bodyDump = dumpWithType<DriveRegister>(this, lock);
      break;
    case serializers::ArchiveQueue_t:
      bodyDump = dumpWithType<cta::objectstore::ArchiveQueue>(this, lock);
      break;
    case serializers::RetrieveQueue_t:
      bodyDump = dumpWithType<cta::objectstore::RetrieveQueue>(this, lock);
      break;
    case serializers::ArchiveRequest_t:
      bodyDump = dumpWithType<ArchiveRequest>(this, lock);
      break;
    case serializers::RetrieveRequest_t:
      bodyDump = dumpWithType<RetrieveRequest>(this, lock);
      break;
    case serializers::SchedulerGlobalLock_t:
      bodyDump = dumpWithType<SchedulerGlobalLock>(this, lock);
      break;
    default:
      std::stringstream err;
      err << "Unsupported type: " << m_header.type();
      throw std::runtime_error(err.str());
  }
  return std::string ("Header dump:\n") + headerDump + "Body dump:\n" + bodyDump;
}

}}
