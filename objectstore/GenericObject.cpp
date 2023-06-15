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

#include "GenericObject.hpp"
#include "AgentRegister.hpp"
#include "Agent.hpp"
#include "ArchiveRequest.hpp"
#include "DriveRegister.hpp"
#include "RootEntry.hpp"
#include "SchedulerGlobalLock.hpp"
#include "ArchiveQueue.hpp"
#include "ArchiveQueueShard.hpp"
#include "RetrieveQueue.hpp"
#include "RetrieveQueueShard.hpp"
#include "DriveRegister.hpp"
#include "RepackIndex.hpp"
#include "RepackQueue.hpp"
#include "RepackRequest.hpp"
#include <stdexcept>
#include <google/protobuf/util/json_util.h>

namespace cta {
namespace objectstore {

serializers::ObjectType GenericObject::type() {
  checkHeaderReadable();
  return m_header.type();
}

void GenericObject::getHeaderFromObjectData(const std::string& objData) {
  if (!m_header.ParseFromString(objData)) {
    // Use the tolerant parser to assess the situation.
    m_header.ParsePartialFromString(objData);
    // Base64 encode the header for diagnostics.
    const bool noNewLineInBase64Output = false;
    std::string objDataBase64;
    CryptoPP::StringSource ss1(
      objData, true, new CryptoPP::Base64Encoder(new CryptoPP::StringSink(objDataBase64), noNewLineInBase64Output));
    throw cta::exception::Exception(
      std::string("In <GenericObject::getHeaderFromObjectData(): could not parse header: ") +
      m_header.InitializationErrorString() + " size=" + std::to_string(objData.size()) + " data(b64)=\"" +
      objDataBase64 + "\" name=" + m_name + "\"");
  }
  m_headerInterpreted = true;
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
  destination.m_noLock = m_noLock;
  destination.m_payloadInterpreted = false;
}

Backend& GenericObject::objectStore() {
  return m_objectStore;
}

namespace {
using cta::objectstore::GenericObject;
using cta::objectstore::ScopedExclusiveLock;

template<class C>
void garbageCollectWithType(GenericObject* gop,
                            ScopedExclusiveLock& lock,
                            const std::string& presumedOwner,
                            AgentReference& agentReference,
                            log::LogContext& lc,
                            cta::catalogue::Catalogue& catalogue) {
  C typedObject(*gop);
  lock.transfer(typedObject);
  typedObject.garbageCollect(presumedOwner, agentReference, lc, catalogue);
  // Release the lock now as if we let the caller do, it will point
  // to the then-removed typedObject.
  lock.release();
}
}  // namespace

void GenericObject::garbageCollect(const std::string& presumedOwner,
                                   AgentReference& agentReference,
                                   log::LogContext& lc,
                                   cta::catalogue::Catalogue& catalogue) {
  throw ForbiddenOperation("In GenericObject::garbageCollect(): GenericObject cannot be garbage collected");
}

void GenericObject::garbageCollectDispatcher(ScopedExclusiveLock& lock,
                                             const std::string& presumedOwner,
                                             AgentReference& agentReference,
                                             log::LogContext& lc,
                                             cta::catalogue::Catalogue& catalogue) {
  checkHeaderWritable();
  switch (m_header.type()) {
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
    case serializers::RepackRequest_t:
      garbageCollectWithType<RepackRequest>(this, lock, presumedOwner, agentReference, lc, catalogue);
      break;
    default: {
      std::stringstream err;
      err << "In GenericObject::garbageCollect, unsupported type: " << m_header.type();
      throw UnsupportedType(err.str());
    }
  }
}

namespace {
using cta::objectstore::GenericObject;
using cta::objectstore::ScopedExclusiveLock;

template<class C>
std::string dumpWithType(GenericObject* gop) {
  C typedObject(*gop);
  ScopedLock::transfer(*gop, typedObject);
  std::string ret = typedObject.dump();
  // Release the lock now as if we let the caller do, it will point
  // to the then-removed typedObject.
  return ret;
}
}  // namespace

std::string GenericObject::dump() {
  checkHeaderReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  std::string bodyDump;
  google::protobuf::util::MessageToJsonString(m_header, &headerDump, options);
  switch (m_header.type()) {
    case serializers::RootEntry_t:
      bodyDump = dumpWithType<RootEntry>(this);
      break;
    case serializers::AgentRegister_t:
      bodyDump = dumpWithType<AgentRegister>(this);
      break;
    case serializers::Agent_t:
      bodyDump = dumpWithType<Agent>(this);
      break;
    case serializers::DriveRegister_t:
      bodyDump = dumpWithType<DriveRegister>(this);
      break;
    case serializers::ArchiveQueue_t:
      bodyDump = dumpWithType<cta::objectstore::ArchiveQueue>(this);
      break;
    case serializers::ArchiveQueueShard_t:
      bodyDump = dumpWithType<cta::objectstore::ArchiveQueueShard>(this);
      break;
    case serializers::RetrieveQueue_t:
      bodyDump = dumpWithType<cta::objectstore::RetrieveQueue>(this);
      break;
    case serializers::RetrieveQueueShard_t:
      bodyDump = dumpWithType<cta::objectstore::RetrieveQueueShard>(this);
      break;
    case serializers::ArchiveRequest_t:
      bodyDump = dumpWithType<ArchiveRequest>(this);
      break;
    case serializers::RetrieveRequest_t:
      bodyDump = dumpWithType<RetrieveRequest>(this);
      break;
    case serializers::SchedulerGlobalLock_t:
      bodyDump = dumpWithType<SchedulerGlobalLock>(this);
      break;
    case serializers::RepackIndex_t:
      bodyDump = dumpWithType<RepackIndex>(this);
      break;
    case serializers::RepackRequest_t:
      bodyDump = dumpWithType<RepackRequest>(this);
      break;
    case serializers::RepackQueue_t:
      bodyDump = dumpWithType<RepackQueue>(this);
      break;
    default:
      std::stringstream err;
      err << "Unsupported type: " << m_header.type();
      throw std::runtime_error(err.str());
  }
  return std::string("Header dump:\n") + headerDump + "Body dump:\n" + bodyDump;
}

}  // namespace objectstore
}  // namespace cta
