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

#include <cxxabi.h>
#include <google/protobuf/util/json_util.h>

#include "Agent.hpp"
#include "AgentReference.hpp"
#include "AgentRegister.hpp"
#include "ArchiveQueue.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "DriveRegister.hpp"
#include "GenericObject.hpp"
#include "ProtocolBuffersAlgorithms.hpp"
#include "RepackIndex.hpp"
#include "RepackQueue.hpp"
#include "RetrieveQueue.hpp"
#include "RootEntry.hpp"
#include "SchedulerGlobalLock.hpp"

namespace cta { namespace objectstore {

const std::string RootEntry::address("root");

// construtor, when the backend store exists.
// Checks the existence and correctness of the root entry
RootEntry::RootEntry(Backend & os):
  ObjectOps<serializers::RootEntry, serializers::RootEntry_t>(os, address) {}

RootEntry::RootEntry(GenericObject& go):
  ObjectOps<serializers::RootEntry, serializers::RootEntry_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

// Initialiser. This uses the base object's initialiser and sets the defaults
// of payload.
void RootEntry::initialize() {
  ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::initialize();
  // There is nothing to do for the payload.
  m_payloadInterpreted = true;
}

bool RootEntry::isEmpty() {
  checkPayloadReadable();
  if (m_payload.has_driveregisterpointer() &&
      m_payload.driveregisterpointer().address().size())
    return false;
  if (m_payload.agentregisterintent().size())
    return false;
  if (m_payload.has_agentregisterpointer() &&
      m_payload.agentregisterpointer().address().size())
    return false;
  if (m_payload.has_schedulerlockpointer() &&
      m_payload.schedulerlockpointer().address().size())
    return false;
  for (auto &qt: {common::dataStructures::JobQueueType::JobsToTransferForUser,
    common::dataStructures::JobQueueType::JobsToReportToUser,
    common::dataStructures::JobQueueType::FailedJobs}) {
    if (archiveQueuePointers(qt).size())
      return false;
  }
  for (auto &qt: {common::dataStructures::JobQueueType::JobsToTransferForUser,
    common::dataStructures::JobQueueType::JobsToReportToUser, common::dataStructures::JobQueueType::FailedJobs,
    common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess}) {
    if (retrieveQueuePointers(qt).size())
      return false;
  }
  return true;
}

void RootEntry::removeIfEmpty(log::LogContext & lc) {
  checkPayloadWritable();
  if (!isEmpty()) {
    throw NotEmpty("In RootEntry::removeIfEmpty(): root entry not empty");
  }
  remove();
  log::ScopedParamContainer params(lc);
  params.add("rootObjectName", getAddressIfSet());
  lc.log(log::INFO, "In RootEntry::removeIfEmpty(): removed root entry.");
}

void RootEntry::garbageCollect(const std::string& presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) {
  // The root entry cannot be garbage collected.
  throw ForbiddenOperation("In RootEntry::garbageCollect(): RootEntry cannot be garbage collected");
}

// =============================================================================
// ========== Queue types and helper functions =================================
// =============================================================================

const ::google::protobuf::RepeatedPtrField<::cta::objectstore::serializers::ArchiveQueuePointer>& RootEntry::archiveQueuePointers(
  common::dataStructures::JobQueueType queueType) {
  switch (queueType) {
  case common::dataStructures::JobQueueType::JobsToTransferForUser:
    return m_payload.archive_queue_to_transfer_for_user_pointers();
  case common::dataStructures::JobQueueType::JobsToReportToUser:
    return m_payload.archive_queue_to_report_for_user_pointers();
  case common::dataStructures::JobQueueType::FailedJobs:
    return m_payload.archive_queue_failed_pointers();
  case common::dataStructures::JobQueueType::JobsToTransferForRepack:
    return m_payload.archive_queue_to_transfer_for_repack_pointers();
  case common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess:
    return m_payload.archive_queue_to_report_to_repack_for_success_pointers();
  case common::dataStructures::JobQueueType::JobsToReportToRepackForFailure:
    return m_payload.archive_queue_to_report_to_repack_for_failure_pointers();
  default:
    throw cta::exception::Exception("In RootEntry::archiveQueuePointers(): unknown queue type.");
  }
}

::google::protobuf::RepeatedPtrField<::cta::objectstore::serializers::ArchiveQueuePointer>* RootEntry::mutableArchiveQueuePointers(
  common::dataStructures::JobQueueType queueType) {
  switch (queueType) {
  case common::dataStructures::JobQueueType::JobsToTransferForUser:
    return m_payload.mutable_archive_queue_to_transfer_for_user_pointers();
  case common::dataStructures::JobQueueType::JobsToReportToUser:
    return m_payload.mutable_archive_queue_to_report_for_user_pointers();
  case common::dataStructures::JobQueueType::FailedJobs:
    return m_payload.mutable_archive_queue_failed_pointers();
  case common::dataStructures::JobQueueType::JobsToTransferForRepack:
    return m_payload.mutable_archive_queue_to_transfer_for_repack_pointers();
  case common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess:
    return m_payload.mutable_archive_queue_to_report_to_repack_for_success_pointers();
  case common::dataStructures::JobQueueType::JobsToReportToRepackForFailure:
    return m_payload.mutable_archive_queue_to_report_to_repack_for_failure_pointers();
  default:
    throw cta::exception::Exception("In RootEntry::mutableArchiveQueuePointers(): unknown queue type.");
  }
}

const ::google::protobuf::RepeatedPtrField<::cta::objectstore::serializers::RetrieveQueuePointer>& RootEntry::retrieveQueuePointers(
  common::dataStructures::JobQueueType queueType) {
  switch (queueType) {
  case common::dataStructures::JobQueueType::JobsToTransferForUser:
    return m_payload.retrieve_queue_to_transfer_for_user_pointers();
  case common::dataStructures::JobQueueType::JobsToReportToUser:
    return m_payload.retrieve_queue_to_report_for_user_pointers();
  case common::dataStructures::JobQueueType::FailedJobs:
    return m_payload.retrieve_queue_failed_pointers();
  case common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess:
     return m_payload.retrieve_queue_to_report_to_repack_for_success_pointers();
  case common::dataStructures::JobQueueType::JobsToReportToRepackForFailure:
    return m_payload.retrieve_queue_to_report_to_repack_for_failure_pointers();
  case common::dataStructures::JobQueueType::JobsToTransferForRepack:
    return m_payload.retrieve_queue_to_transfer_for_repack_pointers();
  default:
    throw cta::exception::Exception("In RootEntry::retrieveQueuePointers(): unknown queue type.");
  }
}

::google::protobuf::RepeatedPtrField<::cta::objectstore::serializers::RetrieveQueuePointer>* RootEntry::mutableRetrieveQueuePointers(
  common::dataStructures::JobQueueType queueType) {
  switch (queueType) {
  case common::dataStructures::JobQueueType::JobsToTransferForUser:
    return m_payload.mutable_retrieve_queue_to_transfer_for_user_pointers();
  case common::dataStructures::JobQueueType::JobsToReportToUser:
    return m_payload.mutable_retrieve_queue_to_report_for_user_pointers();
  case common::dataStructures::JobQueueType::FailedJobs:
    return m_payload.mutable_retrieve_queue_failed_pointers();
  case common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess:
    return m_payload.mutable_retrieve_queue_to_report_to_repack_for_success_pointers();
  case common::dataStructures::JobQueueType::JobsToReportToRepackForFailure:
    return m_payload.mutable_retrieve_queue_to_report_to_repack_for_failure_pointers();
  case common::dataStructures::JobQueueType::JobsToTransferForRepack:
    return m_payload.mutable_retrieve_queue_to_transfer_for_repack_pointers();
  default:
    throw cta::exception::Exception("In RootEntry::mutableRetrieveQueuePointers(): unknown queue type.");
  }
}

// =============================================================================
// ========== Archive queues manipulations =====================================
// =============================================================================


// This operator will be used in the following usage of the findElement
// removeOccurences
namespace {
  bool operator==(const std::string &tp,
    const serializers::ArchiveQueuePointer & tpp) {
    return tpp.name() == tp;
  }
}

std::string RootEntry::addOrGetArchiveQueueAndCommit(const std::string& tapePool, AgentReference& agentRef,
  common::dataStructures::JobQueueType queueType) {
  checkPayloadWritable();
  // Check the archive queue does not already exist
  try {
    return serializers::findElement(archiveQueuePointers(queueType), tapePool).address();
  } catch (serializers::NotFound &) {}
  // Insert the archive queue pointer in the root entry, then the queue.
  std::string archiveQueueNameHeader = "ArchiveQueue";
  switch (queueType) {
  case common::dataStructures::JobQueueType::JobsToTransferForUser: archiveQueueNameHeader+="ToTransferForUser"; break;
  case common::dataStructures::JobQueueType::JobsToReportToRepackForFailure: archiveQueueNameHeader+="ToReportToRepackForSuccess"; break;
  case common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess: archiveQueueNameHeader+="ToReportToRepackForSuccess"; break;
  case common::dataStructures::JobQueueType::JobsToReportToUser: archiveQueueNameHeader+="ToReportForUser"; break;
  case common::dataStructures::JobQueueType::FailedJobs: archiveQueueNameHeader+="Failed"; break;
  case common::dataStructures::JobQueueType::JobsToTransferForRepack: archiveQueueNameHeader+="ToTransferForRepack"; break;
  default: break;
  }
  std::string archiveQueueAddress = agentRef.nextId(archiveQueueNameHeader+"-"+tapePool);
  // Now move create a reference the tape pool's ownership to the root entry
  auto * tpp = mutableArchiveQueuePointers(queueType)->Add();
  tpp->set_address(archiveQueueAddress);
  tpp->set_name(tapePool);
  // We must commit here to ensure the tape pool object is referenced.
  commit();
  // Then insert the queue object
  ArchiveQueue aq(archiveQueueAddress, ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
  aq.initialize(tapePool);
  aq.setOwner(getAddressIfSet());
  aq.setBackupOwner(getAddressIfSet());
  aq.insert();
  return archiveQueueAddress;
}

void RootEntry::removeArchiveQueueAndCommit(const std::string& tapePool, common::dataStructures::JobQueueType queueType,
  log::LogContext & lc) {
  checkPayloadWritable();
  // find the address of the archive queue object
  try {
    auto aqp = serializers::findElement(archiveQueuePointers(queueType), tapePool);
    // Open the tape pool object
    ArchiveQueue aq (aqp.address(), m_objectStore);
    ScopedExclusiveLock aql;
    try {
      // Give a slight grace period to avoid live locks (seen in CI: live lock between creation and deletion of empty queues).
      std::this_thread::sleep_for (std::chrono::milliseconds(100));
      aql.lock(aq);
      aq.fetch();
    } catch (cta::exception::Exception & ex) {
      // The archive queue seems to not be there. Make sure this is the case:
      if (aq.exists()) {
        // We failed to access the queue, yet it is present. This is an error.
        // Let the exception pass through.
        throw;
      } else {
        // The queue object is already gone. We can skip to removing the
        // reference from the RootEntry
        goto deleteFromRootEntry;
      }
    }
    // Verify this is the archive queue we're looking for.
    if (aq.getTapePool() != tapePool) {
      std::stringstream err;
      err << "In RootEntry::removeArchiveQueueAndCommit(): Unexpected tape pool name found in archive queue pointed to for tape pool: "
          << tapePool << " found: " << aq.getTapePool();
      throw WrongArchiveQueue(err.str());
    }
    // Check the archive queue is empty
    if (!aq.isEmpty()) {
      throw ArchiveQueueNotEmpty ("In RootEntry::removeArchiveQueueAndCommit(): trying to "
          "remove a non-empty archive queue");
    }
    // We can delete the queue
    aq.remove();
    {
      log::ScopedParamContainer params(lc);
      params.add("archiveQueueObject", aq.getAddressIfSet());
      lc.log(log::INFO, "In RootEntry::removeArchiveQueueAndCommit(): removed archive queue.");
    }
  deleteFromRootEntry:
    // ... and remove it from our entry
    serializers::removeOccurences(mutableArchiveQueuePointers(queueType), tapePool);
    // We commit for safety and symmetry with the add operation
    commit();
    {
      log::ScopedParamContainer params(lc);
      params.add("tapePool", tapePool)
            .add("queueType", toString(queueType));
      lc.log(log::INFO, "In RootEntry::removeArchiveQueueAndCommit(): removed archive queue reference.");
    }
  } catch (serializers::NotFound &) {
    // No such tape pool. Nothing to to.
    throw NoSuchArchiveQueue("In RootEntry::removeArchiveQueueAndCommit(): trying to remove non-existing archive queue");
  }
}

void RootEntry::removeMissingArchiveQueueReference(const std::string& tapePool, common::dataStructures::JobQueueType queueType) {
  serializers::removeOccurences(mutableArchiveQueuePointers(queueType), tapePool);
}

std::string RootEntry::getArchiveQueueAddress(const std::string& tapePool, common::dataStructures::JobQueueType queueType) {
  checkPayloadReadable();
  try {
    auto & tpp = serializers::findElement(archiveQueuePointers(queueType), tapePool);
    return tpp.address();
  } catch (serializers::NotFound &) {
    throw NoSuchArchiveQueue("In RootEntry::getArchiveQueueAddress: archive queue not allocated");
  }
}

auto RootEntry::dumpArchiveQueues(common::dataStructures::JobQueueType queueType) -> std::list<ArchiveQueueDump> {
  checkPayloadReadable();
  std::list<ArchiveQueueDump> ret;
  auto & tpl = archiveQueuePointers(queueType);
  for (auto i = tpl.begin(); i != tpl.end(); i++) {
    ret.push_back(ArchiveQueueDump());
    ret.back().address = i->address();
    ret.back().tapePool = i->name();
  }
  return ret;
}

// =============================================================================
// ========== Retrieve queues manipulations ====================================
// =============================================================================

// This operator will be used in the following usage of the findElement
// removeOccurences
namespace {
  bool operator==(const std::string &vid,
    const serializers::RetrieveQueuePointer & tpp) {
    return tpp.vid() == vid;
  }
}

std::string RootEntry::addOrGetRetrieveQueueAndCommit(const std::string& vid, AgentReference& agentRef,
  common::dataStructures::JobQueueType queueType) {
  checkPayloadWritable();
  // Check the retrieve queue does not already exist
  try {
    return serializers::findElement(retrieveQueuePointers(queueType), vid).address();
  } catch (serializers::NotFound &) {}
  // Insert the retrieve queue, then its pointer, with agent intent log update
  // First generate the intent. We expect the agent to be passed locked.
  // The make of the vid in the object name will be handy.
  std::string retrieveQueueNameHeader = "RetrieveQueue";
  switch (queueType) {
  case common::dataStructures::JobQueueType::JobsToTransferForUser: retrieveQueueNameHeader+="ToTransferForUser"; break;
  case common::dataStructures::JobQueueType::JobsToReportToUser: retrieveQueueNameHeader+="ToReportForUser"; break;
  case common::dataStructures::JobQueueType::FailedJobs: retrieveQueueNameHeader+="Failed"; break;
  case common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess: retrieveQueueNameHeader+="ToReportToRepackForSuccess"; break;
  case common::dataStructures::JobQueueType::JobsToReportToRepackForFailure: retrieveQueueNameHeader+="ToReportToRepackForFailure"; break;
  case common::dataStructures::JobQueueType::JobsToTransferForRepack: retrieveQueueNameHeader+="ToTransferForRepack"; break;
  default: break;
  }
  std::string retrieveQueueAddress = agentRef.nextId(retrieveQueueNameHeader+"-"+vid);
  // Reference the queue to the root entry before creation
  auto * rqp = mutableRetrieveQueuePointers(queueType)->Add();
  rqp->set_address(retrieveQueueAddress);
  rqp->set_vid(vid);
  // We must commit here to ensure the tape pool object is referenced.
  commit();
  // Then create the tape pool queue object
  RetrieveQueue rq(retrieveQueueAddress, ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
  rq.initialize(vid);
  rq.setOwner(getAddressIfSet());
  rq.setBackupOwner("root");
  rq.insert();
  return retrieveQueueAddress;
}

void RootEntry::removeMissingRetrieveQueueReference(const std::string& vid, common::dataStructures::JobQueueType queueType) {
  serializers::removeOccurences(mutableRetrieveQueuePointers(queueType), vid);
}

void RootEntry::removeRetrieveQueueAndCommit(const std::string& vid, common::dataStructures::JobQueueType queueType,
  log::LogContext & lc) {
  checkPayloadWritable();
  // find the address of the retrieve queue object
  try {
    auto rqp=serializers::findElement(retrieveQueuePointers(queueType), vid);
    // Open the retrieve queue object
    RetrieveQueue rq(rqp.address(), m_objectStore);
    ScopedExclusiveLock rql;
    try {
      // Give a slight grace period to avoid live locks (seen in CI: live lock between creation and deletion of empty queues).
      std::this_thread::sleep_for (std::chrono::milliseconds(100));
      rql.lock(rq);
      rq.fetch();
    } catch (cta::exception::Exception & ex) {
      // The retrieve queue seems to not be there. Make sure this is the case:
      if (rq.exists()) {
        // We failed to access the queue, yet it is present. This is an error.
        // Let the exception pass through.
        throw;
      } else {
        // The queue object is already gone. We can skip to removing the
        // reference from the RootEntry
        goto deleteFromRootEntry;
      }
    }
    // Verify this is the retrieve queue we're looking for.
    if (rq.getVid() != vid) {
      std::stringstream err;
      err << "Unexpected vid found in retrieve queue pointed to for vid: "
          << vid << " found: " << rq.getVid();
      throw WrongRetrieveQueue(err.str());
    }
    // Check the retrieve queue is empty
    if (!rq.isEmpty()) {
      throw RetrieveQueueNotEmpty("In RootEntry::removeTapePoolQueueAndCommit: trying to "
          "remove a non-empty tape pool");
    }
    // We can now delete the queue
    rq.remove();
    {
      log::ScopedParamContainer params(lc);
      params.add("retrieveQueueObject", rq.getAddressIfSet());
      lc.log(log::INFO, "In RootEntry::removeRetrieveQueueAndCommit(): removed retrieve queue.");
    }
  deleteFromRootEntry:
    // ... and remove it from our entry
    serializers::removeOccurences(mutableRetrieveQueuePointers(queueType), vid);
    // We commit for safety and symmetry with the add operation
    commit();
    {
      log::ScopedParamContainer params(lc);
      params.add("tapeVid", vid)
            .add("queueType", toString(queueType));
      lc.log(log::INFO, "In RootEntry::removeRetrieveQueueAndCommit(): removed retrieve queue reference.");
    }
  } catch (serializers::NotFound &) {
    // No such tape pool. Nothing to to.
    throw NoSuchRetrieveQueue("In RootEntry::removeRetrieveQueueAndCommit: trying to remove non-existing retrieve queue");
  }
}


std::string RootEntry::getRetrieveQueueAddress(const std::string& vid, common::dataStructures::JobQueueType queueType) {
  checkPayloadReadable();
  try {
    auto & rqp = serializers::findElement(retrieveQueuePointers(queueType), vid);
    return rqp.address();
  } catch (serializers::NotFound &) {
    throw NoSuchRetrieveQueue(std::string("In RootEntry::getRetreveQueueAddress: retrieve queue not allocated ")+
        vid+"/"+toString(queueType));
  }
}

auto RootEntry::dumpRetrieveQueues(common::dataStructures::JobQueueType queueType) -> std::list<RetrieveQueueDump> {
  checkPayloadReadable();
  std::list<RetrieveQueueDump> ret;
  auto & tpl = retrieveQueuePointers(queueType);
  for (auto i = tpl.begin(); i != tpl.end(); i++) {
    ret.push_back(RetrieveQueueDump());
    ret.back().address = i->address();
    ret.back().vid = i->vid();
  }
  return ret;
}

// =============================================================================
// ================ Drive register manipulation ================================
// =============================================================================

std::string RootEntry::addOrGetDriveRegisterPointerAndCommit(
  AgentReference& agentRef, const EntryLogSerDeser & log) {
  checkPayloadWritable();
  // Check if the drive register exists
  try {
    return getDriveRegisterAddress();
  } catch (NotAllocated &) {
    // decide on the object's name and add to agent's intent.
    std::string drAddress (agentRef.nextId("DriveRegister"));
    agentRef.addToOwnership(drAddress, m_objectStore);
    // Then create the drive register object
    DriveRegister dr(drAddress, m_objectStore);
    dr.initialize();
    dr.setOwner(agentRef.getAgentAddress());
    dr.setBackupOwner(getAddressIfSet());
    dr.insert();
    // Take a lock on drive registry
    ScopedExclusiveLock drLock(dr);
    // Move drive registry ownership to the root entry
    auto * mdrp = m_payload.mutable_driveregisterpointer();
    mdrp->set_address(drAddress);
    log.serialize(*mdrp->mutable_log());
    commit();
    // Record completion in drive registry
    dr.setOwner(getAddressIfSet());
    dr.setBackupOwner(getAddressIfSet());
    dr.commit();
    //... and clean up the agent
    agentRef.removeFromOwnership(drAddress, m_objectStore);
    return drAddress;
  }
}

void RootEntry::removeDriveRegisterAndCommit(log::LogContext & lc) {
  checkPayloadWritable();
  // Get the address of the drive register (nothing to do if there is none)
  if (!m_payload.has_driveregisterpointer() ||
      !m_payload.driveregisterpointer().address().size())
    return;
  std::string drAddr = m_payload.driveregisterpointer().address();
  DriveRegister dr(drAddr, ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
  ScopedExclusiveLock drl(dr);
  dr.fetch();
  // Check the drive register is empty
  if (!dr.isEmpty()) {
    throw DriveRegisterNotEmpty("In RootEntry::removeDriveRegisterAndCommit: "
      "trying to remove a non-empty drive register");
  }
  // we can delete the drive register
  dr.remove();
  log::ScopedParamContainer params(lc);
  params.add("driveRegisterObject", dr.getAddressIfSet());
  lc.log(log::INFO, "In RootEntry::removeDriveRegisterAndCommit(): removed drive register.");
  // And update the root entry
  m_payload.mutable_driveregisterpointer()->set_address("");
  // We commit for safety and symmetry with the add operation
  commit();
}

std::string RootEntry::getDriveRegisterAddress() {
  checkPayloadReadable();
  if (m_payload.has_driveregisterpointer() &&
      m_payload.driveregisterpointer().address().size()) {
    return m_payload.driveregisterpointer().address();
  }
  throw NotAllocated("In RootEntry::getDriveRegisterAddress: drive register not allocated");
}


// =============================================================================
// ================ Agent register manipulation ================================
// =============================================================================
// Get the name of the agent register (or exception if not available)
std::string RootEntry::getAgentRegisterAddress() {
  checkPayloadReadable();
  // If the registry is defined, return it, job done.
  if (m_payload.has_agentregisterpointer() &&
      m_payload.agentregisterpointer().address().size())
    return m_payload.agentregisterpointer().address();
  throw NotAllocated("In RootEntry::getAgentRegister: agentRegister not yet allocated");
}

// Get the name of a (possibly freshly created) agent register
std::string RootEntry::addOrGetAgentRegisterPointerAndCommit(AgentReference& agentRef,
  const EntryLogSerDeser & log, log::LogContext & lc) {
  // Check if the agent register exists
  try {
    return getAgentRegisterAddress();
  } catch (NotAllocated &) {
    // If we get here, the agent register is not created yet, so we have to do it:
    // lock the entry again, for writing. We take the lock ourselves if needed
    // This will make an autonomous transaction
    checkPayloadWritable();
    fetch();
    if (m_payload.has_agentregisterpointer() &&
        m_payload.agentregisterpointer().address().size()) {
      return m_payload.agentregisterpointer().address();
    }
    // decide on the object's name
    std::string arAddress (agentRef.nextId("AgentRegister"));
    // Record the agent registry in our own intent
    addIntendedAgentRegistry(arAddress, lc);
    commit();
    // Create the agent registry
    AgentRegister ar(arAddress, m_objectStore);
    ar.initialize();
    // There is no garbage collection for an agent registry: if it is not
    // plugged to the root entry, it does not exist.
    ar.setOwner("");
    ar.setBackupOwner("");
    ar.insert();
    // Take a lock on agent registry
    ScopedExclusiveLock arLock(ar);
    // Move agent registry from intent to official
    auto * marp = m_payload.mutable_agentregisterpointer();
    marp->set_address(arAddress);
    log.serialize(*marp->mutable_log());
    m_payload.set_agentregisterintent("");
    commit();
    // Record completion in agent registry
    ar.setOwner(getAddressIfSet());
    ar.setBackupOwner(getAddressIfSet());
    ar.commit();
    // And we are done. Release locks
    arLock.release();
    return arAddress;
  }
}

void RootEntry::removeAgentRegisterAndCommit(log::LogContext & lc) {
  checkPayloadWritable();
  // Check that we do have an agent register set. Cleanup a potential intent as
  // well
  if (m_payload.agentregisterintent().size()) {
    AgentRegister iar(m_payload.agentregisterintent(),
      ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
    ScopedExclusiveLock iarl(iar);
    // An agent register only referenced in the intent should not be used
    // and hence empty. We'll see that.
    iar.fetch();
    if (!iar.isEmpty()) {
      throw AgentRegisterNotEmpty("In RootEntry::removeAgentRegister: found "
        "a non-empty intended agent register. Internal error.");
    }
    iar.remove();
    log::ScopedParamContainer params(lc);
    params.add("agentRegisterObject", iar.getAddressIfSet());
    lc.log(log::INFO, "In RootEntry::removeAgentRegisterAndCommit(): removed agent register");
    m_payload.set_agentregisterintent("");
    commit();
  }
  if (m_payload.has_agentregisterpointer() &&
      m_payload.agentregisterpointer().address().size()) {
    AgentRegister ar(m_payload.agentregisterpointer().address(),
      ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
    ScopedExclusiveLock arl(ar);
    ar.fetch();
    if (!ar.isEmpty()) {
      throw AgentRegisterNotEmpty("In RootEntry::removeAgentRegister: the agent "
        "register is not empty. Cannot remove.");
    }
    ar.remove();
    log::ScopedParamContainer params(lc);
    params.add("agentRegisterObject", ar.getAddressIfSet());
    lc.log(log::INFO, "In RootEntry::removeAgentRegisterAndCommit(): removed agent register.");
    m_payload.mutable_agentregisterpointer()->set_address("");
    commit();
  }
}

void RootEntry::addIntendedAgentRegistry(const std::string& address, log::LogContext & lc) {
  checkPayloadWritable();
  // We are supposed to have only one intended agent registry at a time.
  // If we got the lock and there is one entry, this means the previous
  // attempt to create one did not succeed.
  // When getting here, having a set pointer to the registry is an error.
  if (m_payload.has_agentregisterpointer() &&
      m_payload.agentregisterpointer().address().size()) {
    throw exception::Exception("In RootEntry::addIntendedAgentRegistry:"
        " pointer to registry already set");
  }
  if (m_payload.agentregisterintent().size()) {
    // The intended object might not have made it to the official pointer.
    // If it did, we just clean up the intent.
    // If it did not, we clean up the object if present, clean up the intent
    // and replace it with the new one.
    // We do not recycle the object, as the state is doubtful.
    if (ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore.exists(
      m_payload.agentregisterintent())) {
      AgentRegister iar(m_payload.agentregisterintent(),
        ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
      iar.fetch();
      if (!iar.isEmpty()) {
        throw AgentRegisterNotEmpty("In RootEntry::addIntendedAgentRegistry, "
          "found a non-empty intended agent register. Internal Error.");
      }
      iar.remove();
      log::ScopedParamContainer params (lc);
      params.add("agentRegisterObject", iar.getAddressIfSet());
      lc.log(log::INFO, "In RootEntry::addIntendedAgentRegistry(): removed agent register.");
    }
  }
  m_payload.set_agentregisterintent(address);
}

// =============================================================================
// ================ Scheduler global lock manipulation =========================
// =============================================================================

std::string RootEntry::getSchedulerGlobalLock() {
  checkPayloadReadable();
  // If the scheduler lock is defined, return it, job done.
  if (m_payload.has_schedulerlockpointer() &&
      m_payload.schedulerlockpointer().address().size())
    return m_payload.schedulerlockpointer().address();
  throw NotAllocated("In RootEntry::getAgentRegister: scheduler global lock not yet allocated");
}

// Get the name of a (possibly freshly created) scheduler global lock
std::string RootEntry::addOrGetSchedulerGlobalLockAndCommit(AgentReference& agentRef,
  const EntryLogSerDeser & log) {
  checkPayloadWritable();
  // Check if the drive register exists
  try {
    return getSchedulerGlobalLock();
  } catch (NotAllocated &) {
    // decide on the object's name and add to agent's intent.
    std::string sglAddress (agentRef.nextId("SchedulerGlobalLock"));
    agentRef.addToOwnership(sglAddress, m_objectStore);
    // Then create the drive register object
    SchedulerGlobalLock sgl(sglAddress, m_objectStore);
    sgl.initialize();
    sgl.setOwner(agentRef.getAgentAddress());
    sgl.setBackupOwner(getAddressIfSet());
    sgl.insert();
    // Take a lock on scheduler global lock
    ScopedExclusiveLock sglLock(sgl);
    // Move drive registry ownership to the root entry
    auto * msgl = m_payload.mutable_schedulerlockpointer();
    msgl->set_address(sglAddress);
    log.serialize(*msgl->mutable_log());
    commit();
    // Record completion in scheduler global lock
    sgl.setOwner(getAddressIfSet());
    sgl.setBackupOwner(getAddressIfSet());
    sgl.commit();
    //... and clean up the agent
    agentRef.removeFromOwnership(sglAddress, m_objectStore);
    return sglAddress;
  }
}

void RootEntry::removeSchedulerGlobalLockAndCommit(log::LogContext & lc) {
  checkPayloadWritable();
  // Get the address of the scheduler lock (nothing to do if there is none)
  if (!m_payload.has_schedulerlockpointer() ||
      !m_payload.schedulerlockpointer().address().size())
    return;
  std::string sglAddress = m_payload.schedulerlockpointer().address();
  SchedulerGlobalLock sgl(sglAddress, ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
  ScopedExclusiveLock sgll(sgl);
  sgl.fetch();
  // Check the drive register is empty
  if (!sgl.isEmpty()) {
    throw DriveRegisterNotEmpty("In RootEntry::removeSchedulerGlobalLockAndCommit: "
      "trying to remove a non-empty scheduler global lock");
  }
  // we can delete the drive register
  sgl.remove();
  log::ScopedParamContainer params(lc);
  params.add("schedulerGlobalLockObject", sgl.getAddressIfSet());
  lc.log(log::INFO, "In RootEntry::removeSchedulerGlobalLockAndCommit(): removed scheduler global lock object.");
  // And update the root entry
  m_payload.mutable_schedulerlockpointer()->set_address("");
  // We commit for safety and symmetry with the add operation
  commit();
}

// =============================================================================
// ================ Repack index manipulation ==================================
// =============================================================================

std::string RootEntry::getRepackIndexAddress() {
  checkPayloadReadable();
  if (m_payload.has_repackindexpointer() &&
      m_payload.repackindexpointer().address().size()) {
    return m_payload.repackindexpointer().address();
  }
  throw cta::exception::Exception("In RootEntry::getRepackIndexAddress: repack tape register not yet allocated");
}

std::string RootEntry::addOrGetRepackIndexAndCommit(AgentReference& agentRef) {
  checkPayloadWritable();
  // Check if the repack tape register exists
  try {
    return getRepackIndexAddress();
  } catch (cta::exception::Exception &) {
    // TODO: this insertion method is much simpler than the ones used for other objects.
    // It implies the only dangling pointer situation we can get is the one where
    // the object does not exist.
    // As the object never changes ownership, the garbage collection can be left
    // empty. There should never be garbage collection for this object type.
    //
    // decide on the object's name.
    std::string rtrAddress (agentRef.nextId("RepackIndex"));
    // Then prepare the repack tape register object
    RepackIndex ri(rtrAddress, m_objectStore);
    ri.initialize();
    ri.setOwner(getAddressIfSet());
    ri.setBackupOwner(getAddressIfSet());
    // Reference the registry in the root entry
    auto * rtrp = m_payload.mutable_repackindexpointer();
    rtrp->set_address(rtrAddress);
    commit();
    // Create the repack tape register
    ri.insert();
    // done.
    return rtrAddress;
  }
}

void RootEntry::removeRepackIndexAndCommit(log::LogContext& lc) {
  checkPayloadWritable();
  // Get the address of the scheduler lock (nothing to do if there is none)
  if (!m_payload.has_repackindexpointer() ||
      !m_payload.repackindexpointer().address().size())
    return;
  std::string rtrAddress = m_payload.repackindexpointer().address();
  try{
    RepackIndex ri(rtrAddress, ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
    ScopedExclusiveLock rtrl(ri);
    ri.fetch();
    // Check the drive register is empty
    if (!ri.isEmpty()) {
      throw DriveRegisterNotEmpty("In RootEntry::removeRepackIndexAndCommit(): "
        "trying to remove a non-empty repack tape register");
    }
    // we can delete the drive register
    ri.remove();
    log::ScopedParamContainer params(lc);
    params.add("repackIndex", ri.getAddressIfSet());
    lc.log(log::INFO, "In RootEntry::removeRepackIndexAndCommit(): removed repack tape register object.");
  } catch(const cta::exception::NoSuchObject& ex) {
    log::ScopedParamContainer params(lc);
    params.add("errorMsg",ex.getMessageValue());
    lc.log(log::INFO, "In RootEntry::removeRepackIndexAndCommit(): the repack tape register object does not exist in the objectstore.");
  }
  // And update the root entry
  m_payload.mutable_repackindexpointer()->set_address("");
  // We commit for safety and symmetry with the add operation
  commit();
}

// =============================================================================
// ================ Repack index manipulation ==================================
// =============================================================================

std::string RootEntry::getRepackQueueAddress(common::dataStructures::RepackQueueType queueType) {
  checkPayloadReadable();
  switch (queueType) {
  case common::dataStructures::RepackQueueType::Pending:
    if (!m_payload.has_repackrequestspendingqueuepointer())
      throw NoSuchRepackQueue("In RootEntry::getRepackQueueAddress: pending queue no set.");
    return m_payload.repackrequestspendingqueuepointer().address();
  case common::dataStructures::RepackQueueType::ToExpand:
    if (!m_payload.has_repackrequeststoexpandqueuepointer())
      throw NoSuchRepackQueue("In RootEntry::getRepackQueueAddress: toExpand queue not set.");
    return m_payload.repackrequeststoexpandqueuepointer().address();
  }
  throw cta::exception::Exception("In RootEntry::getRepackQueueAddress(): unexptected queue type.");
}

void RootEntry::clearRepackQueueAddress(common::dataStructures::RepackQueueType queueType) {
  checkPayloadWritable();
  switch (queueType) {
  case common::dataStructures::RepackQueueType::Pending:
    if (!m_payload.has_repackrequestspendingqueuepointer())
      throw NoSuchRepackQueue("In RootEntry::clearRepackQueueAddress: pending queue no set.");
    return m_payload.mutable_repackrequestspendingqueuepointer()->Clear();
  case common::dataStructures::RepackQueueType::ToExpand:
    if (!m_payload.has_repackrequeststoexpandqueuepointer())
      throw NoSuchRepackQueue("In RootEntry::clearRepackQueueAddress: toExpand queue not set.");
    return m_payload.mutable_repackrequeststoexpandqueuepointer()->Clear();
  }
  throw cta::exception::Exception("In RootEntry::clearRepackQueueAddress(): unexptected queue type.");
}

void RootEntry::removeRepackQueueAndCommit(common::dataStructures::RepackQueueType queueType, log::LogContext& lc) {
  checkPayloadWritable();
  // find the address of the repack queue object
  try {
    bool hasQueue;
    switch (queueType) {
    case common::dataStructures::RepackQueueType::Pending:
      hasQueue = m_payload.has_repackrequestspendingqueuepointer();
      break;
    case common::dataStructures::RepackQueueType::ToExpand:
      hasQueue = m_payload.has_repackrequeststoexpandqueuepointer();
    }
    if (!hasQueue) {
      throw NoSuchRepackQueue("In RootEntry::removeRepackQueueAndCommit: trying to remove non-existing repack queue");
    }
    std::string queueAddress;
    switch (queueType) {
    case common::dataStructures::RepackQueueType::Pending:
      queueAddress = m_payload.repackrequestspendingqueuepointer().address();
      break;
    case common::dataStructures::RepackQueueType::ToExpand:
      queueAddress = m_payload.repackrequeststoexpandqueuepointer().address();
    }
    // Open the repack queue object
    RepackQueue rq(queueAddress, m_objectStore);
    ScopedExclusiveLock rql;
    try {
      rql.lock(rq);
      rq.fetch();
    } catch (cta::exception::Exception & ex) {
      // The repack queue seems to not be there. Make sure this is the case:
      if (rq.exists()) {
        // We failed to access the queue, yet it is present. This is an error.
        // Let the exception pass through.
        throw;
      } else {
        // The queue object is already gone. We can skip to removing the
        // reference from the RootEntry
        goto deleteFromRootEntry;
      }
    }
    // Check the repack queue is empty
    if (!rq.isEmpty()) {
      throw RepackQueueNotEmpty("In RootEntry::removeRepackQueueAndCommit: trying to "
          "remove a non-empty tape pool");
    }
    // We can now delete the queue
    rq.remove();
    {
      log::ScopedParamContainer params(lc);
      params.add("repackQueueObject", rq.getAddressIfSet())
            .add("queueType", toString(queueType));
      lc.log(log::INFO, "In, RootEntry::removeRepackQueueAndCommit(): removed retrieve queue.");
    }
  deleteFromRootEntry:
    // ... and remove it from our entry
    switch (queueType) {
    case common::dataStructures::RepackQueueType::Pending:
      m_payload.clear_repackrequestspendingqueuepointer();
      break;
    case common::dataStructures::RepackQueueType::ToExpand:
      m_payload.clear_repackrequeststoexpandqueuepointer();
    }
    commit();
    {
      log::ScopedParamContainer params(lc);
      params.add("queueType", toString(queueType));
      lc.log(log::INFO, "In RootEntry::removeRetrieveQueueAndCommit(): removed retrieve queue reference.");
    }
  } catch (serializers::NotFound &) {
    // No such tape pool. Nothing to to.
    throw NoSuchRetrieveQueue("In RootEntry::addOrGetRetrieveQueueAndCommit: trying to remove non-existing retrieve queue");
  }
}

std::string RootEntry::addOrGetRepackQueueAndCommit(AgentReference& agentRef, common::dataStructures::RepackQueueType queueType) {
  checkPayloadWritable();
  // Check the repack queue does not already exist
  try {
    return getRepackQueueAddress(queueType);
  } catch (NoSuchRepackQueue &) {}
  // The queue is not there yet. Create it.
  // Insert the archive queue pointer in the root entry, then the queue.
  std::string repackQueueNameHeader = "RepackQueue";
  switch(queueType) {
  case common::dataStructures::RepackQueueType::Pending: repackQueueNameHeader+="Pending"; break;
  case common::dataStructures::RepackQueueType::ToExpand: repackQueueNameHeader+="ToExpand"; break;
  default: break;
  }
  std::string repackQueueAddress = agentRef.nextId(repackQueueNameHeader);
  // Now move create a reference in the root entry
  switch(queueType) {
  case common::dataStructures::RepackQueueType::Pending:
    m_payload.mutable_repackrequestspendingqueuepointer()->set_address(repackQueueAddress);
    break;
  case common::dataStructures::RepackQueueType::ToExpand:
    m_payload.mutable_repackrequeststoexpandqueuepointer()->set_address(repackQueueAddress);
    break;
  }
  // We must commit here to ensure the repack queue is referenced.
  commit();
  // Then insert the queue object
  RepackQueue rq(repackQueueAddress, ObjectOps<serializers::RootEntry, serializers::RootEntry_t>::m_objectStore);
  rq.initialize();
  rq.setOwner(getAddressIfSet());
  rq.setBackupOwner(getAddressIfSet());
  rq.insert();
  return repackQueueAddress;
}



// =============================================================================
// ================ Dump =======================================================
// =============================================================================

// Dump the root entry
std::string RootEntry::dump () {
  checkPayloadReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}

}} // namespace cta::objectstore
