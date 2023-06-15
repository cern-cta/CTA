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

#pragma once

#include "objectstore/cta.pb.h"

#include <list>

#include "Backend.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/dataStructures/RepackQueueType.hpp"
#include "common/MountControl.hpp"
#include "EntryLogSerDeser.hpp"
#include "ObjectOps.hpp"

namespace cta {
namespace objectstore {

class AgentReference;
class GenericObject;

class RootEntry : public ObjectOps<serializers::RootEntry, serializers::RootEntry_t> {
public:
  // The conventional address of the root entry
  static const std::string address;  // = "root"

  // Constructor
  RootEntry(Backend& os);
  RootEntry(GenericObject& go);

  CTA_GENERATE_EXCEPTION_CLASS(NotAllocated);
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  CTA_GENERATE_EXCEPTION_CLASS(ForbiddenOperation);

  // In memory initialiser
  void initialize();

  // Emptyness checker
  bool isEmpty();

  // Safe remover
  void removeIfEmpty(log::LogContext& lc);

  // Garbage collection (disallowed for root entry).
  void garbageCollect(const std::string& presumedOwner,
                      AgentReference& agentReference,
                      log::LogContext& lc,
                      cta::catalogue::Catalogue& catalogue) override;

  // Queue types and helper functions ==========================================
private:
  const ::google::protobuf::RepeatedPtrField<::cta::objectstore::serializers::ArchiveQueuePointer>&
    archiveQueuePointers(common::dataStructures::JobQueueType queueType);
  ::google::protobuf::RepeatedPtrField<::cta::objectstore::serializers::ArchiveQueuePointer>*
    mutableArchiveQueuePointers(common::dataStructures::JobQueueType queueType);
  const ::google::protobuf::RepeatedPtrField<::cta::objectstore::serializers::RetrieveQueuePointer>&
    retrieveQueuePointers(common::dataStructures::JobQueueType queueType);
  ::google::protobuf::RepeatedPtrField<::cta::objectstore::serializers::RetrieveQueuePointer>*
    mutableRetrieveQueuePointers(common::dataStructures::JobQueueType queueType);

public:
  // ArchiveQueue handling  ====================================================
  CTA_GENERATE_EXCEPTION_CLASS(ArchiveQueueNotEmpty);
  CTA_GENERATE_EXCEPTION_CLASS(WrongArchiveQueue);
  /** This function implicitly creates the archive queue structure and updates
   * the pointer to it. It will implicitly commit the object to the store. */
  std::string addOrGetArchiveQueueAndCommit(const std::string& tapePool,
                                            AgentReference& agentRef,
                                            common::dataStructures::JobQueueType queueType);
  /** This function implicitly deletes the tape pool structure.
   * Fails if it not empty*/
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchArchiveQueue);
  void removeArchiveQueueAndCommit(const std::string& tapePool,
                                   common::dataStructures::JobQueueType queueType,
                                   log::LogContext& lc);
  /** This function is used in a cleanup utility. Removes unconditionally the reference to the archive queue */
  void removeMissingArchiveQueueReference(const std::string& tapePool, common::dataStructures::JobQueueType queueType);
  void removeArchiveQueueIfAddressMatchesAndCommit(const std::string& tapePool,
                                                   const std::string& archiveQueueAddress,
                                                   common::dataStructures::JobQueueType queueType);
  std::string getArchiveQueueAddress(const std::string& tapePool, common::dataStructures::JobQueueType queueType);

  struct ArchiveQueueDump {
    std::string tapePool;
    std::string address;
  };

  std::list<ArchiveQueueDump> dumpArchiveQueues(common::dataStructures::JobQueueType queueType);

  // RetrieveQueue handling ====================================================
  CTA_GENERATE_EXCEPTION_CLASS(RetrieveQueueNotEmpty);
  CTA_GENERATE_EXCEPTION_CLASS(WrongRetrieveQueue);
  /** This function implicitly creates the retrieve queue structure and updates
   * the pointer to it. It will implicitly commit the object to the store. */
  std::string addOrGetRetrieveQueueAndCommit(const std::string& vid,
                                             AgentReference& agentRef,
                                             common::dataStructures::JobQueueType queueType);
  /** This function is used in a cleanup utility. Removes unconditionally the reference to the retrieve queue */
  void removeMissingRetrieveQueueReference(const std::string& address, common::dataStructures::JobQueueType queueType);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchRetrieveQueue);
  void removeRetrieveQueueAndCommit(const std::string& vid,
                                    common::dataStructures::JobQueueType queueType,
                                    log::LogContext& lc);
  std::string getRetrieveQueueAddress(const std::string& vid, common::dataStructures::JobQueueType queueType);

  struct RetrieveQueueDump {
    std::string vid;
    std::string address;
  };

  std::list<RetrieveQueueDump> dumpRetrieveQueues(common::dataStructures::JobQueueType queueType);

  // Drive register manipulations ==============================================
  CTA_GENERATE_EXCEPTION_CLASS(DriveRegisterNotEmpty);
  std::string getDriveRegisterAddress();
  std::string addOrGetDriveRegisterPointerAndCommit(AgentReference& agentRef, const EntryLogSerDeser& log);
  void removeDriveRegisterAndCommit(log::LogContext& lc);

  // Agent register manipulations ==============================================
  CTA_GENERATE_EXCEPTION_CLASS(AgentRegisterNotEmpty);
  std::string getAgentRegisterAddress();
  /** We do pass the agent here even if there is no agent register yet, as it
   * is used to generate the object name. We have the dedicated agent intent
   * log for tracking objects being created. We already use an agent here for
   * object name generation, but not yet tracking. */
  std::string
    addOrGetAgentRegisterPointerAndCommit(AgentReference& agentRef, const EntryLogSerDeser& log, log::LogContext& lc);
  void removeAgentRegisterAndCommit(log::LogContext& lc);

private:
  void addIntendedAgentRegistry(const std::string& address, log::LogContext& lc);

public:
  // Scheduler global lock manipulations =======================================
  std::string getSchedulerGlobalLock();
  std::string addOrGetSchedulerGlobalLockAndCommit(AgentReference& agentRef, const EntryLogSerDeser& log);
  void removeSchedulerGlobalLockAndCommit(log::LogContext& lc);

  // Repack index manipulations ================================================
  std::string getRepackIndexAddress();
  std::string addOrGetRepackIndexAndCommit(AgentReference& agentRef);
  void removeRepackIndexAndCommit(log::LogContext& lc);

  // Repack queues manipulations ===============================================
  CTA_GENERATE_EXCEPTION_CLASS(RepackQueueNotEmpty);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchRepackQueue);
  std::string getRepackQueueAddress(common::dataStructures::RepackQueueType queueType);
  std::string addOrGetRepackQueueAndCommit(AgentReference& agentRef, common::dataStructures::RepackQueueType queueType);
  void removeRepackQueueAndCommit(common::dataStructures::RepackQueueType queueType, log::LogContext& lc);

private:
  void setRepackQueueAddress(common::dataStructures::RepackQueueType queueType, const std::string& queueAddress);
  void clearRepackQueueAddress(common::dataStructures::RepackQueueType queueType);

public:
  // Dump the root entry
  std::string dump();
};

}  // namespace objectstore
}  // namespace cta
