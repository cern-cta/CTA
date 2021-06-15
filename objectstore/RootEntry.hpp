/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "objectstore/cta.pb.h"

#include "JobQueueType.hpp"
#include "common/dataStructures/RepackQueueType.hpp"
#include "Backend.hpp"
#include "ObjectOps.hpp"
#include "EntryLogSerDeser.hpp"
#include "common/MountControl.hpp"
#include <list>

namespace cta { namespace objectstore {

class AgentReference;
class GenericObject;
  
class RootEntry: public ObjectOps<serializers::RootEntry, serializers::RootEntry_t> {
public:
  // The conventional address of the root entry
  static const std::string address; // = "root"
  
  // Constructor
  RootEntry(Backend & os);
  RootEntry(GenericObject & go);
  
  CTA_GENERATE_EXCEPTION_CLASS(NotAllocated);
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  CTA_GENERATE_EXCEPTION_CLASS(ForbiddenOperation);
  
  // In memory initialiser
  void initialize();
  
  // Emptyness checker
  bool isEmpty();
  
  // Safe remover
  void removeIfEmpty(log::LogContext & lc);
  
  // Garbage collection (disallowed for root entry).
  void garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) override;
  
  // Queue types and helper functions ==========================================
private:
  const ::google::protobuf::RepeatedPtrField< ::cta::objectstore::serializers::ArchiveQueuePointer >& 
    archiveQueuePointers(JobQueueType queueType);
  ::google::protobuf::RepeatedPtrField< ::cta::objectstore::serializers::ArchiveQueuePointer >* 
    mutableArchiveQueuePointers(JobQueueType queueType);
  const ::google::protobuf::RepeatedPtrField< ::cta::objectstore::serializers::RetrieveQueuePointer >& 
    retrieveQueuePointers(JobQueueType queueType);
  ::google::protobuf::RepeatedPtrField< ::cta::objectstore::serializers::RetrieveQueuePointer >* 
    mutableRetrieveQueuePointers(JobQueueType queueType);
public:
  
  // ArchiveQueue handling  ====================================================
  CTA_GENERATE_EXCEPTION_CLASS(ArchiveQueueNotEmpty);
  CTA_GENERATE_EXCEPTION_CLASS(WrongArchiveQueue);
  /** This function implicitly creates the archive queue structure and updates 
   * the pointer to it. It will implicitly commit the object to the store. */
  std::string addOrGetArchiveQueueAndCommit(const std::string & tapePool, AgentReference & agentRef, JobQueueType queueType);
  /** This function implicitly deletes the tape pool structure. 
   * Fails if it not empty*/
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchArchiveQueue);
  void removeArchiveQueueAndCommit(const std::string & tapePool, JobQueueType queueType, log::LogContext & lc);
  /** This function is used in a cleanup utility. Removes unconditionally the reference to the archive queue */
  void removeMissingArchiveQueueReference(const std::string & tapePool, JobQueueType queueType);
  void removeArchiveQueueIfAddressMatchesAndCommit(const std::string & tapePool, const std::string & archiveQueueAddress, JobQueueType queueType);
  std::string getArchiveQueueAddress(const std::string & tapePool, JobQueueType queueType);
  struct ArchiveQueueDump {
    std::string tapePool;
    std::string address;
  };
  std::list<ArchiveQueueDump> dumpArchiveQueues(JobQueueType queueType);
  
  // RetrieveQueue handling ====================================================
  CTA_GENERATE_EXCEPTION_CLASS(RetrieveQueueNotEmpty);
  CTA_GENERATE_EXCEPTION_CLASS(WrongRetrieveQueue);
  /** This function implicitly creates the retrieve queue structure and updates 
   * the pointer to it. It will implicitly commit the object to the store. */
  std::string addOrGetRetrieveQueueAndCommit(const std::string & vid, AgentReference & agentRef, JobQueueType queueType);
  /** This function is used in a cleanup utility. Removes unconditionally the reference to the retrieve queue */
  void removeMissingRetrieveQueueReference(const std::string & address, JobQueueType queueType);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchRetrieveQueue);
  void removeRetrieveQueueAndCommit(const std::string & vid, JobQueueType queueType, log::LogContext & lc);
  std::string getRetrieveQueueAddress(const std::string & vid, JobQueueType queueType);
  struct RetrieveQueueDump {
    std::string vid;
    std::string address;
  };
  std::list<RetrieveQueueDump> dumpRetrieveQueues(JobQueueType queueType);
  
  // Drive register manipulations ==============================================
  CTA_GENERATE_EXCEPTION_CLASS(DriveRegisterNotEmpty);
  std::string getDriveRegisterAddress();  
  std::string addOrGetDriveRegisterPointerAndCommit(AgentReference & agentRef, const EntryLogSerDeser & log);
  void removeDriveRegisterAndCommit(log::LogContext & lc);
  
  // Agent register manipulations ==============================================
  CTA_GENERATE_EXCEPTION_CLASS(AgentRegisterNotEmpty);
  std::string getAgentRegisterAddress();
  /** We do pass the agent here even if there is no agent register yet, as it
   * is used to generate the object name. We have the dedicated agent intent
   * log for tracking objects being created. We already use an agent here for
   * object name generation, but not yet tracking. */
  std::string addOrGetAgentRegisterPointerAndCommit(AgentReference & agentRef,
    const EntryLogSerDeser & log, log::LogContext & lc);
  void removeAgentRegisterAndCommit(log::LogContext & lc);
private:
  void addIntendedAgentRegistry(const std::string & address, log::LogContext & lc);
public:  
  // Scheduler global lock manipulations =======================================
  std::string getSchedulerGlobalLock();
  std::string addOrGetSchedulerGlobalLockAndCommit(AgentReference & agentRef, const EntryLogSerDeser & log);
  void removeSchedulerGlobalLockAndCommit(log::LogContext & lc);
  
  // Repack index manipulations ================================================
  std::string getRepackIndexAddress();
  std::string addOrGetRepackIndexAndCommit(AgentReference & agentRef);
  void removeRepackIndexAndCommit(log::LogContext & lc);
  
  // Repack queues manipulations ===============================================
  CTA_GENERATE_EXCEPTION_CLASS(RepackQueueNotEmpty);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchRepackQueue);
  std::string getRepackQueueAddress(common::dataStructures::RepackQueueType queueType);
  std::string addOrGetRepackQueueAndCommit(AgentReference & agentRef, common::dataStructures::RepackQueueType queueType);
  void removeRepackQueueAndCommit(common::dataStructures::RepackQueueType queueType, log::LogContext & lc);
private:
  void setRepackQueueAddress(common::dataStructures::RepackQueueType queueType, const std::string &queueAddress);
  void clearRepackQueueAddress(common::dataStructures::RepackQueueType queueType);
  
public:
  // Dump the root entry
  std::string dump ();
}; 

}}
