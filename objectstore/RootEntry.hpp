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

#pragma once

#include "objectstore/cta.pb.h"

#include "Backend.hpp"
#include "ObjectOps.hpp"
#include "EntryLog.hpp"
#include "UserIdentity.hpp"
#include "common/MountControl.hpp"
#include <list>

namespace cta { namespace objectstore {

class Agent;
class GenericObject;
  
class RootEntry: public ObjectOps<serializers::RootEntry, serializers::RootEntry_t> {
public:
  // Constructor
  RootEntry(Backend & os);
  RootEntry(GenericObject & go);
  
  CTA_GENERATE_EXCEPTION_CLASS(NotAllocated);
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  
  // In memory initialiser
  void initialize();
  
  // Emptyness checker
  bool isEmpty();
  
  // Safe remover
  void removeIfEmpty();
  
  // TapePool Manipulations =====================================================
  CTA_GENERATE_EXCEPTION_CLASS(TapePoolNotEmpty);
  CTA_GENERATE_EXCEPTION_CLASS(WrongTapePool);
  /** This function implicitly creates the tape pool structure and updates 
   * the pointer to it. It needs to implicitly commit the object to the store. */
  std::string addOrGetTapePoolAndCommit(const std::string & tapePool,
    uint32_t nbPartialTapes, uint16_t maxRetriesPerMount, uint16_t maxTotalRetries,
    Agent & agent, const EntryLog & log);
  /** This function implicitly deletes the tape pool structure. 
   * Fails if it not empty*/
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchTapePool);
  CTA_GENERATE_EXCEPTION_CLASS(TapePoolUsedInRoute);
  void removeTapePoolAndCommit(const std::string & tapePool);
  std::string getTapePoolAddress(const std::string & tapePool);
  class TapePoolDump {
  public:
    std::string tapePool;
    std::string address;
    uint32_t nbPartialTapes;
    MountCriteriaByDirection mountCriteriaByDirection;
    EntryLog log;
  };
  std::list<TapePoolDump> dumpTapePools();
  
  // ArchiveQueue Manipulations =====================================================
  CTA_GENERATE_EXCEPTION_CLASS(TapePoolQueueNotEmpty);
  CTA_GENERATE_EXCEPTION_CLASS(WrongTapePoolQueue);
  /** This function implicitly creates the tape pool structure and updates 
   * the pointer to it. It needs to implicitly commit the object to the store. */
  std::string addOrGetArchiveQueueAndCommit(const std::string & tapePool, Agent & agent);
  /** This function implicitly deletes the tape pool structure. 
   * Fails if it not empty*/
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchTapePoolQueue);
  void removeTapePoolQueueAndCommit(const std::string & tapePool);
  std::string getTapePoolQueueAddress(const std::string & tapePool);
  class ArchiveQueueDump {
  public:
    std::string tapePool;
    std::string address;
  };
  std::list<ArchiveQueueDump> dumpArchiveQueues();
  
  // Drive register manipulations ==============================================
  CTA_GENERATE_EXCEPTION_CLASS(DriveRegisterNotEmpty);
  std::string getDriveRegisterAddress();  
  std::string addOrGetDriveRegisterPointerAndCommit(Agent & agent, const EntryLog & log);
  void removeDriveRegisterAndCommit();
  
  // Agent register manipulations ==============================================
  CTA_GENERATE_EXCEPTION_CLASS(AgentRegisterNotEmpty);
  std::string getAgentRegisterAddress();
  /** We do pass the agent here even if there is no agent register yet, as it
   * is used to generate the object name. We have the dedicated agent intent
   * log for tracking objects being created. We already use an agent here for
   * object name generation, but not yet tracking. */
  std::string addOrGetAgentRegisterPointerAndCommit(Agent & agent,
    const EntryLog & log);
  void removeAgentRegisterAndCommit();

  // Agent register manipulations ==============================================
  std::string getSchedulerGlobalLock();
  std::string addOrGetSchedulerGlobalLockAndCommit(Agent & agent, const EntryLog & log);
  void removeSchedulerGlobalLockAndCommit();
  
private:
  void addIntendedAgentRegistry(const std::string & address);
  
public:
  // Dump the root entry
  std::string dump ();
};

}}


