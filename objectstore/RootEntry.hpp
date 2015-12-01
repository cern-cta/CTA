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
#include "CreationLog.hpp"
#include "UserIdentity.hpp"
#include "common/MountControl.hpp"
#include <list>

namespace cta { namespace objectstore {

class Agent;
class GenericObject;
  
class RootEntry: public ObjectOps<serializers::RootEntry> {
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
  
  // Manipulations of AdminHosts ===============================================
  void addAdminHost(const std::string & hostname, const CreationLog & log);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchAdminHost);
  void removeAdminHost(const std::string & hostname);
  bool isAdminHost(const std::string & hostname);
  class AdminHostDump {
  public:
    std::string hostname;
    CreationLog log;
  };
  std::list<AdminHostDump> dumpAdminHosts();
  
  CTA_GENERATE_EXCEPTION_CLASS(DuplicateEntry);
  
  // Manipulations of Admin Users ==============================================
  void addAdminUser(const UserIdentity & user, const CreationLog & log);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchAdminUser);
  void removeAdminUser(const UserIdentity & user);
  bool isAdminUser(const UserIdentity & user);
  class AdminUserDump {
  public:
    UserIdentity user;
    CreationLog log;
  };
  std::list<AdminUserDump> dumpAdminUsers();
  
  // Manipulations of Storage Classes and archive routes =======================
  CTA_GENERATE_EXCEPTION_CLASS(MissingEntry);
  CTA_GENERATE_EXCEPTION_CLASS(IncompleteEntry);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchStorageClass);
  CTA_GENERATE_EXCEPTION_CLASS(StorageClassHasActiveRoutes);
  CTA_GENERATE_EXCEPTION_CLASS(InvalidCopyNumber);
  CTA_GENERATE_EXCEPTION_CLASS(CopyNumberOutOfRange);
private:
  // Totally arbitrary (but ridiculously high) copy number
  static const uint16_t maxCopyCount=100;
  void checkStorageClassCopyCount(uint16_t copyCount);
public:
  void addStorageClass(const std::string storageClass, uint16_t copyCount, 
    const CreationLog & log);
  void removeStorageClass(const std::string storageClass);
  void setStorageClassCopyCount(const std::string & storageClass,
    uint16_t copyCount, const CreationLog & cl);
  uint16_t getStorageClassCopyCount(const std::string & storageClass);
  CTA_GENERATE_EXCEPTION_CLASS(ArchiveRouteAlreadyExists);
  CTA_GENERATE_EXCEPTION_CLASS(TapePoolUsedInOtherRoute);
  void addArchiveRoute(const std::string & storageClass, uint16_t copyNb, 
    const std::string & tapePool, const CreationLog & cl);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchArchiveRoute);
  void removeArchiveRoute(const std::string & storageClass, uint16_t copyNb);

  /** Ordered vector of archive routes */
  std::vector<std::string> getArchiveRoutes (const std::string storageClass);
  class StorageClassDump {
  public:
    class ArchiveRouteDump {
    public:
      uint16_t copyNumber;
      std::string tapePool;
      CreationLog log;
    };
    std::string storageClass;
    uint16_t copyCount;
    std::list<ArchiveRouteDump> routes;
    CreationLog log;
  };
  std::list<StorageClassDump> dumpStorageClasses();
  StorageClassDump dumpStorageClass(const std::string & name);
  
  // Manipulations of libraries ================================================
  void addLibrary(const std::string & library, const CreationLog & log);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchLibrary);
  void removeLibrary(const std::string & library);
  bool libraryExists(const std::string & library);
  class LibraryDump {
  public:
    std::string library;
    CreationLog log;
  };
  std::list<LibraryDump> dumpLibraries();
  
  // TapePool Manipulations =====================================================
  CTA_GENERATE_EXCEPTION_CLASS(TapePoolNotEmpty);
  CTA_GENERATE_EXCEPTION_CLASS(WrongTapePool);
  /** This function implicitly creates the tape pool structure and updates 
   * the pointer to it. It needs to implicitly commit the object to the store. */
  std::string addOrGetTapePoolAndCommit(const std::string & tapePool,
    uint32_t nbPartialTapes, uint16_t maxRetriesPerMount, uint16_t maxTotalRetries,
    Agent & agent, const CreationLog & log);
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
    CreationLog log;
  };
  std::list<TapePoolDump> dumpTapePools();
  
  // Drive register manipulations ==============================================
  CTA_GENERATE_EXCEPTION_CLASS(DriveRegisterNotEmpty);
  std::string getDriveRegisterAddress();  
  std::string addOrGetDriveRegisterPointerAndCommit(Agent & agent, const CreationLog & log);
  void removeDriveRegisterAndCommit();
  
  // Agent register manipulations ==============================================
  CTA_GENERATE_EXCEPTION_CLASS(AgentRegisterNotEmpty);
  std::string getAgentRegisterAddress();
  /** We do pass the agent here even if there is no agent register yet, as it
   * is used to generate the object name. We have the dedicated agent intent
   * log for tracking objects being created. We already use an agent here for
   * object name generation, but not yet tracking. */
  std::string addOrGetAgentRegisterPointerAndCommit(Agent & agent,
    const CreationLog & log);
  void removeAgentRegisterAndCommit();

  // Agent register manipulations ==============================================
  std::string getSchedulerGlobalLock();
  std::string addOrGetSchedulerGlobalLockAndCommit(Agent & agent, const CreationLog & log);
  void removeSchedulerGlobalLockAndCommit();
  
private:
  void addIntendedAgentRegistry(const std::string & address);
  
public:
  // Dump the root entry
  std::string dump ();
};

}}


