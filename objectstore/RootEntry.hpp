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
#include <list>

namespace cta { namespace objectstore {

class Agent;
  
class RootEntry: public ObjectOps<serializers::RootEntry> {
public:
  // Constructor
  RootEntry(Backend & os);
  
  CTA_GENERATE_EXCEPTION_CLASS(NotAllocated);
  
  // In memory initialiser
  void initialize();
  
  // Manipulations of AdminHosts ===============================================
  void addAdminHost(const std::string & hostname, const CreationLog & log);
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
  void removeAdminUser(const UserIdentity & user);
  bool isAdminUser(const UserIdentity & user);
  class AdminUserDump {
  public:
    UserIdentity user;
    CreationLog log;
  };
  std::list<AdminUserDump> dumpAdminUsers();
  
  // Manipulations of Storage Classes and archival routes ======================
  CTA_GENERATE_EXCEPTION_CLASS(MissingEntry);
  CTA_GENERATE_EXCEPTION_CLASS(IncompleteEntry);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchStorageClass);
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
  void setArchiveRoute(const std::string & storageClass, uint16_t copyNb, 
    const std::string & tapePool, const CreationLog & cl);

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
  };
  std::list<StorageClassDump> dumpStorageClasses();
  
  // Manipulations of libraries ================================================
  void addLibrary(const std::string & library, const CreationLog & log);
  void removeLibrary(const std::string & library);
  bool libraryExists(const std::string & library);
  std::list<std::string> dumpLibraries();
  
  // TapePoolManipulations =====================================================
  CTA_GENERATE_EXCEPTION_CLASS(TapePoolNotEmpty);
  CTA_GENERATE_EXCEPTION_CLASS(WrongTapePool);
  /** This function implicitly creates the tape pool structure and updates 
   * the pointer to it. It needs to implicitly commit the object to the store. */
  void addTapePoolAndCommit(const std::string & tapePool, const CreationLog & log, 
    Agent & agent);
  /** This function implicitly deletes the tape pool structure. 
   * Fails if it not empty*/
  void removeTapePoolAndCommit(const std::string & tapePool, Agent & agent);
  std::string getTapePoolAddress(const std::string & tapePool);
  class TapePoolDump {
  public:
    std::string tapePool;
    std::string address;
    CreationLog log;
  };
  std::list<TapePoolDump> dumpTapePool();
  
  // Drive register manipulations ==============================================
  std::string getDriveRegisterPointer();  
  std::string addOrGetDriveRegisterPointer(const CreationLog & log, Agent & agent);
  std::string removeDriveRegister();
  
  // Agent register manipulations ==============================================
  CTA_GENERATE_EXCEPTION_CLASS(AgentRegisterNotEmpty);
  std::string getAgentRegisterAddress();
  /** We do pass the agent here even if there is no agent register yet, as it
   * is used to generate the object name. We have the dedicated agent intent
   * log for tracking objects being created. We already use an agent here for
   * object name generation, but not yet tracking. */
  std::string addOrGetAgentRegisterPointerAndCommit(Agent & agent,
    const CreationLog & log);
  void removeAgentRegister();

private:
  void addIntendedAgentRegistry(const std::string & address);
  
public:
  // Dump the root entry
  std::string dump ();
};

}}


