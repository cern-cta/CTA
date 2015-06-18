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

#include "scheduler/SchedulerDatabase.hpp"
#include "objectstore/Agent.hpp"

namespace cta {
  
namespace objectstore {
  class Backend;
  class Agent;
}
  
class OStoreDB: public SchedulerDatabase {
public:
  OStoreDB(objectstore::Backend & be);
  virtual ~OStoreDB() throw();
  
  /* === Object store and agent handling ==================================== */
  void setAgent(objectstore::Agent &agent);
  CTA_GENERATE_EXCEPTION_CLASS(AgentNotSet);
private:
  void assertAgentSet();
public:

  /* === Admin host handling ================================================ */
  virtual void createAdminHost(const SecurityIdentity& requester,
    const std::string& hostName, const std::string& comment);


    virtual std::list<AdminHost> getAdminHosts() const;

  
  virtual void deleteAdminHost(const SecurityIdentity& requester,
    const std::string& hostName);
  
  /* === Admin host handling ================================================ */
  virtual void createAdminUser(const SecurityIdentity& requester, 
    const UserIdentity& user, const std::string& comment);
    

    virtual std::list<AdminUser> getAdminUsers() const;


  virtual void deleteAdminUser(const SecurityIdentity& requester,
    const UserIdentity& user);

  /* === Admin access handling ============================================== */
  virtual void assertIsAdminOnAdminHost(const SecurityIdentity& id) const;

  /* === Storage class handling  ============================================ */
  virtual void createStorageClass(const std::string& name,
    const uint16_t nbCopies, const CreationLog& creationLog);
    

  virtual StorageClass getStorageClass(const std::string& name) const;


  virtual std::list<StorageClass> getStorageClasses() const;


  virtual void deleteStorageClass(const SecurityIdentity& requester, 
    const std::string& name);

  /* === Archival routes handling  ========================================== */
  virtual void createArchivalRoute(const std::string& storageClassName,
    const uint16_t copyNb, const std::string& tapePoolName,
    const CreationLog& creationLog);

  CTA_GENERATE_EXCEPTION_CLASS(IncompleteRouting);
  virtual std::list<ArchivalRoute> getArchivalRoutes(const std::string& storageClassName) const;

  virtual std::list<ArchivalRoute> getArchivalRoutes() const;

  virtual void deleteArchivalRoute(const SecurityIdentity& requester, 
    const std::string& storageClassName, const uint16_t copyNb);

  /* === Tape pools handling  =============================================== */
  virtual void createTapePool(const std::string& name, 
    const uint32_t nbPartialTapes, const cta::CreationLog &creationLog);

  virtual std::list<TapePool> getTapePools() const;

  virtual void deleteTapePool(const SecurityIdentity& requester, const std::string& name);

  /* === Tapes handling  ==================================================== */
  virtual void createTape(const SecurityIdentity& requester, 
    const std::string& vid, const std::string& logicalLibraryName, 
    const std::string& tapePoolName, const uint64_t capacityInBytes, 
    const std::string& comment);

  virtual std::list<Tape> getTapes() const;

  virtual void deleteTape(const SecurityIdentity& requester, const std::string& vid);

  /* === Libraries handling  ================================================ */
  virtual void createLogicalLibrary(const std::string& name,
    const cta::CreationLog& creationLog);

  virtual std::list<LogicalLibrary> getLogicalLibraries() const;

  virtual void deleteLogicalLibrary(const SecurityIdentity& requester, const std::string& name);

  /* === Archival requests handling  ======================================== */
  virtual void queue(const ArchiveToFileRequest& rqst);

  virtual void queue(const ArchiveToDirRequest& rqst);

  virtual void deleteArchiveRequest(const SecurityIdentity& requester, const std::string& archiveFile);

  virtual void markArchiveRequestForDeletion(const SecurityIdentity &requester, const std::string &archiveFile);

  virtual void fileEntryDeletedFromNS(const SecurityIdentity &requester, const std::string &archiveFile);

  virtual void fileEntryCreatedInNS(const SecurityIdentity &requester, const std::string &archiveFile);

  virtual std::map<TapePool, std::list<ArchiveToTapeCopyRequest> > getArchiveRequests() const;

  virtual std::list<ArchiveToTapeCopyRequest> getArchiveRequests(const std::string& tapePoolName) const;

  /* === Retrieve requests handling  ======================================== */
  virtual void queue(const RetrieveToFileRequest& rqst_);

  virtual void queue(const RetrieveToDirRequest& rqst);

  virtual std::list<RetrieveFromTapeCopyRequest> getRetrieveRequests(const std::string& vid) const;

  virtual std::map<Tape, std::list<RetrieveFromTapeCopyRequest> > getRetrieveRequests() const;

  virtual void deleteRetrieveRequest(const SecurityIdentity& requester, 
    const std::string& remoteFile);
  
private:
  objectstore::Backend & m_objectStore;
  objectstore::Agent * m_agent;
};
  
}
