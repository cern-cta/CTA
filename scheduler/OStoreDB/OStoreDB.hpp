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

namespace cta {
  
namespace objectstore {
  class Backend;    
}
  
class OStoreDB: public SchedulerDatabase {
public:
  OStoreDB(objectstore::Backend & be);
  virtual ~OStoreDB() throw();
  

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
  virtual void createStorageClass(const SecurityIdentity& requester, 
    const std::string& name, const uint16_t nbCopies, const std::string& comment);
    

  virtual StorageClass getStorageClass(const std::string& name) const;


  virtual std::list<StorageClass> getStorageClasses() const;


  virtual void deleteStorageClass(const SecurityIdentity& requester, 
    const std::string& name);

  /* === Archival routes handling  ========================================== */
  virtual void createArchivalRoute(const SecurityIdentity& requester, 
    const std::string& storageClassName, const uint16_t copyNb, 
    const std::string& tapePoolName, const std::string& comment);

  virtual std::list<ArchivalRoute> getArchivalRoutes(const std::string& storageClassName) const;

  virtual std::list<ArchivalRoute> getArchivalRoutes() const;

  virtual void deleteArchivalRoute(const SecurityIdentity& requester, 
    const std::string& storageClassName, const uint16_t copyNb);

  /* === Tape pools handling  =============================================== */
  virtual void createTapePool(const SecurityIdentity& requester, 
    const std::string& name, const uint32_t nbPartialTapes, 
    const std::string& comment);

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
  virtual void createLogicalLibrary(const SecurityIdentity& requester, 
    const std::string& name, const std::string& comment);

  virtual std::list<LogicalLibrary> getLogicalLibraries() const;

  virtual void deleteLogicalLibrary(const SecurityIdentity& requester, const std::string& name);

  /* === Archival requests handling  ======================================== */
  virtual void queue(const ArchiveToFileRequest& rqst);

  virtual void queue(const ArchiveToDirRequest& rqst);

  virtual void deleteArchiveToFileRequest(const SecurityIdentity& requester, 
    const std::string& archiveFile);

  virtual std::map<TapePool, std::list<ArchiveToTapeCopyRequest> > 
    getArchiveRequests() const;

  virtual std::list<ArchiveToTapeCopyRequest> 
    getArchiveRequests(const std::string& tapePoolName) const;

  /* === Retrieve requests handling  ======================================== */
  virtual void queue(const RetrieveToFileRequest& rqst_);

  virtual void queue(const RetrieveToDirRequest& rqst);

  virtual std::list<RetrieveFromTapeCopyRequest> getRetrieveRequests(const std::string& vid) const;

  virtual std::map<Tape, std::list<RetrieveFromTapeCopyRequest> > getRetrieveRequests() const;

  virtual void deleteRetrieveRequest(const SecurityIdentity& requester, 
    const std::string& remoteFile);
  
private:
  objectstore::Backend & m_objectStore;
};
  
}
