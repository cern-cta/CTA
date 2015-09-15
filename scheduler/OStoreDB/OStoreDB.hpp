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
#include "objectstore/ArchiveToFileRequest.hpp"
#include "objectstore/SchedulerGlobalLock.hpp"

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
  
  CTA_GENERATE_EXCEPTION_CLASS(NotImplemented);
  /* === Session handling =================================================== */
  class TapeMountDecisionInfo: public SchedulerDatabase::TapeMountDecisionInfo {
    friend class OStoreDB;
  public:
    CTA_GENERATE_EXCEPTION_CLASS(SchedulingLockNotHeld);
    CTA_GENERATE_EXCEPTION_CLASS(TapeNotWritable);
    CTA_GENERATE_EXCEPTION_CLASS(TapeIsBusy);
    virtual std::unique_ptr<SchedulerDatabase::ArchiveMount> createArchiveMount(
      const std::string & vid, const std::string & tapePool,
      const std::string driveName, const std::string& logicalLibrary, 
      const std::string & hostName, time_t startTime);
    virtual std::unique_ptr<SchedulerDatabase::RetrieveMount> createRetrieveMount(const std::string & vid,
      const std::string driveName);
    virtual ~TapeMountDecisionInfo();
  private:
    TapeMountDecisionInfo (objectstore::Backend &, objectstore::Agent &);
    bool m_lockTaken;
    objectstore::ScopedExclusiveLock m_lockOnSchedulerGlobalLock;
    std::unique_ptr<objectstore::SchedulerGlobalLock> m_schedulerGlobalLock;
    objectstore::Backend & m_objectStore;
    objectstore::Agent & m_agent;
  };

  virtual std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> getMountInfo();
  
  /* === Archive Mount handling ============================================= */
  class ArchiveMount: public SchedulerDatabase::ArchiveMount {
    friend class TapeMountDecisionInfo;
  private:
    ArchiveMount(objectstore::Backend &, objectstore::Agent &);
    objectstore::Backend & m_objectStore;
    objectstore::Agent & m_agent;
    uint64_t m_nextFseq;
  public:
    virtual const MountInfo & getMountInfo();
    virtual std::unique_ptr<ArchiveJob> getNextJob();
  };
  
  /* === Archive Job Handling =============================================== */
  class ArchiveJob: public SchedulerDatabase::ArchiveJob {
    friend class ArchiveMount;
  public:
    CTA_GENERATE_EXCEPTION_CLASS(JobNowOwned);
    CTA_GENERATE_EXCEPTION_CLASS(NoSuchJob);
    virtual void succeed();
    virtual void fail();
    virtual void bumpUpTapeFileCount(const std::string& vid, uint64_t newFileCount);
    virtual ~ArchiveJob();
  private:
    ArchiveJob(const std::string &, objectstore::Backend &, objectstore::Agent &);
    bool m_jobOwned;
    uint16_t m_copyNb;
    uint64_t m_mountId;
    std::string m_tapePool;
    objectstore::Backend & m_objectStore;
    objectstore::Agent & m_agent;
    objectstore::ArchiveToFileRequest m_atfr;
  };

  /* === Admin host handling ================================================ */
  virtual void createAdminHost(const std::string& hostName, 
    const CreationLog& creationLog);


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

  /* === Archive routes handling  =========================================== */
  virtual void createArchiveRoute(const std::string& storageClassName,
    const uint16_t copyNb, const std::string& tapePoolName,
    const CreationLog& creationLog);

  CTA_GENERATE_EXCEPTION_CLASS(IncompleteRouting);
  virtual std::list<ArchiveRoute> getArchiveRoutes(const std::string& storageClassName) const;

  virtual std::list<ArchiveRoute> getArchiveRoutes() const;

  virtual void deleteArchiveRoute(const SecurityIdentity& requester, 
    const std::string& storageClassName, const uint16_t copyNb);

  /* === Tape pools handling  =============================================== */
  virtual void createTapePool(const std::string& name, 
    const uint32_t nbPartialTapes, const cta::CreationLog &creationLog);
  

  virtual void setTapePoolMountCriteria(const std::string& tapePool,
    const MountCriteriaByDirection& mountCriteriaByDirection);


  virtual std::list<TapePool> getTapePools() const;

  virtual void deleteTapePool(const SecurityIdentity& requester, const std::string& name);

  /* === Tapes handling  ==================================================== */
  CTA_GENERATE_EXCEPTION_CLASS(TapeAlreadyExists);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchLibrary);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchTapePool);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchTape);
  virtual void createTape(const std::string& vid, const std::string& logicalLibraryName, 
    const std::string& tapePoolName, const uint64_t capacityInBytes, 
    const cta::CreationLog& creationLog);

  virtual Tape getTape(const std::string &vid) const;

  virtual std::list<Tape> getTapes() const;

  virtual void deleteTape(const SecurityIdentity& requester, const std::string& vid);

  /* === Libraries handling  ================================================ */
  virtual void createLogicalLibrary(const std::string& name,
    const cta::CreationLog& creationLog);

  virtual std::list<LogicalLibrary> getLogicalLibraries() const;

  CTA_GENERATE_EXCEPTION_CLASS(LibraryInUse);
  virtual void deleteLogicalLibrary(const SecurityIdentity& requester, const std::string& name);

  /* === Archive requests handling  ========================================= */
  CTA_GENERATE_EXCEPTION_CLASS(ArchiveRequestHasNoCopies);
  CTA_GENERATE_EXCEPTION_CLASS(ArchiveRequestAlreadyCompleteOrCanceled);
  class ArchiveToFileRequestCreation: 
   public cta::SchedulerDatabase::ArchiveToFileRequestCreation {
  public:
    ArchiveToFileRequestCreation(objectstore::Agent * agent, 
      objectstore::Backend & be): m_request(be), m_lock(), m_objectStore(be), 
      m_agent(agent), m_closed(false) {}
    virtual void complete();
    virtual void cancel();
    virtual ~ArchiveToFileRequestCreation();
  private:
    objectstore::ArchiveToFileRequest m_request;
    objectstore::ScopedExclusiveLock m_lock;
    objectstore::Backend & m_objectStore;
    objectstore::Agent * m_agent;
    bool m_closed;
    friend class cta::OStoreDB;
  };
    
  virtual std::unique_ptr<cta::SchedulerDatabase::ArchiveToFileRequestCreation> 
    queue(const ArchiveToFileRequest& rqst);

  virtual void queue(const ArchiveToDirRequest& rqst);

  CTA_GENERATE_EXCEPTION_CLASS(NoSuchArchiveRequest);
  CTA_GENERATE_EXCEPTION_CLASS(ArchiveRequestAlreadyDeleted);
  virtual void deleteArchiveRequest(const SecurityIdentity& requester, const std::string& archiveFile);
  class ArchiveToFileRequestCancelation:
    public SchedulerDatabase::ArchiveToFileRequestCancelation {
  public:
    ArchiveToFileRequestCancelation(objectstore::Agent * agent, 
      objectstore::Backend & be): m_request(be), m_lock(), m_objectStore(be), 
      m_agent(agent), m_closed(false) {} 
    virtual ~ArchiveToFileRequestCancelation();
    virtual void complete();
  private:
    objectstore::ArchiveToFileRequest m_request;
    objectstore::ScopedExclusiveLock m_lock;
    objectstore::Backend & m_objectStore;
    objectstore::Agent * m_agent;
    bool m_closed;
    friend class OStoreDB;
  };
  virtual std::unique_ptr<SchedulerDatabase::ArchiveToFileRequestCancelation> markArchiveRequestForDeletion(const SecurityIdentity &requester, const std::string &archiveFile);

  virtual std::map<TapePool, std::list<ArchiveToTapeCopyRequest> > getArchiveRequests() const;

  virtual std::list<ArchiveToTapeCopyRequest> getArchiveRequests(const std::string& tapePoolName) const;

  /* === Retrieve requests handling  ======================================== */
  CTA_GENERATE_EXCEPTION_CLASS(RetrieveRequestHasNoCopies);
  CTA_GENERATE_EXCEPTION_CLASS(TapeCopyNumberOutOfRange);
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
