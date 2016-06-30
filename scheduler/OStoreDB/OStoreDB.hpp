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
#include "objectstore/ArchiveRequest.hpp"
#include "objectstore/ArchiveRequest.hpp"
#include "objectstore/DriveRegister.hpp"
#include "objectstore/RetrieveRequest.hpp"
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
    std::unique_ptr<SchedulerDatabase::ArchiveMount> createArchiveMount(
      const catalogue::TapeForWriting & tape,
      const std::string driveName, const std::string& logicalLibrary, 
      const std::string & hostName, time_t startTime) override;
    std::unique_ptr<SchedulerDatabase::RetrieveMount> createRetrieveMount(
      const std::string & vid, const std::string & tapePool,
      const std::string driveName,
      const std::string& logicalLibrary, const std::string& hostName, 
      time_t startTime) override;
    virtual ~TapeMountDecisionInfo();
  private:
    TapeMountDecisionInfo (objectstore::Backend &, objectstore::Agent &);
    bool m_lockTaken;
    objectstore::ScopedExclusiveLock m_lockOnSchedulerGlobalLock;
    std::unique_ptr<objectstore::SchedulerGlobalLock> m_schedulerGlobalLock;
    objectstore::Backend & m_objectStore;
    objectstore::Agent & m_agent;
  };

  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> getMountInfo() override;
  
  /* === Archive Mount handling ============================================= */
  class ArchiveMount: public SchedulerDatabase::ArchiveMount {
    friend class TapeMountDecisionInfo;
  private:
    ArchiveMount(objectstore::Backend &, objectstore::Agent &);
    objectstore::Backend & m_objectStore;
    objectstore::Agent & m_agent;
  public:
    CTA_GENERATE_EXCEPTION_CLASS(MaxFSeqNotGoingUp);
    const MountInfo & getMountInfo() override;
    std::unique_ptr<ArchiveJob> getNextJob() override;
    void complete(time_t completionTime) override;
    void setDriveStatus(cta::common::DriveStatus status, time_t completionTime) override;
  };
  
  /* === Archive Job Handling =============================================== */
  class ArchiveJob: public SchedulerDatabase::ArchiveJob {
    friend class OStoreDB::ArchiveMount;
  public:
    CTA_GENERATE_EXCEPTION_CLASS(JobNowOwned);
    CTA_GENERATE_EXCEPTION_CLASS(NoSuchJob);
    void succeed() override;
    void fail() override;
    void bumpUpTapeFileCount(uint64_t newFileCount) override;
    ~ArchiveJob() override;
  private:
    ArchiveJob(const std::string &, objectstore::Backend &,
      objectstore::Agent &, ArchiveMount &);
    bool m_jobOwned;
    uint64_t m_mountId;
    std::string m_tapePool;
    objectstore::Backend & m_objectStore;
    objectstore::Agent & m_agent;
    objectstore::ArchiveRequest m_archiveRequest;
    ArchiveMount & m_archiveMount;
  };
  
  /* === Retrieve Mount handling ============================================ */
  class RetrieveMount: public SchedulerDatabase::RetrieveMount {
    friend class TapeMountDecisionInfo;
  private:
    RetrieveMount(objectstore::Backend &, objectstore::Agent &);
    objectstore::Backend & m_objectStore;
    objectstore::Agent & m_agent;
  public:
    const MountInfo & getMountInfo() override;
    std::unique_ptr<RetrieveJob> getNextJob() override;
    void complete(time_t completionTime) override;
    void setDriveStatus(cta::common::DriveStatus status, time_t completionTime) override;
  };
  
  /* === Retrieve Job handling ============================================== */
  class RetrieveJob: public SchedulerDatabase::RetrieveJob {
    friend class RetrieveMount;
  public:
    CTA_GENERATE_EXCEPTION_CLASS(JobNowOwned);
    CTA_GENERATE_EXCEPTION_CLASS(NoSuchJob);
    virtual void succeed() override;
    virtual void fail() override;
    virtual ~RetrieveJob() override;
  private:
    RetrieveJob(const std::string &, objectstore::Backend &, objectstore::Agent &);
    bool m_jobOwned;
    uint16_t m_copyNb;
    objectstore::Backend & m_objectStore;
    objectstore::Agent & m_agent;
    objectstore::RetrieveRequest m_retrieveRequest;
    std::map<std::string, std::string> m_vidToAddress; /**< Cache of tape objects
                                                        *  addresses filled up at queuing time */
  };
  
  /* === Archive requests handling  ========================================= */
  CTA_GENERATE_EXCEPTION_CLASS(ArchiveRequestHasNoCopies);
  CTA_GENERATE_EXCEPTION_CLASS(ArchiveRequestAlreadyCompleteOrCanceled);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchArchiveQueue);
  
  void queue(const cta::common::dataStructures::ArchiveRequest &request, 
    const cta::common::dataStructures::ArchiveFileQueueCriteria &criteria) override;

  CTA_GENERATE_EXCEPTION_CLASS(NoSuchArchiveRequest);
  CTA_GENERATE_EXCEPTION_CLASS(ArchiveRequestAlreadyDeleted);
  virtual void deleteArchiveRequest(const common::dataStructures::SecurityIdentity& requester, uint64_t fileId) override;
  class ArchiveToFileRequestCancelation:
    public SchedulerDatabase::ArchiveToFileRequestCancelation {
  public:
    ArchiveToFileRequestCancelation(objectstore::Agent * agent, 
      objectstore::Backend & be): m_request(be), m_lock(), m_objectStore(be), 
      m_agent(agent), m_closed(false) {} 
    virtual ~ArchiveToFileRequestCancelation();
    void complete() override;
  private:
    objectstore::ArchiveRequest m_request;
    objectstore::ScopedExclusiveLock m_lock;
    objectstore::Backend & m_objectStore;
    objectstore::Agent * m_agent;
    bool m_closed;
    friend class OStoreDB;
  };
  std::unique_ptr<SchedulerDatabase::ArchiveToFileRequestCancelation> markArchiveRequestForDeletion(const common::dataStructures::SecurityIdentity &cliIdentity, uint64_t fileId) override;

  std::map<std::string, std::list<common::dataStructures::ArchiveJob> > getArchiveJobs() const override;
  
  std::list<cta::common::dataStructures::ArchiveJob> getArchiveJobs(const std::string& tapePoolName) const override;

  /* === Retrieve requests handling  ======================================== */
  CTA_GENERATE_EXCEPTION_CLASS(RetrieveRequestHasNoCopies);
  CTA_GENERATE_EXCEPTION_CLASS(TapeCopyNumberOutOfRange);
  void queue(const cta::common::dataStructures::RetrieveRequest& rqst,
    const cta::common::dataStructures::RetrieveFileQueueCriteria &criteria) override;

  std::list<RetrieveRequestDump> getRetrieveRequestsByVid(const std::string& vid) const override;
  
  std::list<RetrieveRequestDump> getRetrieveRequestsByRequester(const std::string& vid) const override;

  std::map<std::string, std::list<RetrieveRequestDump> > getRetrieveRequests() const override;

  void deleteRetrieveRequest(const common::dataStructures::SecurityIdentity& requester, 
    const std::string& remoteFile) override;
  
  /* === Drive state handling  ============================================== */
  std::list<cta::common::DriveState> getDriveStates() const override;
private:
  objectstore::Backend & m_objectStore;
  objectstore::Agent * m_agent;
};
  
}
