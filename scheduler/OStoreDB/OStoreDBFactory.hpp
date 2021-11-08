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

#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "scheduler/OStoreDB/OStoreDB.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/RetrieveRequestDump.hpp"
#include "objectstore/RootEntry.hpp"
#include "objectstore/Agent.hpp"
#include "objectstore/AgentReference.hpp"
#include "objectstore/BackendVFS.hpp"
#include "objectstore/BackendRados.hpp"
#include "objectstore/BackendFactory.hpp"
#include "common/log/DummyLogger.hpp"
#include "catalogue/DummyCatalogue.hpp"
#include <memory>

namespace cta {

namespace objectstore {
  class Backend;

/**
 * As slight variation of the SchedulerDatabase interface allowing
 * access to the underlying backend (this is hence OStoreDB specific).
 * This interface allows tests to go behind the scenes and create "broken" situations
 * and test recovery.
 */

class OStoreDBWrapperInterface: public SchedulerDatabase {
public:
  virtual objectstore::Backend & getBackend() = 0;
  virtual objectstore::AgentReference & getAgentReference() = 0;
  virtual cta::OStoreDB & getOstoreDB() = 0;

  //! Create a new agent to allow tests to continue after garbage collection
  virtual void replaceAgent(objectstore::AgentReference * agentReferencePtr) = 0;
};

}

namespace {

/**
 * A self contained class providing a "backend+SchedulerDB" in a box,
 * allowing creation (and deletion) of an object store for each instance of the
 * of the unit tests, as they require a fresh object store for each unit test.
 * It can be instantiated with both types of backends
 */
template <class BackendType>
class OStoreDBWrapper: public cta::objectstore::OStoreDBWrapperInterface {
public:
  OStoreDBWrapper(const std::string &context, std::unique_ptr<cta::catalogue::Catalogue>& catalogue, const std::string &URL = "");

  ~OStoreDBWrapper() throw () {}

  objectstore::Backend& getBackend() override { return *m_backend; }

  objectstore::AgentReference& getAgentReference() override { return *m_agentReferencePtr; }

  void replaceAgent(objectstore::AgentReference *agentReferencePtr) override {
    m_agentReferencePtr.reset(agentReferencePtr);
    objectstore::RootEntry re(*m_backend);
    objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    objectstore::Agent agent(m_agentReferencePtr->getAgentAddress(), *m_backend);
    agent.initialize();
    objectstore::EntryLogSerDeser cl("user0", "systemhost", time(NULL));
    log::LogContext lc(*m_logger);
    re.addOrGetAgentRegisterPointerAndCommit(*m_agentReferencePtr, cl, lc);
    rel.release();
    agent.insertAndRegisterSelf(lc);
    rel.lock(re);
    re.fetch();
    re.addOrGetDriveRegisterPointerAndCommit(*m_agentReferencePtr, cl);
    re.addOrGetSchedulerGlobalLockAndCommit(*m_agentReferencePtr, cl);
    rel.release();
    m_OStoreDB.setAgentReference(m_agentReferencePtr.get());
  }

  cta::OStoreDB& getOstoreDB() override { return m_OStoreDB; }

  void waitSubthreadsComplete() override {
    m_OStoreDB.waitSubthreadsComplete();
  }

  void ping() override {
    m_OStoreDB.ping();
  }

  std::string queueArchive(const std::string &instanceName, const cta::common::dataStructures::ArchiveRequest& request, const cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId& criteria, log::LogContext &logContext) override {
    return m_OStoreDB.queueArchive(instanceName, request, criteria, logContext);
  }

  void deleteRetrieveRequest(const common::dataStructures::SecurityIdentity& cliIdentity, const std::string& remoteFile) override {
    m_OStoreDB.deleteRetrieveRequest(cliIdentity, remoteFile);
  }

  std::list<cta::common::dataStructures::RetrieveJob> getRetrieveJobs(const std::string& tapePoolName) const override {
    return m_OStoreDB.getRetrieveJobs(tapePoolName);
  }

  std::map<std::string, std::list<common::dataStructures::RetrieveJob> > getRetrieveJobs() const override {
    return m_OStoreDB.getRetrieveJobs();
  }

  std::map<std::string, std::list<common::dataStructures::ArchiveJob> > getArchiveJobs() const override {
    return m_OStoreDB.getArchiveJobs();
  }

  std::list<cta::common::dataStructures::ArchiveJob> getArchiveJobs(const std::string& tapePoolName) const override {
    return m_OStoreDB.getArchiveJobs(tapePoolName);
  }

  std::unique_ptr<IArchiveJobQueueItor> getArchiveJobQueueItor(const std::string &tapePoolName,
    common::dataStructures::JobQueueType queueType) const override {
    return m_OStoreDB.getArchiveJobQueueItor(tapePoolName, queueType);
  }

  std::unique_ptr<IRetrieveJobQueueItor> getRetrieveJobQueueItor(const std::string &vid,
    common::dataStructures::JobQueueType queueType) const override {
    return m_OStoreDB.getRetrieveJobQueueItor(vid, queueType);
  }

  std::map<std::string, std::list<RetrieveRequestDump> > getRetrieveRequests() const override {
    return m_OStoreDB.getRetrieveRequests();
  }

  std::list<std::unique_ptr<ArchiveJob>> getNextArchiveJobsToReportBatch(uint64_t filesRequested, log::LogContext &lc) override {
    return m_OStoreDB.getNextArchiveJobsToReportBatch(filesRequested, lc);
  }

  JobsFailedSummary getArchiveJobsFailedSummary(log::LogContext &lc) override {
    return m_OStoreDB.getArchiveJobsFailedSummary(lc);
  }

  std::list<std::unique_ptr<RetrieveJob>> getNextRetrieveJobsToReportBatch(uint64_t filesRequested, log::LogContext &lc) override {
    return m_OStoreDB.getNextRetrieveJobsToReportBatch(filesRequested, lc);
  }

  std::list<std::unique_ptr<RetrieveJob>> getNextRetrieveJobsFailedBatch(uint64_t filesRequested, log::LogContext &lc) override {
    return m_OStoreDB.getNextRetrieveJobsFailedBatch(filesRequested, lc);
  }

  std::unique_ptr<RepackReportBatch> getNextRepackReportBatch(log::LogContext& lc) override {
    return m_OStoreDB.getNextRepackReportBatch(lc);
  }

  std::unique_ptr<RepackReportBatch> getNextSuccessfulRetrieveRepackReportBatch(log::LogContext& lc) override {
    return m_OStoreDB.getNextSuccessfulRetrieveRepackReportBatch(lc);
  }

  std::unique_ptr<RepackReportBatch> getNextSuccessfulArchiveRepackReportBatch(log::LogContext& lc) override {
    return m_OStoreDB.getNextSuccessfulArchiveRepackReportBatch(lc);
  }

  std::unique_ptr<RepackReportBatch> getNextFailedRetrieveRepackReportBatch(log::LogContext& lc) override {
    return m_OStoreDB.getNextFailedRetrieveRepackReportBatch(lc);
  }

  std::unique_ptr<RepackReportBatch> getNextFailedArchiveRepackReportBatch(log::LogContext& lc) override {
    return m_OStoreDB.getNextFailedArchiveRepackReportBatch(lc);
  }

  std::list<std::unique_ptr<SchedulerDatabase::RepackReportBatch>> getRepackReportBatches(log::LogContext &lc) override {
    return m_OStoreDB.getRepackReportBatches(lc);
  }

  JobsFailedSummary getRetrieveJobsFailedSummary(log::LogContext &lc) override {
    return m_OStoreDB.getRetrieveJobsFailedSummary(lc);
  }

  void setArchiveJobBatchReported(std::list<cta::SchedulerDatabase::ArchiveJob*>& jobsBatch, log::TimingList & timingList,
      utils::Timer & t, log::LogContext& lc) override {
    m_OStoreDB.setArchiveJobBatchReported(jobsBatch, timingList, t, lc);
  }

  void setRetrieveJobBatchReportedToUser(std::list<cta::SchedulerDatabase::RetrieveJob*>& jobsBatch, log::TimingList & timingList,
      utils::Timer & t, log::LogContext& lc) override {
    m_OStoreDB.setRetrieveJobBatchReportedToUser(jobsBatch, timingList, t, lc);
  }

  std::list<RetrieveRequestDump> getRetrieveRequestsByVid(const std::string& vid) const override {
    return m_OStoreDB.getRetrieveRequestsByVid(vid);
  }

  std::list<RetrieveRequestDump> getRetrieveRequestsByRequester(const std::string& requester) const override {
    return m_OStoreDB.getRetrieveRequestsByRequester(requester);
  }


  std::unique_ptr<TapeMountDecisionInfo> getMountInfo(log::LogContext& logContext) override {
    return m_OStoreDB.getMountInfo(logContext);
  }

  void trimEmptyQueues(log::LogContext& lc) override {
    m_OStoreDB.trimEmptyQueues(lc);
  }

  std::unique_ptr<TapeMountDecisionInfo> getMountInfoNoLock(PurposeGetMountInfo purpose, log::LogContext& logContext) override {
    return m_OStoreDB.getMountInfoNoLock(purpose,logContext);
  }

  std::list<RetrieveQueueStatistics> getRetrieveQueueStatistics(const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria,
          const std::set<std::string> & vidsToConsider) override {
    return m_OStoreDB.getRetrieveQueueStatistics(criteria, vidsToConsider);
  }

  SchedulerDatabase::RetrieveRequestInfo queueRetrieve(common::dataStructures::RetrieveRequest& rqst,
    const common::dataStructures::RetrieveFileQueueCriteria &criteria, const optional<std::string> diskSystemName,
    log::LogContext &logContext) override {
    return m_OStoreDB.queueRetrieve(rqst, criteria, diskSystemName, logContext);
  }

  void cancelArchive(const common::dataStructures::DeleteArchiveRequest& request, log::LogContext & lc) override {
    m_OStoreDB.cancelArchive(request,lc);
  }

  void cancelRetrieve(const std::string& instanceName, const cta::common::dataStructures::CancelRetrieveRequest& rqst,
    log::LogContext& lc) override {
    m_OStoreDB.cancelRetrieve(instanceName, rqst, lc);
  }

  void deleteFailed(const std::string &objectId, log::LogContext & lc) override {
    m_OStoreDB.deleteFailed(objectId, lc);
  }

  std::string queueRepack(const SchedulerDatabase::QueueRepackRequest & repackRequest, log::LogContext& lc) override {
    return m_OStoreDB.queueRepack(repackRequest, lc);
  }

  std::list<common::dataStructures::RepackInfo> getRepackInfo() override {
    return m_OStoreDB.getRepackInfo();
  }

  common::dataStructures::RepackInfo getRepackInfo(const std::string& vid) override {
    return m_OStoreDB.getRepackInfo(vid);
  }

  void cancelRepack(const std::string& vid, log::LogContext & lc) override {
    m_OStoreDB.cancelRepack(vid, lc);
  }

  std::unique_ptr<RepackRequestStatistics> getRepackStatistics() override {
    return m_OStoreDB.getRepackStatistics();
  }

  std::unique_ptr<RepackRequestStatistics> getRepackStatisticsNoLock() override {
    return m_OStoreDB.getRepackStatisticsNoLock();
  }

  std::unique_ptr<RepackRequest> getNextRepackJobToExpand() override {
    return m_OStoreDB.getNextRepackJobToExpand();
  }

private:
  std::unique_ptr <cta::log::Logger> m_logger;
  std::unique_ptr <cta::objectstore::Backend> m_backend;
  std::unique_ptr <cta::catalogue::Catalogue> & m_catalogue;
  cta::OStoreDB m_OStoreDB;
  std::unique_ptr<objectstore::AgentReference> m_agentReferencePtr;
};

template <>
OStoreDBWrapper<cta::objectstore::BackendVFS>::OStoreDBWrapper(
        const std::string &context, std::unique_ptr<cta::catalogue::Catalogue> & catalogue, const std::string &URL) :
m_logger(new cta::log::DummyLogger("", "")), m_backend(new cta::objectstore::BackendVFS()),
m_catalogue(catalogue),
m_OStoreDB(*m_backend, *m_catalogue, *m_logger),
  m_agentReferencePtr(new objectstore::AgentReference("OStoreDBFactory", *m_logger))
{
  // We need to populate the root entry before using.
  objectstore::RootEntry re(*m_backend);
  re.initialize();
  re.insert();
  objectstore::ScopedExclusiveLock rel(re);
  re.fetch();
  objectstore::Agent agent(m_agentReferencePtr->getAgentAddress(), *m_backend);
  agent.initialize();
  objectstore::EntryLogSerDeser cl("user0", "systemhost", time(NULL));
  log::LogContext lc(*m_logger);
  re.addOrGetAgentRegisterPointerAndCommit(*m_agentReferencePtr, cl, lc);
  rel.release();
  agent.insertAndRegisterSelf(lc);
  rel.lock(re);
  re.fetch();
  re.addOrGetDriveRegisterPointerAndCommit(*m_agentReferencePtr, cl);
  re.addOrGetSchedulerGlobalLockAndCommit(*m_agentReferencePtr, cl);
  rel.release();
  m_OStoreDB.setAgentReference(m_agentReferencePtr.get());
}

template <>
OStoreDBWrapper<cta::objectstore::BackendRados>::OStoreDBWrapper(
        const std::string &context,std::unique_ptr<cta::catalogue::Catalogue> & catalogue, const std::string &URL) :
m_logger(new cta::log::DummyLogger("", "")), m_backend(cta::objectstore::BackendFactory::createBackend(URL, *m_logger).release()),
m_catalogue(catalogue),
m_OStoreDB(*m_backend, *m_catalogue, *m_logger),
  m_agentReferencePtr(new objectstore::AgentReference("OStoreDBFactory", *m_logger))
{
  // We need to first clean up possible left overs in the pool
  auto l = m_backend->list();
  for (auto o=l.begin(); o!=l.end(); o++) {
    try {
      m_backend->remove(*o);
    } catch (std::exception &) {}
  }
  // We need to populate the root entry before using.
  objectstore::RootEntry re(*m_backend);
  re.initialize();
  re.insert();
  objectstore::ScopedExclusiveLock rel(re);
  re.fetch();
  objectstore::Agent agent(m_agentReferencePtr->getAgentAddress(), *m_backend);
  agent.initialize();
  objectstore::EntryLogSerDeser cl("user0", "systemhost", time(NULL));
  log::LogContext lc(*m_logger);
  re.addOrGetAgentRegisterPointerAndCommit(*m_agentReferencePtr, cl, lc);
  rel.release();
  agent.insertAndRegisterSelf(lc);
  rel.lock(re);
  re.fetch();
  re.addOrGetDriveRegisterPointerAndCommit(*m_agentReferencePtr, cl);
  re.addOrGetSchedulerGlobalLockAndCommit(*m_agentReferencePtr, cl);
  rel.release();
  m_OStoreDB.setAgentReference(m_agentReferencePtr.get());
}

}

/**
 * A concrete implementation of a scheduler database factory that creates mock
 * scheduler database objects.
 */
template <class BackendType>
class OStoreDBFactory: public SchedulerDatabaseFactory {
public:
  /**
   * Constructor
   */
  OStoreDBFactory(const std::string & URL = ""): m_URL(URL) {}

  /**
   * Destructor.
   */
  ~OStoreDBFactory() throw() {}

  /**
   * Returns a newly created scheduler database object.
   *
   * @return A newly created scheduler database object.
   */
  std::unique_ptr<SchedulerDatabase> create(std::unique_ptr<cta::catalogue::Catalogue>& catalogue) const {
    return std::unique_ptr<SchedulerDatabase>(new OStoreDBWrapper<BackendType>("UnitTest", catalogue, m_URL));
  }

  private:
    std::string m_URL;
}; // class OStoreDBFactory

} // namespace cta
