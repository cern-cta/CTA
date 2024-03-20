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

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>

#include "scheduler/SchedulerDatabase.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "scheduler/RetrieveRequestDump.hpp"

namespace cta {

namespace catalogue {
class Catalogue;
}

/**
 * Asbtract class specifying the interface to a factory of scheduler database
 * objects.
 */
class SchedulerDatabaseFactory {
public:
  /**
   * Destructor.
   */
  virtual ~SchedulerDatabaseFactory() noexcept = 0;

  /**
   * Returns a newly created scheduler database object.
   *
   * @return A newly created scheduler database object.
   */
  virtual std::unique_ptr<SchedulerDatabase> create(std::unique_ptr<cta::catalogue::Catalogue>& catalogue) const = 0;
};  // class SchedulerDatabaseFactory

/**
 * Base of a wrapper class. This follows the decorator structural pattern. Wrappers are used by
 * SchedulerDatabase specialisations, in conjunction with the factory calss above, to provide
 * test classes for unit tests.
 */
class SchedulerDatabaseDecorator : public SchedulerDatabase {
public:
  explicit SchedulerDatabaseDecorator(SchedulerDatabase &db) : m_SchedDB(&db) { }

  void waitSubthreadsComplete() override {
    m_SchedDB->waitSubthreadsComplete();
  }

  void ping() override {
    m_SchedDB->ping();
  }

  std::string queueArchive(const std::string &instanceName, const cta::common::dataStructures::ArchiveRequest& request, const cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId& criteria, log::LogContext &logContext) override {
    return m_SchedDB->queueArchive(instanceName, request, criteria, logContext);
  }

  void deleteRetrieveRequest(const common::dataStructures::SecurityIdentity& cliIdentity, const std::string& remoteFile) override {
    m_SchedDB->deleteRetrieveRequest(cliIdentity, remoteFile);
  }

  std::list<cta::common::dataStructures::RetrieveJob> getRetrieveJobs(const std::string& tapePoolName) const override {
    return m_SchedDB->getRetrieveJobs(tapePoolName);
  }

  std::map<std::string, std::list<common::dataStructures::RetrieveJob>, std::less<> > getRetrieveJobs() const override {
    return m_SchedDB->getRetrieveJobs();
  }

  std::map<std::string, std::list<common::dataStructures::ArchiveJob>, std::less<> > getArchiveJobs() const override {
    return m_SchedDB->getArchiveJobs();
  }

  std::list<cta::common::dataStructures::ArchiveJob> getArchiveJobs(const std::string& tapePoolName) const override {
    return m_SchedDB->getArchiveJobs(tapePoolName);
  }

  std::unique_ptr<IArchiveJobQueueItor> getArchiveJobQueueItor(const std::string &tapePoolName,
    common::dataStructures::JobQueueType queueType) const override {
    return m_SchedDB->getArchiveJobQueueItor(tapePoolName, queueType);
  }

  std::unique_ptr<IRetrieveJobQueueItor> getRetrieveJobQueueItor(const std::string &vid,
    common::dataStructures::JobQueueType queueType) const override {
    return m_SchedDB->getRetrieveJobQueueItor(vid, queueType);
  }

  std::map<std::string, std::list<RetrieveRequestDump> > getRetrieveRequests() const override {
    return m_SchedDB->getRetrieveRequests();
  }

  std::list<std::unique_ptr<ArchiveJob>> getNextArchiveJobsToReportBatch(uint64_t filesRequested, log::LogContext &lc) override {
    return m_SchedDB->getNextArchiveJobsToReportBatch(filesRequested, lc);
  }

  JobsFailedSummary getArchiveJobsFailedSummary(log::LogContext &lc) override {
    return m_SchedDB->getArchiveJobsFailedSummary(lc);
  }

  std::list<std::unique_ptr<RetrieveJob>> getNextRetrieveJobsToTransferBatch(const std::string & vid, uint64_t filesRequested, log::LogContext &lc) override {
    return m_SchedDB->getNextRetrieveJobsToTransferBatch(vid, filesRequested, lc);
  }

  void requeueRetrieveRequestJobs(std::list<cta::SchedulerDatabase::RetrieveJob *> &jobs, log::LogContext &lc) override {
    m_SchedDB->requeueRetrieveRequestJobs(jobs, lc);
  }

  void reserveRetrieveQueueForCleanup(const std::string & vid, std::optional<uint64_t> cleanupHeartBeatValue) override {
    m_SchedDB->reserveRetrieveQueueForCleanup(vid, cleanupHeartBeatValue);
  }

  void tickRetrieveQueueCleanupHeartbeat(const std::string & vid) override {
    m_SchedDB->tickRetrieveQueueCleanupHeartbeat(vid);
  }

  std::list<RetrieveQueueCleanupInfo> getRetrieveQueuesCleanupInfo(log::LogContext& logContext) override {
    return m_SchedDB->getRetrieveQueuesCleanupInfo(logContext);
  }

  void setRetrieveQueueCleanupFlag(const std::string& vid, bool val, log::LogContext& lc) override {
    m_SchedDB->setRetrieveQueueCleanupFlag(vid, val, lc);
  }

  std::list<std::unique_ptr<RetrieveJob>> getNextRetrieveJobsToReportBatch(uint64_t filesRequested, log::LogContext &lc) override {
    return m_SchedDB->getNextRetrieveJobsToReportBatch(filesRequested, lc);
  }

  std::list<std::unique_ptr<RetrieveJob>> getNextRetrieveJobsFailedBatch(uint64_t filesRequested, log::LogContext &lc) override {
    return m_SchedDB->getNextRetrieveJobsFailedBatch(filesRequested, lc);
  }

  std::unique_ptr<RepackReportBatch> getNextRepackReportBatch(log::LogContext& lc) override {
    return m_SchedDB->getNextRepackReportBatch(lc);
  }

  std::unique_ptr<RepackReportBatch> getNextSuccessfulRetrieveRepackReportBatch(log::LogContext& lc) override {
    return m_SchedDB->getNextSuccessfulRetrieveRepackReportBatch(lc);
  }

  std::unique_ptr<RepackReportBatch> getNextSuccessfulArchiveRepackReportBatch(log::LogContext& lc) override {
    return m_SchedDB->getNextSuccessfulArchiveRepackReportBatch(lc);
  }

  std::unique_ptr<RepackReportBatch> getNextFailedRetrieveRepackReportBatch(log::LogContext& lc) override {
    return m_SchedDB->getNextFailedRetrieveRepackReportBatch(lc);
  }

  std::unique_ptr<RepackReportBatch> getNextFailedArchiveRepackReportBatch(log::LogContext& lc) override {
    return m_SchedDB->getNextFailedArchiveRepackReportBatch(lc);
  }

  std::list<std::unique_ptr<SchedulerDatabase::RepackReportBatch>> getRepackReportBatches(log::LogContext &lc) override {
    return m_SchedDB->getRepackReportBatches(lc);
  }

  JobsFailedSummary getRetrieveJobsFailedSummary(log::LogContext &lc) override {
    return m_SchedDB->getRetrieveJobsFailedSummary(lc);
  }

  void setArchiveJobBatchReported(std::list<cta::SchedulerDatabase::ArchiveJob*>& jobsBatch, log::TimingList & timingList,
      utils::Timer & t, log::LogContext& lc) override {
    m_SchedDB->setArchiveJobBatchReported(jobsBatch, timingList, t, lc);
  }

  void setRetrieveJobBatchReportedToUser(std::list<cta::SchedulerDatabase::RetrieveJob*>& jobsBatch, log::TimingList & timingList,
      utils::Timer & t, log::LogContext& lc) override {
    m_SchedDB->setRetrieveJobBatchReportedToUser(jobsBatch, timingList, t, lc);
  }

  std::list<RetrieveRequestDump> getRetrieveRequestsByVid(const std::string& vid) const override {
    return m_SchedDB->getRetrieveRequestsByVid(vid);
  }

  std::list<RetrieveRequestDump> getRetrieveRequestsByRequester(const std::string& requester) const override {
    return m_SchedDB->getRetrieveRequestsByRequester(requester);
  }

  std::unique_ptr<TapeMountDecisionInfo> getMountInfo(log::LogContext& logContext) override {
    return m_SchedDB->getMountInfo(logContext);
  }

  std::unique_ptr<TapeMountDecisionInfo> getMountInfo(log::LogContext& logContext, uint64_t timeout_us) override {
    return m_SchedDB->getMountInfo(logContext, timeout_us);
  }

  void trimEmptyQueues(log::LogContext& lc) override {
    m_SchedDB->trimEmptyQueues(lc);
  }

  std::unique_ptr<TapeMountDecisionInfo> getMountInfoNoLock(PurposeGetMountInfo purpose, log::LogContext& logContext) override {
    return m_SchedDB->getMountInfoNoLock(purpose, logContext);
  }

  std::list<RetrieveQueueStatistics> getRetrieveQueueStatistics(const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria,
          const std::set<std::string> & vidsToConsider) override {
    return m_SchedDB->getRetrieveQueueStatistics(criteria, vidsToConsider);
  }

  void clearStatisticsCache(const std::string & vid) override {
    return m_SchedDB->clearStatisticsCache(vid);
  }

  void setStatisticsCacheConfig(const StatisticsCacheConfig & conf) override {
    return m_SchedDB->setStatisticsCacheConfig(conf);
  }

  SchedulerDatabase::RetrieveRequestInfo queueRetrieve(common::dataStructures::RetrieveRequest& rqst,
    const common::dataStructures::RetrieveFileQueueCriteria &criteria, const std::optional<std::string> diskSystemName,
    log::LogContext &logContext) override {
    return m_SchedDB->queueRetrieve(rqst, criteria, diskSystemName, logContext);
  }

  void cancelArchive(const common::dataStructures::DeleteArchiveRequest& request, log::LogContext & lc) override {
    m_SchedDB->cancelArchive(request, lc);
  }

  void cancelRetrieve(const std::string& instanceName, const cta::common::dataStructures::CancelRetrieveRequest& rqst,
    log::LogContext& lc) override {
    m_SchedDB->cancelRetrieve(instanceName, rqst, lc);
  }

  void deleteFailed(const std::string &objectId, log::LogContext & lc) override {
    m_SchedDB->deleteFailed(objectId, lc);
  }

  std::string queueRepack(const SchedulerDatabase::QueueRepackRequest & repackRequest, log::LogContext& lc) override {
    return m_SchedDB->queueRepack(repackRequest, lc);
  }

  bool repackExists() override {
    return m_SchedDB->repackExists();
  }

  std::list<common::dataStructures::RepackInfo> getRepackInfo() override {
    return m_SchedDB->getRepackInfo();
  }

  common::dataStructures::RepackInfo getRepackInfo(const std::string& vid) override {
    return m_SchedDB->getRepackInfo(vid);
  }

  void cancelRepack(const std::string& vid, log::LogContext & lc) override {
    m_SchedDB->cancelRepack(vid, lc);
  }

  std::unique_ptr<RepackRequestStatistics> getRepackStatistics() override {
    return m_SchedDB->getRepackStatistics();
  }

  std::unique_ptr<RepackRequestStatistics> getRepackStatisticsNoLock() override {
    return m_SchedDB->getRepackStatisticsNoLock();
  }

  std::unique_ptr<RepackRequest> getNextRepackJobToExpand() override {
    return m_SchedDB->getNextRepackJobToExpand();
  }

protected:
  cta::SchedulerDatabase *m_SchedDB;
};  // class SchedulerDatabaseDecorator

} // namespace cta
