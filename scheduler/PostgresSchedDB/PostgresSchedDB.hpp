/*
 * @project	 The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license	 This program is free software, distributed under the terms of the GNU General Public
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

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include "common/dataStructures/ArchiveFileQueueCriteriaAndFileId.hpp"
#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/dataStructures/CancelRetrieveRequest.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/dataStructures/LabelFormat.hpp"
#include "common/dataStructures/RepackInfo.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"
#include "common/dataStructures/RetrieveJob.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/Logger.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "scheduler/SchedulerDatabase.hpp"

namespace cta {

namespace catalogue {
class Catalogue;
}

class PostgresSchedDB: public SchedulerDatabase {
 public:
  PostgresSchedDB(void *pgstuff, catalogue::Catalogue & catalogue, log::Logger &logger);
  virtual ~PostgresSchedDB() throw();

  class ArchiveMount;
  class ArchiveJob;
  class ArchiveJobQueueItor;
  class RetrieveMount;
  class RetrieveJob;
  class RetrieveJobQueueItor;
  class RepackRequestPromotionStatistics;
  class RepackRequest;
  class RepackReportBatch;
  class TapeMountDecisionInfo;

  void waitSubthreadsComplete() override;

  void ping() override;

  std::string queueArchive(const std::string &instanceName, const cta::common::dataStructures::ArchiveRequest &request,
    const cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId &criteria, log::LogContext &logContext) override;

  std::map<std::string, std::list<common::dataStructures::ArchiveJob>> getArchiveJobs() const override;

  std::list<cta::common::dataStructures::ArchiveJob> getArchiveJobs(const std::string& tapePoolName) const override;

  std::unique_ptr<IArchiveJobQueueItor> getArchiveJobQueueItor(const std::string &tapePoolName,
    common::dataStructures::JobQueueType queueType) const override;

  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > getNextArchiveJobsToReportBatch(uint64_t filesRequested,
    log::LogContext & logContext) override;

  JobsFailedSummary getArchiveJobsFailedSummary(log::LogContext &logContext) override;

  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> getNextRetrieveJobsToTransferBatch(std::string & vid, uint64_t filesRequested, log::LogContext &lc) override;

  void requeueRetrieveRequestJobs(std::list<cta::SchedulerDatabase::RetrieveJob *> &jobs, log::LogContext &lc) override;

  void reserveRetrieveQueueForCleanup(std::string & vid, std::optional<uint64_t> cleanupHeartBeatValue) override;

  void tickRetrieveQueueCleanupHeartbeat(std::string & vid) override;

  void setArchiveJobBatchReported(std::list<SchedulerDatabase::ArchiveJob*> & jobsBatch,
    log::TimingList & timingList, utils::Timer & t, log::LogContext & lc) override;

  void setArchiveJobBatchReported(std::list<SchedulerDatabase::ArchiveJob*> & jobsBatch,
    log::TimingList & timingList, utils::Timer & t, log::LogContext & lc) override;

  std::list<RetrieveQueueStatistics> getRetrieveQueueStatistics(
    const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria, const std::set<std::string>& vidsToConsider) override;

  SchedulerDatabase::RetrieveRequestInfo queueRetrieve(cta::common::dataStructures::RetrieveRequest& rqst,
    const cta::common::dataStructures::RetrieveFileQueueCriteria &criteria, const std::optional<std::string> diskSystemName,
    log::LogContext &logContext) override;

  void clearRetrieveQueueStatisticsCache(const std::string & vid) override;

  void cancelRetrieve(const std::string& instanceName, const cta::common::dataStructures::CancelRetrieveRequest& rqst,
    log::LogContext& lc) override;

  std::map<std::string, std::list<RetrieveRequestDump> > getRetrieveRequests() const override;

  std::list<RetrieveRequestDump> getRetrieveRequestsByVid(const std::string& vid) const override;

  std::list<RetrieveRequestDump> getRetrieveRequestsByRequester(const std::string& vid) const override;

  void deleteRetrieveRequest(const common::dataStructures::SecurityIdentity& requester,
    const std::string& remoteFile) override;

  void cancelArchive(const common::dataStructures::DeleteArchiveRequest& request, log::LogContext & lc) override;

  void deleteFailed(const std::string &objectId, log::LogContext &lc) override;

  std::map<std::string, std::list<common::dataStructures::RetrieveJob>> getRetrieveJobs() const override;

  std::list<cta::common::dataStructures::RetrieveJob> getRetrieveJobs(const std::string &vid) const override;

  std::unique_ptr<IRetrieveJobQueueItor> getRetrieveJobQueueItor(const std::string &vid,
  common::dataStructures::JobQueueType queueType) const override;

  std::string queueRepack(const SchedulerDatabase::QueueRepackRequest & repackRequest, log::LogContext &logContext) override;

  std::list<common::dataStructures::RepackInfo> getRepackInfo() override;

  common::dataStructures::RepackInfo getRepackInfo(const std::string& vid) override;

  void cancelRepack(const std::string& vid, log::LogContext & lc) override;

  std::unique_ptr<RepackRequestStatistics> getRepackStatistics() override;

  std::unique_ptr<RepackRequestStatistics> getRepackStatisticsNoLock() override;

  std::unique_ptr<SchedulerDatabase::RepackRequest> getNextRepackJobToExpand() override;

  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> getNextRetrieveJobsToReportBatch(
    uint64_t filesRequested, log::LogContext &logContext) override;

  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> getNextRetrieveJobsFailedBatch(
    uint64_t filesRequested, log::LogContext &logContext) override;

  std::unique_ptr<SchedulerDatabase::RepackReportBatch> getNextRepackReportBatch(log::LogContext& lc) override;

  std::unique_ptr<SchedulerDatabase::RepackReportBatch> getNextSuccessfulRetrieveRepackReportBatch(log::LogContext& lc) override;

  std::unique_ptr<SchedulerDatabase::RepackReportBatch> getNextSuccessfulArchiveRepackReportBatch(log::LogContext& lc) override;

  std::unique_ptr<SchedulerDatabase::RepackReportBatch> getNextFailedRetrieveRepackReportBatch(log::LogContext& lc) override;

  std::unique_ptr<SchedulerDatabase::RepackReportBatch> getNextFailedArchiveRepackReportBatch(log::LogContext &lc) override;

  std::list<std::unique_ptr<SchedulerDatabase::RepackReportBatch>> getRepackReportBatches(log::LogContext &lc) override;

  void setRetrieveJobBatchReportedToUser(std::list<SchedulerDatabase::RetrieveJob*> & jobsBatch,
    log::TimingList & timingList, utils::Timer & t, log::LogContext & lc) override;

  JobsFailedSummary getRetrieveJobsFailedSummary(log::LogContext &logContext) override;

  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> getMountInfo(log::LogContext& logContext) override;
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> getMountInfo(log::LogContext& logContext, uint64_t globalLockTimeout_us) override;

  void trimEmptyQueues(log::LogContext& lc) override;

  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> getMountInfoNoLock(PurposeGetMountInfo purpose,
    log::LogContext& logContext) override;

  void requeueRetrieveJobs(std::list<SchedulerDatabase::RetrieveJob *> &jobs, log::LogContext& logContext) override;

  // these are not in the baseclass but are beeded by XrdSsiCtaServiceProvider
  void setThreadNumber(uint64_t threadNumber, const std::optional<size_t> &stackSize = std::nullopt);
  void setBottomHalfQueueSize(uint64_t tasksNumber);

};

}  // namespace cta
