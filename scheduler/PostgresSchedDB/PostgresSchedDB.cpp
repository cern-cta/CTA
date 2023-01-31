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

#include "PostgresSchedDB.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "common/exception/Exception.hpp"

namespace cta {

PostgresSchedDB::PostgresSchedDB(void *pgstuff, catalogue::Catalogue & catalogue, log::Logger &logger)
{
   throw cta::exception::Exception("Not implemented");
}

PostgresSchedDB::~PostgresSchedDB() throw()
{
}

void PostgresSchedDB::waitSubthreadsComplete()
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::ping()
{
   throw cta::exception::Exception("Not implemented");
}

std::string PostgresSchedDB::queueArchive(const std::string &instanceName, const cta::common::dataStructures::ArchiveRequest &request,
    const cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId &criteria, log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

std::map<std::string, std::list<common::dataStructures::ArchiveJob>> PostgresSchedDB::getArchiveJobs() const
{
   throw cta::exception::Exception("Not implemented");
}

std::list<cta::common::dataStructures::ArchiveJob> PostgresSchedDB::getArchiveJobs(const std::string& tapePoolName) const
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::IArchiveJobQueueItor> PostgresSchedDB::getArchiveJobQueueItor(const std::string &tapePoolName,
    common::dataStructures::JobQueueType queueType) const
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > PostgresSchedDB::getNextArchiveJobsToReportBatch(uint64_t filesRequested,
     log::LogContext & logContext)
{
   throw cta::exception::Exception("Not implemented");
}

SchedulerDatabase::JobsFailedSummary PostgresSchedDB::getArchiveJobsFailedSummary(log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<RetrieveJob>> PostgresSchedDB::getNextRetrieveJobsToTransferBatch(const std::string & vid, uint64_t filesRequested, log::LogContext &lc)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::requeueRetrieveRequestJobs(std::list<cta::SchedulerDatabase::RetrieveJob *> &jobs, log::LogContext &lc)
{
throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::reserveRetrieveQueueForCleanup(const std::string & vid, std::optional<uint64_t> cleanupHeartBeatValue)
{
  throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::tickRetrieveQueueCleanupHeartbeat(const std::string & vid)
{
  throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::setArchiveJobBatchReported(std::list<SchedulerDatabase::ArchiveJob*> & jobsBatch,
     log::TimingList & timingList, utils::Timer & t, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::list<SchedulerDatabase::RetrieveQueueStatistics> PostgresSchedDB::getRetrieveQueueStatistics(
    const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria, const std::set<std::string>& vidsToConsider)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::clearRetrieveQueueStatisticsCache(const std::string & vid)
{
  throw cta::exception::Exception("Not implemented");
}

SchedulerDatabase::RetrieveRequestInfo PostgresSchedDB::queueRetrieve(cta::common::dataStructures::RetrieveRequest& rqst,
    const cta::common::dataStructures::RetrieveFileQueueCriteria &criteria, const std::optional<std::string> diskSystemName,
    log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::cancelRetrieve(const std::string& instanceName, const cta::common::dataStructures::CancelRetrieveRequest& rqst,
    log::LogContext& lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::map<std::string, std::list<RetrieveRequestDump> > PostgresSchedDB::getRetrieveRequests() const
{
   throw cta::exception::Exception("Not implemented");
}

std::list<RetrieveRequestDump> PostgresSchedDB::getRetrieveRequestsByVid(const std::string& vid) const
{
   throw cta::exception::Exception("Not implemented");
}

std::list<RetrieveRequestDump> PostgresSchedDB::getRetrieveRequestsByRequester(const std::string& vid) const
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::deleteRetrieveRequest(const common::dataStructures::SecurityIdentity& requester,
    const std::string& remoteFile)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::cancelArchive(const common::dataStructures::DeleteArchiveRequest& request, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::deleteFailed(const std::string &objectId, log::LogContext &lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::map<std::string, std::list<common::dataStructures::RetrieveJob>> PostgresSchedDB::getRetrieveJobs() const
{
   throw cta::exception::Exception("Not implemented");
}

std::list<cta::common::dataStructures::RetrieveJob> PostgresSchedDB::getRetrieveJobs(const std::string &vid) const
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::IRetrieveJobQueueItor> PostgresSchedDB::getRetrieveJobQueueItor(const std::string &vid,
    common::dataStructures::JobQueueType queueType) const
{
   throw cta::exception::Exception("Not implemented");
}

std::string PostgresSchedDB::queueRepack(const SchedulerDatabase::QueueRepackRequest & repackRequest, log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

std::list<common::dataStructures::RepackInfo> PostgresSchedDB::getRepackInfo()
{
   throw cta::exception::Exception("Not implemented");
}

common::dataStructures::RepackInfo PostgresSchedDB::getRepackInfo(const std::string& vid)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::cancelRepack(const std::string& vid, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> PostgresSchedDB::getRepackStatistics()
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> PostgresSchedDB::getRepackStatisticsNoLock()
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackRequest> PostgresSchedDB::getNextRepackJobToExpand()
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> PostgresSchedDB::getNextRetrieveJobsToReportBatch(
    uint64_t filesRequested, log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> PostgresSchedDB::getNextRetrieveJobsFailedBatch(
    uint64_t filesRequested, log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> PostgresSchedDB::getNextRepackReportBatch(log::LogContext& lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> PostgresSchedDB::getNextSuccessfulRetrieveRepackReportBatch(log::LogContext& lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> PostgresSchedDB::getNextSuccessfulArchiveRepackReportBatch(log::LogContext& lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> PostgresSchedDB::getNextFailedRetrieveRepackReportBatch(log::LogContext& lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> PostgresSchedDB::getNextFailedArchiveRepackReportBatch(log::LogContext &lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::list<std::unique_ptr<SchedulerDatabase::RepackReportBatch>> PostgresSchedDB::getRepackReportBatches(log::LogContext &lc)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::setRetrieveJobBatchReportedToUser(std::list<SchedulerDatabase::RetrieveJob*> & jobsBatch,
     log::TimingList & timingList, utils::Timer & t, log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

SchedulerDatabase::JobsFailedSummary PostgresSchedDB::getRetrieveJobsFailedSummary(log::LogContext &logContext)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> PostgresSchedDB::getMountInfo(log::LogContext& logContext)
{
  throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> PostgresSchedDB::getMountInfo(log::LogContext& logContext, uint64_t globalLockTimeout_us)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::trimEmptyQueues(log::LogContext& lc)
{
   throw cta::exception::Exception("Not implemented");
}

std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> PostgresSchedDB::getMountInfoNoLock(PurposeGetMountInfo purpose,
    log::LogContext& logContext)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::requeueRetrieveJobs(std::list<SchedulerDatabase::RetrieveJob *> &jobs, log::LogContext& logContext)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::setThreadNumber(uint64_t threadNumber, const std::optional<size_t> &stackSize) {
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::setBottomHalfQueueSize(uint64_t tasksNumber) {
   throw cta::exception::Exception("Not implemented");
}


} //namespace cta
