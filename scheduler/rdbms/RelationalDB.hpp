/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/TapeDrivesCatalogueState.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteriaAndFileId.hpp"
#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/dataStructures/CancelRetrieveRequest.hpp"
#include "common/dataStructures/DeadMountCandidates.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/dataStructures/LabelFormat.hpp"
#include "common/dataStructures/RepackInfo.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"
#include "common/dataStructures/RetrieveJob.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/Logger.hpp"
#include "common/process/threading/Mutex.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace cta {

namespace catalogue {
class Catalogue;
}

namespace schedulerdb {
class ArchiveMount;
class ArchiveRdbJob;
class RetrieveMount;
class RetrieveRdbJob;
class TapeMountDecisionInfo;
}  // namespace schedulerdb

class RelationalDB : public SchedulerDatabase {
public:
  RelationalDB(const std::string& ownerId,
               log::Logger& logger,
               catalogue::Catalogue& catalogue,
               const rdbms::Login& login,
               const uint64_t nbConns);

  ~RelationalDB() override;
  friend class cta::schedulerdb::ArchiveMount;
  friend class cta::schedulerdb::ArchiveRdbJob;
  friend class cta::schedulerdb::RetrieveMount;
  friend class cta::schedulerdb::RetrieveRdbJob;
  friend class cta::schedulerdb::TapeMountDecisionInfo;
  void waitSubthreadsComplete() override;

  /*============ Basic IO check: validate Postgres DB store access ===============*/
  void ping() override;

  /*
   * Insert jobs from ArchiveRequest object to the Scheduler DB backend
   *
   * @param instanceName            A string identifying the disk buffer instance name
   * @param request                 The ArchiveRequest object
   * @param criteria                ArchiveFileQueueCriteriaAndFileId object containing mount policy and archive file ID
   * @param logContext              The logging context
   *
   * @return The 'bogus' archive request ID string currently required
   *         for compatibility with the OStoreDB behaviour
   */
  std::string queueArchive(const std::string& instanceName,
                           const cta::common::dataStructures::ArchiveRequest& request,
                           const cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId& criteria,
                           log::LogContext& logContext) override;

  /*
   * Unless otherwise specified, all of the methods that follow are currently just throwing an exception
   * as they are not required for the basic PGSCHED DB Archival functionality
   */
  std::map<std::string, std::list<common::dataStructures::ArchiveJob>, std::less<>> getArchiveJobs() const override;

  std::list<cta::common::dataStructures::ArchiveJob> getArchiveJobs(const std::string& tapePoolName) const override;

  std::unique_ptr<IArchiveJobQueueItor>
  getArchiveJobQueueItor(const std::string& tapePoolName,
                         common::dataStructures::JobQueueType queueType) const override;

  /**
   * Get all archive queues with status:
   * AJS_ToReportToUserForSuccess or AJS_ToReportToUserForFailure
   *
   * @param filesRequested  number of rows to be reported from the scheduler DB
   * @param logContext      logging context
   *
   * @return                list of pointers to ArchiveJob objects to be reported
   */
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>
  getNextArchiveJobsToReportBatch(uint64_t filesRequested, log::LogContext& logContext) override;

  JobsFailedSummary getArchiveJobsFailedSummary(log::LogContext& logContext) override;

  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>
  getNextRetrieveJobsToTransferBatch(const std::string& vid, uint64_t filesRequested, log::LogContext& lc) override;

  void requeueRetrieveRequestJobs(std::list<cta::SchedulerDatabase::RetrieveJob*>& jobs,
                                  const std::string& toReportQueueAddress,
                                  log::LogContext& lc) override;

  std::string blockRetrieveQueueForCleanup(const std::string& vid) override;

  void unblockRetrieveQueueForCleanup(const std::string& vid) override;

  void setArchiveJobBatchReported(std::list<SchedulerDatabase::ArchiveJob*>& jobsBatch,
                                  log::TimingList& timingList,
                                  utils::Timer& t,
                                  log::LogContext& lc) override;

  std::list<SchedulerDatabase::RetrieveQueueCleanupInfo>
  getRetrieveQueuesCleanupInfo(log::LogContext& logContext) override;

  void setRetrieveQueueCleanupFlag(const std::string& vid, bool val, log::LogContext& logContext) override {
    throw cta::exception::Exception("Not supported for RelationalDB implementation.");
  };

  /**
   * Cleans up retrieve jobs for the specified Tape VID in the scheduler database.
   *
   * This method checks the `RETRIEVE_ERROR_REPORT_URL` column for each job in the
   * `RETRIEVE_PENDING_QUEUE` associated with the given VID.
   *
   * - If the `RETRIEVE_ERROR_REPORT_URL` is **not NULL and not empty**, the job is moved to the
   *   `RETRIEVE_ACTIVE_QUEUE` with its status updated to `RJS_ToReportToUserForFailure`,
   *   indicating that a failure report should be sent to the user.
   *
   * - If the `RETRIEVE_ERROR_REPORT_URL` is **NULL or empty**, the job is considered to not require
   *   reporting and is **deleted** from the `RETRIEVE_PENDING_QUEUE`.
   *
   * @param vid         The volume identifier for which pending retrieve jobs should be processed.
   * @param logContext  The logging context for recording operation details.
   */
  void cleanRetrieveQueueForVid(const std::string& vid, log::LogContext& logContext) override;

  std::list<RetrieveQueueStatistics>
  getRetrieveQueueStatistics(const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria,
                             const std::set<std::string>& vidsToConsider) override;

  SchedulerDatabase::RetrieveRequestInfo
  queueRetrieve(cta::common::dataStructures::RetrieveRequest& rqst,
                const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria,
                const std::optional<std::string> diskSystemName,
                log::LogContext& logContext) override;

  void clearStatisticsCache(const std::string& vid) override;

  void setStatisticsCacheConfig(const StatisticsCacheConfig& conf) override;

  void cancelRetrieve(const std::string& instanceName,
                      const cta::common::dataStructures::CancelRetrieveRequest& rqst,
                      log::LogContext& lc) override;

  std::map<std::string, std::list<RetrieveRequestDump>> getRetrieveRequests() const override;

  std::list<RetrieveRequestDump> getRetrieveRequestsByVid(const std::string& vid) const override;

  std::list<RetrieveRequestDump> getRetrieveRequestsByRequester(const std::string& vid) const override;

  void deleteRetrieveRequest(const common::dataStructures::SecurityIdentity& requester,
                             const std::string& remoteFile) override;

  void cancelArchive(const common::dataStructures::DeleteArchiveRequest& request, log::LogContext& lc) override;

  void deleteFailed(const std::string& objectId, log::LogContext& lc) override;

  std::map<std::string, std::list<common::dataStructures::RetrieveJob>, std::less<>> getRetrieveJobs() const override;

  std::list<cta::common::dataStructures::RetrieveJob> getRetrieveJobs(const std::string& vid) const override;

  std::unique_ptr<IRetrieveJobQueueItor>
  getRetrieveJobQueueItor(const std::string& vid, common::dataStructures::JobQueueType queueType) const override;

  std::string queueRepack(const SchedulerDatabase::QueueRepackRequest& repackRequest,
                          log::LogContext& logContext) override;

  bool repackExists() override;
  std::list<common::dataStructures::RepackInfo> getRepackInfo() override;
  common::dataStructures::RepackInfo getRepackInfo(const std::string& vid) override;

  void cancelRepack(const std::string& vid, log::LogContext& lc) override;

  std::unique_ptr<RepackRequestStatistics> getRepackStatistics() override;

  std::unique_ptr<RepackRequestStatistics> getRepackStatisticsNoLock() override;

  std::unique_ptr<SchedulerDatabase::RepackRequest> getNextRepackJobToExpand() override;

  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>
  getNextRetrieveJobsToReportBatch(uint64_t filesRequested, log::LogContext& logContext) override;

  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>
  getNextRetrieveJobsFailedBatch(uint64_t filesRequested, log::LogContext& logContext) override;

  std::unique_ptr<SchedulerDatabase::RepackReportBatch> getNextRepackReportBatch(log::LogContext& lc) override;

  std::unique_ptr<SchedulerDatabase::RepackReportBatch>
  getNextSuccessfulRetrieveRepackReportBatch(log::LogContext& lc) override;

  std::unique_ptr<SchedulerDatabase::RepackReportBatch>
  getNextSuccessfulArchiveRepackReportBatch(log::LogContext& lc) override;

  std::unique_ptr<SchedulerDatabase::RepackReportBatch>
  getNextFailedRetrieveRepackReportBatch(log::LogContext& lc) override;

  std::unique_ptr<SchedulerDatabase::RepackReportBatch>
  getNextFailedArchiveRepackReportBatch(log::LogContext& lc) override;

  std::list<std::unique_ptr<SchedulerDatabase::RepackReportBatch>> getRepackReportBatches(log::LogContext& lc) override;

  void setRetrieveJobBatchReportedToUser(std::list<SchedulerDatabase::RetrieveJob*>& jobsBatch,
                                         log::TimingList& timingList,
                                         utils::Timer& t,
                                         log::LogContext& lc) override;

  JobsFailedSummary getRetrieveJobsFailedSummary(log::LogContext& logContext) override;

  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> getMountInfo(log::LogContext& logContext) override;

  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> getMountInfo(log::LogContext& logContext,
                                                                         uint64_t timeout_us) override;

  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo>
  getMountInfo(std::string_view logicalLibraryName, log::LogContext& logContext, uint64_t timeout_us) override;
  std::optional<common::dataStructures::VirtualOrganization> getDefaultRepackVo();

  void trimEmptyQueues(log::LogContext& lc) override;
  bool trimEmptyToReportQueue(const std::string& queueName, log::LogContext& lc) override;

  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> getMountInfoNoLock(PurposeGetMountInfo purpose,
                                                                               log::LogContext& logContext) override;

  /**
   * Provides access to a connection from the connection pool
   */
  cta::rdbms::Conn getConn();

  /*
   * for retrieve queue sleep mechanism (filter out disk systems which do not have space)
   * we put in place DiskSleepEntry
   */
  struct DiskSleepEntry {
    uint64_t sleepTime;
    uint64_t timestamp;

    DiskSleepEntry() : sleepTime(0), timestamp(0) {}

    DiskSleepEntry(uint64_t st, uint64_t ts) : sleepTime(st), timestamp(ts) {}
  };

  cta::threading::Mutex m_diskSystemSleepMutex;
  cta::threading::Mutex m_inactiveMountRoutineMutex;
  /*
   * Get list of diskSystemNames for which the system should
   * not be picking up jobs for retrieve
   * due to insufficient disk space for a specified sleep time interval
   *
   * @return list of diskSystemName strings
   */
  std::unordered_map<std::string, RelationalDB::DiskSleepEntry>
  getActiveSleepDiskSystemNamesToFilter(log::LogContext& logContext);
  uint64_t insertOrUpdateDiskSleepEntry(schedulerdb::Transaction& txn,
                                        const std::string& diskSystemName,
                                        const DiskSleepEntry& entry);

  uint64_t removeDiskSystemSleepEntries(schedulerdb::Transaction& txn,
                                        const std::vector<std::string>& expiredDiskSystemNames);

  std::unordered_map<std::string, DiskSleepEntry> getDiskSystemSleepStatus(rdbms::Conn& conn);

  // MountQueueCleanup routine methods

  /*
   * Get list of distinct Mount IDs which were inactive
   * since a long time, i.e. they had no jobs fetched since mount_gc_delay seconds ago
   * from the Scheduler DB
   *
   * @param mount_gc_delay  Looking at activity older than the number of seconds in this parameter
   * @param ls                     Log context
   * @return DeadMountCandidateIDs object containing vectors of Mount IDs sorted by queue type
   */
  cta::common::dataStructures::DeadMountCandidateIDs getDeadMountCandidates(uint64_t mount_gc_delay,
                                                                            log::LogContext& lc);
  std::string getQueueTypePrefix(bool isArchive, bool isRepack);
  void handleInactiveMountPendingQueues(const std::vector<uint64_t>& deadMountIds,
                                        size_t batchSize,
                                        bool isArchive,
                                        bool isRepack,
                                        log::LogContext& lc);
  void handleInactiveMountActiveQueues(const std::vector<uint64_t>& deadMountIds,
                                       size_t batchSize,
                                       bool isArchive,
                                       bool isRepack,
                                       log::LogContext& lc);
  void deleteOldFailedQueues(uint64_t deletionAge, uint64_t batchSize, log::LogContext& lc);
  void cleanOldMountHeartbeats(uint64_t deletionAge, uint64_t batchSize, log::LogContext& lc);

private:
  /*
   * Get all the tape pools and tapes with queues (potential mounts)
   *
   * @param tmdi     The TapeMountDecisionInfo object which will be
   *                 populated with the potential mounts
   * @param purpose  The purpose of the mount (not in use - we might want to remove it)
   * @param lc       The logging context
   */
  void fetchMountInfo(SchedulerDatabase::TapeMountDecisionInfo& tmdi,
                      [[maybe_unused]] SchedulerDatabase::PurposeGetMountInfo purpose,
                      log::LogContext& lc);
  bool deleteDiskFiles(std::unordered_set<std::string>& jobSrcUrls, log::LogContext& lc);
  std::list<common::dataStructures::RepackInfo> fetchRepackInfo(const std::string& vid);
  std::string m_ownerId;
  rdbms::ConnPool m_connPool;
  catalogue::Catalogue& m_catalogue;
  log::Logger& m_logger;
  std::unique_ptr<TapeDrivesCatalogueState> m_tapeDrivesState;

  const size_t c_repackArchiveReportBatchSize = 10000;
  const size_t c_repackRetrieveReportBatchSize = 10000;

  void populateRepackRequestsStatistics(SchedulerDatabase::RepackRequestStatistics& stats);

  /**
  * Candidate for redesign/removal once we start improving Scheduler algorithm
  * A class holding a lock on the pending repack request queue. This is the first
  * container we will have to lock if we decide to pop a/some request(s)
  */
  class RepackRequestPromotionStatistics : public SchedulerDatabase::RepackRequestStatistics {
    friend class RelationalDB;

  public:
    PromotionToToExpandResult promotePendingRequestsForExpansion(size_t requestCount, log::LogContext& lc) override;
    explicit RepackRequestPromotionStatistics(RelationalDB& parent);

  private:
    RelationalDB& m_parentdb;
  };

  /**
  * Candidate for redesign/removal once we start improving Scheduler algorithm
  */
  class RepackRequestPromotionStatisticsNoLock : public SchedulerDatabase::RepackRequestStatistics {
    friend class RelationalDB;

  public:
    PromotionToToExpandResult promotePendingRequestsForExpansion(size_t requestCount, log::LogContext& lc) override {
      throw SchedulingLockNotHeld("In RepackRequestPromotionStatisticsNoLock::promotePendingRequestsForExpansion");
    }

    ~RepackRequestPromotionStatisticsNoLock() override = default;
  };
};

}  // namespace cta
