/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "QueueItor.hpp"
#include "catalogue/TapeDrivesCatalogueState.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/dataStructures/LabelFormat.hpp"
#include "common/exception/NotImplementedException.hpp"
#include "common/log/Logger.hpp"
#include "common/process/threading/BlockingQueue.hpp"
#include "common/process/threading/Thread.hpp"
#include "objectstore/Agent.hpp"
#include "objectstore/AgentReference.hpp"
#include "objectstore/ArchiveQueue.hpp"
#include "objectstore/ArchiveRequest.hpp"
#include "objectstore/DriveRegister.hpp"
#include "objectstore/RepackQueue.hpp"
#include "objectstore/RepackRequest.hpp"
#include "objectstore/RetrieveActivityCountMap.hpp"
#include "objectstore/RetrieveQueue.hpp"
#include "objectstore/RetrieveRequest.hpp"
#include "objectstore/RootEntry.hpp"
#include "objectstore/SchedulerGlobalLock.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "scheduler/SchedulerDatabase.hpp"

#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <variant>
#include <vector>

namespace cta {

namespace catalogue {
class Catalogue;
}

namespace ostoredb {
template<class, class>
class MemQueue;
}

class OStoreDB : public SchedulerDatabase {
  template<class, class>
  friend class cta::ostoredb::MemQueue;

public:
  OStoreDB(objectstore::Backend& be, catalogue::Catalogue& catalogue, log::Logger& logger);
  virtual ~OStoreDB() noexcept override;

  /* === Object store and agent handling ==================================== */
  void setAgentReference(objectstore::AgentReference* agentReference);
  CTA_GENERATE_EXCEPTION_CLASS(AgentNotSet);

private:
  void assertAgentAddressSet();

public:
  CTA_GENERATE_EXCEPTION_CLASS(NotImplemented);
  /*============ Thread pool for queueing bottom halfs ======================*/
private:
  using EnqueueingTask = std::function<void()>;
  cta::threading::BlockingQueue<EnqueueingTask*> m_enqueueingTasksQueue;

  class EnqueueingWorkerThread : private cta::threading::Thread {
  public:
    explicit EnqueueingWorkerThread(cta::threading::BlockingQueue<EnqueueingTask*>& etq)
        : m_enqueueingTasksQueue(etq) {}

    EnqueueingWorkerThread(cta::threading::BlockingQueue<EnqueueingTask*>& etq, std::optional<size_t> stackSize)
        : cta::threading::Thread(stackSize),
          m_enqueueingTasksQueue(etq) {}

    void start() { cta::threading::Thread::start(); }

    void wait() { cta::threading::Thread::wait(); }

  private:
    void run() override;
    cta::threading::BlockingQueue<EnqueueingTask*>& m_enqueueingTasksQueue;
  };

  std::vector<EnqueueingWorkerThread*> m_enqueueingWorkerThreads;
  std::atomic<uint64_t> m_taskQueueSize =
    0;  // < This counter ensures destruction happens after the last thread completed.
  /// Delay introduced before posting to the task queue when it becomes too long.
  void delayIfNecessary(log::LogContext& lc);
  cta::threading::Semaphore m_taskPostingSemaphore {5};

public:
  void waitSubthreadsComplete() override;

  /**
   * Initialise and start the OStoreDB threads
   *
   * @param osThreadPoolSize number of threads to start
   * @param osThreadStackSize the thread stack size in MB
   */
  void initConfig(const std::optional<int>& osThreadPoolSize, const std::optional<int>& osThreadStackSize) override;

private:
  /**
   * Start the OStoreDB threads
   *
   * @param threadNumber number of threads to start
   * @param stackSize the thread stack size in bytes
   */
  void setThreadNumber(uint64_t threadNumber, const std::optional<size_t>& stackSize = std::nullopt);
  void setBottomHalfQueueSize(uint64_t tasksNumber);

public:
  /*============ Basic IO check: validate object store access ===============*/
  void ping() override;

  /* === Session handling =================================================== */
  class TapeMountDecisionInfo : public SchedulerDatabase::TapeMountDecisionInfo {
    friend class OStoreDB;

  public:
    CTA_GENERATE_EXCEPTION_CLASS(SchedulingLockNotHeld);
    CTA_GENERATE_EXCEPTION_CLASS(TapeNotWritable);
    CTA_GENERATE_EXCEPTION_CLASS(TapeIsBusy);
    std::unique_ptr<SchedulerDatabase::ArchiveMount>
    createArchiveMount(const cta::SchedulerDatabase::PotentialMount& mount,
                       const catalogue::TapeForWriting& tape,
                       const std::string& driveName,
                       const std::string& logicalLibrary,
                       const std::string& hostName) override;
    std::unique_ptr<SchedulerDatabase::RetrieveMount>
    createRetrieveMount(const cta::SchedulerDatabase::PotentialMount& mount,
                        const std::string& driveName,
                        const std::string& logicalLibrary,
                        const std::string& hostName) override;
    ~TapeMountDecisionInfo() override;

  private:
    explicit TapeMountDecisionInfo(OStoreDB& oStoreDB);
    bool m_lockTaken = false;
    objectstore::ScopedExclusiveLock m_lockOnSchedulerGlobalLock;
    std::unique_ptr<objectstore::SchedulerGlobalLock> m_schedulerGlobalLock;
    OStoreDB& m_oStoreDB;
  };
  friend class TapeMountDecisionInfo;

  class TapeMountDecisionInfoNoLock : public SchedulerDatabase::TapeMountDecisionInfo {
  public:
    std::unique_ptr<SchedulerDatabase::ArchiveMount>
    createArchiveMount(const cta::SchedulerDatabase::PotentialMount& mount,
                       const catalogue::TapeForWriting& tape,
                       const std::string& driveName,
                       const std::string& logicalLibrary,
                       const std::string& hostName) override;
    std::unique_ptr<SchedulerDatabase::RetrieveMount>
    createRetrieveMount(const cta::SchedulerDatabase::PotentialMount& mount,
                        const std::string& driveName,
                        const std::string& logicalLibrary,
                        const std::string& hostName) override;
    ~TapeMountDecisionInfoNoLock() override = default;
  };

private:
  /**
   * An internal helper function with commonalities of both following functions
   * @param tmdi The TapeMountDecisionInfo where to store the data.
   * @param re A RootEntry object that should be locked and fetched.
   */
  void fetchMountInfo(SchedulerDatabase::TapeMountDecisionInfo& tmdi,
                      objectstore::RootEntry& re,
                      SchedulerDatabase::PurposeGetMountInfo purpose,
                      log::LogContext& logContext);

public:
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> getMountInfo(log::LogContext& logContext) override;
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> getMountInfo(log::LogContext& logContext,
                                                                         uint64_t timeout_us) override;

  // Only for RDBMS Scheduler backend
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo>
  getMountInfo(std::optional<std::string_view> logicalLibraryName,
               log::LogContext& logContext,
               uint64_t timeout_us) override {
    throw cta::exception::Exception("Not supported for OStoreDB implementation.");
  }

  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> getMountInfoNoLock(PurposeGetMountInfo purpose,
                                                                               log::LogContext& logContext) override;
  void trimEmptyQueues(log::LogContext& lc) override;
  bool trimEmptyToReportQueue(const std::string& queueName, log::LogContext& lc) override;

  /* === Archive Mount handling ============================================= */
  class ArchiveJob;

  class ArchiveMount : public SchedulerDatabase::ArchiveMount {
    friend class TapeMountDecisionInfo;

  private:
    ArchiveMount(OStoreDB& oStoreDB, const common::dataStructures::JobQueueType& queueType);
    OStoreDB& m_oStoreDB;
    common::dataStructures::JobQueueType m_queueType;

  public:
    CTA_GENERATE_EXCEPTION_CLASS(MaxFSeqNotGoingUp);
    const MountInfo& getMountInfo() override;
    std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>
    getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested, log::LogContext& logContext) override;

    void setDriveStatus(cta::common::dataStructures::DriveStatus status,
                        cta::common::dataStructures::MountType mountType,
                        time_t completionTime,
                        const std::optional<std::string>& reason = std::nullopt) override;
    void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats& stats) override;

  public:
    uint64_t requeueJobBatch(const std::list<std::string>& jobIDsList, log::LogContext& logContext) const override {
      // This implementation, serves only PGSCHED implementation
      throw cta::exception::NotImplementedException();
    }

    void setJobBatchTransferred(std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>& jobsBatch,
                                log::LogContext& lc) override;
  };
  friend class ArchiveMount;

  /* === Archive Job Handling =============================================== */
  class ArchiveJob : public SchedulerDatabase::ArchiveJob {
    friend class OStoreDB::ArchiveMount;
    friend class OStoreDB;

  public:
    CTA_GENERATE_EXCEPTION_CLASS(JobNotOwned);
    CTA_GENERATE_EXCEPTION_CLASS(NoSuchJob);
    void failTransfer(const std::string& failureReason, log::LogContext& lc) override;
    void failReport(const std::string& failureReason, log::LogContext& lc) override;
    // initialize and releaseToPool methods are here with empty implementation only since it is needed by PGSCHED in the baseclass
    void initialize(const rdbms::Rset& resultSet, bool jobIsRepack) override {};

    bool releaseToPool() override { throw exception::NotImplementedException(); };

  private:
    void asyncSucceedTransfer();
    /** Returns true if the jobs was the last one and the request should be queued for report. */
    void waitAsyncSucceed();
    objectstore::ArchiveRequest::RepackInfo getRepackInfoAfterAsyncSuccess();
    bool isLastAfterAsyncSuccess();
    void asyncDeleteRequest();
    void waitAsyncDelete();

  public:
    void bumpUpTapeFileCount(uint64_t newFileCount) override;
    ~ArchiveJob() override;

  private:
    ArchiveJob(const std::string& jobAddress, OStoreDB& oStoreDB);
    bool m_jobOwned = false;
    uint64_t m_mountId = 0;
    std::string m_tapePool;
    OStoreDB& m_oStoreDB;
    objectstore::ArchiveRequest m_archiveRequest;
    std::unique_ptr<objectstore::ArchiveRequest::AsyncTransferSuccessfulUpdater> m_succesfulTransferUpdater;
    std::unique_ptr<objectstore::ArchiveRequest::AsyncRequestDeleter> m_requestDeleter;
  };

  static ArchiveJob* castFromSchedDBJob(SchedulerDatabase::ArchiveJob* job);

  /* === Retrieve Mount handling ============================================ */
  class RetrieveJob;

  class RetrieveMount : public SchedulerDatabase::RetrieveMount {
    friend class TapeMountDecisionInfo;
    friend class cta::RetrieveJob;

  private:
    explicit RetrieveMount(OStoreDB& oStoreDB);
    OStoreDB& m_oStoreDB;

  public:
    const MountInfo& getMountInfo() override;
    std::list<std::unique_ptr<cta::SchedulerDatabase::RetrieveJob>>
    getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested, log::LogContext& logContext) override;
    void requeueJobBatch(std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>& jobBatch,
                         log::LogContext& logContext) override;

    uint64_t requeueJobBatch(const std::list<std::string>& jobIDsList, log::LogContext& logContext) const override {
      // Do nothing in this implementation, serves only PGSCHED implementation
      return 0;
    }

    void
    putQueueToSleep(const std::string& diskSystemName, const uint64_t sleepTime, log::LogContext& logContext) override;

  private:
    std::set<DiskSystemToSkip> m_diskSystemsToSkip;

    struct AsyncJobCaller {
      OStoreDB::RetrieveJob* m_pOsdbJob;
      common::dataStructures::MountPolicy& m_mountPolicy;
      std::list<std::string>& m_rjToUnown;
      std::map<std::string, std::list<OStoreDB::RetrieveJob*>>& m_jobsToRequeueForRepackMap;
      std::map<std::string, std::list<OStoreDB::RetrieveJob*>>& m_jobsToRequeueForReportToUser;
      log::LogContext& m_lc;

      void
      operator()(const std::unique_ptr<objectstore::RetrieveRequest::AsyncJobSucceedForRepackReporter>& upAsyncJob);
      void operator()(const std::unique_ptr<objectstore::RetrieveRequest::AsyncJobDeleter>& upAsyncJob);
      void operator()(const std::unique_ptr<objectstore::RetrieveRequest::AsyncJobSucceedReporter>& upAsyncJob);
    };

  public:
    void setDriveStatus(cta::common::dataStructures::DriveStatus status,
                        cta::common::dataStructures::MountType mountType,
                        time_t completionTime,
                        const std::optional<std::string>& reason = std::nullopt) override;
    void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats& stats) override;
    void addDiskSystemToSkip(const SchedulerDatabase::RetrieveMount::DiskSystemToSkip& diskSystemToSkip) override;
    void flushAsyncSuccessReports(std::list<cta::SchedulerDatabase::RetrieveJob*>& jobsBatch,
                                  log::LogContext& lc) override;

    void setJobBatchTransferred(std::list<std::unique_ptr<cta::SchedulerDatabase::RetrieveJob>>& jobsBatch,
                                log::LogContext& lc) override {
      throw cta::exception::NotImplementedException("For RelationalDB compatibility only");
    }
  };
  friend class RetrieveMount;

  /* === Retrieve Job handling ============================================== */
  class RetrieveJob : public SchedulerDatabase::RetrieveJob {
    friend class OStoreDB::RetrieveMount;
    friend class OStoreDB;

  public:
    CTA_GENERATE_EXCEPTION_CLASS(JobNotOwned);
    CTA_GENERATE_EXCEPTION_CLASS(NoSuchJob);
    /** Async set job successful.  Either delete (user transfer) or change status (repack)
     * Wait will happen in RetrieveMount::flushAsyncSuccessReports().
     */
    void asyncSetSuccessful() override;
    void failTransfer(const std::string& failureReason, log::LogContext& lc) override;
    void failReport(const std::string& failureReason, log::LogContext& lc) override;
    // initialize and releaseToPool methods are here with empty implementation only since it is needed by PGSCHED in the baseclass
    void initialize(const rdbms::Rset& resultSet, bool jobIsRepack) override {};

    bool releaseToPool() override { throw cta::exception::NotImplementedException(); };

    void abort(const std::string& abortReason, log::LogContext& lc) override;
    void fail() override;
    ~RetrieveJob() override;

  private:
    // Can be instantiated from a mount (queue to transfer) or a report queue
    RetrieveJob(const std::string& jobAddress, OStoreDB& oStoreDB, RetrieveMount* rm)
        : m_oStoreDB(oStoreDB),
          m_retrieveRequest(jobAddress, m_oStoreDB.m_objectStore),
          m_retrieveMount(rm) {}

    void asyncDeleteJob();
    void waitAsyncDelete();

  private:
    bool m_jobOwned = false;
    uint64_t m_mountId = 0;
    OStoreDB& m_oStoreDB;
    objectstore::RetrieveRequest m_retrieveRequest;
    OStoreDB::RetrieveMount* m_retrieveMount;
    std::variant<std::unique_ptr<objectstore::RetrieveRequest::AsyncJobDeleter>,
                 std::unique_ptr<objectstore::RetrieveRequest::AsyncJobSucceedForRepackReporter>,
                 std::unique_ptr<objectstore::RetrieveRequest::AsyncJobSucceedReporter>>
      m_variantAsyncJob;
    objectstore::RetrieveRequest::RepackInfo m_repackInfo;
    std::optional<std::string> m_activity;
    std::optional<std::string> m_diskSystemName;
    std::unique_ptr<objectstore::RetrieveRequest::AsyncJobDeleter> m_jobDeleter;
  };

  static RetrieveJob* castFromSchedDBJob(SchedulerDatabase::RetrieveJob* job);

  /* === Archive requests handling  ========================================= */
  CTA_GENERATE_EXCEPTION_CLASS(ArchiveRequestHasNoCopies);
  CTA_GENERATE_EXCEPTION_CLASS(ArchiveRequestAlreadyCompleteOrCanceled);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchArchiveQueue);

  std::string queueArchive(const std::string& instanceName,
                           const cta::common::dataStructures::ArchiveRequest& request,
                           const cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId& criteria,
                           log::LogContext& logContext) override;

  std::map<std::string, std::list<common::dataStructures::ArchiveJob>, std::less<>> getArchiveJobs() const override;

  std::list<cta::common::dataStructures::ArchiveJob>
  getArchiveJobs(const std::optional<std::string>& tapePoolName) const override;

  class ArchiveJobQueueItor : public IArchiveJobQueueItor {
  public:
    ArchiveJobQueueItor(objectstore::Backend* objectStore,
                        common::dataStructures::JobQueueType queueType,
                        const std::string& queue_id = "")
        : m_archiveQueueItor(*objectStore, queueType, queue_id) {}

    ~ArchiveJobQueueItor() override = default;
    const std::string& qid() const override;
    bool end() const override;
    void operator++() override;
    const common::dataStructures::ArchiveJob& operator*() const override;

  private:
    QueueItor<objectstore::RootEntry::ArchiveQueueDump, objectstore::ArchiveQueue> m_archiveQueueItor;
  };

  std::unique_ptr<IArchiveJobQueueItor>
  getArchiveJobQueueItor(const std::string& tapePoolName,
                         common::dataStructures::JobQueueType queueType) const override;

  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>>
  getNextArchiveJobsToReportBatch(uint64_t filesRequested, log::LogContext& logContext) override;

  JobsFailedSummary getArchiveJobsFailedSummary(log::LogContext& logContext) override;

  void setArchiveJobBatchReported(std::list<cta::SchedulerDatabase::ArchiveJob*>& jobsBatch,
                                  log::TimingList& timingList,
                                  utils::Timer& t,
                                  log::LogContext& lc) override;

  /* === Retrieve requests handling  ======================================== */
  std::list<RetrieveQueueCleanupInfo> getRetrieveQueuesCleanupInfo(log::LogContext& logContext) override;
  void setRetrieveQueueCleanupFlag(const std::string& vid, bool val, log::LogContext& logContext) override;

  void cleanRetrieveQueueForVid(const std::string& vid, log::LogContext& logContext) override {
    throw cta::exception::Exception("Not supported for OStoreDB implementation.");
  };

  std::list<RetrieveQueueStatistics>
  getRetrieveQueueStatistics(const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria,
                             const std::set<std::string>& vidsToConsider) override;

  void clearStatisticsCache(const std::string& vid) override;

  void setStatisticsCacheConfig(const StatisticsCacheConfig& conf) override;

  CTA_GENERATE_EXCEPTION_CLASS(RetrieveRequestHasNoCopies);
  CTA_GENERATE_EXCEPTION_CLASS(TapeCopyNumberOutOfRange);
  SchedulerDatabase::RetrieveRequestInfo
  queueRetrieve(cta::common::dataStructures::RetrieveRequest& rqst,
                const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria,
                const std::optional<std::string>& diskSystemName,
                log::LogContext& logContext) override;
  void cancelRetrieve(const std::string& instanceName,
                      const cta::common::dataStructures::CancelRetrieveRequest& rqst,
                      log::LogContext& lc) override;

  std::list<RetrieveRequestDump> getRetrieveRequestsByVid(const std::string& vid) const override;

  std::list<RetrieveRequestDump> getRetrieveRequestsByRequester(const std::string& vid) const override;

  std::map<std::string, std::list<RetrieveRequestDump>> getRetrieveRequests() const override;

  void deleteRetrieveRequest(const common::dataStructures::SecurityIdentity& requester,
                             const std::string& remoteFile) override;

  /**
   * Idempotently deletes the specified ArchiveRequest from the objectstore
   * @param address, the address of the ArchiveRequest
   * @param archiveFileID the archiveFileID of the file to delete.
   */
  void cancelArchive(const common::dataStructures::DeleteArchiveRequest& request, log::LogContext& lc) override;

  void deleteFailed(const std::string& objectId, log::LogContext& lc) override;

  std::list<cta::common::dataStructures::RetrieveJob>
  getPendingRetrieveJobs(const std::optional<std::string>& vid) const override;

  std::map<std::string, std::list<common::dataStructures::RetrieveJob>, std::less<>>
  getPendingRetrieveJobs() const override;

  class RetrieveJobQueueItor : public IRetrieveJobQueueItor {
  public:
    RetrieveJobQueueItor(objectstore::Backend* objectStore,
                         common::dataStructures::JobQueueType queueType,
                         const std::string& queue_id = "")
        : m_retrieveQueueItor(*objectStore, queueType, queue_id) {}

    ~RetrieveJobQueueItor() override = default;
    const std::string& qid() const override;
    bool end() const override;
    void operator++() override;
    const common::dataStructures::RetrieveJob& operator*() const override;

  private:
    QueueItor<objectstore::RootEntry::RetrieveQueueDump, objectstore::RetrieveQueue> m_retrieveQueueItor;
  };

  std::unique_ptr<IRetrieveJobQueueItor>
  getRetrieveJobQueueItor(const std::string& vid, common::dataStructures::JobQueueType queueType) const override;

  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>
  getNextRetrieveJobsToTransferBatch(const std::string& vid,
                                     uint64_t filesRequested,
                                     log::LogContext& logContext) override;
  void requeueRetrieveRequestJobs(std::list<cta::SchedulerDatabase::RetrieveJob*>& jobs,
                                  const std::string& toReportQueueAddress,
                                  log::LogContext& logContext) override;
  std::string blockRetrieveQueueForCleanup(const std::string& vid) override;
  void unblockRetrieveQueueForCleanup(const std::string& vid) override;

  CTA_GENERATE_EXCEPTION_CLASS(RetrieveQueueNotReservedForCleanup);
  CTA_GENERATE_EXCEPTION_CLASS(RetrieveQueueAlreadyReserved);
  CTA_GENERATE_EXCEPTION_CLASS(RetrieveQueueNotFound);

  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>
  getNextRetrieveJobsToReportBatch(uint64_t filesRequested, log::LogContext& logContext) override;

  void setRetrieveJobBatchReportedToUser(std::list<cta::SchedulerDatabase::RetrieveJob*>& jobsBatch,
                                         log::TimingList& timingList,
                                         utils::Timer& t,
                                         log::LogContext& lc) override;

  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>
  getNextRetrieveJobsFailedBatch(uint64_t filesRequested, log::LogContext& logContext) override;

  JobsFailedSummary getRetrieveJobsFailedSummary(log::LogContext& logContext) override;

  /* === Repack requests handling =========================================== */
  std::string queueRepack(const SchedulerDatabase::QueueRepackRequest& repackRequest,
                          log::LogContext& logContext) override;

  bool repackExists() override;
  std::list<common::dataStructures::RepackInfo> getRepackInfo() override;
  common::dataStructures::RepackInfo getRepackInfo(const std::string& vid) override;
  void cancelRepack(const std::string& vid, log::LogContext& lc) override;

  class RepackRequest : public SchedulerDatabase::RepackRequest {
    friend class OStoreDB;

  public:
    RepackRequest(const std::string& jobAddress, OStoreDB& oStoreDB)
        : m_oStoreDB(oStoreDB),
          m_repackRequest(jobAddress, m_oStoreDB.m_objectStore) {}

    uint64_t addSubrequestsAndUpdateStats(const std::list<Subrequest>& repackSubrequests,
                                          const cta::common::dataStructures::ArchiveRoute::FullMap& archiveRoutesMap,
                                          uint64_t maxFSeqLowBound,
                                          uint64_t maxAddedFSeq,
                                          const TotalStatsFiles& totalStatsFiles,
                                          const disk::DiskSystemList& diskSystemList,
                                          log::LogContext& lc) override;
    void expandDone() override;
    void fail() override;
    void requeueInToExpandQueue(log::LogContext& lc) override;
    void setExpandStartedAndChangeStatus() override;
    void fillLastExpandedFSeqAndTotalStatsFile(uint64_t& fSeq, TotalStatsFiles& totalStatsFiles) override;
    uint64_t getLastExpandedFSeq() override;
    void setLastExpandedFSeq(uint64_t fseq) override;

  private:
    OStoreDB& m_oStoreDB;
    objectstore::RepackRequest m_repackRequest;
  };
  friend class RepackRequest;

  /**
   * A class holding a lock on the pending repack request queue. This is the first
   * container we will have to lock if we decide to pop a/some request(s)
   */
  class RepackRequestPromotionStatistics : public SchedulerDatabase::RepackRequestStatistics {
    friend class OStoreDB;

  public:
    PromotionToToExpandResult promotePendingRequestsForExpansion(size_t requestCount, log::LogContext& lc) override;
    ~RepackRequestPromotionStatistics() final = default;

  private:
    RepackRequestPromotionStatistics(objectstore::Backend& backend, objectstore::AgentReference& agentReference);
    objectstore::Backend& m_backend;
    objectstore::AgentReference& m_agentReference;
    objectstore::RepackQueuePending m_pendingRepackRequestQueue;
    objectstore::ScopedExclusiveLock m_lockOnPendingRepackRequestsQueue;
  };

  class RepackRequestPromotionStatisticsNoLock : public SchedulerDatabase::RepackRequestStatistics {
    friend class OStoreDB;

  public:
    PromotionToToExpandResult promotePendingRequestsForExpansion(size_t requestCount, log::LogContext& lc) override {
      throw SchedulingLockNotHeld("In RepackRequestPromotionStatisticsNoLock::promotePendingRequestsForExpansion");
    }

    virtual ~RepackRequestPromotionStatisticsNoLock() final = default;
  };

private:
  void populateRepackRequestsStatistics(objectstore::RootEntry& re, SchedulerDatabase::RepackRequestStatistics& stats);

public:
  std::unique_ptr<RepackRequestStatistics> getRepackStatistics() override;
  std::unique_ptr<RepackRequestStatistics> getRepackStatisticsNoLock() override;

  /**
   * Returns the Object Store representation of a RepackRequest that is in
   * the RepackQueueToExpand queue (the repack request has Status "ToExpand"
   * @return a unique_ptr holding the RepackRequest
   */
  std::unique_ptr<SchedulerDatabase::RepackRequest> getNextRepackJobToExpand() override;

  class RepackRetrieveSuccessesReportBatch;
  class RepackRetrieveFailureReportBatch;
  class RepackArchiveReportBatch;
  class RepackArchiveSuccessesReportBatch;
  class RepackArchiveFailureReportBatch;
  friend class RepackRetrieveSuccessesReportBatch;
  friend class RepackRetrieveFailureReportBatch;
  friend class RepackArchiveSuccessesReportBatch;
  friend class RepackArchiveFailureReportBatch;

  /**
   * Base class handling the commonalities
   */
  class RepackReportBatch : public SchedulerDatabase::RepackReportBatch {
    friend class OStoreDB;
    friend class RepackRetrieveSuccessesReportBatch;
    friend class RepackRetrieveFailureReportBatch;
    friend class RepackArchiveSuccessesReportBatch;
    friend class RepackArchiveFailureReportBatch;

    RepackReportBatch(objectstore::Backend& backend, OStoreDB& oStoreDb)
        : m_repackRequest(backend),
          m_oStoreDb(oStoreDb) {}

  protected:
    objectstore::RepackRequest m_repackRequest;
    OStoreDB& m_oStoreDb;

    template<class SR>
    struct SubrequestInfo {
      /// CopyNb is only useful for archive requests where we want to distinguish several jobs.
      uint32_t archivedCopyNb = 0;
      /** Status map is only useful for archive requests, where we need to know other job's status to decide
       * whether we should delete the request (all done). It's more efficient to get the information on pop
       * in order to save a read in the most common case (only one job), and trigger immediate deletion of
       * the request after succeeding/failing. */
      std::map<uint32_t, objectstore::serializers::ArchiveJobStatus> archiveJobsStatusMap;
      cta::objectstore::AgentReference* owner;
      std::shared_ptr<SR> subrequest;
      common::dataStructures::ArchiveFile archiveFile;
      typename SR::RepackInfo repackInfo;
      using List = std::list<SubrequestInfo>;
    };
  };

  class RepackRetrieveSuccessesReportBatch : public RepackReportBatch {
    friend class OStoreDB;

    RepackRetrieveSuccessesReportBatch(objectstore::Backend& backend, OStoreDB& oStoreDb)
        : RepackReportBatch(backend, oStoreDb) {}

  public:
    void report(log::LogContext& lc) override;

  private:
    using SubrequestInfo = RepackReportBatch::SubrequestInfo<objectstore::RetrieveRequest>;
    SubrequestInfo::List m_subrequestList;
  };

  class RepackRetrieveFailureReportBatch : public RepackReportBatch {
    friend class OStoreDB;

    RepackRetrieveFailureReportBatch(objectstore::Backend& backend, OStoreDB& oStoreDb)
        : RepackReportBatch(backend, oStoreDb) {}

  public:
    void report(log::LogContext& lc) override;

  private:
    using SubrequestInfo = RepackReportBatch::SubrequestInfo<objectstore::RetrieveRequest>;
    SubrequestInfo::List m_subrequestList;
  };

  /**
   * Super class that holds the common code for the reporting
   * of Archive successes and failures
   */
  class RepackArchiveReportBatch : public RepackReportBatch {
    friend class OStoreDB;

  protected:
    using SubrequestInfo = RepackReportBatch::SubrequestInfo<objectstore::ArchiveRequest>;
    SubrequestInfo::List m_subrequestList;

  public:
    RepackArchiveReportBatch(objectstore::Backend& backend, OStoreDB& oStoreDb)
        : RepackReportBatch(backend, oStoreDb) {}

    void report(log::LogContext& lc) override;

  private:
    objectstore::RepackRequest::SubrequestStatistics::List prepareReport();
    virtual cta::objectstore::serializers::RepackRequestStatus
    recordReport(objectstore::RepackRequest::SubrequestStatistics::List& ssl,
                 log::TimingList& timingList,
                 utils::Timer& t) = 0;
    virtual cta::objectstore::serializers::ArchiveJobStatus getNewStatus() = 0;
  };

  class RepackArchiveSuccessesReportBatch : public RepackArchiveReportBatch {
    friend class OStoreDB;

    RepackArchiveSuccessesReportBatch(objectstore::Backend& backend, OStoreDB& oStoreDb)
        : RepackArchiveReportBatch(backend, oStoreDb) {}

  public:
    void report(log::LogContext& lc) override;

  private:
    cta::objectstore::serializers::RepackRequestStatus
    recordReport(objectstore::RepackRequest::SubrequestStatistics::List& ssl,
                 log::TimingList& timingList,
                 utils::Timer& t) override;
    cta::objectstore::serializers::ArchiveJobStatus getNewStatus() override;
  };

  class RepackArchiveFailureReportBatch : public RepackArchiveReportBatch {
    friend class OStoreDB;

    RepackArchiveFailureReportBatch(objectstore::Backend& backend, OStoreDB& oStoreDb)
        : RepackArchiveReportBatch(backend, oStoreDb) {}

  public:
    void report(log::LogContext& lc) override;

  private:
    cta::objectstore::serializers::RepackRequestStatus
    recordReport(objectstore::RepackRequest::SubrequestStatistics::List& ssl,
                 log::TimingList& timingList,
                 utils::Timer& t) override;
    cta::objectstore::serializers::ArchiveJobStatus getNewStatus() override;
  };

  std::unique_ptr<SchedulerDatabase::RepackReportBatch> getNextRepackReportBatch(log::LogContext& lc) override;

  std::list<std::unique_ptr<SchedulerDatabase::RepackReportBatch>> getRepackReportBatches(log::LogContext& lc) override;

  std::unique_ptr<SchedulerDatabase::RepackReportBatch>
  getNextSuccessfulRetrieveRepackReportBatch(log::LogContext& lc) final;
  std::unique_ptr<SchedulerDatabase::RepackReportBatch>
  getNextFailedRetrieveRepackReportBatch(log::LogContext& lc) final;
  std::unique_ptr<SchedulerDatabase::RepackReportBatch>
  getNextSuccessfulArchiveRepackReportBatch(log::LogContext& lc) final;
  std::unique_ptr<SchedulerDatabase::RepackReportBatch>
  getNextFailedArchiveRepackReportBatch(log::LogContext& lc) final;

private:
  const size_t c_repackArchiveReportBatchSize = 10000;
  const size_t c_repackRetrieveReportBatchSize = 10000;

private:
  objectstore::Backend& m_objectStore;
  catalogue::Catalogue& m_catalogue;
  log::Logger& m_logger;
  std::unique_ptr<TapeDrivesCatalogueState> m_tapeDrivesState;
  objectstore::AgentReference* m_agentReference = nullptr;
};

}  // namespace cta
