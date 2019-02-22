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

#include "common/log/StdoutLogger.hpp"

#include "OStoreDB.hpp"
#include "MemQueues.hpp"
#include "objectstore/ArchiveQueueAlgorithms.hpp"
#include "objectstore/RetrieveQueueAlgorithms.hpp"
#include "objectstore/RepackQueueAlgorithms.hpp"
#include "objectstore/DriveRegister.hpp"
#include "objectstore/DriveState.hpp"
#include "objectstore/RepackRequest.hpp"
#include "objectstore/RepackIndex.hpp"
#include "objectstore/RepackQueue.hpp"
#include "objectstore/Helpers.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/make_unique.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/TapeSessionStats.hpp"
#include "Scheduler.hpp"
#include <algorithm>
#include <cmath>
#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */
#include <stdexcept>
#include <set>
#include <iostream>
#include <bits/unique_ptr.h>

namespace cta {  
using namespace objectstore;

//------------------------------------------------------------------------------
// OStoreDB::OStoreDB()
//------------------------------------------------------------------------------
OStoreDB::OStoreDB(objectstore::Backend& be, catalogue::Catalogue & catalogue, log::Logger &logger):
  m_taskQueueSize(0), m_taskPostingSemaphore(5), m_objectStore(be), m_catalogue(catalogue), m_logger(logger) {
  for (size_t i=0; i<5; i++) {
    m_enqueueingWorkerThreads.emplace_back(new EnqueueingWorkerThread(m_enqueueingTasksQueue));
    m_enqueueingWorkerThreads.back()->start();
  }
}

//------------------------------------------------------------------------------
// OStoreDB::~OStoreDB()
//------------------------------------------------------------------------------
OStoreDB::~OStoreDB() throw() {
  while (m_taskQueueSize) sleep(1);
  for (__attribute__((unused)) auto &t: m_enqueueingWorkerThreads) m_enqueueingTasksQueue.push(nullptr);
  for (auto &t: m_enqueueingWorkerThreads) {
    t->wait();
    delete t;
    t=nullptr;
  }
}

//------------------------------------------------------------------------------
// OStoreDB::setAgentReference()
//------------------------------------------------------------------------------
void OStoreDB::setAgentReference(objectstore::AgentReference *agentReference) {
  m_agentReference = agentReference;
}

//------------------------------------------------------------------------------
// OStoreDB::assertAgentAddressSet()
//------------------------------------------------------------------------------
void OStoreDB::assertAgentAddressSet() {
  if (!m_agentReference)
    throw AgentNotSet("In OStoreDB::assertAgentSet: Agent address not set");
}

//------------------------------------------------------------------------------
// OStoreDB::waitSubthreadsComplete()
//------------------------------------------------------------------------------
void OStoreDB::waitSubthreadsComplete() {
  while (m_taskQueueSize) std::this_thread::yield();
}

//------------------------------------------------------------------------------
// OStoreDB::setThreadNumber()
//------------------------------------------------------------------------------
void OStoreDB::setThreadNumber(uint64_t threadNumber) {
  // Clear all threads.
  for (__attribute__((unused)) auto &t: m_enqueueingWorkerThreads) m_enqueueingTasksQueue.push(nullptr);
  for (auto &t: m_enqueueingWorkerThreads) {
    t->wait();
    delete t;
    t=nullptr;
  }
  m_enqueueingWorkerThreads.clear();
  // Create the new ones.
  for (size_t i=0; i<threadNumber; i++) {
    m_enqueueingWorkerThreads.emplace_back(new EnqueueingWorkerThread(m_enqueueingTasksQueue));
    m_enqueueingWorkerThreads.back()->start();
  }
}

//------------------------------------------------------------------------------
// OStoreDB::setBottomHalfQueueSize()
//------------------------------------------------------------------------------
void OStoreDB::setBottomHalfQueueSize(uint64_t tasksNumber) {
  // 5 is the default queue size.
  m_taskPostingSemaphore.release(tasksNumber - 5);
}


//------------------------------------------------------------------------------
// OStoreDB::EnqueueingWorkerThread::run()
//------------------------------------------------------------------------------
void OStoreDB::EnqueueingWorkerThread::run() {
  while (true) {
    std::unique_ptr<EnqueueingTask> et(m_enqueueingTasksQueue.pop());
    if (!et.get()) break;
    ANNOTATE_HAPPENS_AFTER(et.get());
    (*et)();
    ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(et.get());
  }
}

//------------------------------------------------------------------------------
// OStoreDB::delayIfNecessary()
//------------------------------------------------------------------------------
void OStoreDB::delayIfNecessary(log::LogContext& lc) {
  bool delayInserted = false;
  utils::Timer t;
  uint64_t taskQueueSize = m_taskQueueSize;
  double sleepDelay = 0;
  double lockDelay = 0;
  if (m_taskQueueSize > 10000) {
    // Put a linear delay starting at 0 at 10000 element and going to 10s at 25000
    double delay = 10.0 * (taskQueueSize - 10000) / 15000;
    double delayInt;
    ::timespec ts;
    ts.tv_nsec = std::modf(delay, &delayInt) * 1000 * 1000 *1000;
    ts.tv_sec = delayInt;
    nanosleep(&ts, nullptr);
    sleepDelay = t.secs(utils::Timer::resetCounter);
    delayInserted = true;
  }
  if (!m_taskPostingSemaphore.tryAcquire()) {
    m_taskPostingSemaphore.acquire();
    lockDelay = t.secs(utils::Timer::resetCounter);
    delayInserted = true;
  }
  if (delayInserted) {
    log::ScopedParamContainer params(lc);
    params.add("sleepDelay", sleepDelay)
          .add("lockDelay", lockDelay)
          .add("taskQueueSize", taskQueueSize);
    lc.log(log::INFO, "In OStoreDB::delayIfNecessary(): inserted delay.");
  }
}

//------------------------------------------------------------------------------
// OStoreDB::ping()
//------------------------------------------------------------------------------
void OStoreDB::ping() {
  // Validate we can lock and fetch the root entry.
  objectstore::RootEntry re(m_objectStore);
  re.fetchNoLock();
}

//------------------------------------------------------------------------------
// OStoreDB::fetchMountInfo()
//------------------------------------------------------------------------------
void OStoreDB::fetchMountInfo(SchedulerDatabase::TapeMountDecisionInfo& tmdi, RootEntry& re, 
    log::LogContext & logContext) {
  utils::Timer t, t2;
  // Walk the archive queues for statistics
  for (auto & aqp: re.dumpArchiveQueues(JobQueueType::JobsToTransfer)) {
    objectstore::ArchiveQueue aqueue(aqp.address, m_objectStore);
    // debug utility variable
    std::string __attribute__((__unused__)) poolName = aqp.tapePool;
    objectstore::ScopedSharedLock aqlock;
    double queueLockTime = 0;
    double queueFetchTime = 0;
    try {
      aqueue.fetchNoLock();
      queueFetchTime = t.secs(utils::Timer::resetCounter);
    } catch (cta::exception::Exception &ex) {
      log::ScopedParamContainer params (logContext);
      params.add("queueObject", aqp.address)
            .add("tapePool", aqp.tapePool)
            .add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::WARNING, "In OStoreDB::fetchMountInfo(): failed to lock/fetch an archive queue. Skipping it.");
      continue;
    }
    // If there are files queued, we create an entry for this tape pool in the
    // mount candidates list.
    if (aqueue.getJobsSummary().jobs) {
      tmdi.potentialMounts.push_back(SchedulerDatabase::PotentialMount());
      auto & m = tmdi.potentialMounts.back();
      m.tapePool = aqp.tapePool;
      m.type = cta::common::dataStructures::MountType::Archive;
      m.bytesQueued = aqueue.getJobsSummary().bytes;
      m.filesQueued = aqueue.getJobsSummary().jobs;      
      m.oldestJobStartTime = aqueue.getJobsSummary().oldestJobStartTime;
      m.priority = aqueue.getJobsSummary().priority;
      m.maxDrivesAllowed = aqueue.getJobsSummary().maxDrivesAllowed;
      m.minRequestAge = aqueue.getJobsSummary().minArchiveRequestAge;
      m.logicalLibrary = "";
    } else {
      tmdi.queueTrimRequired = true;
    }
    auto processingTime = t.secs(utils::Timer::resetCounter);
    log::ScopedParamContainer params (logContext);
    params.add("queueObject", aqp.address)
          .add("tapePool", aqp.tapePool)
          .add("queueLockTime", queueLockTime)
          .add("queueFetchTime", queueFetchTime)
          .add("processingTime", processingTime);
    logContext.log(log::INFO, "In OStoreDB::fetchMountInfo(): fetched an archive queue.");
  }
  // Walk the retrieve queues for statistics
  for (auto & rqp: re.dumpRetrieveQueues(JobQueueType::JobsToTransfer)) {
    RetrieveQueue rqueue(rqp.address, m_objectStore);
    // debug utility variable
    std::string __attribute__((__unused__)) vid = rqp.vid;
    ScopedSharedLock rqlock;
    double queueLockTime = 0;
    double queueFetchTime = 0;
    try {
      rqueue.fetchNoLock();
      queueFetchTime = t.secs(utils::Timer::resetCounter);
    } catch (cta::exception::Exception &ex) {
      log::LogContext lc(m_logger);
      log::ScopedParamContainer params (lc);
      params.add("queueObject", rqp.address)
            .add("tapeVid", rqp.vid)
            .add("exceptionMessage", ex.getMessageValue());
      lc.log(log::WARNING, "In OStoreDB::fetchMountInfo(): failed to lock/fetch a retrieve queue. Skipping it.");
      continue;
    }
    // If there are files queued, we create an entry for this retrieve queue in the
    // mount candidates list.
    if (rqueue.getJobsSummary().jobs) {
      tmdi.potentialMounts.push_back(SchedulerDatabase::PotentialMount());
      auto & m = tmdi.potentialMounts.back();
      m.vid = rqp.vid;
      m.type = cta::common::dataStructures::MountType::Retrieve;
      m.bytesQueued = rqueue.getJobsSummary().bytes;
      m.filesQueued = rqueue.getJobsSummary().jobs;      
      m.oldestJobStartTime = rqueue.getJobsSummary().oldestJobStartTime;
      m.priority = rqueue.getJobsSummary().priority;
      m.maxDrivesAllowed = rqueue.getJobsSummary().maxDrivesAllowed;
      m.minRequestAge = rqueue.getJobsSummary().minRetrieveRequestAge;
      m.logicalLibrary = ""; // The logical library is not known here, and will be determined by the caller.
    } else {
      tmdi.queueTrimRequired = true;
    }
    auto processingTime = t.secs(utils::Timer::resetCounter);
    log::ScopedParamContainer params (logContext);
    params.add("queueObject", rqp.address)
          .add("tapeVid", rqp.vid)
          .add("queueLockTime", queueLockTime)
          .add("queueFetchTime", queueFetchTime)
          .add("processingTime", processingTime);
    logContext.log(log::INFO, "In OStoreDB::fetchMountInfo(): fetched a retrieve queue.");
  }
  // Collect information about the existing and next mounts
  // If a next mount exists the drive "counts double", but the corresponding drive
  // is either about to mount, or about to replace its current mount.
  double registerFetchTime = 0;
  auto driveStates = Helpers::getAllDriveStates(m_objectStore, logContext);
  registerFetchTime = t.secs(utils::Timer::resetCounter);
  using common::dataStructures::DriveStatus;
  std::set<int> activeDriveStatuses = {
    (int)cta::common::dataStructures::DriveStatus::Starting,
    (int)cta::common::dataStructures::DriveStatus::Mounting,
    (int)cta::common::dataStructures::DriveStatus::Transferring,
    (int)cta::common::dataStructures::DriveStatus::Unloading,
    (int)cta::common::dataStructures::DriveStatus::Unmounting,
    (int)cta::common::dataStructures::DriveStatus::DrainingToDisk,
    (int)cta::common::dataStructures::DriveStatus::CleaningUp };
  std::set<int> activeMountTypes = {
    (int)cta::common::dataStructures::MountType::Archive,
    (int)cta::common::dataStructures::MountType::Retrieve,
    (int)cta::common::dataStructures::MountType::Label };
  for (const auto &d : driveStates) {
    if (activeDriveStatuses.count((int)d.driveStatus)) {
      tmdi.existingOrNextMounts.push_back(ExistingMount());
      tmdi.existingOrNextMounts.back().type = d.mountType;
      tmdi.existingOrNextMounts.back().tapePool = d.currentTapePool;
      tmdi.existingOrNextMounts.back().driveName = d.driveName;
      tmdi.existingOrNextMounts.back().vid = d.currentVid;
      tmdi.existingOrNextMounts.back().currentMount = true;
      tmdi.existingOrNextMounts.back().bytesTransferred = d.bytesTransferredInSession;
      tmdi.existingOrNextMounts.back().filesTransferred = d.filesTransferredInSession;
      tmdi.existingOrNextMounts.back().latestBandwidth = d.latestBandwidth;
    }
    if (activeMountTypes.count((int)d.nextMountType)) {
      tmdi.existingOrNextMounts.push_back(ExistingMount());
      tmdi.existingOrNextMounts.back().type = d.nextMountType;
      tmdi.existingOrNextMounts.back().tapePool = d.nextTapepool;
      tmdi.existingOrNextMounts.back().driveName = d.driveName;
      tmdi.existingOrNextMounts.back().vid = d.nextVid;
      tmdi.existingOrNextMounts.back().currentMount = false;
      tmdi.existingOrNextMounts.back().bytesTransferred = 0;
      tmdi.existingOrNextMounts.back().filesTransferred = 0;
      tmdi.existingOrNextMounts.back().latestBandwidth = 0;
    }
  }
  auto registerProcessingTime = t.secs(utils::Timer::resetCounter);
  log::ScopedParamContainer params (logContext);
  params.add("queueFetchTime", registerFetchTime)
        .add("processingTime", registerProcessingTime);
  logContext.log(log::INFO, "In OStoreDB::fetchMountInfo(): fetched the drive register.");
}

//------------------------------------------------------------------------------
// OStoreDB::getMountInfo()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> 
  OStoreDB::getMountInfo(log::LogContext& logContext) {
  utils::Timer t;
  //Allocate the getMountInfostructure to return.
  assertAgentAddressSet();
  std::unique_ptr<OStoreDB::TapeMountDecisionInfo> privateRet (new OStoreDB::TapeMountDecisionInfo(*this));
  TapeMountDecisionInfo & tmdi=*privateRet;
  // Get all the tape pools and tapes with queues (potential mounts)
  objectstore::RootEntry re(m_objectStore);
  re.fetchNoLock();
  auto rootFetchNoLockTime = t.secs(utils::Timer::resetCounter);
  // Take an exclusive lock on the scheduling and fetch it.
  tmdi.m_schedulerGlobalLock.reset(
    new SchedulerGlobalLock(re.getSchedulerGlobalLock(), m_objectStore));
  tmdi.m_lockOnSchedulerGlobalLock.lock(*tmdi.m_schedulerGlobalLock);
  auto lockSchedGlobalTime = t.secs(utils::Timer::resetCounter);
  tmdi.m_lockTaken = true;
  tmdi.m_schedulerGlobalLock->fetch();
  auto fetchSchedGlobalTime = t.secs(utils::Timer::resetCounter);;
  fetchMountInfo(tmdi, re, logContext);
  auto fetchMountInfoTime = t.secs(utils::Timer::resetCounter);
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> ret(std::move(privateRet));
  {
    log::ScopedParamContainer params(logContext);
    params.add("rootFetchNoLockTime", rootFetchNoLockTime)
          .add("lockSchedGlobalTime", lockSchedGlobalTime)
          .add("fetchSchedGlobalTime", fetchSchedGlobalTime)
          .add("fetchMountInfoTime", fetchMountInfoTime);
    logContext.log(log::INFO, "In OStoreDB::getMountInfo(): success.");
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getMountInfoNoLock()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> OStoreDB::getMountInfoNoLock(log::LogContext & logContext) {
  utils::Timer t;
  //Allocate the getMountInfostructure to return.
  assertAgentAddressSet();
  std::unique_ptr<OStoreDB::TapeMountDecisionInfoNoLock> privateRet (new OStoreDB::TapeMountDecisionInfoNoLock);
  // Get all the tape pools and tapes with queues (potential mounts)
  objectstore::RootEntry re(m_objectStore);
  re.fetchNoLock();
  auto rootFetchNoLockTime = t.secs(utils::Timer::resetCounter);
  TapeMountDecisionInfoNoLock & tmdi=*privateRet;
  fetchMountInfo(tmdi, re, logContext);
  auto fetchMountInfoTime = t.secs(utils::Timer::resetCounter);
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> ret(std::move(privateRet));
  {
    log::ScopedParamContainer params(logContext);
    params.add("rootFetchNoLockTime", rootFetchNoLockTime)
          .add("fetchMountInfoTime", fetchMountInfoTime);
    logContext.log(log::INFO, "In OStoreDB::getMountInfoNoLock(): success.");
  }
  return ret;
}
//------------------------------------------------------------------------------
// OStoreDB::trimEmptyQueues()
//------------------------------------------------------------------------------
void OStoreDB::trimEmptyQueues(log::LogContext& lc) {
  // We will trim empty queues from the root entry.
  lc.log(log::INFO, "In OStoreDB::trimEmptyQueues(): will start trimming empty queues");
  // Get an exclusive lock on the root entry, we have good chances to need it.
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  for (auto & queueType: { JobQueueType::JobsToTransfer, JobQueueType::JobsToReportToUser, JobQueueType::FailedJobs} ) {
    try {
      auto archiveQueueList = re.dumpArchiveQueues(queueType);
      for (auto & a: archiveQueueList) {
        ArchiveQueue aq(a.address, m_objectStore);
        ScopedSharedLock aql(aq);
        aq.fetch();
        if (!aq.getJobsSummary().jobs) {
          aql.release();
          re.removeArchiveQueueAndCommit(a.tapePool, queueType, lc);
          log::ScopedParamContainer params(lc);
          params.add("tapePool", a.tapePool)
                .add("queueType", toString(queueType))
                .add("queueObject", a.address);
          lc.log(log::INFO, "In OStoreDB::trimEmptyQueues(): deleted empty archive queue.");
        }
      }
      auto retrieveQueueList = re.dumpRetrieveQueues(queueType);
      for (auto &r : retrieveQueueList) {
        RetrieveQueue rq(r.address, m_objectStore);
        ScopedSharedLock rql(rq);
        rq.fetch();
        if (!rq.getJobsSummary().jobs) {
          rql.release();
          re.removeRetrieveQueueAndCommit(r.vid, queueType, lc);
          log::ScopedParamContainer params(lc);
          params.add("tapeVid", r.vid)
                .add("queueType", toString(queueType))
                .add("queueObject", r.address);
          lc.log(log::INFO, "In OStoreDB::trimEmptyQueues(): deleted empty retrieve queue.");
        }
      }
    } catch (cta::exception::Exception & ex) {
      log::ScopedParamContainer params(lc);
      params.add("exceptionMessage", ex.getMessageValue());
      lc.log(log::ERR, "In OStoreDB::trimEmptyQueues(): got an exception. Stack trace follows.");
      lc.logBacktrace(log::ERR, ex.backtrace());
    }
  }
}

//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfoNoLock::createArchiveMount()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::ArchiveMount> OStoreDB::TapeMountDecisionInfoNoLock::createArchiveMount(
        const catalogue::TapeForWriting& tape,
        const std::string driveName,
        const std::string& logicalLibrary,
        const std::string& hostName,
        const std::string& vo,
        const std::string& mediaType,
        const std::string& vendor,
        const uint64_t capacityInBytes,
        time_t startTime) {
  throw cta::exception::Exception("In OStoreDB::TapeMountDecisionInfoNoLock::createArchiveMount(): This function should not be called");
}

//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfoNoLock::createRetrieveMount()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::RetrieveMount> OStoreDB::TapeMountDecisionInfoNoLock::createRetrieveMount(const std::string& vid,
        const std::string& tapePool,
        const std::string driveName,
        const std::string& logicalLibrary,
        const std::string& hostName,
        const std::string& vo,
        const std::string& mediaType,
        const std::string& vendor,
        const uint64_t capacityInBytes,
        time_t startTime) {
  throw cta::exception::Exception("In OStoreDB::TapeMountDecisionInfoNoLock::createRetrieveMount(): This function should not be called");
}

//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfoNoLock::~TapeMountDecisionInfoNoLock()
//------------------------------------------------------------------------------
OStoreDB::TapeMountDecisionInfoNoLock::~TapeMountDecisionInfoNoLock() {}

//------------------------------------------------------------------------------
// OStoreDB::queueArchive()
//------------------------------------------------------------------------------
void OStoreDB::queueArchive(const std::string &instanceName, const cta::common::dataStructures::ArchiveRequest &request, 
        const cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId &criteria, log::LogContext &logContext) {
  assertAgentAddressSet();
  cta::utils::Timer timer;
  auto mutexForHelgrind = cta::make_unique<cta::threading::Mutex>();
  cta::threading::MutexLocker mlForHelgrind(*mutexForHelgrind);
  auto * mutexForHelgrindAddr = mutexForHelgrind.release();
  // Construct the archive request object in memory
  auto aReq = cta::make_unique<cta::objectstore::ArchiveRequest> (m_agentReference->nextId("ArchiveRequest"), m_objectStore);
  aReq->initialize();
  // Summarize all as an archiveFile
  cta::common::dataStructures::ArchiveFile aFile;
  aFile.archiveFileID = criteria.fileId;
  aFile.checksumType = request.checksumType;
  aFile.checksumValue = request.checksumValue;
  // TODO: fully fledged archive file should not be the representation of the request.
  aFile.creationTime = std::numeric_limits<decltype(aFile.creationTime)>::min();
  aFile.reconciliationTime = 0;
  aFile.diskFileId = request.diskFileID;
  aFile.diskFileInfo = request.diskFileInfo;
  aFile.diskInstance = instanceName;
  aFile.fileSize = request.fileSize;
  aFile.storageClass = request.storageClass;
  aReq->setArchiveFile(aFile);
  aReq->setMountPolicy(criteria.mountPolicy);
  aReq->setArchiveReportURL(request.archiveReportURL);
  aReq->setArchiveErrorReportURL(request.archiveErrorReportURL);
  aReq->setRequester(request.requester);
  aReq->setSrcURL(request.srcURL);
  aReq->setEntryLog(request.creationLog);
  std::list<cta::objectstore::ArchiveRequest::JobDump> jl;
  for (auto & copy:criteria.copyToPoolMap) {
    const uint32_t hardcodedRetriesWithinMount = 2;
    const uint32_t hardcodedTotalRetries = 2;
    const uint32_t hardcodedReportRetries = 2;
    aReq->addJob(copy.first, copy.second, m_agentReference->getAgentAddress(),
        hardcodedRetriesWithinMount, hardcodedTotalRetries, hardcodedReportRetries);
    jl.push_back(cta::objectstore::ArchiveRequest::JobDump());
    jl.back().copyNb = copy.first;
    jl.back().tapePool = copy.second;
  }
  if (jl.empty()) {
    throw ArchiveRequestHasNoCopies("In OStoreDB::queue: the archive to file request has no copy");
  }
  // We create the object here
  m_agentReference->addToOwnership(aReq->getAddressIfSet(), m_objectStore);
  double agentReferencingTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
  aReq->insert();
  double insertionTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
  // The request is now safe in the object store. We can now return to the caller and fire (and forget) a thread
  // complete the bottom half of it.
  m_taskQueueSize++;
  uint64_t taskQueueSize = m_taskQueueSize;
  // Prepare the logs to avoid multithread access on the object.
  log::ScopedParamContainer params(logContext);
  params.add("jobObject", aReq->getAddressIfSet())
        .add("fileId", aFile.archiveFileID)
        .add("diskInstance", aFile.diskInstance)
        .add("diskFilePath", aFile.diskFileInfo.path)
        .add("diskFileId", aFile.diskFileId)
        .add("agentReferencingTime", agentReferencingTime)
        .add("insertionTime", insertionTime);
  delayIfNecessary(logContext);
  auto * aReqPtr = aReq.release();
  auto * et = new EnqueueingTask([aReqPtr, mutexForHelgrindAddr, this]{
    std::unique_ptr<cta::threading::Mutex> mutexForHelgrind(mutexForHelgrindAddr);
    cta::threading::MutexLocker mlForHelgrind(*mutexForHelgrind);
    std::unique_ptr<cta::objectstore::ArchiveRequest> aReq(aReqPtr);
    // This unique_ptr's destructor will ensure the OStoreDB object is not deleted before the thread exits.
    auto scopedCounterDecrement = [this](void *){ 
      m_taskQueueSize--;
      m_taskPostingSemaphore.release();
    };
    // A bit ugly, but we need a non-null pointer for the "deleter" to be called.
    std::unique_ptr<void, decltype(scopedCounterDecrement)> scopedCounterDecrementerInstance((void *)1, scopedCounterDecrement);
    log::LogContext logContext(m_logger);
    utils::Timer timer;
    ScopedExclusiveLock arl(*aReq);
    double arRelockTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
    double arTotalQueueingTime = 0;
    double arTotalCommitTime = 0;
    double arTotalQueueUnlockTime = 0;
    // We can now enqueue the requests
    std::list<std::string> linkedTapePools;
    std::string currentTapepool;
    try {
      for (auto &j: aReq->dumpJobs()) {
        currentTapepool = j.tapePool;
        // The shared lock will be released automatically at the end of this scope.
        // The queueing implicitly sets the job owner as the queue (as should be). The queue should not
        // be unlocked before we commit the archive request (otherwise, the pointer could be seen as
        // stale and the job would be dereferenced from the queue.
        auto shareLock = ostoredb::MemArchiveQueue::sharedAddToQueue(j, j.tapePool, *aReq, *this, logContext);
        double qTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
        arTotalQueueingTime += qTime;
        aReq->commit();
        double cTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
        arTotalCommitTime += cTime;
        // Now we can let go off the queue.
        shareLock.reset();
        double qUnlockTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
        arTotalQueueUnlockTime += qUnlockTime;
        linkedTapePools.push_back(j.owner);
        log::ScopedParamContainer params(logContext);
        params.add("tapePool", j.tapePool)
              .add("queueObject", j.owner)
              .add("jobObject", aReq->getAddressIfSet())
              .add("queueingTime", qTime)
              .add("commitTime", cTime)
              .add("queueUnlockTime", qUnlockTime);
        logContext.log(log::INFO, "In OStoreDB::queueArchive_bottomHalf(): added job to queue.");
      }
    } catch (NoSuchArchiveQueue &ex) {
      // Unlink the request from already connected tape pools
      for (auto tpa=linkedTapePools.begin(); tpa!=linkedTapePools.end(); tpa++) {
        objectstore::ArchiveQueue aq(*tpa, m_objectStore);
        ScopedExclusiveLock aql(aq);
        aq.fetch();
        aq.removeJobsAndCommit({aReq->getAddressIfSet()});
      }
      aReq->remove();
      log::ScopedParamContainer params(logContext);
      params.add("tapePool", currentTapepool)
            .add("archiveRequestObject", aReq->getAddressIfSet())
            .add("exceptionMessage", ex.getMessageValue())
            .add("jobObject", aReq->getAddressIfSet());
      logContext.log(log::ERR, "In OStoreDB::queueArchive_bottomHalf(): failed to enqueue job");
      return;
    }
    // The request is now fully set.
    auto archiveFile = aReq->getArchiveFile();
    double arOwnerResetTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
    arl.release();
    double arLockRelease = timer.secs(cta::utils::Timer::reset_t::resetCounter);
    // And remove reference from the agent
    m_agentReference->removeFromOwnership(aReq->getAddressIfSet(), m_objectStore);
    double agOwnershipResetTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
    log::ScopedParamContainer params(logContext);
    params.add("jobObject", aReq->getAddressIfSet())
          .add("fileId", archiveFile.archiveFileID)
          .add("diskInstance", archiveFile.diskInstance)
          .add("diskFilePath", archiveFile.diskFileInfo.path)
          .add("diskFileId", archiveFile.diskFileId)
          .add("creationAndRelockTime", arRelockTime)
          .add("totalQueueingTime", arTotalQueueingTime)
          .add("totalCommitTime", arTotalCommitTime)
          .add("totalQueueUnlockTime", arTotalQueueUnlockTime)
          .add("ownerResetTime", arOwnerResetTime)
          .add("lockReleaseTime", arLockRelease)
          .add("agentOwnershipResetTime", agOwnershipResetTime)
          .add("totalTime", arRelockTime + arTotalQueueingTime + arTotalCommitTime
             + arTotalQueueUnlockTime + arOwnerResetTime + arLockRelease 
             + agOwnershipResetTime);
    logContext.log(log::INFO, "In OStoreDB::queueArchive_bottomHalf(): Finished enqueueing request.");
  });
  ANNOTATE_HAPPENS_BEFORE(et);
  mlForHelgrind.unlock();
  m_enqueueingTasksQueue.push(et);
  double taskPostingTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
  params.add("taskPostingTime", taskPostingTime)
        .add("taskQueueSize", taskQueueSize)
        .add("totalTime", agentReferencingTime + insertionTime + taskPostingTime);
  logContext.log(log::INFO, "In OStoreDB::queueArchive(): recorded request for queueing (enqueueing posted to thread pool).");  
}

void OStoreDB::queueArchiveForRepack(std::unique_ptr<cta::objectstore::ArchiveRequest> request, log::LogContext& logContext){
  assertAgentAddressSet();
  auto mutexForHelgrind = cta::make_unique<cta::threading::Mutex>();
  cta::threading::MutexLocker mlForHelgrind(*mutexForHelgrind);
  auto * mutexForHelgrindAddr = mutexForHelgrind.release();
  std::unique_ptr<cta::objectstore::ArchiveRequest> aReqUniqPtr(request.release());
  objectstore::ScopedExclusiveLock rReqL(*aReqUniqPtr);
  aReqUniqPtr->fetch();
  uint64_t taskQueueSize = m_taskQueueSize;
  // We create the object here
  //m_agentReference->addToOwnership(aReqUniqPtr->getAddressIfSet(), m_objectStore);
  utils::Timer timer;
  double agentReferencingTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
  // Prepare the logs to avoid multithread access on the object.
  log::ScopedParamContainer params(logContext);
  cta::common::dataStructures::ArchiveFile aFile = aReqUniqPtr->getArchiveFile();
  params.add("jobObject", aReqUniqPtr->getAddressIfSet())
        .add("fileId", aFile.archiveFileID)
        .add("diskInstance", aFile.diskInstance)
        .add("diskFilePath", aFile.diskFileInfo.path)
        .add("diskFileId", aFile.diskFileId)
        .add("agentReferencingTime", agentReferencingTime);
  delayIfNecessary(logContext);
  cta::objectstore::ArchiveRequest *aReqPtr = aReqUniqPtr.release();
  
  m_taskQueueSize++;

  auto * et = new EnqueueingTask([aReqPtr, mutexForHelgrindAddr, this]{
    std::unique_ptr<cta::threading::Mutex> mutexForHelgrind(mutexForHelgrindAddr);
    std::unique_ptr<cta::objectstore::ArchiveRequest> aReq(aReqPtr);
    // This unique_ptr's destructor will ensure the OStoreDB object is not deleted before the thread exits.
    auto scopedCounterDecrement = [this](void *){ 
      m_taskQueueSize--;
      m_taskPostingSemaphore.release();
    };
    // A bit ugly, but we need a non-null pointer for the "deleter" to be called at the end of the thread execution
    std::unique_ptr<void, decltype(scopedCounterDecrement)> scopedCounterDecrementerInstance((void *)1, scopedCounterDecrement);
    
    log::LogContext logContext(m_logger);
    utils::Timer timer;
    ScopedExclusiveLock arl(*aReq);
    aReq->fetch();
    timer.secs(cta::utils::Timer::reset_t::resetCounter);
    double arTotalQueueUnlockTime = 0;
    double arTotalQueueingTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
    // We can now enqueue the requests
    std::list<std::string> linkedTapePools;
    std::string currentTapepool;
    try {
      for (auto &j: aReq->dumpJobs()) {
        //Queue each job into the ArchiveQueue
        double qTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
        currentTapepool = j.tapePool;
        linkedTapePools.push_back(j.owner);
        auto shareLock = ostoredb::MemArchiveQueue::sharedAddToQueue(j, j.tapePool, *aReq, *this, logContext);
        arTotalQueueingTime += qTime;
        aReq->commit();
        // Now we can let go off the queue.
        shareLock.reset();
        double qUnlockTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
        arTotalQueueUnlockTime += qUnlockTime;
        linkedTapePools.push_back(j.owner);
        log::ScopedParamContainer params(logContext);
        params.add("tapePool", j.tapePool)
              .add("queueObject", j.owner)
              .add("jobObject", aReq->getAddressIfSet())
              .add("queueingTime", qTime)
              .add("queueUnlockTime", qUnlockTime);
        logContext.log(log::INFO, "In OStoreDB::queueArchiveForRepack(): added job to queue.");
      }
    } catch (NoSuchArchiveQueue &ex) {
      // Unlink the request from already connected tape pools
      for (auto tpa=linkedTapePools.begin(); tpa!=linkedTapePools.end(); tpa++) {
        objectstore::ArchiveQueue aq(*tpa, m_objectStore);
        ScopedExclusiveLock aql(aq);
        aq.fetch();
        aq.removeJobsAndCommit({aReq->getAddressIfSet()});
      }
      aReq->remove();
      log::ScopedParamContainer params(logContext);
      params.add("tapePool", currentTapepool)
            .add("archiveRequestObject", aReq->getAddressIfSet())
            .add("exceptionMessage", ex.getMessageValue())
            .add("jobObject", aReq->getAddressIfSet());
      logContext.log(log::ERR, "In OStoreDB::queueArchiveForRepack(): failed to enqueue job");
      return;
    }
    // The request is now fully set.
    double arOwnerResetTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
    
    double arLockRelease = timer.secs(cta::utils::Timer::reset_t::resetCounter);
    
    // And remove reference from the agent
    m_agentReference->removeFromOwnership(aReq->getAddressIfSet(), m_objectStore);
    double agOwnershipResetTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);

    auto archiveFile = aReq->getArchiveFile();
    log::ScopedParamContainer params(logContext);
    params.add("jobObject", aReq->getAddressIfSet())
          .add("fileId", archiveFile.archiveFileID)
          .add("diskInstance", archiveFile.diskInstance)
          .add("diskFilePath", archiveFile.diskFileInfo.path)
          .add("diskFileId", archiveFile.diskFileId)
          .add("totalQueueingTime", arTotalQueueingTime)
          .add("totalQueueUnlockTime", arTotalQueueUnlockTime)
          .add("ownerResetTime", arOwnerResetTime)
          .add("lockReleaseTime", arLockRelease)
          .add("agentOwnershipResetTime", agOwnershipResetTime)
          .add("totalTime", arTotalQueueingTime
             + arTotalQueueUnlockTime + arOwnerResetTime + arLockRelease 
             + agOwnershipResetTime);
    
    logContext.log(log::INFO, "In OStoreDB::queueArchiveForRepack(): Finished enqueueing request.");
  });
  ANNOTATE_HAPPENS_BEFORE(et);
  mlForHelgrind.unlock();
  rReqL.release();
  m_enqueueingTasksQueue.push(et);
  //TODO Time measurement
  double taskPostingTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
  params.add("taskPostingTime", taskPostingTime)
        .add("taskQueueSize", taskQueueSize)
        .add("totalTime", agentReferencingTime + taskPostingTime);
  logContext.log(log::INFO, "In OStoreDB::queueArchiveForRepack(): recorded request for queueing (enqueueing posted to thread pool).");
}

//------------------------------------------------------------------------------
// OStoreDB::getArchiveJobs()
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveJob>
  OStoreDB::getArchiveJobs(const std::string &tapePoolName) const
{
  std::list<cta::common::dataStructures::ArchiveJob> ret;

  for(ArchiveQueueItor_t q_it(m_objectStore, JobQueueType::JobsToTransfer, tapePoolName); !q_it.end() ; ++q_it) {
    ret.push_back(*q_it);
  }

  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getArchiveJobs()
//------------------------------------------------------------------------------
std::map<std::string, std::list<common::dataStructures::ArchiveJob>>
   OStoreDB::getArchiveJobs() const
{
  std::map<std::string, std::list<common::dataStructures::ArchiveJob>> ret;

  for(ArchiveQueueItor_t q_it(m_objectStore, JobQueueType::JobsToTransfer); !q_it.end(); ++q_it) {
    ret[q_it.qid()].push_back(*q_it);
  }

  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getArchiveJobItor()
//------------------------------------------------------------------------------
OStoreDB::ArchiveQueueItor_t OStoreDB::getArchiveJobItor(const std::string &tapePoolName, JobQueueType queueType) const
{
  return ArchiveQueueItor_t(m_objectStore, queueType, tapePoolName);
}
//------------------------------------------------------------------------------
// OStoreDB::getArchiveJobItorPtr()
//------------------------------------------------------------------------------
OStoreDB::ArchiveQueueItor_t* OStoreDB::getArchiveJobItorPtr(const std::string &tapePoolName, JobQueueType queueType) const
{
  return new ArchiveQueueItor_t(m_objectStore, queueType, tapePoolName);
}

//------------------------------------------------------------------------------
// OStoreDB::getNextArchiveJobsToReportBatch()
//------------------------------------------------------------------------------
std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > OStoreDB::getNextArchiveJobsToReportBatch(
    uint64_t filesRequested, log::LogContext& logContext) {
  typedef objectstore::ContainerAlgorithms<ArchiveQueue,ArchiveQueueToReport> AQTRAlgo;
  AQTRAlgo aqtrAlgo(m_objectStore, *m_agentReference);
  // Decide from which queue we are going to pop.
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  auto queueList = re.dumpArchiveQueues(JobQueueType::JobsToReportToUser);
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > ret;
  if (queueList.empty()) return ret;
  // Try to get jobs from the first non-empty queue.
  AQTRAlgo::PopCriteria criteria;
  criteria.files = filesRequested;
  AQTRAlgo::PoppedElementsBatch jobs;
  for (auto & q: queueList) {
    jobs = aqtrAlgo.popNextBatch(q.tapePool, criteria, logContext);
    if (!jobs.elements.empty()) break;
  }
  for (auto & j: jobs.elements) {
    std::unique_ptr<OStoreDB::ArchiveJob> aj(new OStoreDB::ArchiveJob(j.archiveRequest->getAddressIfSet(), *this));
    aj->tapeFile.copyNb = j.copyNb;
    aj->archiveFile = j.archiveFile;
    aj->srcURL = j.srcURL;
    aj->archiveReportURL = j.archiveReportURL;
    aj->errorReportURL = j.errorReportURL;
    aj->latestError = j.latestError;
    aj->reportType = j.reportType;
    // We leave the tape file not set. It does not exist in all cases (not in case of failure).
    aj->m_jobOwned = true;
    aj->m_mountId = 0;
    ret.emplace_back(std::move(aj));
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getArchiveJobsFailedSummary
//------------------------------------------------------------------------------
SchedulerDatabase::JobsFailedSummary OStoreDB::getArchiveJobsFailedSummary(log::LogContext &logContext) {
  RootEntry re(m_objectStore);
  re.fetchNoLock();

  SchedulerDatabase::JobsFailedSummary ret;
  auto queueList = re.dumpArchiveQueues(JobQueueType::FailedJobs);
  for(auto &aj : queueList) {
    ArchiveQueue aq(aj.address, m_objectStore);
    try {
      aq.fetchNoLock();
    } catch (cta::exception::Exception &ex) {
      log::ScopedParamContainer params (logContext);
      params.add("queueObject", aj.address)
            .add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::WARNING, "In OStoreDB::getArchiveJobsFailedSummary(): failed to lock/fetch an archive queue.");
      continue;
    }
    auto summary = aq.getCandidateSummary();
    ret.totalFiles += summary.candidateFiles;
    ret.totalBytes += summary.candidateBytes;
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::setArchiveJobBatchReported()
//------------------------------------------------------------------------------
void OStoreDB::setArchiveJobBatchReported(std::list<cta::SchedulerDatabase::ArchiveJob*> & jobsBatch,
  log::TimingList & timingList, utils::Timer & t, log::LogContext& lc)
{
  // We can have a mixture of failed and successful jobs, so we will sort them before batch queue/discarding them.
  // First, sort the jobs. Done jobs get deleted (no need to sort further) and failed jobs go to their per-VID queues/containers.
  // Status gets updated on the fly on the latter case.
  std::list<ArchiveJob *> completeJobsToDelete;
  struct FailedJobToQueue {
    ArchiveJob * job;
  };
  // Sort jobs to be updated.
  std::map<std::string, std::list<FailedJobToQueue>> failedQueues;
  for (auto &j: jobsBatch) {
    switch (j->reportType) {
    case SchedulerDatabase::ArchiveJob::ReportType::CompletionReport:
      completeJobsToDelete.push_back(castFromSchedDBJob(j));
      break;
    case SchedulerDatabase::ArchiveJob::ReportType::FailureReport:
      failedQueues[j->tapeFile.vid].push_back(FailedJobToQueue());
      failedQueues[j->tapeFile.vid].back().job = castFromSchedDBJob(j);
      break;
    default:
      {
        log::ScopedParamContainer params(lc);
        params.add("fileId", j->archiveFile.archiveFileID)
              .add("objectAddress", castFromSchedDBJob(j)->m_archiveRequest.getAddressIfSet());
        lc.log(log::ERR, "In OStoreDB::setArchiveJobBatchReported(): unexpected job status. Leaving the job as-is.");
      }
    }
  }
  if (completeJobsToDelete.size()) {
    std::list<std::string> jobsToUnown;
    // Launch deletion.
    for (auto &j: completeJobsToDelete) {
      j->asyncDeleteRequest();
    }
    timingList.insertAndReset("deleteLaunchTime", t);
    for (auto &j: completeJobsToDelete) {
      try {
        j->waitAsyncDelete();
        log::ScopedParamContainer params(lc);
        params.add("fileId", j->archiveFile.archiveFileID)
              .add("objectAddress", j->m_archiveRequest.getAddressIfSet());
        lc.log(log::INFO, "In OStoreDB::setArchiveJobBatchReported(): deleted ArchiveRequest after completion and reporting.");
        // We remove the job from ownership.
        jobsToUnown.push_back(j->m_archiveRequest.getAddressIfSet());
      } catch (cta::exception::Exception & ex) {
        log::ScopedParamContainer params(lc);
        params.add("fileId", j->archiveFile.archiveFileID)
              .add("objectAddress", j->m_archiveRequest.getAddressIfSet())
              .add("exceptionMSG", ex.getMessageValue());
        lc.log(log::ERR, "In OStoreDB::setArchiveJobBatchReported(): failed to delete ArchiveRequest after completion and reporting.");
        // We have to remove the job from ownership.
        jobsToUnown.push_back(j->m_archiveRequest.getAddressIfSet());
      }
    } 
    timingList.insertAndReset("deletionCompletionTime", t);
    m_agentReference->removeBatchFromOwnership(jobsToUnown, m_objectStore);
    timingList.insertAndReset("unownDeletedJobsTime", t);
  }
  for (auto & queue: failedQueues) {
    // Put the jobs in the failed queue
    typedef objectstore::ContainerAlgorithms<ArchiveQueue,ArchiveQueueFailed> CaAQF;
    CaAQF caAQF(m_objectStore, *m_agentReference);
    // TODOTODO: also switch status in one step.
    CaAQF::InsertedElement::list insertedElements;
    for (auto &j: queue.second) {
      insertedElements.emplace_back(CaAQF::InsertedElement{&j.job->m_archiveRequest, j.job->tapeFile.copyNb, j.job->archiveFile,
          cta::nullopt, serializers::ArchiveJobStatus::AJS_Failed});
    }
    try {
      caAQF.referenceAndSwitchOwnership(queue.first, m_agentReference->getAgentAddress(), insertedElements, lc);
    } catch (exception::Exception & ex) {
      log::ScopedParamContainer params(lc);
      
    }
    log::TimingList tl;
    tl.insertAndReset("queueAndSwitchStateTime", t);
    timingList += tl;
  }
}

//------------------------------------------------------------------------------
// OStoreDB::setRetrieveJobBatchReported()
//------------------------------------------------------------------------------
void OStoreDB::setRetrieveJobBatchReported(std::list<cta::SchedulerDatabase::RetrieveJob*> & jobsBatch,
  log::TimingList & timingList, utils::Timer & t, log::LogContext & lc)
{
  struct FailedJobToQueue {
    RetrieveJob * job;
  };

  // Sort jobs to be updated
  std::map<std::string, std::list<FailedJobToQueue>> failedQueues;
  for(auto &j : jobsBatch) {
    switch(j->reportType) {
      case SchedulerDatabase::RetrieveJob::ReportType::FailureReport: {
        auto &vid = j->archiveFile.tapeFiles.at(j->selectedCopyNb).vid;
        failedQueues[vid].push_back(FailedJobToQueue{ castFromSchedDBJob(j) });
        break;
      }
      default: {
        log::ScopedParamContainer params(lc);
        params.add("fileId", j->archiveFile.archiveFileID)
              .add("objectAddress", castFromSchedDBJob(j)->m_retrieveRequest.getAddressIfSet());
        lc.log(log::ERR, "In OStoreDB::setRetrieveJobBatchReported(): unexpected job status. Leaving the job as-is.");
      }
    }
  }

  // Put the failed jobs in the failed queue
  for(auto &queue : failedQueues) {
    typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueFailed> CaRQF;
    CaRQF caRQF(m_objectStore, *m_agentReference);
    CaRQF::InsertedElement::list insertedElements;
    for(auto &j : queue.second) {
      auto tf_it = j.job->archiveFile.tapeFiles.begin();
      while(tf_it != j.job->archiveFile.tapeFiles.end()) {
        if(queue.first == tf_it->second.vid) break;
      }
      if(tf_it == j.job->archiveFile.tapeFiles.end()) throw cta::exception::Exception(
        "In OStoreDB::setRetrieveJobBatchReported(): tape copy not found"
      );
      insertedElements.emplace_back(CaRQF::InsertedElement{
        &j.job->m_retrieveRequest, tf_it->second.copyNb, tf_it->second.fSeq, tf_it->second.compressedSize,
        common::dataStructures::MountPolicy(), serializers::RetrieveJobStatus::RJS_Failed
      });
    }
    try {
      caRQF.referenceAndSwitchOwnership(queue.first, m_agentReference->getAgentAddress(), insertedElements, lc);
    } catch(exception::Exception &ex) {
      log::ScopedParamContainer params(lc);
    }
    log::TimingList tl;
    tl.insertAndReset("queueAndSwitchStateTime", t);
    timingList += tl;
  }
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveQueueStatistics()
//------------------------------------------------------------------------------
std::list<SchedulerDatabase::RetrieveQueueStatistics> OStoreDB::getRetrieveQueueStatistics(
  const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria,
  const std::set<std::string> & vidsToConsider) {
  return Helpers::getRetrieveQueueStatistics(criteria, vidsToConsider, m_objectStore);
}

//------------------------------------------------------------------------------
// OStoreDB::queueRetrieve()
//------------------------------------------------------------------------------
std::string OStoreDB::queueRetrieve(const cta::common::dataStructures::RetrieveRequest& rqst,
  const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria, log::LogContext &logContext) {
  assertAgentAddressSet();
  auto mutexForHelgrind = cta::make_unique<cta::threading::Mutex>();
  cta::threading::MutexLocker mlForHelgrind(*mutexForHelgrind);
  auto *mutexForHelgrindAddr = mutexForHelgrind.release();
  cta::utils::Timer timer;
  // Get the best vid from the cache
  std::set<std::string> candidateVids;
  for (auto & tf:criteria.archiveFile.tapeFiles) candidateVids.insert(tf.second.vid);
  std::string bestVid=Helpers::selectBestRetrieveQueue(candidateVids, m_catalogue, m_objectStore);
  // Check that the requested retrieve job (for the provided vid) exists, and record the copynb.
  uint64_t bestCopyNb;
  for (auto & tf: criteria.archiveFile.tapeFiles) {
    if (tf.second.vid == bestVid) {
      bestCopyNb = tf.second.copyNb;
      goto vidFound;
    }
  }
  {
    std::stringstream err;
    err << "In OStoreDB::queueRetrieve(): no tape file for requested vid. archiveId=" << criteria.archiveFile.archiveFileID
        << " vid=" << bestVid;
    throw RetrieveRequestHasNoCopies(err.str());
  }
  vidFound:
  // In order to post the job, construct it first in memory.
  auto rReq = cta::make_unique<objectstore::RetrieveRequest> (m_agentReference->nextId("RetrieveRequest"), m_objectStore);
  rReq->initialize();
  rReq->setSchedulerRequest(rqst);
  rReq->setRetrieveFileQueueCriteria(criteria);
  rReq->setTapePool(rqst.tapePool);
  // Find the job corresponding to the vid (and check we indeed have one).
  auto jobs = rReq->getJobs();
  objectstore::RetrieveRequest::JobDump job;
  for (auto & j:jobs) {
    if (j.copyNb == bestCopyNb) {
      job = j;
      goto jobFound;
    }
  }
  {
    std::stringstream err;
    err << "In OStoreDB::queueRetrieve(): no job for requested copyNb. archiveId=" << criteria.archiveFile.archiveFileID
        << " vid=" << bestVid << " copyNb=" << bestCopyNb;
    throw RetrieveRequestHasNoCopies(err.str());
  }
  jobFound:
  {
    // We are ready to enqueue the request. Let's make the data safe and do the rest behind the scenes.
    // Reference the request in the process's agent.
    double vidSelectionTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
    m_agentReference->addToOwnership(rReq->getAddressIfSet(), m_objectStore);
    double agentReferencingTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
    rReq->setOwner(m_agentReference->getAgentAddress());
    // "Select" an arbitrary copy number. This is needed to serialize the object.
    rReq->setActiveCopyNumber(criteria.archiveFile.tapeFiles.begin()->second.copyNb);
    rReq->setIsRepack(rqst.isRepack);
    rReq->setTapePool(rqst.tapePool);
    rReq->insert();
    double insertionTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
    m_taskQueueSize++;
    uint64_t taskQueueSize = m_taskQueueSize;
    // Prepare the logs to avoid multithread access on the object.
    log::ScopedParamContainer params(logContext);
    params.add("tapeVid", bestVid)
          .add("jobObject", rReq->getAddressIfSet())
          .add("fileId", rReq->getArchiveFile().archiveFileID)
          .add("diskInstance", rReq->getArchiveFile().diskInstance)
          .add("diskFilePath", rReq->getArchiveFile().diskFileInfo.path)
          .add("diskFileId", rReq->getArchiveFile().diskFileId)
          .add("vidSelectionTime", vidSelectionTime)
          .add("agentReferencingTime", agentReferencingTime)
          .add("insertionTime", insertionTime);
    delayIfNecessary(logContext);
    auto rReqPtr = rReq.release();
    auto * et = new EnqueueingTask([rReqPtr, job, bestVid, mutexForHelgrindAddr, this]{
      std::unique_ptr<cta::threading::Mutex> mutexForHelgrind(mutexForHelgrindAddr);
      std::unique_ptr<objectstore::RetrieveRequest> rReq(rReqPtr);
      cta::threading::MutexLocker mlForHelgrind(*mutexForHelgrind);
      // This unique_ptr's destructor will ensure the OStoreDB object is not deleted before the thread exits.
      auto scopedCounterDecrement = [this](void *){ 
        m_taskQueueSize--;
        m_taskPostingSemaphore.release();
      };
      // A bit ugly, but we need a non-null pointer for the "deleter" to be called.
      std::unique_ptr<void, decltype(scopedCounterDecrement)> scopedCounterDecrementerInstance((void *)1, scopedCounterDecrement);
      log::LogContext logContext(m_logger);
      utils::Timer timer;
      // Add the request to the queue (with a shared access).
      auto nonConstJob = job;
      objectstore::ScopedExclusiveLock rReqL(*rReq);
      double rLockTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
      rReq->fetch();
      auto sharedLock = ostoredb::MemRetrieveQueue::sharedAddToQueue(nonConstJob, bestVid, *rReq, *this, logContext);
      double qTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
      // The object ownership was set in SharedAdd.
      // We need to extract the owner before inserting. After, we would need to hold a lock.
      auto owner = rReq->getOwner();
      rReq->commit();
      double cTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
      // The lock on the queue is released here (has to be after the request commit for consistency.
      sharedLock.reset();
      double qUnlockTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
      rReqL.release();
      double rUnlockTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
      // And remove reference from the agent
      m_agentReference->removeFromOwnership(rReq->getAddressIfSet(), m_objectStore);
      double agOwnershipResetTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
      log::ScopedParamContainer params(logContext);
      params.add("tapeVid", bestVid)
            .add("queueObject", owner)
            .add("jobObject", rReq->getAddressIfSet())
            .add("fileId", rReq->getArchiveFile().archiveFileID)
            .add("diskInstance", rReq->getArchiveFile().diskInstance)
            .add("diskFilePath", rReq->getArchiveFile().diskFileInfo.path)
            .add("diskFileId", rReq->getArchiveFile().diskFileId)
            .add("requestLockTime", rLockTime)
            .add("queueingTime", qTime)
            .add("commitTime", cTime)
            .add("queueUnlockTime", qUnlockTime)
            .add("requestUnlockTime", rUnlockTime)
            .add("agentOwnershipResetTime", agOwnershipResetTime)
            .add("totalTime", rLockTime + qTime + cTime + qUnlockTime 
                + rUnlockTime + agOwnershipResetTime);
      logContext.log(log::INFO, "In OStoreDB::queueRetrieve_bottomHalf(): added job to queue (enqueueing finished).");
    });
    ANNOTATE_HAPPENS_BEFORE(et);
    mlForHelgrind.unlock();
    m_enqueueingTasksQueue.push(et);
    double taskPostingTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
    params.add("taskPostingTime", taskPostingTime)
          .add("taskQueueSize", taskQueueSize)
          .add("totalTime", vidSelectionTime + agentReferencingTime + insertionTime + taskPostingTime);
    logContext.log(log::INFO, "In OStoreDB::queueRetrieve(): recorded request for queueing (enqueueing posted to thread pool).");  
  }
  return bestVid;
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveRequestsByVid()
//------------------------------------------------------------------------------
std::list<RetrieveRequestDump> OStoreDB::getRetrieveRequestsByVid(const std::string& vid) const {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveRequestsByRequester()
//------------------------------------------------------------------------------
std::list<RetrieveRequestDump> OStoreDB::getRetrieveRequestsByRequester(const std::string& vid) const {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveRequests()
//------------------------------------------------------------------------------
std::map<std::string, std::list<RetrieveRequestDump> > OStoreDB::getRetrieveRequests() const {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
//  std::map<cta::Tape, std::list<RetrieveRequestDump> > ret;
//  // Get list of tape pools and then tapes
//  objectstore::RootEntry re(m_objectStore);
//  objectstore::ScopedSharedLock rel(re);
//  re.fetch();
//  auto tpl=re.dumpTapePools();
//  rel.release();
//  for (auto tpp = tpl.begin(); tpp != tpl.end(); tpp++) {
//    // Get the list of tapes for the tape pool
//    objectstore::TapePool tp(tpp->address, m_objectStore);
//    objectstore::ScopedSharedLock tplock(tp);
//    tp.fetch();
//    auto tl = tp.dumpTapes();
//    for (auto tptr = tl.begin(); tptr!= tl.end(); tptr++) {
//      // Get the list of retrieve requests for the tape.
//      objectstore::Tape t(tptr->address, m_objectStore);
//      objectstore::ScopedSharedLock tlock(t);
//      t.fetch();
//      auto jobs = t.dumpAndFetchRetrieveRequests();
//      // If the list is not empty, add to the map.
//      if (jobs.size()) {
//        cta::Tape tkey;
//        // TODO tkey.capacityInBytes;
//        tkey.creationLog = t.getCreationLog();
//        // TODO tkey.dataOnTapeInBytes;
//        tkey.logicalLibraryName = t.getLogicalLibrary();
//        tkey.nbFiles = t.getLastFseq();
//        // TODO tkey.status
//        tkey.tapePoolName = tp.getName();
//        tkey.vid = t.getVid();
//        ret[tkey] = std::move(jobs);
//      }
//    }
//  }
//  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::deleteRetrieveRequest()
//------------------------------------------------------------------------------
void OStoreDB::deleteRetrieveRequest(const common::dataStructures::SecurityIdentity& requester, 
  const std::string& remoteFile) {
  throw exception::Exception("Not Implemented");
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveJobs()
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::RetrieveJob>
OStoreDB::getRetrieveJobs(const std::string &vid) const
{
  std::list<common::dataStructures::RetrieveJob> ret;

  for(RetrieveQueueItor_t q_it(m_objectStore, JobQueueType::JobsToTransfer, vid); !q_it.end(); ++q_it) {
    ret.push_back(*q_it);
  }

  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveJobs()
//------------------------------------------------------------------------------
std::map<std::string, std::list<common::dataStructures::RetrieveJob>>
OStoreDB::getRetrieveJobs() const
{
  std::map<std::string, std::list<common::dataStructures::RetrieveJob>> ret;

  for(RetrieveQueueItor_t q_it(m_objectStore, JobQueueType::JobsToTransfer); !q_it.end(); ++q_it) {
    ret[q_it.qid()].push_back(*q_it);
  }

  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveJobItor()
//------------------------------------------------------------------------------
OStoreDB::RetrieveQueueItor_t OStoreDB::getRetrieveJobItor(const std::string &vid, JobQueueType queueType) const
{
  return RetrieveQueueItor_t(m_objectStore, queueType, vid);
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveJobItorPtr()
//------------------------------------------------------------------------------
OStoreDB::RetrieveQueueItor_t* OStoreDB::getRetrieveJobItorPtr(const std::string &vid, JobQueueType queueType) const
{
  return new RetrieveQueueItor_t(m_objectStore, queueType, vid);
}

//------------------------------------------------------------------------------
// OStoreDB::queueRepack()
//------------------------------------------------------------------------------
void OStoreDB::queueRepack(const std::string& vid, const std::string& bufferURL,
    common::dataStructures::RepackInfo::Type repackType, log::LogContext & lc) {
  // Prepare the repack request object in memory.
  assertAgentAddressSet();
  cta::utils::Timer t;
  auto rr=cta::make_unique<cta::objectstore::RepackRequest>(m_agentReference->nextId("RepackRequest"), m_objectStore);
  rr->initialize();
  // We need to own the request until it is queued in the the pending queue.
  rr->setOwner(m_agentReference->getAgentAddress());
  rr->setVid(vid);
  rr->setType(repackType);
  // Try to reference the object in the index (will fail if there is already a request with this VID.
  try {
    Helpers::registerRepackRequestToIndex(vid, rr->getAddressIfSet(), *m_agentReference, m_objectStore, lc);
  } catch (objectstore::RepackIndex::VidAlreadyRegistered &) {
    throw exception::UserError("A repack request already exists for this VID.");
  }
  // We're good to go to create the object. We need to own it.
  m_agentReference->addToOwnership(rr->getAddressIfSet(), m_objectStore);
  rr->insert();
  // If latency needs to the improved, the next steps could be deferred like they are for archive and retrieve requests.
  typedef objectstore::ContainerAlgorithms<RepackQueue, RepackQueuePending> RQPAlgo;
  {
    RQPAlgo::InsertedElement::list elements;
    elements.push_back(RQPAlgo::InsertedElement());
    elements.back().repackRequest=std::move(rr);
    RQPAlgo rqpAlgo(m_objectStore, *m_agentReference);
    rqpAlgo.referenceAndSwitchOwnership(nullopt, m_agentReference->getAgentAddress(), elements, lc);
  }
}

//------------------------------------------------------------------------------
// OStoreDB::getRepackInfo()
//------------------------------------------------------------------------------
std::list<common::dataStructures::RepackInfo> OStoreDB::getRepackInfo() {
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  RepackIndex ri(m_objectStore);
  std::list<common::dataStructures::RepackInfo> ret;
  // First, try to get the address of of the repack index lockfree.
  try {
    ri.setAddress(re.getRepackIndexAddress());
  } catch (RootEntry::NotAllocated &) {
    return ret;
  }
  ri.fetchNoLock();
  auto rrAddresses = ri.getRepackRequestsAddresses();
  for (auto & rra: rrAddresses) {
    try {
      objectstore::RepackRequest rr(rra.repackRequestAddress, m_objectStore);
      rr.fetchNoLock();
      ret.push_back(rr.getInfo());
    } catch (cta::exception::Exception &) {}
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getRepackInfo()
//------------------------------------------------------------------------------
common::dataStructures::RepackInfo OStoreDB::getRepackInfo(const std::string& vid) {
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  RepackIndex ri(m_objectStore);
  // First, try to get the address of of the repack index lockfree.
  try {
    ri.setAddress(re.getRepackIndexAddress());
  } catch (RootEntry::NotAllocated &) {
    throw NoSuchRepackRequest("In OStoreDB::getRepackInfo(): No repack request for this VID (index not present).");
  }
  ri.fetchNoLock();
  auto rrAddresses = ri.getRepackRequestsAddresses();
  for (auto & rra: rrAddresses) {
    if (rra.vid == vid) {
      try {
        objectstore::RepackRequest rr(rra.repackRequestAddress, m_objectStore);
        rr.fetchNoLock();
        if (rr.getInfo().vid != vid)
          throw exception::Exception("In OStoreDB::getRepackInfo(): unexpected vid when reading request");
        return rr.getInfo();  
      } catch (cta::exception::Exception &) {}
    }
  }
  throw NoSuchRepackRequest("In OStoreDB::getRepackInfo(): No repack request for this VID.");
}

//------------------------------------------------------------------------------
// OStoreDB::RepackRequestPromotionStatistics::RepackRequestPromotionStatistics()
//------------------------------------------------------------------------------
OStoreDB::RepackRequestPromotionStatistics::RepackRequestPromotionStatistics(
  objectstore::Backend& backend, objectstore::AgentReference &agentReference):
  m_backend(backend), m_agentReference(agentReference),
  m_pendingRepackRequestQueue(backend) {}

//------------------------------------------------------------------------------
// OStoreDB::RepackRequestPromotionStatistics::promotePendingRequestsForExpansion()
//------------------------------------------------------------------------------
auto OStoreDB::RepackRequestPromotionStatistics::promotePendingRequestsForExpansion(size_t requestCount,
        log::LogContext &lc) -> PromotionToToExpandResult {
  // Check we still hold the lock
  if (!m_lockOnPendingRepackRequestsQueue.isLocked())
    throw SchedulingLockNotHeld("In RepackRequestPromotionStatistics::promotePendingRequestsForExpansion(): lock not held anymore.");
  // We have a write lock on the repack queue. We will pop the requested amount of requests.
  PromotionToToExpandResult ret;
  typedef common::dataStructures::RepackInfo::Status Status; 
  ret.pendingBefore = at(Status::Pending);
  ret.toEnpandBefore = at(Status::ToExpand);
  typedef objectstore::ContainerAlgorithms<RepackQueue, RepackQueuePending> RQPAlgo;
  RQPAlgo::PoppedElementsBatch poppedElements;
  {
    RQPAlgo rqpAlgo(m_backend, m_agentReference);
    objectstore::ContainerTraits<RepackQueue, RepackQueuePending>::PopCriteria criteria;
    criteria.requests = requestCount;
    cta::optional<serializers::RepackRequestStatus> newStatus(serializers::RepackRequestStatus::RRS_ToExpand);
    poppedElements = rqpAlgo.popNextBatchFromContainerAndSwitchStatus(
      m_pendingRepackRequestQueue, m_lockOnPendingRepackRequestsQueue, 
      criteria, newStatus, lc);
    // We now switched the status of the requests. The state change is permanent and visible. The lock was released by
    // the previous call.
  }
  // And we will push the requests to the TeExpand queue.
  typedef objectstore::ContainerAlgorithms<RepackQueue, RepackQueueToExpand> RQTEAlgo;
  {
    RQTEAlgo rqteAlgo(m_backend, m_agentReference);
    RQTEAlgo::InsertedElement::list insertedElements;
    for (auto &rr: poppedElements.elements) {
      insertedElements.push_back(RQTEAlgo::InsertedElement());
      insertedElements.back().repackRequest = std::move(rr.repackRequest);
    }
    rqteAlgo.referenceAndSwitchOwnership(nullopt, m_agentReference.getAgentAddress(), insertedElements, lc);
  }
  ret.promotedRequests = poppedElements.summary.requests;
  ret.pendingAfter = ret.pendingBefore - ret.promotedRequests;
  ret.toExpandAfter = ret.toEnpandBefore + ret.promotedRequests;
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::populateRepackRequestsStatistics()
//------------------------------------------------------------------------------
void OStoreDB::populateRepackRequestsStatistics(RootEntry & re, SchedulerDatabase::RepackRequestStatistics& stats) {
  objectstore::RepackIndex ri(m_objectStore);
  try {
    ri.setAddress(re.getRepackIndexAddress());
  } catch (RootEntry::NotAllocated &) {
    return;
  }
  ri.fetchNoLock();
  std::list<objectstore::RepackRequest> requests;
  std::list<std::unique_ptr<objectstore::RepackRequest::AsyncLockfreeFetcher>> fetchers;
  for (auto &rra: ri.getRepackRequestsAddresses()) {
    requests.emplace_back(objectstore::RepackRequest(rra.repackRequestAddress, m_objectStore));
    fetchers.emplace_back(requests.back().asyncLockfreeFetch());
  }
  // Ensure existence of stats for important statuses
  typedef common::dataStructures::RepackInfo::Status Status; 
  for (auto s: {Status::Pending, Status::ToExpand, Status::Starting}) {
    stats[s] = 0;
  }
  auto fet = fetchers.begin();
  for (auto &req: requests) {
    try {
      (*fet)->wait();
      try {
        stats.at(req.getInfo().status)++;
      } catch (std::out_of_range&) {
        stats[req.getInfo().status] = 1;
      }
    } catch (...) {}
    fet++;
  }
}

//------------------------------------------------------------------------------
// OStoreDB::getRepackStatistics()
//------------------------------------------------------------------------------
auto OStoreDB::getRepackStatistics() -> std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> {
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  // We need to take a lock on an object to allow global locking of the repack
  // requests scheduling.
  std::unique_ptr<OStoreDB::RepackRequestPromotionStatistics>
    typedRet(new OStoreDB::RepackRequestPromotionStatistics(m_objectStore, *m_agentReference));
  // Try to get the lock
  try {
    typedRet->m_pendingRepackRequestQueue.setAddress(re.getRepackQueueAddress(RepackQueueType::Pending));
    typedRet->m_lockOnPendingRepackRequestsQueue.lock(typedRet->m_pendingRepackRequestQueue);
  } catch (...) {
    throw SchedulerDatabase::RepackRequestStatistics::NoPendingRequestQueue("In OStoreDB::getRepackStatistics(): could not lock the pending requests queue.");
  }
  // In all cases, we get the information from the index and individual requests.
  populateRepackRequestsStatistics(re, *typedRet);
  std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> ret(typedRet.release());
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getRepackStatisticsNoLock()
//------------------------------------------------------------------------------
auto OStoreDB::getRepackStatisticsNoLock() -> std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> {
  auto typedRet = make_unique<OStoreDB::RepackRequestPromotionStatisticsNoLock>();
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  populateRepackRequestsStatistics(re, *typedRet);
  std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> ret(typedRet.release());
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getNextRepackJobToExpand()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::RepackRequest> OStoreDB::getNextRepackJobToExpand() {
  typedef objectstore::ContainerAlgorithms<RepackQueue,RepackQueueToExpand> RQTEAlgo;
  RQTEAlgo rqteAlgo(m_objectStore, *m_agentReference);  
  log::LogContext lc(m_logger);
  while(true){
    RQTEAlgo::PopCriteria criteria;
    //pop request that is in the RepackQueueToExpandRequest
    auto jobs = rqteAlgo.popNextBatch(cta::nullopt,criteria,lc);
    if(jobs.elements.empty()){
      //If there is no request, return a nullptr
      return nullptr;
    }
    //Get the first one request that is in elements
    auto repackRequest = jobs.elements.front().repackRequest.get();
    auto repackInfo = jobs.elements.front().repackInfo;
    //build the repackRequest with the repack infos
    std::unique_ptr<OStoreDB::RepackRequest> ret;
    ret.reset(new OStoreDB::RepackRequest(repackRequest->getAddressIfSet(),*this));
    ret->repackInfo.vid = repackInfo.vid;
    ret->repackInfo.type = repackInfo.type;
    ret->repackInfo.status = repackInfo.status;
    return std::move(ret);
  }
}

//------------------------------------------------------------------------------
// OStoreDB::cancelRepack()
//------------------------------------------------------------------------------
void OStoreDB::cancelRepack(const std::string& vid, log::LogContext & lc) {
  // Find the request
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  RepackIndex ri(m_objectStore);
  // First, try to get the address of of the repack index lockfree.
  try {
    ri.setAddress(re.getRepackIndexAddress());
  } catch (RootEntry::NotAllocated &) {
    throw NoSuchRepackRequest("In OStoreDB::cancelRepack(): No repack request for this VID (index not present).");
  }
  ri.fetchNoLock();
  auto rrAddresses = ri.getRepackRequestsAddresses();
  for (auto & rra: rrAddresses) {
    if (rra.vid == vid) {
      try {
        objectstore::RepackRequest rr(rra.repackRequestAddress, m_objectStore);
        ScopedExclusiveLock rrl(rr);
        rr.fetch();
        if (rr.getInfo().vid != vid)
          throw  exception::Exception("In OStoreDB::getRepackInfo(): unexpected vid when reading request");
        // We now have a hold of the repack request.
        // We should delete all the file level subrequests.
        // TODO
        // And then delete the request
        std::string repackRequestOwner = rr.getOwner();
        rr.remove();
        // We now need to dereference, from a queue if needed and from the index for sure.
        Helpers::removeRepackRequestToIndex(vid, m_objectStore, lc);
        if (repackRequestOwner.size()) {
          // Find the queue into which the request was queued. If the request was not owned by a queue (i.e., another type
          // of object), we do not care as the garbage collection will remove the reference.
          objectstore::RepackQueue rq(repackRequestOwner, m_objectStore);
          objectstore::ScopedExclusiveLock rql;
          try {
            rql.lock(rq);
            rq.fetch();
          } 
          catch (objectstore::Backend::NoSuchObject &) { return; }
          catch (objectstore::ObjectOpsBase::WrongType &) { return; }
          std::list<std::string> reqs{rr.getAddressIfSet()};
          rq.removeRequestsAndCommit(reqs);
        }
        return;
      } catch (cta::exception::Exception &) {}
    }
  }
  throw NoSuchRepackRequest("In OStoreDB::cancelRepack(): No repack request for this VID.");
}

//------------------------------------------------------------------------------
// OStoreDB::getNextRetrieveJobsToReportBatch()
//------------------------------------------------------------------------------
std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> OStoreDB::
getNextRetrieveJobsToReportBatch(uint64_t filesRequested, log::LogContext &logContext)
{
  typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueToReport> RQTRAlgo;
  RQTRAlgo rqtrAlgo(m_objectStore, *m_agentReference);
  // Decide from which queue we are going to pop
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  while(true) {
    auto queueList = re.dumpRetrieveQueues(JobQueueType::JobsToReportToUser);
    std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> ret;
    if (queueList.empty()) return ret;

    // Try to get jobs from the first queue. If it is empty, it will be trimmed, so we can go for another round.
    RQTRAlgo::PopCriteria criteria;
    criteria.files = filesRequested;
    auto jobs = rqtrAlgo.popNextBatch(queueList.front().vid, criteria, logContext);
    if(jobs.elements.empty()) continue;
    for(auto &j : jobs.elements)
    {
      std::unique_ptr<OStoreDB::RetrieveJob> rj(new OStoreDB::RetrieveJob(j.retrieveRequest->getAddressIfSet(), *this, nullptr));
      rj->archiveFile = j.archiveFile;
      rj->retrieveRequest = j.rr;
      rj->selectedCopyNb = j.copyNb;
      rj->errorReportURL = j.errorReportURL;
      rj->reportType = j.reportType;
      rj->setJobOwned();
      ret.emplace_back(std::move(rj));
    }
    return ret;
  }
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveJobsFailedSummary
//------------------------------------------------------------------------------
SchedulerDatabase::JobsFailedSummary OStoreDB::getRetrieveJobsFailedSummary(log::LogContext &logContext) {
  RootEntry re(m_objectStore);
  re.fetchNoLock();

  SchedulerDatabase::JobsFailedSummary ret;
  auto queueList = re.dumpRetrieveQueues(JobQueueType::FailedJobs);
  for(auto &rj : queueList) {
    RetrieveQueue rq(rj.address, m_objectStore);
    try {
      rq.fetchNoLock();
    } catch (cta::exception::Exception &ex) {
      log::ScopedParamContainer params (logContext);
      params.add("queueObject", rj.address)
            .add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::WARNING, "In OStoreDB::getRetrieveJobsFailedSummary(): failed to lock/fetch a retrieve queue.");
      continue;
    }
    auto summary = rq.getCandidateSummary();
    ret.totalFiles += summary.candidateFiles;
    ret.totalBytes += summary.candidateBytes;
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getNextRetrieveJobsFailedBatch()
//------------------------------------------------------------------------------
std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> OStoreDB::
getNextRetrieveJobsFailedBatch(uint64_t filesRequested, log::LogContext &logContext)
{
  typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueFailed> RQTRAlgo;
  RQTRAlgo rqtrAlgo(m_objectStore, *m_agentReference);
  // Decide from which queue we are going to pop
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  while(true) {
    auto queueList = re.dumpRetrieveQueues(JobQueueType::FailedJobs);
    std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> ret;
    if (queueList.empty()) return ret;

    // Try to get jobs from the first queue. If it is empty, it will be trimmed, so we can go for another round.
    RQTRAlgo::PopCriteria criteria;
    criteria.files = filesRequested;
    auto jobs = rqtrAlgo.popNextBatch(queueList.front().vid, criteria, logContext);
    if(jobs.elements.empty()) continue;
    for(auto &j : jobs.elements)
    {
      std::unique_ptr<OStoreDB::RetrieveJob> rj(new OStoreDB::RetrieveJob(j.retrieveRequest->getAddressIfSet(), *this, nullptr));
      rj->archiveFile = j.archiveFile;
      rj->retrieveRequest = j.rr;
      rj->selectedCopyNb = j.copyNb;
      rj->setJobOwned();
      ret.emplace_back(std::move(rj));
    }
    return ret;
  }
}

//------------------------------------------------------------------------------
// OStoreDB::getDriveStates()
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::DriveState> OStoreDB::getDriveStates(log::LogContext & lc) const {
  return Helpers::getAllDriveStates(m_objectStore, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::setDesiredDriveState()
//------------------------------------------------------------------------------
void OStoreDB::setDesiredDriveState(const std::string& drive, const common::dataStructures::DesiredDriveState & desiredState, log::LogContext &lc) {
  objectstore::DriveState ds(m_objectStore);
  ScopedExclusiveLock dsl;
  Helpers::getLockedAndFetchedDriveState(ds, dsl, *m_agentReference, drive, lc);
  auto driveState = ds.getState();
  driveState.desiredDriveState = desiredState;
  ds.setState(driveState);
  ds.commit();
}

//------------------------------------------------------------------------------
// OStoreDB::removeDrive()
//------------------------------------------------------------------------------
void OStoreDB::removeDrive(const std::string& drive, log::LogContext &lc) {
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  auto driveRegisterAddress = re.getDriveRegisterAddress();
  objectstore::DriveRegister dr(driveRegisterAddress, m_objectStore);
  objectstore::ScopedExclusiveLock drl(dr);
  dr.fetch();
  // If the driveStatus exists, take ownership before deleting and finally dereferencing.
  auto driveAddresses = dr.getDriveAddresses();
  // If the drive is present, find status and lock it. If not, nothing to do.
  auto di = std::find_if(driveAddresses.begin(), driveAddresses.end(),
      [&](const DriveRegister::DriveAddress & da){ return da.driveName == drive; });
  if (di!=driveAddresses.end()) {
    // Try and take an exclusive lock on the object
    log::ScopedParamContainer params(lc);
    params.add("driveName", drive)
          .add("driveRegisterObject", dr.getAddressIfSet())
          .add("driveStateObject", di->driveStateAddress);
    try {
      objectstore::DriveState ds(di->driveStateAddress, m_objectStore);
      objectstore::ScopedExclusiveLock dsl(ds);
      ds.fetch();
      params.add("driveName", drive)
            .add("driveRegisterObject", dr.getAddressIfSet())
            .add("driveStateObject", ds.getAddressIfSet());
      if (ds.getOwner() == dr.getAddressIfSet()) {
        // The drive state is owned as expected, delete it and then de-reference it.
        ds.remove();
        dr.removeDrive(drive);
        dr.commit();
        log::ScopedParamContainer params(lc);
        params.add("driveStateObject", ds.getAddressIfSet());
        lc.log(log::INFO, "In OStoreDB::removeDrive(): removed and dereferenced drive state object.");
      } else {
        dr.removeDrive(drive);
        dr.commit();
        lc.log(log::WARNING, "In OStoreDB::removeDrive(): just dereferenced drive state object not owned by drive register.");
      }
    } catch (cta::exception::Exception & ex) {
      // The drive might not exist anymore, in which case we can proceed to
      // dereferencing from DriveRegister.
      if (!m_objectStore.exists(di->driveStateAddress)) {
        dr.removeDrive(drive);
        dr.commit();
        lc.log(log::WARNING, "In OStoreDB::removeDrive(): dereferenced non-existing drive state object.");
      } else {
        params.add("exceptionMessage", ex.getMessageValue());
        lc.log(log::ERR, "In OStoreDB::removeDrive(): unexpected error dereferencing drive state. Doing nothing.");
      }
    }
  } else {
    log::ScopedParamContainer params (lc);
    params.add("driveName", drive);
    lc.log(log::INFO, "In OStoreDB::removeDrive(): no such drive reference in register.");
  }
}
 
//------------------------------------------------------------------------------
// OStoreDB::reportDriveStatus()
//------------------------------------------------------------------------------
void OStoreDB::reportDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
  cta::common::dataStructures::MountType mountType, common::dataStructures::DriveStatus status,
  time_t reportTime, log::LogContext & lc, uint64_t mountSessionId, uint64_t byteTransferred,
  uint64_t filesTransferred, double latestBandwidth, const std::string& vid, 
  const std::string& tapepool) {
  using common::dataStructures::DriveStatus;
  // Wrap all the parameters together for easier manipulation by sub-functions
  ReportDriveStatusInputs inputs;
  inputs.mountType = mountType;
  inputs.byteTransferred = byteTransferred;
  inputs.filesTransferred = filesTransferred;
  inputs.latestBandwidth = latestBandwidth;
  inputs.mountSessionId = mountSessionId;
  inputs.reportTime = reportTime;
  inputs.status = status;
  inputs.vid = vid;
  inputs.tapepool = tapepool;
  updateDriveStatus(driveInfo, inputs, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::updateDriveStatus()
//------------------------------------------------------------------------------
void OStoreDB::updateDriveStatus(const common::dataStructures::DriveInfo& driveInfo, const ReportDriveStatusInputs& inputs,
  log::LogContext &lc) {
  using common::dataStructures::DriveStatus;
  // First, get the drive state.
  objectstore::DriveState ds(m_objectStore);
  objectstore::ScopedExclusiveLock dsl;
  Helpers::getLockedAndFetchedDriveState(ds, dsl, *m_agentReference, driveInfo.driveName, lc);
  // The drive state might not be present, in which case we have to fill it up with default values.
  cta::common::dataStructures::DriveState driveState = ds.getState();
  // Set the parameters that we always set
  driveState.host = driveInfo.host;
  driveState.logicalLibrary = driveInfo.logicalLibrary;
  // Set the status
  switch (inputs.status) {
    case DriveStatus::Down:
      setDriveDown(driveState, inputs);
      break;
    case DriveStatus::Up:
      setDriveUpOrMaybeDown(driveState, inputs);
      break;
    case DriveStatus::Probing:
      setDriveProbing(driveState, inputs);
      break;
    case DriveStatus::Starting:
      setDriveStarting(driveState, inputs);
      break;
    case DriveStatus::Mounting:
      setDriveMounting(driveState, inputs);
      break;
    case DriveStatus::Transferring:
      setDriveTransferring(driveState, inputs);
      break;
    case DriveStatus::Unloading:
      setDriveUnloading(driveState, inputs);
      break;
    case DriveStatus::Unmounting:
      setDriveUnmounting(driveState, inputs);
      break;
    case DriveStatus::DrainingToDisk:
      setDriveDrainingToDisk(driveState, inputs);
      break;
    case DriveStatus::CleaningUp:
      setDriveCleaningUp(driveState, inputs);
      break;
    case DriveStatus::Shutdown:
      setDriveShutdown(driveState, inputs);
      break;
    default:
      throw exception::Exception("Unexpected status in DriveRegister::reportDriveStatus");
  }
  ds.setState(driveState);
  ds.commit();
}

//------------------------------------------------------------------------------
// OStoreDB::updateDriveStatsInRegitry()
//------------------------------------------------------------------------------
void OStoreDB::updateDriveStatistics(const common::dataStructures::DriveInfo& driveInfo, const ReportDriveStatsInputs& inputs, log::LogContext & lc) {
  using common::dataStructures::DriveStatus;
  // The drive state might not be present, in which case we do nothing.
  cta::common::dataStructures::DriveState driveState;
  objectstore::DriveState ds(m_objectStore);
  objectstore::ScopedExclusiveLock dsl;
  try { 
    Helpers::getLockedAndFetchedDriveState(ds, dsl, *m_agentReference, driveInfo.driveName,
      lc, Helpers::CreateIfNeeded::doNotCreate);
  } catch (cta::exception::Exception & ex) {
    // The drive is missing in the registry. Nothing to update
    return;    
  }
  driveState = ds.getState();
  // Set the parameters that we always set
  driveState.host = driveInfo.host;
  driveState.logicalLibrary = driveInfo.logicalLibrary;
  
  switch (driveState.driveStatus) {    
    case DriveStatus::Transferring:
    {
      const time_t timeDifference = inputs.reportTime -  driveState.lastUpdateTime;
      const uint64_t bytesDifference = inputs.bytesTransferred - driveState.bytesTransferredInSession;   
      driveState.lastUpdateTime=inputs.reportTime;
      driveState.bytesTransferredInSession=inputs.bytesTransferred;
      driveState.filesTransferredInSession=inputs.filesTransferred;
      driveState.latestBandwidth = timeDifference?1.0*bytesDifference/timeDifference:0.0;
      break;
    }
    default:
      return;
  }
  ds.setState(driveState);
  ds.commit();
}  

//------------------------------------------------------------------------------
// OStoreDB::setDriveDown()
//------------------------------------------------------------------------------
void OStoreDB::setDriveDown(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we were already down, then we only update the last update time.
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Down) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset.
  driveState.sessionId=0;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=inputs.reportTime;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=common::dataStructures::MountType::NoMount;
  driveState.driveStatus=common::dataStructures::DriveStatus::Down;
  driveState.desiredDriveState.up=false;
  driveState.desiredDriveState.forceDown=false;
  driveState.currentVid="";
  driveState.currentTapePool="";
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveUp()
//------------------------------------------------------------------------------
void OStoreDB::setDriveUpOrMaybeDown(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  using common::dataStructures::DriveStatus;
  // Decide whether we should be up or down
  DriveStatus  targetStatus=DriveStatus::Up;
  if (!driveState.desiredDriveState.up) {
    driveState.driveStatus = common::dataStructures::DriveStatus::Down;
  }
  // If we were already up (or down), then we only update the last update time.
  if (driveState.driveStatus == targetStatus) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset.
  driveState.sessionId=0;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=inputs.reportTime;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=common::dataStructures::MountType::NoMount;
  driveState.driveStatus=targetStatus;
  driveState.currentVid="";
  driveState.currentTapePool="";
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveUp()
//------------------------------------------------------------------------------
void OStoreDB::setDriveProbing(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  using common::dataStructures::DriveStatus;
  // If we were already up (or down), then we only update the last update time.
  if (driveState.driveStatus == inputs.status) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset.
  driveState.sessionId=0;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=inputs.reportTime;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=common::dataStructures::MountType::NoMount;
  driveState.driveStatus=inputs.status;
  driveState.currentVid="";
  driveState.currentTapePool="";
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveStarting()
//------------------------------------------------------------------------------
void OStoreDB::setDriveStarting(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we were already starting, then we only update the last update time.
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Starting) {
    driveState.lastUpdateTime = inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId=inputs.mountSessionId;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=inputs.reportTime;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.startStartTime=inputs.reportTime;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::Starting;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveMounting()
//------------------------------------------------------------------------------
void OStoreDB::setDriveMounting(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we were already starting, then we only update the last update time.
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Mounting) {
    driveState.lastUpdateTime = inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId=inputs.mountSessionId;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  //driveState.sessionstarttime=inputs.reportTime;
  driveState.mountStartTime=inputs.reportTime;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::Mounting;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveTransferring()
//------------------------------------------------------------------------------
void OStoreDB::setDriveTransferring(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we were already transferring, we update the full statistics
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Transferring) {
    driveState.lastUpdateTime=inputs.reportTime;
    driveState.bytesTransferredInSession=inputs.byteTransferred;
    driveState.filesTransferredInSession=inputs.filesTransferred;
    driveState.latestBandwidth=inputs.latestBandwidth;
    return;
  }
  driveState.sessionId=inputs.mountSessionId;
  driveState.bytesTransferredInSession=inputs.byteTransferred;
  driveState.filesTransferredInSession=inputs.filesTransferred;
  driveState.latestBandwidth=inputs.latestBandwidth;
  //driveState.sessionstarttime=inputs.reportTime;
  //driveState.mountstarttime=inputs.reportTime;
  driveState.transferStartTime=inputs.reportTime;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::Transferring;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveUnloading()
//------------------------------------------------------------------------------
void OStoreDB::setDriveUnloading(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Unloading) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId=inputs.mountSessionId;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=inputs.reportTime;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::Unloading;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveUnmounting()
//------------------------------------------------------------------------------
void OStoreDB::setDriveUnmounting(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Unmounting) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId=inputs.mountSessionId;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=inputs.reportTime;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::Unmounting;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveDrainingToDisk()
//------------------------------------------------------------------------------
void OStoreDB::setDriveDrainingToDisk(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  if (driveState.driveStatus == common::dataStructures::DriveStatus::DrainingToDisk) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId=inputs.mountSessionId;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=inputs.reportTime;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::DrainingToDisk;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveCleaningUp()
//------------------------------------------------------------------------------
void OStoreDB::setDriveCleaningUp(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  if (driveState.driveStatus == common::dataStructures::DriveStatus::CleaningUp) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId=inputs.mountSessionId;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=inputs.reportTime;
  driveState.shutdownTime=0;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::CleaningUp;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}

//------------------------------------------------------------------------------
// OStoreDB::setDriveShutdown()
//------------------------------------------------------------------------------
void OStoreDB::setDriveShutdown(common::dataStructures::DriveState & driveState,
  const ReportDriveStatusInputs & inputs) {
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Shutdown) {
    driveState.lastUpdateTime=inputs.reportTime;
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId=0;
  driveState.bytesTransferredInSession=0;
  driveState.filesTransferredInSession=0;
  driveState.latestBandwidth=0;
  driveState.sessionStartTime=0;
  driveState.mountStartTime=0;
  driveState.transferStartTime=0;
  driveState.unloadStartTime=0;
  driveState.unmountStartTime=0;
  driveState.drainingStartTime=0;
  driveState.downOrUpStartTime=0;
  driveState.probeStartTime=0;
  driveState.cleanupStartTime=0;
  driveState.shutdownTime=inputs.reportTime;
  driveState.lastUpdateTime=inputs.reportTime;
  driveState.mountType=inputs.mountType;
  driveState.driveStatus=common::dataStructures::DriveStatus::CleaningUp;
  driveState.currentVid=inputs.vid;
  driveState.currentTapePool=inputs.tapepool;
}
//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfo::createArchiveMount()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::ArchiveMount> 
  OStoreDB::TapeMountDecisionInfo::createArchiveMount(
    const catalogue::TapeForWriting & tape, const std::string driveName,
    const std::string& logicalLibrary, const std::string& hostName, const std::string& vo, const std::string& mediaType,
      const std::string& vendor,uint64_t capacityInBytes, time_t startTime) {
  // In order to create the mount, we have to:
  // Check we actually hold the scheduling lock
  // Set the drive status to up, and indicate which tape we use.
  std::unique_ptr<OStoreDB::ArchiveMount> privateRet(
    new OStoreDB::ArchiveMount(m_oStoreDB));
  auto &am = *privateRet;
  // Check we hold the scheduling lock
  if (!m_lockTaken)
    throw SchedulingLockNotHeld("In OStoreDB::TapeMountDecisionInfo::createArchiveMount: "
      "cannot create mount without holding scheduling lock");
  objectstore::RootEntry re(m_oStoreDB.m_objectStore);
  re.fetchNoLock();
  auto driveRegisterAddress = re.getDriveRegisterAddress();
  am.nbFilesCurrentlyOnTape = tape.lastFSeq;
  am.mountInfo.vid = tape.vid;
  // Fill up the mount info
  am.mountInfo.drive = driveName;
  am.mountInfo.host = hostName;
  am.mountInfo.vo = vo;
  am.mountInfo.mediaType = mediaType;
  am.mountInfo.vendor = vendor;
  am.mountInfo.mountId = m_schedulerGlobalLock->getIncreaseCommitMountId();
  am.mountInfo.capacityInBytes = capacityInBytes;
  m_schedulerGlobalLock->commit();
  am.mountInfo.tapePool = tape.tapePool;
  am.mountInfo.logicalLibrary = logicalLibrary;
  // Update the status of the drive in the registry
  {
    // The drive is already in-session, to prevent double scheduling before it 
    // goes to mount state. If the work to be done gets depleted in the mean time,
    // we will switch back to up.
    common::dataStructures::DriveInfo driveInfo;
    driveInfo.driveName=driveName;
    driveInfo.logicalLibrary=logicalLibrary;
    driveInfo.host=hostName;
    ReportDriveStatusInputs inputs;
    inputs.mountType = common::dataStructures::MountType::Archive;
    inputs.byteTransferred = 0;
    inputs.filesTransferred = 0;
    inputs.latestBandwidth = 0;
    inputs.mountSessionId = am.mountInfo.mountId;
    inputs.reportTime = startTime;
    inputs.status = common::dataStructures::DriveStatus::Mounting;
    inputs.vid = tape.vid;
    inputs.tapepool = tape.tapePool;
    log::LogContext lc(m_oStoreDB.m_logger);
    m_oStoreDB.updateDriveStatus(driveInfo, inputs, lc);
  }
  // We committed the scheduling decision. We can now release the scheduling lock.
  m_lockOnSchedulerGlobalLock.release();
  m_lockTaken = false;
  // We can now return the mount session object to the user.
  std::unique_ptr<SchedulerDatabase::ArchiveMount> ret(privateRet.release());
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfo::TapeMountDecisionInfo()
//------------------------------------------------------------------------------
OStoreDB::TapeMountDecisionInfo::TapeMountDecisionInfo(OStoreDB & oStoreDb): m_lockTaken(false), m_oStoreDB(oStoreDb) {}

//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfo::createArchiveMount()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::RetrieveMount> 
  OStoreDB::TapeMountDecisionInfo::createRetrieveMount(
    const std::string& vid, const std::string & tapePool, const std::string driveName, 
    const std::string& logicalLibrary, const std::string& hostName,const std::string& vo, const std::string& mediaType,
      const std::string& vendor,const uint64_t capacityInBytes, time_t startTime) {
  // In order to create the mount, we have to:
  // Check we actually hold the scheduling lock
  // Check the tape exists, add it to ownership and set its activity status to 
  // busy, with the current agent pointing to it for unbusying
  // Set the drive status to up, but do not commit anything to the drive register
  // the drive register does not need garbage collection as it should reflect the
  // latest known state of the drive (and its absence of updating if needed)
  // Prepare the return value
  std::unique_ptr<OStoreDB::RetrieveMount> privateRet(
    new OStoreDB::RetrieveMount(m_oStoreDB));
  auto &rm = *privateRet;
  // Check we hold the scheduling lock
  if (!m_lockTaken)
    throw SchedulingLockNotHeld("In OStoreDB::TapeMountDecisionInfo::createRetrieveMount: "
      "cannot create mount without holding scheduling lock");
  // Find the tape and update it
  objectstore::RootEntry re(m_oStoreDB.m_objectStore);
  re.fetchNoLock();
  auto driveRegisterAddress = re.getDriveRegisterAddress();
  // Fill up the mount info
  rm.mountInfo.vid = vid;
  rm.mountInfo.drive = driveName;
  rm.mountInfo.host = hostName;
  rm.mountInfo.mountId = m_schedulerGlobalLock->getIncreaseCommitMountId();
  m_schedulerGlobalLock->commit();
  rm.mountInfo.tapePool = tapePool;
  rm.mountInfo.logicalLibrary = logicalLibrary;
  rm.mountInfo.vo = vo;
  rm.mountInfo.mediaType = mediaType;
  rm.mountInfo.vendor = vendor;
  rm.mountInfo.capacityInBytes = capacityInBytes;
  // Update the status of the drive in the registry
  {
    // Get hold of the drive registry
    // The drive is already in-session, to prevent double scheduling before it 
    // goes to mount state. If the work to be done gets depleted in the mean time,
    // we will switch back to up.
    common::dataStructures::DriveInfo driveInfo;
    driveInfo.driveName=driveName;
    driveInfo.logicalLibrary=logicalLibrary;
    driveInfo.host=hostName;
    ReportDriveStatusInputs inputs;
    inputs.mountType = common::dataStructures::MountType::Retrieve;
    inputs.mountSessionId = rm.mountInfo.mountId;
    inputs.reportTime = startTime;
    inputs.status = common::dataStructures::DriveStatus::Mounting;
    inputs.vid = rm.mountInfo.vid;
    inputs.tapepool = rm.mountInfo.tapePool;
    log::LogContext lc(m_oStoreDB.m_logger);
    m_oStoreDB.updateDriveStatus(driveInfo, inputs, lc);
  }
  // We committed the scheduling decision. We can now release the scheduling lock.
  m_lockOnSchedulerGlobalLock.release();
  m_lockTaken = false;
  // We can now return the mount session object to the user.
  std::unique_ptr<SchedulerDatabase::RetrieveMount> ret(privateRet.release());
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfo::~TapeMountDecisionInfo()
//------------------------------------------------------------------------------
OStoreDB::TapeMountDecisionInfo::~TapeMountDecisionInfo() {
  // The lock should be released before the objectstore object 
  // m_schedulerGlobalLock gets destroyed. We explicitely release the lock,
  // and then destroy the object
  if (m_lockTaken)
    m_lockOnSchedulerGlobalLock.release();
  m_schedulerGlobalLock.reset(NULL);
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::ArchiveMount()
//------------------------------------------------------------------------------
OStoreDB::ArchiveMount::ArchiveMount(OStoreDB & oStoreDB): m_oStoreDB(oStoreDB) {}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::getMountInfo()
//------------------------------------------------------------------------------
const SchedulerDatabase::ArchiveMount::MountInfo& OStoreDB::ArchiveMount::getMountInfo() {
  return mountInfo;
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::getNextJobBatch()
//------------------------------------------------------------------------------
std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > OStoreDB::ArchiveMount::getNextJobBatch(uint64_t filesRequested,
    uint64_t bytesRequested, log::LogContext& logContext) {
  typedef objectstore::ContainerAlgorithms<ArchiveQueue,ArchiveQueueToTransfer> AQAlgos;
  AQAlgos aqAlgos(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
  AQAlgos::PopCriteria popCriteria(filesRequested, bytesRequested);
  auto jobs = aqAlgos.popNextBatch(mountInfo.tapePool, popCriteria, logContext);
  // We can construct the return value.
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > ret;
  for (auto & j: jobs.elements) {
    std::unique_ptr<OStoreDB::ArchiveJob> aj(new OStoreDB::ArchiveJob(j.archiveRequest->getAddressIfSet(), m_oStoreDB));
    aj->tapeFile.copyNb = j.copyNb;
    aj->archiveFile = j.archiveFile;
    aj->archiveReportURL = j.archiveReportURL;
    aj->errorReportURL = j.errorReportURL;
    aj->srcURL = j.srcURL;
    aj->tapeFile.fSeq = ++nbFilesCurrentlyOnTape;
    aj->tapeFile.vid = mountInfo.vid;
    aj->tapeFile.blockId =
        std::numeric_limits<decltype(aj->tapeFile.blockId)>::max();
    aj->m_jobOwned = true;
    aj->m_mountId = mountInfo.mountId;
    aj->m_tapePool = mountInfo.tapePool;
    ret.emplace_back(std::move(aj));
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::complete()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveMount::complete(time_t completionTime) {
  // When the session is complete, we can reset the status of the drive.
  // Tape will be implicitly released
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=mountInfo.drive;
  driveInfo.logicalLibrary=mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;
  ReportDriveStatusInputs inputs;
  inputs.mountType = common::dataStructures::MountType::NoMount;
  inputs.mountSessionId = mountInfo.mountId;
  inputs.reportTime = completionTime;
  inputs.status = common::dataStructures::DriveStatus::Up;
  inputs.vid = mountInfo.vid;
  inputs.tapepool = mountInfo.tapePool;
  log::LogContext lc(m_oStoreDB.m_logger);
  m_oStoreDB.updateDriveStatus(driveInfo, inputs, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::ArchiveJob()
//------------------------------------------------------------------------------
OStoreDB::ArchiveJob::ArchiveJob(const std::string& jobAddress, OStoreDB& oStoreDB): m_jobOwned(false),
  m_oStoreDB(oStoreDB), m_archiveRequest(jobAddress, m_oStoreDB.m_objectStore) { }

//------------------------------------------------------------------------------
// OStoreDB::castFromSchedDBJob()
//------------------------------------------------------------------------------
OStoreDB::ArchiveJob * OStoreDB::castFromSchedDBJob(SchedulerDatabase::ArchiveJob * job) {
  OStoreDB::ArchiveJob * ret = dynamic_cast<OStoreDB::ArchiveJob *>(job);
  if (!ret) throw cta::exception::Exception("In OStoreDB::castFromSchedDBJob(ArchiveJob*): wrong type.");
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::RetrieveMount()
//------------------------------------------------------------------------------
OStoreDB::RetrieveMount::RetrieveMount(OStoreDB & oStoreDB): m_oStoreDB(oStoreDB) {}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::getMountInfo()
//------------------------------------------------------------------------------
const OStoreDB::RetrieveMount::MountInfo& OStoreDB::RetrieveMount::getMountInfo() {
  return mountInfo;
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::getNextJobBatch()
//------------------------------------------------------------------------------
std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> OStoreDB::RetrieveMount::
getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested, log::LogContext &logContext)
{
  typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransfer> RQAlgos;
  RQAlgos rqAlgos(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
  RQAlgos::PopCriteria popCriteria(filesRequested, bytesRequested);
  auto jobs = rqAlgos.popNextBatch(mountInfo.vid, popCriteria, logContext);
  // We can construct the return value
  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> ret;
  for(auto &j : jobs.elements)
  {
    std::unique_ptr<OStoreDB::RetrieveJob> rj(new OStoreDB::RetrieveJob(j.retrieveRequest->getAddressIfSet(), m_oStoreDB, this));
    rj->archiveFile = j.archiveFile;
    rj->retrieveRequest = j.rr;
    rj->selectedCopyNb = j.copyNb;
    rj->m_jobOwned = true;
    rj->m_mountId = mountInfo.mountId;
    ret.emplace_back(std::move(rj));
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::complete()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveMount::complete(time_t completionTime) {
  // When the session is complete, we can reset the status of the tape and the
  // drive
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=mountInfo.drive;
  driveInfo.logicalLibrary=mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;
  ReportDriveStatusInputs inputs;
  inputs.mountType = common::dataStructures::MountType::NoMount;
  inputs.mountSessionId = mountInfo.mountId;
  inputs.reportTime = completionTime;
  inputs.status = common::dataStructures::DriveStatus::Up;
  inputs.vid = mountInfo.vid;
  inputs.tapepool = mountInfo.tapePool;
  log::LogContext lc(m_oStoreDB.m_logger);
  m_oStoreDB.updateDriveStatus(driveInfo, inputs, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::setDriveStatus()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveMount::setDriveStatus(cta::common::dataStructures::DriveStatus status, time_t completionTime) {
  // We just report the drive status as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=mountInfo.drive;
  driveInfo.logicalLibrary=mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;
  ReportDriveStatusInputs inputs;
  inputs.mountType = common::dataStructures::MountType::Retrieve;
  inputs.mountSessionId = mountInfo.mountId;
  inputs.reportTime = completionTime;
  inputs.status = status;
  inputs.vid = mountInfo.vid;
  inputs.tapepool = mountInfo.tapePool;
  // TODO: statistics!
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  inputs.latestBandwidth = 0;
  log::LogContext lc(m_oStoreDB.m_logger);
  m_oStoreDB.updateDriveStatus(driveInfo, inputs, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::setTapeSessionStats()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) {
  // We just report tthe tape session statistics as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=mountInfo.drive;
  driveInfo.logicalLibrary=mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;
  
  ReportDriveStatsInputs inputs;
  inputs.reportTime = time(nullptr); 
  inputs.bytesTransferred = stats.dataVolume;
  inputs.filesTransferred = stats.filesCount;
  
  log::LogContext lc(m_oStoreDB.m_logger);
  m_oStoreDB.updateDriveStatistics(driveInfo, inputs, lc);
}


//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::castFromSchedDBJob()
//------------------------------------------------------------------------------
OStoreDB::RetrieveJob * OStoreDB::castFromSchedDBJob(SchedulerDatabase::RetrieveJob * job) {
  OStoreDB::RetrieveJob * ret = dynamic_cast<OStoreDB::RetrieveJob *>(job);
  if (!ret) {
    std::string unexpectedType = typeid(*job).name();
    throw cta::exception::Exception(
      "In OStoreDB::RetrieveMount::castFromSchedDBJob(): unexpected retrieve job type while casting: " + unexpectedType
    );
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::waitAndFinishSettingJobsBatchSuccessful()
//------------------------------------------------------------------------------
std::set<cta::SchedulerDatabase::RetrieveJob*> OStoreDB::RetrieveMount::finishSettingJobsBatchSuccessful(
  std::list<cta::SchedulerDatabase::RetrieveJob*>& jobsBatch, log::LogContext& lc) {
  std::set<cta::SchedulerDatabase::RetrieveJob*> ret;
  std::list<std::string> rjToUnown;
  // We will wait on the asynchronously started reports of jobs and remove them from
  // ownership.
  for (auto & sDBJob: jobsBatch) {
    auto osdbJob = castFromSchedDBJob(sDBJob);
    rjToUnown.push_back(osdbJob->m_retrieveRequest.getAddressIfSet());
    ret.insert(sDBJob);
  }
  m_oStoreDB.m_agentReference->removeBatchFromOwnership(rjToUnown, m_oStoreDB.m_objectStore);
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::batchSucceedRetrieveForRepack()
//------------------------------------------------------------------------------
std::set<cta::SchedulerDatabase::RetrieveJob *> OStoreDB::RetrieveMount::batchSucceedRetrieveForRepack(
    std::list<cta::SchedulerDatabase::RetrieveJob *> & jobsBatch, cta::log::LogContext & lc)
{
  std::set<cta::SchedulerDatabase::RetrieveJob *> ret;
  typedef objectstore::ContainerAlgorithms<objectstore::RetrieveQueue,objectstore::RetrieveQueueToReportToRepackForSuccess> AqtrtrfsCa;
  AqtrtrfsCa aqtrtrfsCa(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
  AqtrtrfsCa::InsertedElement::list insertedElementsLists;
  std::string vid;
  for(auto & retrieveJob : jobsBatch){
    auto osdbJob = castFromSchedDBJob(retrieveJob);
    ret.insert(retrieveJob);
    osdbJob->asyncReportSucceedForRepack();
    osdbJob->checkReportSucceedForRepack();
    auto & tapeFile = osdbJob->archiveFile.tapeFiles[osdbJob->selectedCopyNb];
    vid = osdbJob->m_retrieveMount->mountInfo.vid;
    insertedElementsLists.push_back(AqtrtrfsCa::InsertedElement{&osdbJob->m_retrieveRequest, (uint16_t)osdbJob->selectedCopyNb, tapeFile.fSeq,osdbJob->archiveFile.fileSize,cta::common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,serializers::RetrieveJobStatus::RJS_ToReportToRepackForSuccess});
  }
  aqtrtrfsCa.referenceAndSwitchOwnership(vid,insertedElementsLists,lc);
  return ret;
}
//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::setDriveStatus()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveMount::setDriveStatus(cta::common::dataStructures::DriveStatus status, time_t completionTime) {
  // We just report the drive status as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=mountInfo.drive;
  driveInfo.logicalLibrary=mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;
  ReportDriveStatusInputs inputs;
  inputs.mountType = common::dataStructures::MountType::Archive;
  inputs.mountSessionId = mountInfo.mountId;
  inputs.reportTime = completionTime;
  inputs.status = status;
  inputs.vid = mountInfo.vid;
  inputs.tapepool = mountInfo.tapePool;
  // TODO: statistics!
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  inputs.latestBandwidth = 0;
  log::LogContext lc(m_oStoreDB.m_logger);
  m_oStoreDB.updateDriveStatus(driveInfo, inputs, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::setTapeSessionStats()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) {
  // We just report the tape session statistics as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=mountInfo.drive;
  driveInfo.logicalLibrary=mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;
  
  ReportDriveStatsInputs inputs;
  inputs.reportTime = time(nullptr); 
  inputs.bytesTransferred = stats.dataVolume;
  inputs.filesTransferred = stats.filesCount;
  
  log::LogContext lc(m_oStoreDB.m_logger);
  m_oStoreDB.updateDriveStatistics(driveInfo, inputs, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::setJobBatchTransferred()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveMount::setJobBatchTransferred(std::list<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>>& jobsBatch,
    log::LogContext & lc) {
  std::set<cta::OStoreDB::ArchiveJob*> jobsToQueueForReporting;
  std::list<std::string> ajToUnown;
  utils::Timer t;
  log::TimingList timingList;
  // We will asynchronously report the archive jobs (which MUST be OStoreDBJobs).
  // We let the exceptions through as failing to report is fatal.
  for (auto & sDBJob: jobsBatch) {
    castFromSchedDBJob(sDBJob.get())->asyncSucceedTransfer();
  }
  timingList.insertAndReset("asyncSucceedLaunchTime", t);
  // TODO : could be optimized with single copy requests: we know we have to requeue. (minor optimization)
  // We will only know whether we need to queue the requests for reporting after updating request. So on a first
  // pass we update the request and on the second, we will queue a batch of them to the report queue. Report queues
  // are per VID and not tape pool: this limits contention (one tape written to at a time per mount, so the queuing should
  // be without contention.
  // Jobs that do not require queuing are done from our perspective and we should just remove them from agent ownership.
  for (auto & sDBJob: jobsBatch) {
    if (castFromSchedDBJob(sDBJob.get())->waitAsyncSucceed())
      jobsToQueueForReporting.insert(castFromSchedDBJob(sDBJob.get()));
    else
      ajToUnown.push_back(castFromSchedDBJob(sDBJob.get())->m_archiveRequest.getAddressIfSet());
  }
  timingList.insertAndReset("asyncSucceedCompletionTime", t);
  if (jobsToQueueForReporting.size()) {
    typedef objectstore::ContainerAlgorithms<objectstore::ArchiveQueue,objectstore::ArchiveQueueToReport> AqtrCa;
    AqtrCa aqtrCa(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
    std::map<std::string, AqtrCa::InsertedElement::list> insertedElementsLists;
    for (auto & j: jobsToQueueForReporting) {
      insertedElementsLists[j->tapeFile.vid].emplace_back(AqtrCa::InsertedElement{&j->m_archiveRequest, j->tapeFile.copyNb, 
          j->archiveFile, cta::nullopt, cta::nullopt});
      log::ScopedParamContainer params (lc);
      params.add("tapeVid", j->tapeFile.vid)
            .add("fileId", j->archiveFile.archiveFileID)
            .add("requestObject", j->m_archiveRequest.getAddressIfSet());
      lc.log(log::INFO, "In OStoreDB::ArchiveMount::setJobBatchTransferred(): will queue request for reporting.");
    }
    for (auto &list: insertedElementsLists) {
      try {
        utils::Timer tLocal;
        aqtrCa.referenceAndSwitchOwnership(list.first, m_oStoreDB.m_agentReference->getAgentAddress(), list.second, lc);
        log::ScopedParamContainer params(lc);
        params.add("tapeVid", list.first)
              .add("jobs", list.second.size())
              .add("enqueueTime", t.secs());
        lc.log(log::INFO, "In OStoreDB::ArchiveMount::setJobBatchTransferred(): queued a batch of requests for reporting.");
      } catch (cta::exception::Exception & ex) {
        log::ScopedParamContainer params(lc);
        params.add("tapeVid", list.first)
              .add("exceptionMSG", ex.getMessageValue());
        lc.log(log::ERR, "In OStoreDB::ArchiveMount::setJobBatchTransferred(): failed to queue a batch of requests for reporting.");
        lc.logBacktrace(log::ERR, ex.backtrace());
      }
    }
    timingList.insertAndReset("queueingToReportTime", t);
  }
  if (ajToUnown.size()) {
    m_oStoreDB.m_agentReference->removeBatchFromOwnership(ajToUnown, m_oStoreDB.m_objectStore);
    timingList.insertAndReset("removeFromOwnershipTime", t);
  }
  {
    log::ScopedParamContainer params(lc);
    params.add("QueuedRequests", jobsToQueueForReporting.size())
          .add("PartiallyCompleteRequests", ajToUnown.size());
    timingList.addToLog(params);
    lc.log(log::INFO, "In OStoreDB::ArchiveMount::setJobBatchTransferred(): set ArchiveRequests successful and queued for reporting.");
  }
}


//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::failTransfer()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveJob::failTransfer(const std::string& failureReason, log::LogContext& lc) {
  if (!m_jobOwned)
    throw JobNotOwned("In OStoreDB::ArchiveJob::failTransfer: cannot fail a job not owned");
  // Lock the archive request. Fail the job.
  objectstore::ScopedExclusiveLock arl(m_archiveRequest);
  m_archiveRequest.fetch();
  // Add a job failure. We will know what to do next..
  typedef objectstore::ArchiveRequest::EnqueueingNextStep EnqueueingNextStep;
  typedef EnqueueingNextStep::NextStep NextStep;
  EnqueueingNextStep enQueueingNextStep = 
      m_archiveRequest.addTransferFailure(tapeFile.copyNb, m_mountId, failureReason, lc);
  // First set the job status
  m_archiveRequest.setJobStatus(tapeFile.copyNb, enQueueingNextStep.nextStatus);
  // Now apply the decision.
  // TODO: this will probably need to be factored out.
  switch (enQueueingNextStep.nextStep) {
  case NextStep::Nothing: {
      m_archiveRequest.commit();
      auto retryStatus = m_archiveRequest.getRetryStatus(tapeFile.copyNb);
      log::ScopedParamContainer params(lc);
      params.add("fileId", archiveFile.archiveFileID)
            .add("copyNb", tapeFile.copyNb)
            .add("failureReason", failureReason)
            .add("requestObject", m_archiveRequest.getAddressIfSet())
            .add("retriesWithinMount", retryStatus.retriesWithinMount)
            .add("maxRetriesWithinMount", retryStatus.maxRetriesWithinMount)
            .add("totalRetries", retryStatus.totalRetries)
            .add("maxTotalRetries", retryStatus.maxTotalRetries);
      lc.log(log::INFO,
          "In ArchiveJob::failTransfer(): left the request owned, to be garbage collected for retry at the end of the mount.");
      return;
    }
  case NextStep::Delete: {
      auto retryStatus = m_archiveRequest.getRetryStatus(tapeFile.copyNb);
      m_archiveRequest.remove();
      log::ScopedParamContainer params(lc);
      params.add("fileId", archiveFile.archiveFileID)
            .add("copyNb", tapeFile.copyNb)
            .add("failureReason", failureReason)
            .add("requestObject", m_archiveRequest.getAddressIfSet())
            .add("retriesWithinMount", retryStatus.retriesWithinMount)
            .add("maxRetriesWithinMount", retryStatus.maxRetriesWithinMount)
            .add("totalRetries", retryStatus.totalRetries)
            .add("maxTotalRetries", retryStatus.maxTotalRetries);
      lc.log(log::INFO, "In ArchiveJob::failTransfer(): removed request");
      return;
    }
  case NextStep::EnqueueForReport: {
      m_archiveRequest.commit();
      auto tapepool = m_archiveRequest.getTapePoolForJob(tapeFile.copyNb);
      auto retryStatus = m_archiveRequest.getRetryStatus(tapeFile.copyNb);
      // Algorithms suppose the objects are not locked.
      arl.release();
      typedef objectstore::ContainerAlgorithms<ArchiveQueue,ArchiveQueueToReport> CaAqtr;
      CaAqtr caAqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
      CaAqtr::InsertedElement::list insertedElements;
      insertedElements.push_back(CaAqtr::InsertedElement{&m_archiveRequest, tapeFile.copyNb, archiveFile, cta::nullopt, cta::nullopt });
      caAqtr.referenceAndSwitchOwnership(tapepool, insertedElements, lc);
      log::ScopedParamContainer params(lc);
      params.add("fileId", archiveFile.archiveFileID)
            .add("copyNb", tapeFile.copyNb)
            .add("failureReason", failureReason)
            .add("requestObject", m_archiveRequest.getAddressIfSet())
            .add("retriesWithinMount", retryStatus.retriesWithinMount)
            .add("maxRetriesWithinMount", retryStatus.maxRetriesWithinMount)
            .add("totalRetries", retryStatus.totalRetries)
            .add("maxTotalRetries", retryStatus.maxTotalRetries);
      lc.log(log::INFO, "In ArchiveJob::failTransfer(): enqueued job for reporting");
      return;
    }
  case NextStep::EnqueueForTransfer: {
      m_archiveRequest.commit();
      auto tapepool = m_archiveRequest.getTapePoolForJob(tapeFile.copyNb);
      auto retryStatus = m_archiveRequest.getRetryStatus(tapeFile.copyNb);
      // Algorithms suppose the objects are not locked.
      arl.release();
      typedef objectstore::ContainerAlgorithms<ArchiveQueue,ArchiveQueueToTransfer> CaAqtr;
      CaAqtr caAqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
      CaAqtr::InsertedElement::list insertedElements;
      insertedElements.push_back(CaAqtr::InsertedElement{&m_archiveRequest, tapeFile.copyNb, archiveFile, cta::nullopt, cta::nullopt });
      caAqtr.referenceAndSwitchOwnership(tapepool, insertedElements, lc);
      log::ScopedParamContainer params(lc);
      params.add("fileId", archiveFile.archiveFileID)
            .add("copyNb", tapeFile.copyNb)
            .add("failureReason", failureReason)
            .add("requestObject", m_archiveRequest.getAddressIfSet())
            .add("retriesWithinMount", retryStatus.retriesWithinMount)
            .add("maxRetriesWithinMount", retryStatus.maxRetriesWithinMount)
            .add("totalRetries", retryStatus.totalRetries)
            .add("maxTotalRetries", retryStatus.maxTotalRetries);
      lc.log(log::INFO,
          "In ArchiveJob::failTransfer(): requeued job for (potentially in-mount) retry.");
      return;
    }
  case NextStep::StoreInFailedJobsContainer: {
      m_archiveRequest.commit();
      auto tapepool = m_archiveRequest.getTapePoolForJob(tapeFile.copyNb);
      auto retryStatus = m_archiveRequest.getRetryStatus(tapeFile.copyNb);
      // Algorithms suppose the objects are not locked.
      arl.release();
      typedef objectstore::ContainerAlgorithms<ArchiveQueue,ArchiveQueueFailed> CaAqtr;
      CaAqtr caAqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
      CaAqtr::InsertedElement::list insertedElements;
      insertedElements.push_back(CaAqtr::InsertedElement{&m_archiveRequest, tapeFile.copyNb, archiveFile, cta::nullopt, cta::nullopt });
      caAqtr.referenceAndSwitchOwnership(tapepool, insertedElements, lc);
      log::ScopedParamContainer params(lc);
      params.add("fileId", archiveFile.archiveFileID)
            .add("copyNb", tapeFile.copyNb)
            .add("failureReason", failureReason)
            .add("requestObject", m_archiveRequest.getAddressIfSet())
            .add("retriesWithinMount", retryStatus.retriesWithinMount)
            .add("maxRetriesWithinMount", retryStatus.maxRetriesWithinMount)
            .add("totalRetries", retryStatus.totalRetries)
            .add("maxTotalRetries", retryStatus.maxTotalRetries);
      lc.log(log::INFO,
          "In ArchiveJob::failTransfer(): stored job in failed container for operator handling.");
      return;
    }
  }
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::failReport()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveJob::failReport(const std::string& failureReason, log::LogContext& lc) {
  if (!m_jobOwned)
    throw JobNotOwned("In OStoreDB::ArchiveJob::failReport: cannot fail a job not owned");
  // Lock the archive request. Fail the job.
  objectstore::ScopedExclusiveLock arl(m_archiveRequest);
  m_archiveRequest.fetch();
  // Add a job failure. We will know what to do next..
  typedef objectstore::ArchiveRequest::EnqueueingNextStep EnqueueingNextStep;
  typedef EnqueueingNextStep::NextStep NextStep;
  EnqueueingNextStep enQueueingNextStep = 
      m_archiveRequest.addReportFailure(tapeFile.copyNb, m_mountId, failureReason, lc);
  // First set the job status
  m_archiveRequest.setJobStatus(tapeFile.copyNb, enQueueingNextStep.nextStatus);
  // Now apply the decision.
  // TODO: this will probably need to be factored out.
  switch (enQueueingNextStep.nextStep) {
    // We have a reduced set of supported next steps as some are not compatible with this event (see default).
  case NextStep::EnqueueForReport: {
      m_archiveRequest.commit();
      auto tapepool = m_archiveRequest.getTapePoolForJob(tapeFile.copyNb);
      auto retryStatus = m_archiveRequest.getRetryStatus(tapeFile.copyNb);
      // Algorithms suppose the objects are not locked.
      arl.release();
      typedef objectstore::ContainerAlgorithms<ArchiveQueue,ArchiveQueueToReport> CaAqtr;
      CaAqtr caAqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
      CaAqtr::InsertedElement::list insertedElements;
      insertedElements.push_back(CaAqtr::InsertedElement{&m_archiveRequest, tapeFile.copyNb, archiveFile, cta::nullopt, cta::nullopt });
      caAqtr.referenceAndSwitchOwnership(tapepool, insertedElements, lc);
      log::ScopedParamContainer params(lc);
      params.add("fileId", archiveFile.archiveFileID)
            .add("copyNb", tapeFile.copyNb)
            .add("failureReason", failureReason)
            .add("requestObject", m_archiveRequest.getAddressIfSet())
            .add("reportRetries", retryStatus.reportRetries)
            .add("maxReportRetries", retryStatus.maxReportRetries);
      lc.log(log::INFO, "In ArchiveJob::failReport(): requeued job for report retry.");
      return;
    }
  default: {
      m_archiveRequest.commit();
      auto tapepool = m_archiveRequest.getTapePoolForJob(tapeFile.copyNb);
      auto retryStatus = m_archiveRequest.getRetryStatus(tapeFile.copyNb);
      // Algorithms suppose the objects are not locked.
      arl.release();
      typedef objectstore::ContainerAlgorithms<ArchiveQueue,ArchiveQueueFailed> CaAqtr;
      CaAqtr caAqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
      CaAqtr::InsertedElement::list insertedElements;
      insertedElements.push_back(CaAqtr::InsertedElement{&m_archiveRequest, tapeFile.copyNb, archiveFile, cta::nullopt, cta::nullopt });
      caAqtr.referenceAndSwitchOwnership(tapepool, insertedElements, lc);
      log::ScopedParamContainer params(lc);
      params.add("fileId", archiveFile.archiveFileID)
            .add("copyNb", tapeFile.copyNb)
            .add("failureReason", failureReason)
            .add("requestObject", m_archiveRequest.getAddressIfSet())
            .add("reportRetries", retryStatus.reportRetries)
            .add("maxReportRetries", retryStatus.maxReportRetries);
      if (enQueueingNextStep.nextStep == NextStep::StoreInFailedJobsContainer)
        lc.log(log::INFO,
            "In ArchiveJob::failReport(): stored job in failed container for operator handling.");
      else
        lc.log(log::ERR,
            "In ArchiveJob::failReport(): stored job in failed container after unexpected next step.");
      return;
    }
  }
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::bumpUpTapeFileCount()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveJob::bumpUpTapeFileCount(uint64_t newFileCount) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
//  m_archiveMount.setTapeMaxFileCount(newFileCount);
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::asyncSucceed()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveJob::asyncSucceedTransfer() {
  log::LogContext lc(m_oStoreDB.m_logger);
  log::ScopedParamContainer params(lc);
  params.add("requestObject", m_archiveRequest.getAddressIfSet());
  lc.log(log::DEBUG, "Will start async update archiveRequest for transfer success");
  m_succesfulTransferUpdater.reset(m_archiveRequest.asyncUpdateTransferSuccessful(tapeFile.copyNb));
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::waitAsyncSucceed()
//------------------------------------------------------------------------------
bool OStoreDB::ArchiveJob::waitAsyncSucceed() {  
  m_succesfulTransferUpdater->wait();
  log::LogContext lc(m_oStoreDB.m_logger);
  log::ScopedParamContainer params(lc);
  params.add("requestObject", m_archiveRequest.getAddressIfSet());
  lc.log(log::DEBUG, "Async update of archiveRequest for transfer success complete");
  // We no more own the job (which could be gone)
  m_jobOwned = false;
  // Ownership removal will be done globally by the caller.
  return m_succesfulTransferUpdater->m_doReportTransferSuccess;
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::asyncDeleteRequest()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveJob::asyncDeleteRequest() {
  log::LogContext lc(m_oStoreDB.m_logger);
  log::ScopedParamContainer params(lc);
  params.add("requestObject", m_archiveRequest.getAddressIfSet());
  lc.log(log::DEBUG, "Will start async delete archiveRequest");
  m_requestDeleter.reset(m_archiveRequest.asyncDeleteRequest());
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::waitAsyncDelete()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveJob::waitAsyncDelete() {
  m_requestDeleter->wait();
  log::LogContext lc(m_oStoreDB.m_logger);
  log::ScopedParamContainer params(lc);
  params.add("requestObject", m_archiveRequest.getAddressIfSet());
  lc.log(log::DEBUG, "Async delete of archiveRequest complete");
  // We no more own the job (which could be gone)
  m_jobOwned = false;
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::~ArchiveJob()
//------------------------------------------------------------------------------
OStoreDB::ArchiveJob::~ArchiveJob() {
  if (m_jobOwned) {
    log::LogContext lc(m_oStoreDB.m_logger);
    // Just log that the object will be left in agent's ownership.
    log::ScopedParamContainer params(lc);
    params.add("agentObject", m_oStoreDB.m_agentReference->getAgentAddress())
          .add("jobObject", m_archiveRequest.getAddressIfSet());
    lc.log(log::INFO, "In OStoreDB::ArchiveJob::~ArchiveJob(): will leave the job owned after destruction.");
  }
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::failTransfer()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveJob::failTransfer(const std::string &failureReason, log::LogContext &lc) {
  typedef objectstore::RetrieveRequest::EnqueueingNextStep EnqueueingNextStep;
  typedef EnqueueingNextStep::NextStep NextStep;

  if (!m_jobOwned)
    throw JobNotOwned("In OStoreDB::RetrieveJob::failTransfer: cannot fail a job not owned");

  // Lock the retrieve request. Fail the job.
  objectstore::ScopedExclusiveLock rel(m_retrieveRequest);
  m_retrieveRequest.fetch();

  // Add a job failure. We will know what to do next.
  EnqueueingNextStep enQueueingNextStep = m_retrieveRequest.addTransferFailure(selectedCopyNb, m_mountId,
    failureReason, lc);

  // First set the job status
  m_retrieveRequest.setJobStatus(selectedCopyNb, enQueueingNextStep.nextStatus);

  // Now apply the decision
  //
  // TODO: this will probably need to be factored out
  switch(enQueueingNextStep.nextStep)
  {
    case NextStep::Nothing: {
      m_retrieveRequest.commit();
      auto retryStatus = m_retrieveRequest.getRetryStatus(selectedCopyNb);
      log::ScopedParamContainer params(lc);
      params.add("fileId", archiveFile.archiveFileID)
            .add("copyNb", selectedCopyNb)
            .add("failureReason", failureReason)
            .add("requestObject", m_retrieveRequest.getAddressIfSet())
            .add("retriesWithinMount", retryStatus.retriesWithinMount)
            .add("maxRetriesWithinMount", retryStatus.maxRetriesWithinMount)
            .add("totalRetries", retryStatus.totalRetries)
            .add("maxTotalRetries", retryStatus.maxTotalRetries);
      lc.log(log::INFO,
          "In RetrieveJob::failTransfer(): left the request owned, to be garbage collected for retry at the end of the mount.");
      return;
    }

    case NextStep::Delete: {
      auto retryStatus = m_retrieveRequest.getRetryStatus(selectedCopyNb);
      m_retrieveRequest.remove();
      log::ScopedParamContainer params(lc);
      params.add("fileId", archiveFile.archiveFileID)
            .add("copyNb", selectedCopyNb)
            .add("failureReason", failureReason)
            .add("requestObject", m_retrieveRequest.getAddressIfSet())
            .add("retriesWithinMount", retryStatus.retriesWithinMount)
            .add("maxRetriesWithinMount", retryStatus.maxRetriesWithinMount)
            .add("totalRetries", retryStatus.totalRetries)
            .add("maxTotalRetries", retryStatus.maxTotalRetries);
      lc.log(log::INFO, "In RetrieveJob::failTransfer(): removed request");
      return;
    }

    case NextStep::EnqueueForReport: {
      typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueToReport> CaRqtr;

      // Algorithms suppose the objects are not locked
      auto  retryStatus = m_retrieveRequest.getRetryStatus(selectedCopyNb);
      auto  rfqc        = m_retrieveRequest.getRetrieveFileQueueCriteria();
      auto &af          = rfqc.archiveFile;

      std::set<std::string> candidateVids;
      for(auto &tf: af.tapeFiles) {
        if(m_retrieveRequest.getJobStatus(tf.second.copyNb) == serializers::RetrieveJobStatus::RJS_ToReportForFailure)
          candidateVids.insert(tf.second.vid);
      }
      if(candidateVids.empty()) {
        throw cta::exception::Exception(
          "In OStoreDB::RetrieveJob::failTransfer(): no active job after addJobFailure() returned false."
        );
      }

      // Check that the requested retrieve job (for the provided VID) exists, and record the copy number
      std::string bestVid = Helpers::selectBestRetrieveQueue(candidateVids, m_oStoreDB.m_catalogue,
        m_oStoreDB.m_objectStore);

      auto tf_it = af.tapeFiles.begin();
      for( ; tf_it != af.tapeFiles.end() && tf_it->second.vid != bestVid; ++tf_it) ;
      if(tf_it == af.tapeFiles.end()) {
        std::stringstream err;
        err << "In OStoreDB::RetrieveJob::failTransfer(): no tape file for requested vid."
            << " archiveId=" << af.archiveFileID << " vid=" << bestVid;
        throw RetrieveRequestHasNoCopies(err.str());
      }
      auto &tf = tf_it->second;

      CaRqtr::InsertedElement::list insertedElements;
      insertedElements.push_back(CaRqtr::InsertedElement{
        &m_retrieveRequest, tf.copyNb, tf.fSeq, af.fileSize, rfqc.mountPolicy, serializers::RetrieveJobStatus::RJS_Failed
      });
      m_retrieveRequest.commit();
      rel.release();

      CaRqtr caRqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
      caRqtr.referenceAndSwitchOwnership(tf.vid, insertedElements, lc);
      log::ScopedParamContainer params(lc);
      params.add("fileId", archiveFile.archiveFileID)
            .add("copyNb", selectedCopyNb)
            .add("failureReason", failureReason)
            .add("requestObject", m_retrieveRequest.getAddressIfSet())
            .add("retriesWithinMount", retryStatus.retriesWithinMount)
            .add("maxRetriesWithinMount", retryStatus.maxRetriesWithinMount)
            .add("totalRetries", retryStatus.totalRetries)
            .add("maxTotalRetries", retryStatus.maxTotalRetries);
      lc.log(log::INFO, "In RetrieveJob::failTransfer(): enqueued job for reporting");
      return;
    }

    case NextStep::EnqueueForTransfer: {
      typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransfer> CaRqtr;

      // Algorithms suppose the objects are not locked
      auto  retryStatus = m_retrieveRequest.getRetryStatus(selectedCopyNb);
      auto  rfqc        = m_retrieveRequest.getRetrieveFileQueueCriteria();
      auto &af          = rfqc.archiveFile;

      std::set<std::string> candidateVids;
      for(auto &tf : af.tapeFiles) {
        if(m_retrieveRequest.getJobStatus(tf.second.copyNb) == serializers::RetrieveJobStatus::RJS_ToTransfer)
          candidateVids.insert(tf.second.vid);
      }
      if(candidateVids.empty()) {
        throw cta::exception::Exception(
          "In OStoreDB::RetrieveJob::failTransfer(): no active job after addJobFailure() returned false."
        );
      }
      m_retrieveRequest.commit();
      rel.release();

      // Check that the requested retrieve job (for the provided VID) exists, and record the copy number
      std::string bestVid = Helpers::selectBestRetrieveQueue(candidateVids, m_oStoreDB.m_catalogue,
        m_oStoreDB.m_objectStore);

      auto tf_it = af.tapeFiles.begin();
      for( ; tf_it != af.tapeFiles.end() && tf_it->second.vid != bestVid; ++tf_it) ;
      if(tf_it == af.tapeFiles.end()) {
        std::stringstream err;
        err << "In OStoreDB::RetrieveJob::failTransfer(): no tape file for requested vid."
            << " archiveId=" << af.archiveFileID << " vid=" << bestVid;
        throw RetrieveRequestHasNoCopies(err.str());
      }
      auto &tf = tf_it->second;

      CaRqtr::InsertedElement::list insertedElements;
      insertedElements.push_back(CaRqtr::InsertedElement{
        &m_retrieveRequest, tf.copyNb, tf.fSeq, af.fileSize, rfqc.mountPolicy, serializers::RetrieveJobStatus::RJS_ToTransfer
      });

      CaRqtr caRqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
      caRqtr.referenceAndSwitchOwnership(bestVid, insertedElements, lc);
      log::ScopedParamContainer params(lc);
      params.add("fileId", archiveFile.archiveFileID)
            .add("copyNb", selectedCopyNb)
            .add("failureReason", failureReason)
            .add("requestObject", m_retrieveRequest.getAddressIfSet())
            .add("retriesWithinMount", retryStatus.retriesWithinMount)
            .add("maxRetriesWithinMount", retryStatus.maxRetriesWithinMount)
            .add("totalRetries", retryStatus.totalRetries)
            .add("maxTotalRetries", retryStatus.maxTotalRetries);
      lc.log(log::INFO, "In RetrieveJob::failTransfer(): requeued job for (potentially in-mount) retry.");
      return;
    }

    case NextStep::StoreInFailedJobsContainer:
      // For retrieve queues, once the job has been queued for report, we don't retry any more, so we
      // can never arrive at this case
      throw cta::exception::Exception("In OStoreDB::RetrieveJob::failTransfer(): case NextStep::StoreInFailedJobsContainer is not implemented");
  }
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::failReport()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveJob::failReport(const std::string &failureReason, log::LogContext &lc) {
  typedef objectstore::RetrieveRequest::EnqueueingNextStep EnqueueingNextStep;
  typedef EnqueueingNextStep::NextStep NextStep;

  if(!m_jobOwned)
    throw JobNotOwned("In OStoreDB::RetrieveJob::failReport: cannot fail a job not owned");

  // Lock the retrieve request. Fail the job.
  objectstore::ScopedExclusiveLock rrl(m_retrieveRequest);
  m_retrieveRequest.fetch();

  // Algorithms suppose the objects are not locked
  auto  rfqc = m_retrieveRequest.getRetrieveFileQueueCriteria();
  auto &af   = rfqc.archiveFile;

  // Handle report failures for all tape copies in turn
  for(auto &aftf: af.tapeFiles) {
    auto &tf = aftf.second;

    // Add a job failure and decide what to do next
    EnqueueingNextStep enQueueingNextStep = m_retrieveRequest.addReportFailure(tf.copyNb, m_mountId, failureReason, lc);

    // Set the job status
    m_retrieveRequest.setJobStatus(tf.copyNb, enQueueingNextStep.nextStatus);
    m_retrieveRequest.commit();
    auto retryStatus = m_retrieveRequest.getRetryStatus(tf.copyNb);
    // Algorithms suppose the objects are not locked.
    rrl.release();

    // Apply the decision
    switch (enQueueingNextStep.nextStep) {
      // We have a reduced set of supported next steps as some are not compatible with this event
      case NextStep::EnqueueForReport: {
        typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueToReport> CaRqtr;
        CaRqtr caRqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
        CaRqtr::InsertedElement::list insertedElements;
        insertedElements.push_back(CaRqtr::InsertedElement{
          &m_retrieveRequest, tf.copyNb, tf.fSeq, af.fileSize, rfqc.mountPolicy, serializers::RetrieveJobStatus::RJS_ToReportForFailure
        });
        caRqtr.referenceAndSwitchOwnership(tf.vid, insertedElements, lc);
        log::ScopedParamContainer params(lc);
        params.add("fileId", archiveFile.archiveFileID)
              .add("copyNb", tf.copyNb)
              .add("failureReason", failureReason)
              .add("requestObject", m_retrieveRequest.getAddressIfSet())
              .add("totalReportRetries", retryStatus.totalReportRetries)
              .add("maxReportRetries", retryStatus.maxReportRetries);
        lc.log(log::INFO, "In RetrieveJob::failReport(): requeued job for report retry.");
        return;
      }
      default: {
        typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueFailed> CaRqtr;
        CaRqtr caRqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
        CaRqtr::InsertedElement::list insertedElements;
        insertedElements.push_back(CaRqtr::InsertedElement{
          &m_retrieveRequest, tf.copyNb, tf.fSeq, af.fileSize, rfqc.mountPolicy, serializers::RetrieveJobStatus::RJS_Failed
        });
        caRqtr.referenceAndSwitchOwnership(tf.vid, insertedElements, lc);
        log::ScopedParamContainer params(lc);
        params.add("fileId", archiveFile.archiveFileID)
              .add("copyNb", tf.copyNb)
              .add("failureReason", failureReason)
              .add("requestObject", m_retrieveRequest.getAddressIfSet())
              .add("totalReportRetries", retryStatus.totalReportRetries)
              .add("maxReportRetries", retryStatus.maxReportRetries);
        if (enQueueingNextStep.nextStep == NextStep::StoreInFailedJobsContainer)
          lc.log(log::INFO,
              "In RetrieveJob::failReport(): stored job in failed container for operator handling.");
        else
          lc.log(log::ERR,
              "In RetrieveJob::failReport(): stored job in failed container after unexpected next step.");
        return;
      }
    }
  }
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::~RetrieveJob()
//------------------------------------------------------------------------------
OStoreDB::RetrieveJob::~RetrieveJob() {
  if (m_jobOwned) {
    log::LogContext lc(m_oStoreDB.m_logger);
    // Just log that the object will be left in agent's ownership.
    log::ScopedParamContainer params(lc);
    params.add("agentObject", m_oStoreDB.m_agentReference->getAgentAddress())
          .add("jobObject", m_retrieveRequest.getAddressIfSet());
    lc.log(log::INFO, "In OStoreDB::RetrieveJob::~RetrieveJob(): will leave the job owned after destruction.");
  }
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::asyncSucceed()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveJob::asyncSucceed() {
  // set the request as successful (delete it).
  m_jobDelete.reset(m_retrieveRequest.asyncDeleteJob());
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::checkSucceed()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveJob::checkSucceed() {
  m_jobDelete->wait();
  m_retrieveRequest.resetValues();
  // We no more own the job (which could be gone)
  m_jobOwned = false;
  // Ownership will be removed from agent by caller through retrieve mount object.
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::asyncReportSucceedForRepack()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveJob::asyncReportSucceedForRepack()
{
  m_jobSucceedForRepackReporter.reset(m_retrieveRequest.asyncReportSucceedForRepack(this->selectedCopyNb));
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::checkReportSucceedForRepack()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveJob::checkReportSucceedForRepack(){
  m_jobSucceedForRepackReporter->wait();
}

std::list<std::unique_ptr<cta::objectstore::RetrieveRequest>> OStoreDB::getNextSucceededRetrieveRequestForRepackBatch(uint64_t filesRequested, log::LogContext& lc)
{
  std::list<std::unique_ptr<cta::objectstore::RetrieveRequest>> ret;
  typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueToReportToRepackForSuccess> Carqtrtrfs;
  Carqtrtrfs algo(this->m_objectStore, *m_agentReference);
  // Decide from which queue we are going to pop.
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  while(true) {
    auto queueList = re.dumpRetrieveQueues(JobQueueType::JobsToReportToRepackForSuccess);
    if (queueList.empty()) return ret;

    // Try to get jobs from the first queue. If it is empty, it will be trimmed, so we can go for another round.
    Carqtrtrfs::PopCriteria criteria;
    criteria.files = filesRequested;
    auto jobs = algo.popNextBatch(queueList.front().vid, criteria, lc);
    if(jobs.elements.empty()) continue;
    for(auto &j : jobs.elements)
    {
      ret.emplace_back(std::move(j.retrieveRequest));
    }
    return ret;
  }
}

} // namespace cta
