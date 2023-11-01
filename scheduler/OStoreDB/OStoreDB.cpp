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

#include <algorithm>
#include <bits/unique_ptr.h>
#include <cmath>
#include <iostream>
#include <limits>
#include <numeric>      /* std::accumulate */
#include <set>
#include <stdexcept>
#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */
#include <tuple>

#include "catalogue/Catalogue.hpp"
#include "catalogue/TapeDrivesCatalogueState.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/utils/utils.hpp"
#include "disk/DiskFile.hpp"
#include "MemQueues.hpp"
#include "objectstore/AgentWrapper.hpp"
#include "objectstore/ArchiveQueueAlgorithms.hpp"
#include "objectstore/DriveRegister.hpp"
#include "objectstore/Helpers.hpp"
#include "objectstore/RepackIndex.hpp"
#include "objectstore/RepackQueue.hpp"
#include "objectstore/RepackQueueAlgorithms.hpp"
#include "objectstore/RepackRequest.hpp"
#include "objectstore/Sorter.hpp"
#include "Scheduler.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/OStoreDB/OStoreDB.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/TapeSessionStats.hpp"

namespace cta {
using namespace objectstore;

//------------------------------------------------------------------------------
// OStoreDB::OStoreDB()
//------------------------------------------------------------------------------
OStoreDB::OStoreDB(objectstore::Backend& be, catalogue::Catalogue & catalogue, log::Logger &logger):
  m_taskQueueSize(0), m_taskPostingSemaphore(5), m_objectStore(be), m_catalogue(catalogue), m_logger(logger) {
  m_tapeDrivesState = std::make_unique<TapeDrivesCatalogueState>(m_catalogue);
  for (size_t i=0; i<5; i++) {
    m_enqueueingWorkerThreads.emplace_back(new EnqueueingWorkerThread(m_enqueueingTasksQueue));
    m_enqueueingWorkerThreads.back()->start();
  }
}

//------------------------------------------------------------------------------
// OStoreDB::~OStoreDB()
//------------------------------------------------------------------------------
OStoreDB::~OStoreDB() {
  while(m_taskQueueSize) sleep(1);

  for(__attribute__((unused)) auto& t : m_enqueueingWorkerThreads) {
    try {
      m_enqueueingTasksQueue.push(nullptr);
    } catch(const exception::Exception& ex) {
      log::LogContext lc(m_logger);
      log::ScopedParamContainer params(lc);
      params.add("exceptionMessage", ex.what());
      lc.log(log::ERR, "In OStoreDB::~OStoreDB(): caught unexpected exception.");
    }
  }
  for(auto& t : m_enqueueingWorkerThreads) {
    t->wait();
    delete t;
    t = nullptr;
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
  // This method is only used by unit tests so calling usleep() is good enough
  while (m_taskQueueSize) ::usleep(1000);
}

//------------------------------------------------------------------------------
// OStoreDB::setThreadNumber()
//------------------------------------------------------------------------------
void OStoreDB::setThreadNumber(uint64_t threadNumber, const std::optional<size_t> &stackSize) {
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
    m_enqueueingWorkerThreads.emplace_back(new EnqueueingWorkerThread(m_enqueueingTasksQueue, stackSize));
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
    (*et)();
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
// OStoreDB::fetchRetrieveQueueCleanupInfo()
//------------------------------------------------------------------------------
std::list<SchedulerDatabase::RetrieveQueueCleanupInfo> OStoreDB::getRetrieveQueuesCleanupInfo(log::LogContext& logContext) {

  utils::Timer t;
  assertAgentAddressSet();
  std::list<SchedulerDatabase::RetrieveQueueCleanupInfo> ret;

  // Get all tapes with retrieve queues
  objectstore::RootEntry re(m_objectStore);
  re.fetchNoLock();

  // Walk the retrieve queues for cleanup flag
  for (auto &rqp: re.dumpRetrieveQueues(common::dataStructures::JobQueueType::JobsToTransferForUser)) {
    RetrieveQueue rqueue(rqp.address, m_objectStore);

    try {
      rqueue.fetchNoLock();
    } catch (cta::exception::Exception &ex) {
      log::ScopedParamContainer params(logContext);
      params.add("queueObject", rqp.address)
              .add("tapeVid", rqp.vid)
              .add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::DEBUG,
                     "WARNING: In OStoreDB::getRetrieveQueuesCleanupInfo(): failed to lock/fetch a retrieve queue. Skipping it.");
      continue;
    }

    ret.push_back(SchedulerDatabase::RetrieveQueueCleanupInfo());
    ret.back().vid = rqueue.getVid();
    ret.back().doCleanup = rqueue.getQueueCleanupDoCleanup();
    ret.back().assignedAgent = rqueue.getQueueCleanupAssignedAgent();
    ret.back().heartbeat = rqueue.getQueueCleanupHeartbeat();
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::fetchMountInfo()
//------------------------------------------------------------------------------
void OStoreDB::fetchMountInfo(SchedulerDatabase::TapeMountDecisionInfo& tmdi, RootEntry& re,
  SchedulerDatabase::PurposeGetMountInfo /* not used */, log::LogContext & logContext) {
  utils::Timer t, t2;
  std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue.MountPolicy()->getCachedMountPolicies();
  // Walk the archive queues for USER for statistics
  for (auto & aq_dump : re.dumpArchiveQueues(common::dataStructures::JobQueueType::JobsToTransferForUser)) {
    objectstore::ArchiveQueue aqueue(aq_dump.address, m_objectStore);
    // debug utility variable
    std::string __attribute__((__unused__)) poolName = aq_dump.cId;
    objectstore::ScopedSharedLock aqlock;
    double queueLockTime = 0;
    double queueFetchTime = 0;
    try {
      aqueue.fetchNoLock();
      queueFetchTime = t.secs(utils::Timer::resetCounter);
    } catch (cta::exception::Exception &ex) {
      log::ScopedParamContainer params(logContext);
      params.add("queueObject", aq_dump.address)
            .add("containerId", aq_dump.cId)
            .add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::DEBUG, "WARNING: In OStoreDB::fetchMountInfo(): failed to lock/fetch an archive queue for user. Skipping it.");
      continue;
    }
    // If there are files queued, we create an entry for this tape pool in the
    // mount candidates list.
    cta::objectstore::ArchiveQueue::JobsSummary aqueueJobsSummary = aqueue.getJobsSummary();
    if (aqueueJobsSummary.jobs) {
      tmdi.potentialMounts.push_back(SchedulerDatabase::PotentialMount());
      auto & m = tmdi.potentialMounts.back();
      m.tapePool = aq_dump.cId;
      m.type = cta::common::dataStructures::MountType::ArchiveForUser;
      m.bytesQueued = aqueueJobsSummary.bytes;
      m.filesQueued = aqueueJobsSummary.jobs;
      m.oldestJobStartTime = aqueueJobsSummary.oldestJobStartTime;
      m.youngestJobStartTime = aqueueJobsSummary.youngestJobStartTime;
      //By default, we get the mountPolicies from the objectstore's queue counters
      m.priority = aqueueJobsSummary.priority;
      m.minRequestAge = aqueueJobsSummary.minArchiveRequestAge;
      //If there are mount policies in the Catalogue
      if(mountPolicies.size()) {
        //We get all the mount policies that are on the queue from the catalogue list
        auto mountPoliciesInQueueList = getMountPoliciesInQueue(mountPolicies,aqueueJobsSummary.mountPolicyCountMap);
        m.mountPolicyNames = std::list<std::string>();
        //If an operator removed the queue mountPolicies from the catalogue, we will have no results...
        if(mountPoliciesInQueueList.size()){
          std::transform(mountPoliciesInQueueList.begin(), mountPoliciesInQueueList.end(), std::back_inserter(m.mountPolicyNames.value()),
            [](const cta::common::dataStructures::MountPolicy &policy) -> std::string {return policy.name;});
          auto mountPolicyToUse = createBestArchiveMountPolicy(mountPoliciesInQueueList);
          m.priority = mountPolicyToUse.archivePriority;
          m.minRequestAge = mountPolicyToUse.archiveMinRequestAge;
          m.highestPriorityMountPolicyName = getHighestPriorityArchiveMountPolicyName(mountPoliciesInQueueList);
          m.lowestRequestAgeMountPolicyName = getLowestRequestAgeArchiveMountPolicyName(mountPoliciesInQueueList);
        }
      }
      m.logicalLibrary = "";
    } else {
      tmdi.queueTrimRequired = true;
    }
    auto processingTime = t.secs(utils::Timer::resetCounter);
    log::ScopedParamContainer params(logContext);
    params.add("queueObject", aq_dump.address)
          .add("containerId", aq_dump.cId)
          .add("queueType", toString(cta::common::dataStructures::MountType::ArchiveForUser))
          .add("queueLockTime", queueLockTime)
          .add("queueFetchTime", queueFetchTime)
          .add("processingTime", processingTime);
    if(queueLockTime > 1 || queueFetchTime > 1) {
      logContext.log(log::WARNING, "In OStoreDB::fetchMountInfo(): fetched an archive for user queue and that lasted more than 1 second.");
    }
  }
  // Walk the archive queues for REPACK for statistics
  for (auto & aq_dump : re.dumpArchiveQueues(common::dataStructures::JobQueueType::JobsToTransferForRepack)) {
    objectstore::ArchiveQueue aqueue(aq_dump.address, m_objectStore);
    // debug utility variable
    std::string __attribute__((__unused__)) poolName = aq_dump.cId;
    objectstore::ScopedSharedLock aqlock;
    double queueLockTime = 0;
    double queueFetchTime = 0;
    try {
      aqueue.fetchNoLock();
      queueFetchTime = t.secs(utils::Timer::resetCounter);
    } catch (cta::exception::Exception &ex) {
      log::ScopedParamContainer params(logContext);
      params.add("queueObject", aq_dump.address)
            .add("tapePool", aq_dump.cId)
            .add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::DEBUG, "WARNING: In OStoreDB::fetchMountInfo(): failed to lock/fetch an archive queue for repack. Skipping it.");
      continue;
    }
    // If there are files queued, we create an entry for this tape pool in the
    // mount candidates list.
    cta::objectstore::ArchiveQueue::JobsSummary aqueueRepackJobsSummary = aqueue.getJobsSummary();
    if (aqueueRepackJobsSummary.jobs) {
      tmdi.potentialMounts.push_back(SchedulerDatabase::PotentialMount());
      auto & m = tmdi.potentialMounts.back();
      m.tapePool = aq_dump.cId;
      m.type = cta::common::dataStructures::MountType::ArchiveForRepack;
      m.bytesQueued = aqueueRepackJobsSummary.bytes;
      m.filesQueued = aqueueRepackJobsSummary.jobs;
      m.oldestJobStartTime = aqueueRepackJobsSummary.oldestJobStartTime;
      m.youngestJobStartTime = aqueueRepackJobsSummary.youngestJobStartTime;
      m.priority = aqueueRepackJobsSummary.priority;
      m.minRequestAge = aqueueRepackJobsSummary.minArchiveRequestAge;
      //If there are mount policies in the Catalogue
      if(mountPolicies.size()) {
        //We get all the mount policies that are on the queue from the catalogue list
        auto mountPoliciesInQueueList = getMountPoliciesInQueue(mountPolicies,aqueueRepackJobsSummary.mountPolicyCountMap);
        m.mountPolicyNames = std::list<std::string>();
        //If an operator removed the queue mountPolicies from the catalogue, we will have no results...
        if(mountPoliciesInQueueList.size()){
          std::transform(mountPoliciesInQueueList.begin(), mountPoliciesInQueueList.end(), std::back_inserter(m.mountPolicyNames.value()),
            [](const cta::common::dataStructures::MountPolicy &policy) -> std::string {return policy.name;});
          auto mountPolicyToUse = createBestArchiveMountPolicy(mountPoliciesInQueueList);
          m.priority = mountPolicyToUse.archivePriority;
          m.minRequestAge = mountPolicyToUse.archiveMinRequestAge;
          m.highestPriorityMountPolicyName = getHighestPriorityArchiveMountPolicyName(mountPoliciesInQueueList);
          m.lowestRequestAgeMountPolicyName = getLowestRequestAgeArchiveMountPolicyName(mountPoliciesInQueueList);
        }
      }
      m.logicalLibrary = "";
    } else {
      tmdi.queueTrimRequired = true;
    }
    auto processingTime = t.secs(utils::Timer::resetCounter);
    log::ScopedParamContainer params (logContext);
    params.add("queueObject", aq_dump.address)
          .add("tapePool", aq_dump.cId)
          .add("queueType", toString(cta::common::dataStructures::MountType::ArchiveForRepack))
          .add("queueLockTime", queueLockTime)
          .add("queueFetchTime", queueFetchTime)
          .add("processingTime", processingTime);
    if(queueLockTime > 1 || queueFetchTime > 1) {
      logContext.log(log::WARNING, "In OStoreDB::fetchMountInfo(): fetched an archive for repack queue and that lasted more than 1 second.");
    }
  }
  // Walk the retrieve queues for statistics
  for (auto & rqp : re.dumpRetrieveQueues(common::dataStructures::JobQueueType::JobsToTransferForUser)) {
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
      log::ScopedParamContainer params(lc);
      params.add("queueObject", rqp.address)
            .add("tapeVid", rqp.vid)
            .add("exceptionMessage", ex.getMessageValue());
      lc.log(log::DEBUG, "WARNING: In OStoreDB::fetchMountInfo(): failed to lock/fetch a retrieve queue. Skipping it.");
      continue;
    }
    // If there are files queued, we create an entry for this retrieve queue in the
    // mount candidates list.
    auto rqSummary = rqueue.getJobsSummary();
    if (rqSummary.jobs) {
      //Getting the default mountPolicies parameters from the queue summary
      uint64_t minRetrieveRequestAge = rqSummary.minRetrieveRequestAge;
      uint64_t priority = rqSummary.priority;
      std::optional<std::string> highestPriorityMountPolicyName;
      std::optional<std::string> lowestRequestAgeMountPolicyName;
      std::list<std::string> queueMountPolicyNames;
      //Try to get the last values of the mountPolicies from the ones in the Catalogue
      if(mountPolicies.size()){
        auto mountPoliciesInQueueList = getMountPoliciesInQueue(mountPolicies,rqSummary.mountPolicyCountMap);
        if(mountPoliciesInQueueList.size()){
          std::transform(mountPoliciesInQueueList.begin(), mountPoliciesInQueueList.end(), std::back_inserter(queueMountPolicyNames),
            [](const cta::common::dataStructures::MountPolicy &policy) -> std::string {return policy.name;});
          //We need to get the most advantageous mountPolicy
          //As the Init element of the reduce function is the first element of the list, we start the reduce with the second element (++mountPolicyInQueueList.begin())
          common::dataStructures::MountPolicy mountPolicyToUse = createBestRetrieveMountPolicy(mountPoliciesInQueueList);
          priority = mountPolicyToUse.retrievePriority;
          minRetrieveRequestAge = mountPolicyToUse.retrieveMinRequestAge;
          highestPriorityMountPolicyName = getHighestPriorityRetrieveMountPolicyName(mountPoliciesInQueueList);
          lowestRequestAgeMountPolicyName = getLowestRequestAgeRetrieveMountPolicyName(mountPoliciesInQueueList);
        }
      }
      // Check if we have activities and if all the jobs are covered by one or not (possible mixed case).
      bool jobsWithoutActivity = true;
      if (rqSummary.activityCounts.size()) {
        if (rqSummary.activityCounts.size() >= rqSummary.jobs)
          jobsWithoutActivity = false;
        // In all cases, we create one potential mount per activity
        for (auto ac: rqSummary.activityCounts) {
          tmdi.potentialMounts.push_back(SchedulerDatabase::PotentialMount());
          auto & m = tmdi.potentialMounts.back();
          m.vid = rqp.vid;
          m.type = cta::common::dataStructures::MountType::Retrieve;
          m.bytesQueued = rqSummary.bytes;
          m.filesQueued = rqSummary.jobs;
          m.oldestJobStartTime = rqueue.getJobsSummary().oldestJobStartTime;
          m.youngestJobStartTime = rqueue.getJobsSummary().youngestJobStartTime;
          m.priority = priority;
          m.minRequestAge = minRetrieveRequestAge;
          m.highestPriorityMountPolicyName = highestPriorityMountPolicyName;
          m.lowestRequestAgeMountPolicyName = lowestRequestAgeMountPolicyName;
          m.logicalLibrary = ""; // The logical library is not known here, and will be determined by the caller.
          m.tapePool = "";       // The tape pool is not know and will be determined by the caller.
          m.vendor = "";         // The vendor is not known here, and will be determined by the caller.
          m.mediaType = "";      // The logical library is not known here, and will be determined by the caller.
          m.vo = "";             // The vo is not known here, and will be determined by the caller.
          m.capacityInBytes = 0; // The capacity is not known here, and will be determined by the caller.
          m.labelFormat = std::nullopt; // The labelFormat is not known here, and may be determined by the caller.
          m.activity = ac.activity;
          m.mountPolicyNames = queueMountPolicyNames;
          // We will display the sleep flag only if it is not expired (15 minutes timeout, hardcoded).
          // This allows having a single decision point instead of implementing is at the consumer levels.
          if (rqSummary.sleepInfo && (::time(nullptr) < (rqSummary.sleepInfo.value().sleepStartTime
              + (int64_t) rqSummary.sleepInfo.value().sleepTime)) ) {
            m.sleepingMount = true;
            m.sleepStartTime = rqSummary.sleepInfo.value().sleepStartTime;
            m.diskSystemSleptFor = rqSummary.sleepInfo.value().diskSystemSleptFor;
            m.sleepTime = rqSummary.sleepInfo.value().sleepTime;
          }
        }
      }
      if (jobsWithoutActivity) {
        tmdi.potentialMounts.push_back(SchedulerDatabase::PotentialMount());
        auto & m = tmdi.potentialMounts.back();
        m.vid = rqp.vid;
        m.type = cta::common::dataStructures::MountType::Retrieve;
        m.bytesQueued = rqSummary.bytes;
        m.filesQueued = rqSummary.jobs;
        m.oldestJobStartTime = rqSummary.oldestJobStartTime;
        m.youngestJobStartTime = rqSummary.youngestJobStartTime;
        m.priority = priority;
        m.minRequestAge = minRetrieveRequestAge;
        m.highestPriorityMountPolicyName = highestPriorityMountPolicyName;
        m.lowestRequestAgeMountPolicyName = lowestRequestAgeMountPolicyName;
        m.logicalLibrary = ""; // The logical library is not known here, and will be determined by the caller.
        m.tapePool = "";       // The tape pool is not know and will be determined by the caller.
        m.vendor = "";         // The vendor is not known here, and will be determined by the caller.
        m.mediaType = "";      // The logical library is not known here, and will be determined by the caller.
        m.vo = "";             // The vo is not known here, and will be determined by the caller.
        m.capacityInBytes = 0; // The capacity is not known here, and will be determined by the caller.
        m.labelFormat = std::nullopt; // The labelFormat is not known here, and may be determined by the caller.
        m.mountPolicyNames = queueMountPolicyNames;
        // We will display the sleep flag only if it is not expired (15 minutes timeout, hardcoded).
        // This allows having a single decision point instead of implementing is at the consumer levels.
        if (rqSummary.sleepInfo && (::time(nullptr) < (rqSummary.sleepInfo.value().sleepStartTime
            + (int64_t) rqSummary.sleepInfo.value().sleepTime)) ) {
          m.sleepingMount = true;
          m.sleepStartTime = rqSummary.sleepInfo.value().sleepStartTime;
          m.diskSystemSleptFor = rqSummary.sleepInfo.value().diskSystemSleptFor;
          m.sleepTime = rqSummary.sleepInfo.value().sleepTime;
        }
      }
    } else {
      if(!rqSummary.jobs)
        tmdi.queueTrimRequired = true;
    }
    auto processingTime = t.secs(utils::Timer::resetCounter);
    log::ScopedParamContainer params (logContext);
    params.add("queueObject", rqp.address)
          .add("tapeVid", rqp.vid)
          .add("queueLockTime", queueLockTime)
          .add("queueFetchTime", queueFetchTime)
          .add("processingTime", processingTime);
    if(queueLockTime > 1 || queueFetchTime > 1){
      logContext.log(log::WARNING, "In OStoreDB::fetchMountInfo(): fetched a retrieve queue and that lasted more than 1 second.");
    }
  }
  // Collect information about the existing and next mounts
  // If a next mount exists the drive "counts double", but the corresponding drive
  // is either about to mount, or about to replace its current mount.
  double registerFetchTime = 0;
  const auto driveStates = m_catalogue.DriveState()->getTapeDrives();
  registerFetchTime = t.secs(utils::Timer::resetCounter);
  using common::dataStructures::DriveStatus;
  std::set<int> activeDriveStatuses = {
    (int)cta::common::dataStructures::DriveStatus::Starting,
    (int)cta::common::dataStructures::DriveStatus::Mounting,
    (int)cta::common::dataStructures::DriveStatus::Transferring,
    (int)cta::common::dataStructures::DriveStatus::Unloading,
    (int)cta::common::dataStructures::DriveStatus::Unmounting,
    (int)cta::common::dataStructures::DriveStatus::DrainingToDisk};
  std::set<int> activeMountTypes = {
    (int)cta::common::dataStructures::MountType::ArchiveForUser,
    (int)cta::common::dataStructures::MountType::ArchiveForRepack,
    (int)cta::common::dataStructures::MountType::Retrieve,
    (int)cta::common::dataStructures::MountType::Label };
  for (const auto& driveState : driveStates) {
    if (activeDriveStatuses.count(static_cast<int>(driveState.driveStatus))) {
      if (driveState.mountType == common::dataStructures::MountType::NoMount) {
        log::ScopedParamContainer params (logContext);
        params.add("driveName", driveState.driveName);
        params.add("mountType", common::dataStructures::toString(driveState.mountType));
        params.add("driveStatus", common::dataStructures::toString(driveState.driveStatus));
        logContext.log(log::WARNING, "In OStoreDB::fetchMountInfo(): the drive has an active status but no mount.");
        continue;
      }
      tmdi.existingOrNextMounts.push_back(ExistingMount());
      tmdi.existingOrNextMounts.back().type = driveState.mountType;
      tmdi.existingOrNextMounts.back().tapePool = driveState.currentTapePool.value_or("");
      tmdi.existingOrNextMounts.back().vo = driveState.currentVo.value_or("");
      tmdi.existingOrNextMounts.back().driveName = driveState.driveName;
      tmdi.existingOrNextMounts.back().vid = driveState.currentVid.value_or("");
      tmdi.existingOrNextMounts.back().currentMount = true;
      tmdi.existingOrNextMounts.back().bytesTransferred = driveState.bytesTransferedInSession.value_or(0);
      tmdi.existingOrNextMounts.back().filesTransferred = driveState.filesTransferedInSession.value_or(0);
      if(driveState.filesTransferedInSession && driveState.sessionElapsedTime && driveState.sessionElapsedTime.value() > 0) {
        tmdi.existingOrNextMounts.back().averageBandwidth = driveState.bytesTransferedInSession.value() / driveState.sessionElapsedTime.value();
      } else {
        tmdi.existingOrNextMounts.back().averageBandwidth = 0.0;
      }
      tmdi.existingOrNextMounts.back().activity = driveState.currentActivity.value_or("");
    }
    if(driveState.nextMountType == common::dataStructures::MountType::NoMount) continue;
    if (activeMountTypes.count(static_cast<int>(driveState.nextMountType))) {
      tmdi.existingOrNextMounts.push_back(ExistingMount());
      tmdi.existingOrNextMounts.back().type = driveState.nextMountType;
      tmdi.existingOrNextMounts.back().tapePool = driveState.nextTapePool.value_or("");
      tmdi.existingOrNextMounts.back().vo = driveState.nextVo.value_or("");
      tmdi.existingOrNextMounts.back().driveName = driveState.driveName;
      tmdi.existingOrNextMounts.back().vid = driveState.nextVid.value_or("");
      tmdi.existingOrNextMounts.back().currentMount = false;
      tmdi.existingOrNextMounts.back().bytesTransferred = 0;
      tmdi.existingOrNextMounts.back().filesTransferred = 0;
      tmdi.existingOrNextMounts.back().averageBandwidth = 0;
      tmdi.existingOrNextMounts.back().activity = driveState.nextActivity.value_or("");
    }
  }
  auto registerProcessingTime = t.secs(utils::Timer::resetCounter);
  log::ScopedParamContainer params (logContext);
  params.add("queueFetchTime", registerFetchTime)
        .add("processingTime", registerProcessingTime);
  if ((registerFetchTime > 1) || (registerProcessingTime > 1))
    logContext.log(log::INFO, "In OStoreDB::fetchMountInfo(): fetched the drive register.");
}

//------------------------------------------------------------------------------
// OStoreDB::getMountPoliciesInQueue()
//------------------------------------------------------------------------------
std::list<common::dataStructures::MountPolicy> OStoreDB::getMountPoliciesInQueue(const std::list<common::dataStructures::MountPolicy> & mountPoliciesInCatalogue, const std::map<std::string, uint64_t>& queueMountPolicyMap) {
  std::list<cta::common::dataStructures::MountPolicy> mountPolicyRet;
  std::copy_if(mountPoliciesInCatalogue.begin(),mountPoliciesInCatalogue.end(),std::back_inserter(mountPolicyRet),[&queueMountPolicyMap](const cta::common::dataStructures::MountPolicy & mp){
    return queueMountPolicyMap.find(mp.name) != queueMountPolicyMap.end();
  });
  return mountPolicyRet;
}

//------------------------------------------------------------------------------
// OStoreDB::createBestArchiveMountPolicy()
//------------------------------------------------------------------------------
common::dataStructures::MountPolicy OStoreDB::createBestArchiveMountPolicy(const std::list<common::dataStructures::MountPolicy>& mountPolicies) {
  if(mountPolicies.empty()){
    throw cta::exception::Exception("In OStoreDB::createBestArchiveMountPolicy(), empty mount policy list.");
  }
  //We need to get the most advantageous mountPolicy
  //As the Init element of the reduce function is the first element of the list, we start the reduce with the second element (++mountPolicyInQueueList.begin())
  common::dataStructures::MountPolicy bestMountPolicy = cta::utils::reduce(++mountPolicies.begin(), mountPolicies.end(),mountPolicies.front(),[](const common::dataStructures::MountPolicy & mp1, const common::dataStructures::MountPolicy & mp2){
    common::dataStructures::MountPolicy mp = mp1;
    if(mp1.archivePriority > mp2.archivePriority){
      mp.archivePriority = mp1.archivePriority;
    }
    if(mp1.archiveMinRequestAge < mp2.archiveMinRequestAge){
      mp.archiveMinRequestAge = mp1.archiveMinRequestAge;
    }
    return mp;
  });
  return bestMountPolicy;
}

//------------------------------------------------------------------------------
// OStoreDB::getHighestPriorityArchiveMountPolicyName()
//------------------------------------------------------------------------------
std::string OStoreDB::getHighestPriorityArchiveMountPolicyName(const std::list<common::dataStructures::MountPolicy>& mountPolicies) {
  if(mountPolicies.empty()){
    throw cta::exception::Exception("In OStoreDB::getHighestPriorityArchiveMountPolicyName(), empty mount policy list.");
  }
  //We need to get the most advantageous mountPolicy
  //As the Init element of the reduce function is the first element of the list, we start the reduce with the second element (++mountPolicyInQueueList.begin())
  common::dataStructures::MountPolicy bestMountPolicy = cta::utils::reduce(++mountPolicies.begin(), mountPolicies.end(),mountPolicies.front(),
    [](const common::dataStructures::MountPolicy & mp1, const common::dataStructures::MountPolicy & mp2){
    if(mp2.archivePriority > mp1.archivePriority){
      return mp2;
    }
    return mp1;
  });
  return bestMountPolicy.name;
}

//------------------------------------------------------------------------------
// OStoreDB::getLowestRequestAgeArchiveMountPolicyName()
//------------------------------------------------------------------------------
std::string OStoreDB::getLowestRequestAgeArchiveMountPolicyName(const std::list<common::dataStructures::MountPolicy>& mountPolicies) {
  if(mountPolicies.empty()){
    throw cta::exception::Exception("In OStoreDB::getLowestRequestAgeArchiveMountPolicyName(), empty mount policy list.");
  }
  //We need to get the most advantageous mountPolicy
  //As the Init element of the reduce function is the first element of the list, we start the reduce with the second element (++mountPolicyInQueueList.begin())
  common::dataStructures::MountPolicy bestMountPolicy = cta::utils::reduce(++mountPolicies.begin(), mountPolicies.end(),mountPolicies.front(),
    [](const common::dataStructures::MountPolicy & mp1, const common::dataStructures::MountPolicy & mp2){
    if(mp1.archiveMinRequestAge < mp2.archiveMinRequestAge){
      return mp1;
    }
    return mp2;
  });
  return bestMountPolicy.name;
}

//------------------------------------------------------------------------------
// OStoreDB::createBestRetrieveMountPolicy()
//------------------------------------------------------------------------------
common::dataStructures::MountPolicy OStoreDB::createBestRetrieveMountPolicy(const std::list<common::dataStructures::MountPolicy>& mountPolicies) {
  if(mountPolicies.empty()){
    throw cta::exception::Exception("In OStoreDB::createBestRetrieveMountPolicy(), empty mount policy list.");
  }
  //We need to get the most advantageous mountPolicy
  //As the Init element of the reduce function is the first element of the list, we start the reduce with the second element (++mountPolicyInQueueList.begin())
  common::dataStructures::MountPolicy bestMountPolicy = cta::utils::reduce(++mountPolicies.begin(), mountPolicies.end(),mountPolicies.front(),[](const common::dataStructures::MountPolicy & mp1, const common::dataStructures::MountPolicy & mp2){
    common::dataStructures::MountPolicy mp = mp1;
    if(mp1.retrievePriority > mp2.retrievePriority){
      mp.retrievePriority = mp1.retrievePriority;
    }
    if(mp1.retrieveMinRequestAge < mp2.retrieveMinRequestAge){
      mp.retrieveMinRequestAge = mp1.retrieveMinRequestAge;
    }
    return mp;
  });
  return bestMountPolicy;
}

//------------------------------------------------------------------------------
// OStoreDB::getHighestPriorityRetrieveMountPolicyName()
//------------------------------------------------------------------------------
std::string OStoreDB::getHighestPriorityRetrieveMountPolicyName(const std::list<common::dataStructures::MountPolicy>& mountPolicies) {
  if(mountPolicies.empty()){
    throw cta::exception::Exception("In OStoreDB::getHighestPriorityRetrieveMountPolicyName(), empty mount policy list.");
  }
  //We need to get the most advantageous mountPolicy
  //As the Init element of the reduce function is the first element of the list, we start the reduce with the second element (++mountPolicyInQueueList.begin())
  common::dataStructures::MountPolicy bestMountPolicy = cta::utils::reduce(++mountPolicies.begin(), mountPolicies.end(),mountPolicies.front(),
    [](const common::dataStructures::MountPolicy & mp1, const common::dataStructures::MountPolicy & mp2){
    if(mp1.retrievePriority > mp2.retrievePriority){
      return mp1;
    }
    return mp2;
  });
  return bestMountPolicy.name;
}

//------------------------------------------------------------------------------
// OStoreDB::getLowestRequestAgeRetrieveMountPolicyName()
//------------------------------------------------------------------------------
std::string OStoreDB::getLowestRequestAgeRetrieveMountPolicyName(const std::list<common::dataStructures::MountPolicy>& mountPolicies) {
  if(mountPolicies.empty()){
    throw cta::exception::Exception("In OStoreDB::getLowestRequestAgeRetrieveMountPolicyName(), empty mount policy list.");
  }
  //We need to get the most advantageous mountPolicy
  //As the Init element of the reduce function is the first element of the list, we start the reduce with the second element (++mountPolicyInQueueList.begin())
  common::dataStructures::MountPolicy bestMountPolicy = cta::utils::reduce(++mountPolicies.begin(), mountPolicies.end(),mountPolicies.front(),
    [](const common::dataStructures::MountPolicy & mp1, const common::dataStructures::MountPolicy & mp2){
    if(mp1.retrieveMinRequestAge < mp2.retrieveMinRequestAge){
      return mp1;
    }
    return mp2;
  });
  return bestMountPolicy.name;
}

//------------------------------------------------------------------------------
// OStoreDB::getMountInfo()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo>
OStoreDB::getMountInfo(log::LogContext& logContext) {
  return OStoreDB::getMountInfo(logContext, 0);
}

//------------------------------------------------------------------------------
// OStoreDB::getMountInfo()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo>
  OStoreDB::getMountInfo(log::LogContext& logContext, uint64_t globalLockTimeout_us) {
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
  try {
    tmdi.m_lockOnSchedulerGlobalLock.lock(*tmdi.m_schedulerGlobalLock, globalLockTimeout_us);
  } catch (cta::exception::TimeoutException &e) {
    auto lockSchedGlobalTime = t.secs(utils::Timer::resetCounter);
    log::ScopedParamContainer params(logContext);
    params.add("rootFetchNoLockTime", rootFetchNoLockTime)
          .add("lockSchedGlobalTime", lockSchedGlobalTime)
          .add("fetchSchedGlobalTime", 0.0)
          .add("status", "FAILURE");
    logContext.log(log::INFO, "In OStoreDB::getMountInfo(): global lock acquisition.");
    throw;
  }
  auto lockSchedGlobalTime = t.secs(utils::Timer::resetCounter);
  tmdi.m_lockTaken = true;
  tmdi.m_schedulerGlobalLock->fetch();
  auto fetchSchedGlobalTime = t.secs(utils::Timer::resetCounter);
  {

    log::ScopedParamContainer params(logContext);
    params.add("rootFetchNoLockTime", rootFetchNoLockTime)
          .add("lockSchedGlobalTime", lockSchedGlobalTime)
          .add("fetchSchedGlobalTime", fetchSchedGlobalTime)
          .add("status", "SUCCESS");
    logContext.log(log::INFO, "In OStoreDB::getMountInfo(): global lock acquisition.");
  }
  fetchMountInfo(tmdi, re, SchedulerDatabase::PurposeGetMountInfo::GET_NEXT_MOUNT, logContext);
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
std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> OStoreDB::getMountInfoNoLock(PurposeGetMountInfo purpose, log::LogContext & logContext) {
  utils::Timer t;
  //Allocate the getMountInfostructure to return.
  assertAgentAddressSet();
  std::unique_ptr<OStoreDB::TapeMountDecisionInfoNoLock> privateRet (new OStoreDB::TapeMountDecisionInfoNoLock);
  // Get all the tape pools and tapes with queues (potential mounts)
  objectstore::RootEntry re(m_objectStore);
  re.fetchNoLock();
  auto rootFetchNoLockTime = t.secs(utils::Timer::resetCounter);
  TapeMountDecisionInfoNoLock & tmdi=*privateRet;
  fetchMountInfo(tmdi, re, purpose, logContext);
  auto fetchMountInfoTime = t.secs(utils::Timer::resetCounter);
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> ret(std::move(privateRet));
  {
    log::ScopedParamContainer params(logContext);
    params.add("rootFetchNoLockTime", rootFetchNoLockTime)
          .add("fetchMountInfoTime", fetchMountInfoTime);
    if ((rootFetchNoLockTime > 1) || (fetchMountInfoTime > 1))
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
  for (auto & queueType : {common::dataStructures::JobQueueType::JobsToTransferForUser,
    common::dataStructures::JobQueueType::JobsToReportToUser,
    common::dataStructures::JobQueueType::FailedJobs} ) {
    try {
      auto archiveQueueList = re.dumpArchiveQueues(queueType);
      for (auto & aq_dump : archiveQueueList) {
        ArchiveQueue aq(aq_dump.address, m_objectStore);
        ScopedSharedLock aql(aq);
        aq.fetch();
        if (!aq.getJobsSummary().jobs) {
          aql.release();
          re.removeArchiveQueueAndCommit(aq_dump.cId, queueType, lc);
          log::ScopedParamContainer params(lc);
          params.add("containerId", aq_dump.cId)
                .add("queueType", toString(queueType))
                .add("queueObject", aq_dump.address);
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
      lc.logBacktrace(log::INFO, ex.backtrace());
    }
  }
}

//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfoNoLock::createArchiveMount()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::ArchiveMount> OStoreDB::TapeMountDecisionInfoNoLock::createArchiveMount(
  const cta::SchedulerDatabase::PotentialMount& mount, const catalogue::TapeForWriting& tape,
  const std::string& driveName, const std::string& logicalLibrary,
  const std::string& hostName) {
  throw cta::exception::Exception(
    "In OStoreDB::TapeMountDecisionInfoNoLock::createArchiveMount(): This function should not be called");
}

//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfoNoLock::createRetrieveMount()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::RetrieveMount> OStoreDB::TapeMountDecisionInfoNoLock::createRetrieveMount(
  const cta::SchedulerDatabase::PotentialMount& mount, const std::string& driveName, const std::string& logicalLibrary,
  const std::string& hostName) {
  throw cta::exception::Exception(
    "In OStoreDB::TapeMountDecisionInfoNoLock::createRetrieveMount(): This function should not be called");
}

//------------------------------------------------------------------------------
// OStoreDB::TapeMountDecisionInfoNoLock::~TapeMountDecisionInfoNoLock()
//------------------------------------------------------------------------------
OStoreDB::TapeMountDecisionInfoNoLock::~TapeMountDecisionInfoNoLock() {}

//------------------------------------------------------------------------------
// OStoreDB::queueArchive()
//------------------------------------------------------------------------------
std::string OStoreDB::queueArchive(const std::string &instanceName, const cta::common::dataStructures::ArchiveRequest &request,
        const cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId &criteria, log::LogContext &logContext) {
  assertAgentAddressSet();
  cta::utils::Timer timer;
  auto mutexForHelgrind = std::make_unique<cta::threading::Mutex>();
  cta::threading::MutexLocker mlForHelgrind(*mutexForHelgrind);
  auto * mutexForHelgrindAddr = mutexForHelgrind.release();
  // Construct the archive request object in memory
  auto aReq = std::make_unique<cta::objectstore::ArchiveRequest> (m_agentReference->nextId("ArchiveRequest"), m_objectStore);
  aReq->initialize();
  // Summarize all as an archiveFile
  cta::common::dataStructures::ArchiveFile aFile;
  aFile.archiveFileID = criteria.fileId;
  aFile.checksumBlob = request.checksumBlob;
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
  std::string archiveRequestAddr = aReq->getAddressIfSet();
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
        aq.removeJobsAndCommit({aReq->getAddressIfSet()}, logContext);
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
  mlForHelgrind.unlock();
  m_enqueueingTasksQueue.push(et);
  double taskPostingTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
  params.add("taskPostingTime", taskPostingTime)
        .add("taskQueueSize", taskQueueSize)
        .add("totalTime", agentReferencingTime + insertionTime + taskPostingTime);
  logContext.log(log::INFO, "In OStoreDB::queueArchive(): recorded request for queueing (enqueueing posted to thread pool).");
  return archiveRequestAddr;
}

//------------------------------------------------------------------------------
// OStoreDB::getArchiveJobs()
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveJob> OStoreDB::getArchiveJobs(const std::string &tapePoolName) const {
  std::list<cta::common::dataStructures::ArchiveJob> ret;
  using common::dataStructures::JobQueueType;
  for (ArchiveJobQueueItor q_it(&m_objectStore, JobQueueType::JobsToTransferForUser, tapePoolName); !q_it.end() ; ++q_it) {
    ret.push_back(*q_it);
  }

  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getArchiveJobs()
//------------------------------------------------------------------------------
std::map<std::string, std::list<common::dataStructures::ArchiveJob>, std::less<>> OStoreDB::getArchiveJobs() const {
  std::map<std::string, std::list<common::dataStructures::ArchiveJob>, std::less<>> ret;
  using common::dataStructures::JobQueueType;
  for (ArchiveJobQueueItor q_it(&m_objectStore, JobQueueType::JobsToTransferForUser); !q_it.end(); ++q_it) {
    ret[q_it.qid()].push_back(*q_it);
  }

  return ret;
}

const std::string& OStoreDB::ArchiveJobQueueItor::qid() const {
  return m_archiveQueueItor.qid();
}

bool OStoreDB::ArchiveJobQueueItor::end() const {
  return m_archiveQueueItor.end();
}

void OStoreDB::ArchiveJobQueueItor::operator++() {
  ++m_archiveQueueItor;
}

const common::dataStructures::ArchiveJob &OStoreDB::ArchiveJobQueueItor::operator*() const {
  return *m_archiveQueueItor;
}

//------------------------------------------------------------------------------
// OStoreDB::getArchiveJobItor()
//------------------------------------------------------------------------------
std::unique_ptr<cta::SchedulerDatabase::IArchiveJobQueueItor> OStoreDB::getArchiveJobQueueItor(
  const std::string &tapePoolName, common::dataStructures::JobQueueType queueType) const {
  return std::make_unique<ArchiveJobQueueItor>(&m_objectStore, queueType, tapePoolName);
}

//------------------------------------------------------------------------------
// OStoreDB::getNextArchiveJobsToReportBatch()
//------------------------------------------------------------------------------
std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > OStoreDB::getNextArchiveJobsToReportBatch(
    uint64_t filesRequested, log::LogContext& logContext) {
  using AQTRAlgo = objectstore::ContainerAlgorithms<ArchiveQueue, ArchiveQueueToReportForUser>;
  AQTRAlgo aqtrAlgo(m_objectStore, *m_agentReference);
  // Decide from which queue we are going to pop.
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  auto queueList = re.dumpArchiveQueues(common::dataStructures::JobQueueType::JobsToReportToUser);
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > ret;
  if (queueList.empty()) return ret;
  // Try to get jobs from the first non-empty queue.
  AQTRAlgo::PopCriteria criteria;
  criteria.files = filesRequested;
  AQTRAlgo::PoppedElementsBatch jobs;
  std::string tapePool;
  for (auto & aq_dump : queueList) {
    jobs = aqtrAlgo.popNextBatch(aq_dump.cId, criteria, logContext);
    if (!jobs.elements.empty()) {
      // The tapepool of all jobs are the same within the queue
      tapePool = aq_dump.cId;
      break;
    }
  }
  for (auto & j : jobs.elements) {
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
    aj->m_tapePool = tapePool;
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
  auto queueList = re.dumpArchiveQueues(common::dataStructures::JobQueueType::FailedJobs);
  for (auto &aj : queueList) {
    ArchiveQueue aq(aj.address, m_objectStore);
    try {
      aq.fetchNoLock();
    } catch (cta::exception::Exception &ex) {
      log::ScopedParamContainer params(logContext);
      params.add("queueObject", aj.address)
            .add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::DEBUG, "WARNING: In OStoreDB::getArchiveJobsFailedSummary(): failed to lock/fetch an archive queue.");
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
    case SchedulerDatabase::ArchiveJob::ReportType::FailureReport:{
      ArchiveJob * osdbJob = castFromSchedDBJob(j);
      std::string tapePool = osdbJob->m_tapePool;
      failedQueues[tapePool].push_back(FailedJobToQueue());
      failedQueues[tapePool].back().job = osdbJob;
    }
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
    auto completeJobsToDeleteItor = completeJobsToDelete.begin();
    while(completeJobsToDeleteItor != completeJobsToDelete.end())
    {
      auto & j = *completeJobsToDeleteItor;
      try {
        j->asyncDeleteRequest();
      } catch(const cta::exception::NoSuchObject & ex) {
        log::ScopedParamContainer params(lc);
        params.add("fileId",j->archiveFile.archiveFileID);
        lc.log(log::WARNING,"In OStoreDB::setArchiveJobBatchReported() failed to asyncDeleteRequest because it does not exist in the objectstore.");
        completeJobsToDeleteItor = completeJobsToDelete.erase(completeJobsToDeleteItor);
        continue;
      }
      completeJobsToDeleteItor++;
    }
    timingList.insertAndReset("deleteLaunchTime", t);
    for (auto &j: completeJobsToDelete) {
      try {
        try {
          j->waitAsyncDelete();
        } catch(const cta::exception::NoSuchObject & ex){
          //No need to delete from the completeJobsToDelete list
          //as it is not used later
          log::ScopedParamContainer params(lc);
          params.add("fileId",j->archiveFile.archiveFileID);
          lc.log(log::WARNING,"In OStoreDB::setArchiveJobBatchReported() failed to asyncDeleteRequest because it does not exist in the objectstore.");
          continue;
        }
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
          std::nullopt, serializers::ArchiveJobStatus::AJS_Failed});
    }
    try {
      caAQF.referenceAndSwitchOwnership(queue.first, m_agentReference->getAgentAddress(), insertedElements, lc);
    } catch (typename CaAQF::OwnershipSwitchFailure &failure) {
      for(auto &failedAR: failure.failedElements){
        try{
          std::rethrow_exception(failedAR.failure);
        } catch (const cta::exception::NoSuchObject &ex) {
          log::ScopedParamContainer params(lc);
          params.add("fileId",failedAR.element->archiveFile.archiveFileID);
          lc.log(log::WARNING,"In OStoreDB::setArchiveJobBatchReported(), queueing impossible, job do not exist in the objectstore.");
        }
      }
    } catch (exception::Exception & ex) {
      log::ScopedParamContainer params(lc);
      params.add("exceptionMSG",ex.getMessageValue());
      lc.log(log::ERR,"In OStoreDB::setArchiveJobBatchReported(), failed to queue in the ArchiveQueueFailed.");
    }
    log::TimingList tl;
    tl.insertAndReset("queueAndSwitchStateTime", t);
    timingList += tl;
  }
}

//------------------------------------------------------------------------------
// OStoreDB::setRetrieveJobBatchReported()
//------------------------------------------------------------------------------
void OStoreDB::setRetrieveJobBatchReportedToUser(std::list<cta::SchedulerDatabase::RetrieveJob*> & jobsBatch,
  log::TimingList & timingList, utils::Timer & t, log::LogContext & lc)
{
  struct FailedJobToQueue {
    RetrieveJob * job;
  };

  // Sort jobs to be updated
  std::map<std::string, std::list<FailedJobToQueue>> failedQueues;
  auto jobsBatchItor = jobsBatch.begin();
  while(jobsBatchItor != jobsBatch.end()) {
    auto & j = *jobsBatchItor;
    switch(j->reportType) {
      case SchedulerDatabase::RetrieveJob::ReportType::FailureReport: {
        try {
          j->fail();
        } catch (const cta::exception::NoSuchObject &ex){
          log::ScopedParamContainer params(lc);
          params.add("fileId",j->archiveFile.archiveFileID)
                .add("exceptionMsg",ex.getMessageValue());
          lc.log(log::WARNING,"In OStoreDB::setRetrieveJobBatchReportedToUser(), fail to fail the job because it does not exists in the objectstore.");
          jobsBatch.erase(jobsBatchItor);
          goto next;
        }
        auto &vid = j->archiveFile.tapeFiles.at(j->selectedCopyNb).vid;
        failedQueues[vid].push_back(FailedJobToQueue{ castFromSchedDBJob(j) });
        break;
      }
      default: {
        log::ScopedParamContainer params(lc);
        params.add("fileId", j->archiveFile.archiveFileID)
              .add("objectAddress", castFromSchedDBJob(j)->m_retrieveRequest.getAddressIfSet());
        lc.log(log::ERR, "In OStoreDB::setRetrieveJobBatchReportedToUser(): unexpected job status. Leaving the job as-is.");
      }
    }
    next:
    jobsBatchItor++;
  }

  // Put the failed jobs in the failed queue
  for(auto &queue : failedQueues) {
    typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueFailed> CaRQF;
    CaRQF caRQF(m_objectStore, *m_agentReference);
    CaRQF::InsertedElement::list insertedElements;
    for(auto &j : queue.second) {
      auto tf_it = j.job->archiveFile.tapeFiles.begin();
      while(tf_it != j.job->archiveFile.tapeFiles.end()) {
        if(queue.first == tf_it->vid) break;
      }
      if(tf_it == j.job->archiveFile.tapeFiles.end()) throw cta::exception::Exception(
        "In OStoreDB::setRetrieveJobBatchReported(): tape copy not found"
      );
      insertedElements.emplace_back(CaRQF::InsertedElement{
        &j.job->m_retrieveRequest, tf_it->copyNb, tf_it->fSeq, j.job->archiveFile.fileSize,
        common::dataStructures::MountPolicy(),
        j.job->m_activity, j.job->m_diskSystemName
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
// OStoreDB::clearRetrieveQueueStatisticsCache()
//------------------------------------------------------------------------------
void OStoreDB::clearRetrieveQueueStatisticsCache(const std::string & vid) {
  return Helpers::flushRetrieveQueueStatisticsCacheForVid(vid);
}

//------------------------------------------------------------------------------
// OStoreDB::queueRetrieve()
//------------------------------------------------------------------------------
SchedulerDatabase::RetrieveRequestInfo OStoreDB::queueRetrieve(cta::common::dataStructures::RetrieveRequest& rqst,
  const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria, const std::optional<std::string> diskSystemName,
  log::LogContext &logContext) {
  assertAgentAddressSet();
  auto mutexForHelgrind = std::make_unique<cta::threading::Mutex>();
  cta::threading::MutexLocker mlForHelgrind(*mutexForHelgrind);
  cta::utils::Timer timer;
  // Get the best vid from the cache
  std::set<std::string> candidateVids;
  for (auto & tf:criteria.archiveFile.tapeFiles) candidateVids.insert(tf.vid);
  SchedulerDatabase::RetrieveRequestInfo ret;
  ret.selectedVid=Helpers::selectBestRetrieveQueue(candidateVids, m_catalogue, m_objectStore);
  // Check that the requested retrieve job (for the provided vid) exists, and record the copynb.
  uint64_t bestCopyNb;
  for (auto & tf: criteria.archiveFile.tapeFiles) {
    if (tf.vid == ret.selectedVid) {
      bestCopyNb = tf.copyNb;
      // Appending the file size to the dstURL so that
      // XrootD will fail to retrieve if there is not enough free space
      // in the eos disk
      rqst.appendFileSizeToDstURL(tf.fileSize);
      goto vidFound;
    }
  }
  {
    std::stringstream err;
    err << "In OStoreDB::queueRetrieve(): no tape file for requested vid. archiveId=" << criteria.archiveFile.archiveFileID
        << " vid=" << ret.selectedVid;
    throw RetrieveRequestHasNoCopies(err.str());
  }
  vidFound:
  // In order to post the job, construct it first in memory.
  auto rReq = std::make_unique<objectstore::RetrieveRequest> (m_agentReference->nextId("RetrieveRequest"), m_objectStore);
  ret.requestId = rReq->getAddressIfSet();
  rReq->initialize();
  rReq->setSchedulerRequest(rqst);
  rReq->setRetrieveFileQueueCriteria(criteria);
  rReq->setActivityIfNeeded(rqst, criteria);
  rReq->setCreationTime(rqst.creationLog.time);
  rReq->setIsVerifyOnly(rqst.isVerifyOnly);
  if (diskSystemName) rReq->setDiskSystemName(diskSystemName.value());
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
        << " vid=" << ret.selectedVid << " copyNb=" << bestCopyNb;
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
    rReq->setActiveCopyNumber(criteria.archiveFile.tapeFiles.begin()->copyNb);
    rReq->insert();
    double insertionTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
    m_taskQueueSize++;
    uint64_t taskQueueSize = m_taskQueueSize;
    // Prepare the logs to avoid multithread access on the object.
    log::ScopedParamContainer params(logContext);
    params.add("tapeVid", ret.selectedVid)
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
    auto *mutexForHelgrindAddr = mutexForHelgrind.release();
    auto * et = new EnqueueingTask([rReqPtr, job, ret, mutexForHelgrindAddr, this]{
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
      auto sharedLock = ostoredb::MemRetrieveQueue::sharedAddToQueue(nonConstJob, ret.selectedVid, *rReq, *this, logContext);
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
      params.add("tapeVid", ret.selectedVid)
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
    mlForHelgrind.unlock();
    m_enqueueingTasksQueue.push(et);
    double taskPostingTime = timer.secs(cta::utils::Timer::reset_t::resetCounter);
    params.add("taskPostingTime", taskPostingTime)
          .add("taskQueueSize", taskQueueSize)
          .add("totalTime", vidSelectionTime + agentReferencingTime + insertionTime + taskPostingTime);
    logContext.log(log::INFO, "In OStoreDB::queueRetrieve(): recorded request for queueing (enqueueing posted to thread pool).");
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::cancelRetrieve()
//------------------------------------------------------------------------------
void OStoreDB::cancelRetrieve(const std::string& instanceName, const cta::common::dataStructures::CancelRetrieveRequest& rqst,
  log::LogContext& lc) {
  // We should have the retrieve request's address in the request. Let's check it is the right one
  objectstore::RetrieveRequest rr(rqst.retrieveRequestId, m_objectStore);
  // We try to lock the request. It might not be present (in which case we have nothing to do.
  objectstore::ScopedExclusiveLock rrl;
  try{
    rrl.lock(rr);
    rr.fetch();
  } catch (cta::exception::NoSuchObject &) {
    log::ScopedParamContainer params(lc);
    params.add("archiveFileId", rqst.archiveFileID)
          .add("retrieveRequestId", rqst.retrieveRequestId);
    lc.log(log::ERR, "In OStoreDB::cancelRetrieve(): no such retrieve request.");
    return;
  } catch (exception::Exception & ex) {
    log::ScopedParamContainer params(lc);
    params.add("archiveFileId", rqst.archiveFileID)
          .add("retrieveRequestId", rqst.retrieveRequestId)
          .add("exceptionMessage", ex.getMessageValue());
    lc.log(log::ERR, "In OStoreDB::cancelRetrieve(): failed to lock of fetch the retreive request.");
    throw;
  }
  // We have the objectstore request. Let's validate it is matching the cancellation request's.
  if (rqst.archiveFileID != rr.getArchiveFile().archiveFileID) {
    log::ScopedParamContainer params(lc);
    params.add("ArchiveFileID", rqst.archiveFileID)
          .add("RetrieveRequest", rqst.retrieveRequestId)
          .add("ArchiveFileIdFromRequest", rr.getArchiveFile().archiveFileID);
    lc.log(log::ERR, "In OStoreDB::cancelRetrieve(): archive file Id mismatch.");
    throw exception::Exception("In OStoreDB::cancelRetrieve(): archiveFileID mismatch.");
  }
  if (rr.isFailed()) {
    log::ScopedParamContainer params(lc);
    params.add("ArchiveFileID", rqst.archiveFileID)
          .add("RetrieveRequest", rqst.retrieveRequestId);
    lc.log(log::ERR, "In OStoreDB::cancelRetrieve(): request is in failed requests, skipping.");
    return;
  }
  // Looks fine, we delete the request
  log::ScopedParamContainer params(lc);
  params.add("ArchiveFileID", rqst.archiveFileID)
        .add("RetrieveRequest", rqst.retrieveRequestId);
  lc.log(log::INFO, "OStoreDB::cancelRetrieve(): will delete the retrieve request");
  rr.remove();
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
// OStoreDB::deleteArchiveRequest()
//------------------------------------------------------------------------------
void OStoreDB::cancelArchive(const common::dataStructures::DeleteArchiveRequest & request, log::LogContext & lc){
  if(!request.address){
    log::ScopedParamContainer params(lc);
    params.add("ArchiveFileID", request.archiveFileID);
    lc.log(log::ERR, "In OStoreDB::cancelArchive(): no archive request address provided");
    throw exception::Exception("In OStoreDB::cancelArchive(): no archive request address provided");
  }
  objectstore::ArchiveRequest ar(request.address.value(),m_objectStore);
  objectstore::ScopedExclusiveLock sel;
  try {
    sel.lock(ar);
    ar.fetch();
  } catch (cta::exception::NoSuchObject &) {
    log::ScopedParamContainer params(lc);
    params.add("archiveFileId", request.archiveFileID)
          .add("archiveRequestId", request.address.value());
    lc.log(log::WARNING, "In OStoreDB::cancelArchive(): no such archive request.");
    return;
  } catch (exception::Exception & ex) {
    log::ScopedParamContainer params(lc);
    params.add("archiveFileId", request.archiveFileID)
          .add("archiveRequestId",  request.address.value())
          .add("exceptionMessage", ex.getMessageValue());
    lc.log(log::ERR, "In OStoreDB::cancelArchive(): failed to lock of fetch the archive request.");
    throw;
  }
  // We have the objectstore request. Let's validate it is matching the cancellation request's.
  if (request.archiveFileID != ar.getArchiveFile().archiveFileID) {
    log::ScopedParamContainer params(lc);
    params.add("ArchiveFileID", request.archiveFileID)
          .add("archiveRequest", request.address.value())
          .add("ArchiveFileIdFromRequest", ar.getArchiveFile().archiveFileID);
    lc.log(log::ERR, "In OStoreDB::cancelArchive(): archive file Id mismatch.");
    throw exception::Exception("In OStoreDB::cancelArchive(): archiveFileID mismatch.");
  }
  if (ar.isFailed()) {
    log::ScopedParamContainer params(lc);
    params.add("ArchiveFileID", request.archiveFileID)
          .add("archiveRequest", request.address.value());
    lc.log(log::ERR, "In OStoreDB::cancelArchive(): request is in failed requests, skipping.");
    return;
  }
  // Looks fine, we delete the request
  log::ScopedParamContainer params(lc);
  params.add("ArchiveFileID", request.archiveFileID)
        .add("archiveRequestId", request.address.value());
  lc.log(log::INFO, "OStoreDB::cancelArchive(): will delete the archive request");
  ar.remove();
}

//------------------------------------------------------------------------------
// OStoreDB::deleteFailed()
//------------------------------------------------------------------------------
void OStoreDB::deleteFailed(const std::string &objectId, log::LogContext &lc)
{
  // Fetch the object
  objectstore::GenericObject fr(objectId, m_objectStore);
  objectstore::ScopedExclusiveLock sel;
  try {
    sel.lock(fr);
    fr.fetch();
  } catch (cta::exception::NoSuchObject &) {
    throw exception::Exception("Object " + objectId + " not found.");
  }

  // Validate that the object is an Archive or Retrieve request
  if(fr.type() != objectstore::serializers::ArchiveRequest_t &&
     fr.type() != objectstore::serializers::RetrieveRequest_t) {
    throw exception::Exception("Object " + objectId + " is not an archive or retrieve request.");
  }

  // Make a list of all owners of the object
  std::list<std::string> queueIds;
  if(!fr.getOwner().empty()) queueIds.push_back(fr.getOwner());
  if(!fr.getBackupOwner().empty()) queueIds.push_back(fr.getBackupOwner());
  if(fr.type() == objectstore::serializers::ArchiveRequest_t) {
    // Archive requests can be in two queues, so ownership is per job not per request
    objectstore::ArchiveRequest ar(fr);
    // We already hold a lock on the generic object
    ar.fetchNoLock();
    for(auto &job : ar.dumpJobs()) {
      if(!job.owner.empty()) queueIds.push_back(job.owner);
    }
  }
  if(queueIds.empty()) {
    throw exception::Exception("Object " + objectId + " is not owned by any queue.");
  }

  // Make a set of failed archive or retrieve queues
  std::set<std::string> failedQueueIds;
  {
    RootEntry re(m_objectStore);
    ScopedExclusiveLock rel(re);
    re.fetch();

    switch(fr.type()) {
      case objectstore::serializers::ArchiveRequest_t: {
        auto archiveQueueList = re.dumpArchiveQueues(common::dataStructures::JobQueueType::FailedJobs);
        for(auto &r : archiveQueueList) {
          failedQueueIds.insert(r.address);
        }
        break;
      }
      case objectstore::serializers::RetrieveRequest_t: {
        auto retrieveQueueList = re.dumpRetrieveQueues(common::dataStructures::JobQueueType::FailedJobs);
        for(auto &r : retrieveQueueList) {
          failedQueueIds.insert(r.address);
        }
        break;
      }
      default: ;
    }
  }

  // Validate that all owners of the object are failed queues and therefore it is safe to delete the job
  for(auto &qid : queueIds) {
    if(failedQueueIds.find(qid) == failedQueueIds.end()) {
      throw exception::Exception("Will not delete object " + objectId + "\nwhich is owned by " + qid);
    }
  }

  // Checks passed, delete the request
  log::ScopedParamContainer params(lc);
  params.add("objectId", objectId);
  int owner_no = 0;
  for(auto &qid : queueIds) {
    params.add("owner" + std::to_string(owner_no++), qid);
  }

  // Delete the references
  lc.log(log::INFO, "OStoreDB::deleteFailed(): deleting references");
  bool isQueueEmpty = false;
  switch(fr.type()) {
    case objectstore::serializers::ArchiveRequest_t: {
      for(auto &arq_id : queueIds) {
        ArchiveQueue arq(arq_id, m_objectStore);
        ScopedExclusiveLock arq_el(arq);
        arq.fetch();
        arq.removeJobsAndCommit(std::list<std::string>(1, objectId), lc);
        if(arq.isEmpty()) isQueueEmpty = true;
      }
      break;
    }
    case objectstore::serializers::RetrieveRequest_t: {
      for(auto &rrq_id : queueIds) {
        RetrieveQueue rrq(rrq_id, m_objectStore);
        ScopedExclusiveLock rrq_el(rrq);
        rrq.fetch();
        rrq.removeJobsAndCommit(std::list<std::string>(1, objectId), lc);
        if(rrq.isEmpty()) isQueueEmpty = true;
      }
      break;
    }
    default: ;
  }

  // Delete the request
  lc.log(log::INFO, "OStoreDB::deleteFailed(): deleting failed request");
  fr.remove();

  // Trim empty queues
  if(isQueueEmpty) trimEmptyQueues(lc);
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveJobs()
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::RetrieveJob> OStoreDB::getRetrieveJobs(const std::string &vid) const {
  std::list<common::dataStructures::RetrieveJob> ret;
  using common::dataStructures::JobQueueType;
  for (RetrieveJobQueueItor q_it(&m_objectStore, JobQueueType::JobsToTransferForUser, vid); !q_it.end(); ++q_it) {
    ret.push_back(*q_it);
  }

  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveJobs()
//------------------------------------------------------------------------------
std::map<std::string, std::list<common::dataStructures::RetrieveJob>, std::less<>> OStoreDB::getRetrieveJobs() const {
  std::map<std::string, std::list<common::dataStructures::RetrieveJob>, std::less<>> ret;
  using common::dataStructures::JobQueueType;
  for (RetrieveJobQueueItor q_it(&m_objectStore, JobQueueType::JobsToTransferForUser); !q_it.end(); ++q_it) {
    ret[q_it.qid()].push_back(*q_it);
  }

  return ret;
}

const std::string& OStoreDB::RetrieveJobQueueItor::qid() const {
  return m_retrieveQueueItor.qid();
}

bool OStoreDB::RetrieveJobQueueItor::end() const {
  return m_retrieveQueueItor.end();
}

void OStoreDB::RetrieveJobQueueItor::operator++() {
  ++m_retrieveQueueItor;
}

const common::dataStructures::RetrieveJob &OStoreDB::RetrieveJobQueueItor::operator*() const {
  return *m_retrieveQueueItor;
}

//------------------------------------------------------------------------------
// OStoreDB::getRetrieveJobItor()
//------------------------------------------------------------------------------
std::unique_ptr<cta::SchedulerDatabase::IRetrieveJobQueueItor> OStoreDB::getRetrieveJobQueueItor(const std::string &vid,
  common::dataStructures::JobQueueType queueType) const {
  return std::make_unique<RetrieveJobQueueItor>(&m_objectStore, queueType, vid);
}

//------------------------------------------------------------------------------
// OStoreDB::queueRepack()
//------------------------------------------------------------------------------
std::string OStoreDB::queueRepack(const SchedulerDatabase::QueueRepackRequest & repackRequest,log::LogContext & lc) {
  std::string vid = repackRequest.m_vid;
  common::dataStructures::RepackInfo::Type repackType = repackRequest.m_repackType;
  std::string bufferURL = repackRequest.m_repackBufferURL;
  common::dataStructures::MountPolicy mountPolicy = repackRequest.m_mountPolicy;
  // Prepare the repack request object in memory.
  assertAgentAddressSet();
  cta::utils::Timer t;
  auto rr=std::make_unique<cta::objectstore::RepackRequest>(m_agentReference->nextId("RepackRequest"), m_objectStore);
  rr->initialize();
  // We need to own the request until it is queued in the the pending queue.
  rr->setOwner(m_agentReference->getAgentAddress());
  rr->setVid(vid);
  rr->setType(repackType);
  rr->setBufferURL(bufferURL);
  rr->setMountPolicy(mountPolicy);
  rr->setNoRecall(repackRequest.m_noRecall);
  rr->setCreationLog(repackRequest.m_creationLog);
  // Try to reference the object in the index (will fail if there is already a request with this VID.
  try {
    Helpers::registerRepackRequestToIndex(vid, rr->getAddressIfSet(), *m_agentReference, m_objectStore, lc);
  } catch (objectstore::RepackIndex::VidAlreadyRegistered &) {
    throw exception::UserError("A repack request already exists for this VID.");
  }
  // We're good to go to create the object. We need to own it.
  m_agentReference->addToOwnership(rr->getAddressIfSet(), m_objectStore);
  rr->insert();

  std::string repackRequestAddress = rr->getAddressIfSet();

  // If latency needs to the improved, the next steps could be deferred like they are for archive and retrieve requests.
  typedef objectstore::ContainerAlgorithms<RepackQueue, RepackQueuePending> RQPAlgo;
  {
    RQPAlgo::InsertedElement::list elements;
    elements.push_back(RQPAlgo::InsertedElement());
    elements.back().repackRequest=std::move(rr);
    RQPAlgo rqpAlgo(m_objectStore, *m_agentReference);
    rqpAlgo.referenceAndSwitchOwnership(std::nullopt, m_agentReference->getAgentAddress(), elements, lc);
  }
  return repackRequestAddress;
}

//------------------------------------------------------------------------------
// OStoreDB::repackExists()
//------------------------------------------------------------------------------
bool OStoreDB::repackExists() {
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  RepackIndex ri(m_objectStore);
  // First, try to get the address of of the repack index lockfree.
  try {
    ri.setAddress(re.getRepackIndexAddress());
  } catch (cta::exception::Exception &) {
    return false;
  }
  ri.fetchNoLock();
  auto rrAddresses = ri.getRepackRequestsAddresses();
  for (auto & rra: rrAddresses) {
    try {
      objectstore::RepackRequest rr(rra.repackRequestAddress, m_objectStore);
      rr.fetchNoLock();
      return true;
    } catch (cta::exception::Exception &) {}
  }
  return false;
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
  } catch (cta::exception::Exception &) {
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
  } catch (cta::exception::Exception &) {
    throw exception::UserError("In OStoreDB::getRepackInfo(): No repack request for this VID (index not present).");
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
  throw exception::UserError("In OStoreDB::getRepackInfo(): No repack request for this VID.");
}

//------------------------------------------------------------------------------
// OStoreDB::resheduleRetrieveRequest()
//------------------------------------------------------------------------------
void OStoreDB::requeueRetrieveRequestJobs(std::list<cta::SchedulerDatabase::RetrieveJob *> &jobs, log::LogContext& logContext) {
  std::list<std::shared_ptr<objectstore::RetrieveRequest>> rrlist;
  std::list<objectstore::ScopedExclusiveLock> locks;
  for (auto &job: jobs) {
    auto oStoreJob = dynamic_cast<OStoreDB::RetrieveJob *>(job);
    auto rr = std::make_shared<objectstore::RetrieveRequest>(oStoreJob->m_retrieveRequest.getAddressIfSet(), m_objectStore);
    rrlist.push_back(rr);
    try {
      locks.emplace_back(*rr);
      rr->fetch();
    } catch (cta::exception::NoSuchObject &) {
      log::ScopedParamContainer params(logContext);
      params.add("retrieveRequestId", oStoreJob->m_retrieveRequest.getAddressIfSet());
      logContext.log(log::INFO, "In OStoreDB::requeueRetrieveRequestJobs(): no such retrieve request. Ignoring.");
      continue;
    }
    rr->garbageCollectRetrieveRequest(m_agentReference->getAgentAddress(), *m_agentReference, logContext, m_catalogue, true);
  }
  locks.clear();
  rrlist.clear();
}

//------------------------------------------------------------------------------
// OStoreDB::reserveRetrieveQueueForCleanup()
//------------------------------------------------------------------------------
void OStoreDB::reserveRetrieveQueueForCleanup(const std::string & vid, std::optional<uint64_t> cleanupHeartBeatValue) {

  RootEntry re(m_objectStore);
  RetrieveQueue rq(m_objectStore);
  ScopedExclusiveLock rql;
  re.fetchNoLock();

  try {
    rq.setAddress(re.getRetrieveQueueAddress(vid, common::dataStructures::JobQueueType::JobsToTransferForUser));
    rql.lock(rq);
    rq.fetch();
  } catch (cta::objectstore::RootEntry::NoSuchRetrieveQueue & ex) {
    throw RetrieveQueueNotFound("Retrieve queue of vid " + vid + " not found. " + ex.getMessageValue());
  } catch (cta::exception::NoSuchObject &ex) {
    throw RetrieveQueueNotFound("Retrieve queue of vid " + vid + " not found. " + ex.getMessageValue());
  } catch (cta::exception::Exception &ex) {
    throw;
  }

  // After locking a queue, check again if the cleanup flag is still true
  if (!rq.getQueueCleanupDoCleanup()) {
    throw RetrieveQueueNotReservedForCleanup("Queue no longer has the cleanup flag enabled after fetching. Skipping it.");
  }

  // Check if heartbeat has been updated, which means that another agent is still tracking it
  if (rq.getQueueCleanupAssignedAgent().has_value()) {
    if (rq.getQueueCleanupHeartbeat() != (cleanupHeartBeatValue.has_value() ? cleanupHeartBeatValue.value() : 0)) {
      throw RetrieveQueueNotReservedForCleanup("Another agent is alive and cleaning up the queue. Skipping it.");
    }
  }

  // Otherwise, carry on with cleanup of this queue
  rq.setQueueCleanupAssignedAgent(m_agentReference->getAgentAddress());
  rq.tickQueueCleanupHeartbeat();
  rq.commit();
}

//------------------------------------------------------------------------------
// OStoreDB::tickRetrieveQueueCleanupHeartbeat()
//------------------------------------------------------------------------------
void OStoreDB::tickRetrieveQueueCleanupHeartbeat(const std::string & vid) {

  RootEntry re(m_objectStore);
  RetrieveQueue rq(m_objectStore);
  ScopedExclusiveLock rql;
  re.fetchNoLock();

  try {
    rq.setAddress(re.getRetrieveQueueAddress(vid, common::dataStructures::JobQueueType::JobsToTransferForUser));
    rql.lock(rq);
    rq.fetch();
    if (rq.getQueueCleanupAssignedAgent().has_value() &&
        (rq.getQueueCleanupAssignedAgent() != m_agentReference->getAgentAddress())) {
      throw RetrieveQueueNotReservedForCleanup(
              "Another agent is alive and cleaning up the retrieve queue of tape " + vid + ". Heartbeat not ticked.");
    }
    rq.tickQueueCleanupHeartbeat();
    rq.commit();
  } catch (RetrieveQueueNotReservedForCleanup & ex) {
    throw; // Just pass this exception to the outside
  } catch (cta::objectstore::RootEntry::NoSuchRetrieveQueue & ex) {
    throw RetrieveQueueNotFound("Retrieve queue of vid " + vid + " not found. " + ex.getMessageValue());
  } catch (cta::exception::NoSuchObject & ex) {
    throw RetrieveQueueNotFound("Retrieve queue of vid " + vid + " not found. " + ex.getMessageValue());
  } catch (cta::exception::Exception & ex) {
    throw;
  }
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
    std::optional<serializers::RepackRequestStatus> newStatus(serializers::RepackRequestStatus::RRS_ToExpand);
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
    rqteAlgo.referenceAndSwitchOwnership(std::nullopt, m_agentReference.getAgentAddress(), insertedElements, lc);
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
  } catch (cta::exception::Exception &) {
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
  for (auto s: {Status::Pending, Status::ToExpand, Status::Starting, Status::Running}) {
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
    typedRet->m_pendingRepackRequestQueue.setAddress(re.getRepackQueueAddress(common::dataStructures::RepackQueueType::Pending));
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
  auto typedRet = std::make_unique<OStoreDB::RepackRequestPromotionStatisticsNoLock>();
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  populateRepackRequestsStatistics(re, *typedRet);
  std::unique_ptr<SchedulerDatabase::RepackRequestStatistics> ret(typedRet.release());
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getNextRetrieveJobsToTransferBatch()
//------------------------------------------------------------------------------
std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> OStoreDB::getNextRetrieveJobsToTransferBatch(
        const std::string & vid, uint64_t filesRequested, log::LogContext &logContext) {

  using RQTTAlgo = objectstore::ContainerAlgorithms<RetrieveQueue, RetrieveQueueToTransfer>;
  RQTTAlgo rqttAlgo(m_objectStore, *m_agentReference);
  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> ret;

  // Try to get jobs from the queue. If it is empty, it will be trimmed.
  RQTTAlgo::PopCriteria criteria;
  criteria.files = filesRequested;
  criteria.bytes = std::numeric_limits<decltype(criteria.bytes)>::max();

  // Pop the objects
  auto jobs = rqttAlgo.popNextBatch(vid, criteria, logContext);

  for (auto &j : jobs.elements) {
    std::unique_ptr<OStoreDB::RetrieveJob> rj(new OStoreDB::RetrieveJob(j.retrieveRequest->getAddressIfSet(), *this, nullptr));
    rj->archiveFile = j.archiveFile;
    rj->retrieveRequest = j.rr;
    rj->selectedCopyNb = j.copyNb;
    rj->errorReportURL = j.errorReportURL;
    rj->reportType = j.reportType;
    rj->m_repackInfo = j.repackInfo;
    rj->setJobOwned();
    ret.emplace_back(std::move(rj));
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::setRetrieveQueueCleanupFlag()
//------------------------------------------------------------------------------
void OStoreDB::setRetrieveQueueCleanupFlag(const std::string& vid, bool val, log::LogContext& logContext) {

  cta::utils::Timer t;
  double rootFetchNoLockTime = 0;
  double rootRelockExclusiveTime = 0;
  double rootRefetchTime = 0;
  double addOrGetQueueandCommitTime = 0;
  double queueLockTime = 0;
  double queueFetchTime = 0;

  std::string qAddress;
  RetrieveQueue rqueue(m_objectStore);
  ScopedExclusiveLock rqlock;

  {
    RootEntry re(m_objectStore);
    re.fetchNoLock();
    rootFetchNoLockTime = t.secs(utils::Timer::resetCounter);
    try {
      qAddress = re.getRetrieveQueueAddress(vid, common::dataStructures::JobQueueType::JobsToTransferForUser);
      rqueue.setAddress(qAddress);
    } catch (cta::exception::Exception & ex) {
      ScopedExclusiveLock rexl(re);
      rootRelockExclusiveTime = t.secs(utils::Timer::resetCounter);
      re.fetch();
      rootRefetchTime = t.secs(utils::Timer::resetCounter);
      qAddress = re.addOrGetRetrieveQueueAndCommit(vid, *m_agentReference, common::dataStructures::JobQueueType::JobsToTransferForUser);
      rqueue.setAddress(qAddress);
      addOrGetQueueandCommitTime = t.secs(utils::Timer::resetCounter);
    }
  }

  try {
    rqlock.lock(rqueue);
    queueLockTime = t.secs(utils::Timer::resetCounter);
    rqueue.fetch();
    queueFetchTime = t.secs(utils::Timer::resetCounter);
    rqueue.setQueueCleanupDoCleanup(val);
    rqueue.commit();
  } catch (cta::exception::Exception &ex) {
    log::ScopedParamContainer params(logContext);
    params.add("queueObject", qAddress)
          .add("tapeVid", vid)
          .add("cleanupFlagValue", val)
          .add("exceptionMessage", ex.getMessageValue());
    logContext.log(log::DEBUG, "WARNING: In OStoreDB::setRetrieveQueueCleanupFlag(): failed to set cleanup flag value on retrieve queue.");
  }

  double processingTime = t.secs(utils::Timer::resetCounter);
  log::ScopedParamContainer params (logContext);
  params.add("queueObject", qAddress)
        .add("tapeVid", vid)
        .add("cleanupFlagValue", val)
        .add("rootFetchNoLockTime", rootFetchNoLockTime)
        .add("rootRelockExclusiveTime", rootRelockExclusiveTime)
        .add("rootRefetchTime", rootRefetchTime)
        .add("addOrGetQueueandCommitTime", addOrGetQueueandCommitTime)
        .add("queueLockTime", queueLockTime)
        .add("queueFetchTime", queueFetchTime)
        .add("processingTime", processingTime);
  if(queueLockTime > 1 || queueFetchTime > 1){
    logContext.log(log::WARNING, "In OStoreDB::setRetrieveQueueCleanupFlag(): fetched a retrieve queue and that lasted more than 1 second.");
  }
}

//------------------------------------------------------------------------------
// OStoreDB::getNextRepackJobToExpand()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::RepackRequest> OStoreDB::getNextRepackJobToExpand() {
  typedef objectstore::ContainerAlgorithms<RepackQueue,RepackQueueToExpand> RQTEAlgo;
  RQTEAlgo rqteAlgo(m_objectStore, *m_agentReference);
  log::LogContext lc(m_logger);

  RQTEAlgo::PopCriteria criteria;
  // pop request that is in RepackQueueToExpandRequest
  auto jobs = rqteAlgo.popNextBatch(std::nullopt, criteria, lc);
  if(jobs.elements.empty()) {
    // If there is no request
    return nullptr;
  }
  // Get the first request that is in elements
  auto repackRequest = jobs.elements.front().repackRequest.get();
  auto repackInfo = jobs.elements.front().repackInfo;
  // build the repackRequest with the repack info
  std::unique_ptr<SchedulerDatabase::RepackRequest> ret = std::make_unique<OStoreDB::RepackRequest>(repackRequest->getAddressIfSet(), *this);
  ret->repackInfo.vid = repackInfo.vid;
  ret->repackInfo.type = repackInfo.type;
  ret->repackInfo.status = repackInfo.status;
  ret->repackInfo.repackBufferBaseURL = repackInfo.repackBufferBaseURL;
  ret->repackInfo.noRecall = repackInfo.noRecall;
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getNextRepackReportBatch()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::RepackReportBatch> OStoreDB::getNextRepackReportBatch(log::LogContext& lc) {
  try {
    return getNextSuccessfulRetrieveRepackReportBatch(lc);
  } catch (NoRepackReportBatchFound &) {}
  try{
    return getNextFailedRetrieveRepackReportBatch(lc);
  } catch(const NoRepackReportBatchFound &) {}
  try {
    return getNextSuccessfulArchiveRepackReportBatch(lc);
  } catch (NoRepackReportBatchFound &) {}
  try{
    return getNextFailedArchiveRepackReportBatch(lc);
  } catch (NoRepackReportBatchFound &) {}
  return nullptr;
}

//------------------------------------------------------------------------------
// OStoreDB::getRepackReportBatches()
//------------------------------------------------------------------------------
std::list<std::unique_ptr<SchedulerDatabase::RepackReportBatch>> OStoreDB::getRepackReportBatches(log::LogContext &lc){
  std::list<std::unique_ptr<SchedulerDatabase::RepackReportBatch>> ret;
  try{
    ret.push_back(std::move(getNextSuccessfulRetrieveRepackReportBatch(lc)));
  } catch (const NoRepackReportBatchFound &){}
  try{
    ret.push_back(std::move(getNextFailedRetrieveRepackReportBatch(lc)));
  } catch (const NoRepackReportBatchFound &){}
  try{
    ret.push_back(std::move(getNextSuccessfulArchiveRepackReportBatch(lc)));
  } catch (const NoRepackReportBatchFound &){}
  try{
    ret.push_back(std::move(getNextFailedArchiveRepackReportBatch(lc)));
  } catch (const NoRepackReportBatchFound &){}
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::getNextSuccessfulRetrieveRepackReportBatch()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::RepackReportBatch> OStoreDB::getNextSuccessfulRetrieveRepackReportBatch(log::LogContext& lc) {
  typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueToReportToRepackForSuccess> Carqtrtrfs;
  Carqtrtrfs algo(this->m_objectStore, *m_agentReference);
  // Decide from which queue we are going to pop.
  RootEntry re(m_objectStore);
  while(true) {
    re.fetchNoLock();
    auto queueList = re.dumpRetrieveQueues(common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess);
    if (queueList.empty()) throw NoRepackReportBatchFound("In OStoreDB::getNextSuccessfulRetrieveRepackReportBatch(): no queue found.");

    // Try to get jobs from the first queue. If it is empty, it will be trimmed, so we can go for another round.
    Carqtrtrfs::PopCriteria criteria;
    criteria.files = c_repackRetrieveReportBatchSize;
    auto jobs = algo.popNextBatch(queueList.front().vid, criteria, lc);
    if(jobs.elements.empty()) continue;
    std::unique_ptr<RepackRetrieveSuccessesReportBatch> privateRet;
    privateRet.reset(new RepackRetrieveSuccessesReportBatch(m_objectStore, *this));
    std::set<std::string> repackRequestAddresses;
    for(auto &j : jobs.elements)
    {
      privateRet->m_subrequestList.emplace_back(RepackRetrieveSuccessesReportBatch::SubrequestInfo());
      auto & sr = privateRet->m_subrequestList.back();
      sr.repackInfo = j.repackInfo;
      sr.archiveFile = j.archiveFile;
      sr.owner = m_agentReference;
      sr.subrequest.reset(j.retrieveRequest.release());
      repackRequestAddresses.insert(j.repackInfo.repackRequestAddress);
    }
    // As we are popping from a single report queue, all requests should concern only one repack request.
    if (repackRequestAddresses.size() != 1) {
      std::stringstream err;
      err << "In OStoreDB::getNextSuccessfulRetrieveRepackReportBatch(): reports for several repack requests in the same queue. ";
      for (auto & rr: repackRequestAddresses) { err << rr << " "; }
      throw exception::Exception(err.str());
    }
    privateRet->m_repackRequest.setAddress(*repackRequestAddresses.begin());

    return std::unique_ptr<SchedulerDatabase::RepackReportBatch>(privateRet.release());
  }
  throw NoRepackReportBatchFound("In OStoreDB::getNextSuccessfulRetrieveRepackReportBatch(): no report found.");
}

//------------------------------------------------------------------------------
// OStoreDB::getNextFailedRetrieveRepackReportBatch()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::RepackReportBatch> OStoreDB::getNextFailedRetrieveRepackReportBatch(log::LogContext &lc){
  typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueToReportToRepackForFailure> CaRqtrtrff;
  CaRqtrtrff algo(this->m_objectStore,*m_agentReference);
  // Decide from which queue we are going to pop.
  RootEntry re(m_objectStore);
  while(true) {
    re.fetchNoLock();
    auto queueList = re.dumpRetrieveQueues(common::dataStructures::JobQueueType::JobsToReportToRepackForFailure);
    if (queueList.empty()) throw NoRepackReportBatchFound("In OStoreDB::getNextFailedRetrieveRepackReportBatch(): no queue found.");

    // Try to get jobs from the first queue. If it is empty, it will be trimmed, so we can go for another round.
    CaRqtrtrff::PopCriteria criteria;
    criteria.files = c_repackRetrieveReportBatchSize;
    auto jobs = algo.popNextBatch(queueList.front().vid, criteria, lc);
    if(jobs.elements.empty()) continue;
    std::unique_ptr<RepackRetrieveFailureReportBatch> privateRet;
    privateRet.reset(new RepackRetrieveFailureReportBatch(m_objectStore, *this));
    std::set<std::string> repackRequestAddresses;
    for(auto &j : jobs.elements)
    {
      privateRet->m_subrequestList.emplace_back(RepackRetrieveFailureReportBatch::SubrequestInfo());
      auto & sr = privateRet->m_subrequestList.back();
      sr.repackInfo = j.repackInfo;
      sr.archiveFile = j.archiveFile;
      sr.subrequest.reset(j.retrieveRequest.release());
      repackRequestAddresses.insert(j.repackInfo.repackRequestAddress);
    }
    // As we are popping from a single report queue, all requests should concern only one repack request.
    if (repackRequestAddresses.size() != 1) {
      std::stringstream err;
      err << "In OStoreDB::getNextFailedRetrieveRepackReportBatch(): reports for several repack requests in the same queue. ";
      for (auto & rr: repackRequestAddresses) { err << rr << " "; }
      throw exception::Exception(err.str());
    }
    privateRet->m_repackRequest.setAddress(*repackRequestAddresses.begin());

    return std::unique_ptr<SchedulerDatabase::RepackReportBatch>(privateRet.release());
  }
  throw NoRepackReportBatchFound("In OStoreDB::getNextFailedRetrieveRepackReportBatch(): no report found.");
}

//------------------------------------------------------------------------------
// OStoreDB::getNextSuccessfulArchiveRepackReportBatch()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::RepackReportBatch> OStoreDB::getNextSuccessfulArchiveRepackReportBatch(log::LogContext& lc) {
  typedef objectstore::ContainerAlgorithms<ArchiveQueue, ArchiveQueueToReportToRepackForSuccess> Caaqtrtrfs;
  Caaqtrtrfs algo(this->m_objectStore, *m_agentReference);
  // Decide from which queue we are going to pop.
  RootEntry re(m_objectStore);
  while(true) {
    re.fetchNoLock();
    auto queueList = re.dumpArchiveQueues(common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess);
    if (queueList.empty()) throw NoRepackReportBatchFound("In OStoreDB::getNextSuccessfulArchiveRepackReportBatch(): no queue found.");

    // Try to get jobs from the first queue. If it is empty, it will be trimmed, so we can go for another round.
    Caaqtrtrfs::PopCriteria criteria;
    criteria.files = c_repackArchiveReportBatchSize;
    auto jobs = algo.popNextBatch(queueList.front().cId, criteria, lc);
    if(jobs.elements.empty()) continue;
    std::unique_ptr<RepackArchiveSuccessesReportBatch> privateRet;
    privateRet.reset(new RepackArchiveSuccessesReportBatch(m_objectStore, *this));
    std::set<std::string> repackRequestAddresses;
    for(auto &j : jobs.elements)
    {
      privateRet->m_subrequestList.emplace_back(RepackArchiveSuccessesReportBatch::SubrequestInfo());
      auto & sr = privateRet->m_subrequestList.back();
      sr.repackInfo = j.repackInfo;
      sr.archivedCopyNb = j.copyNb;
      sr.archiveJobsStatusMap = j.archiveJobsStatusMap;
      sr.archiveFile = j.archiveFile;
      sr.subrequest.reset(j.archiveRequest.release());
      repackRequestAddresses.insert(j.repackInfo.repackRequestAddress);
    }
    // As we are popping from a single report queue, all requests should concern only one repack request.
    if (repackRequestAddresses.size() != 1) {
      std::stringstream err;
      err << "In OStoreDB::getNextSuccessfulArchiveRepackReportBatch(): reports for several repack requests in the same queue. ";
      for (auto & rr: repackRequestAddresses) { err << rr << " "; }
      throw exception::Exception(err.str());
    }
    privateRet->m_repackRequest.setAddress(*repackRequestAddresses.begin());

    return std::unique_ptr<SchedulerDatabase::RepackReportBatch>(privateRet.release());
  }
  throw NoRepackReportBatchFound("In OStoreDB::getNextSuccessfulArchiveRepackReportBatch(): no report found.");
}

std::unique_ptr<SchedulerDatabase::RepackReportBatch> OStoreDB::getNextFailedArchiveRepackReportBatch(log::LogContext& lc){
  typedef objectstore::ContainerAlgorithms<ArchiveQueue, ArchiveQueueToReportToRepackForFailure> Caaqtrtrff;
  Caaqtrtrff algo(this->m_objectStore, *m_agentReference);
  // Decide from which queue we are going to pop.
  RootEntry re(m_objectStore);
  while(true) {
    re.fetchNoLock();
    auto queueList = re.dumpArchiveQueues(common::dataStructures::JobQueueType::JobsToReportToRepackForFailure);
    if (queueList.empty()) throw NoRepackReportBatchFound("In OStoreDB::getNextFailedArchiveRepackReportBatch(): no queue found.");
    // Try to get jobs from the first queue. If it is empty, it will be trimmed, so we can go for another round.
    Caaqtrtrff::PopCriteria criteria;
    criteria.files = c_repackArchiveReportBatchSize;
    auto jobs = algo.popNextBatch(queueList.front().cId, criteria, lc);
    if(jobs.elements.empty()) continue;
    std::unique_ptr<RepackArchiveFailureReportBatch> privateRet;
    privateRet.reset(new RepackArchiveFailureReportBatch(m_objectStore, *this));
    std::set<std::string> repackRequestAddresses;
    for(auto &j : jobs.elements)
    {
      privateRet->m_subrequestList.emplace_back(RepackArchiveFailureReportBatch::SubrequestInfo());
      auto & sr = privateRet->m_subrequestList.back();
      sr.repackInfo = j.repackInfo;
      sr.archivedCopyNb = j.copyNb;
      sr.archiveJobsStatusMap = j.archiveJobsStatusMap;
      sr.archiveFile = j.archiveFile;
      sr.subrequest.reset(j.archiveRequest.release());
      repackRequestAddresses.insert(j.repackInfo.repackRequestAddress);
    }
    // As we are popping from a single report queue, all requests should concern only one repack request.
    if (repackRequestAddresses.size() != 1) {
      std::stringstream err;
      err << "In OStoreDB::getNextFailedArchiveRepackReportBatch(): reports for several repack requests in the same queue. ";
      for (auto & rr: repackRequestAddresses) { err << rr << " "; }
      throw exception::Exception(err.str());
    }
    privateRet->m_repackRequest.setAddress(*repackRequestAddresses.begin());

    return std::unique_ptr<SchedulerDatabase::RepackReportBatch>(privateRet.release());
  }
   throw NoRepackReportBatchFound("In OStoreDB::getNextFailedArchiveRepackReportBatch(): no report found.");
}

//------------------------------------------------------------------------------
// OStoreDB::getNextSuccessfulRetrieveRepackReportBatch()
//------------------------------------------------------------------------------
void OStoreDB::RepackRetrieveSuccessesReportBatch::report(log::LogContext& lc) {
  // We have a batch of popped requests to report. We will first record them in the repack requests (update statistics),
  // then transform the requests (retrieve to archives) and finally queue the archive jobs in the right queues.
  // As usual there are many opportunities for failure.
  utils::Timer t;
  log::TimingList timingList;
  cta::common::dataStructures::MountPolicy mountPolicy;
  // 1) Update statistics. As the repack request is protected against double reporting, we can release its lock
  // before the next step.
  {
    // Prepare the report
    objectstore::RepackRequest::SubrequestStatistics::List ssl;
    for (auto &rr: m_subrequestList) {
      ssl.push_back(objectstore::RepackRequest::SubrequestStatistics());
      ssl.back().bytes = rr.archiveFile.fileSize;
      ssl.back().files = 1;
      ssl.back().fSeq = rr.repackInfo.fSeq;
      ssl.back().hasUserProvidedFile = rr.repackInfo.hasUserProvidedFile;
    }
    // Record it.
    timingList.insertAndReset("successStatsPrepareTime", t);
    objectstore::ScopedExclusiveLock rrl(m_repackRequest);
    timingList.insertAndReset("successStatsLockTime", t);
    m_repackRequest.fetch();
    mountPolicy = m_repackRequest.getMountPolicy();
    timingList.insertAndReset("successStatsFetchTime", t);
    m_repackRequest.reportRetriveSuccesses(ssl);
    timingList.insertAndReset("successStatsUpdateTime", t);
    m_repackRequest.commit();
    timingList.insertAndReset("successStatsCommitTime", t);
  }

  // 2) We should async transform the retrieve requests into archive requests.
  // From this point on, failing to transform is counted as a failure to archive.
  struct SuccessfullyTranformedRequest {
    std::shared_ptr<objectstore::ArchiveRequest> archiveRequest;
    SubrequestInfo & subrequestInfo;
    SorterArchiveRequest sorterArchiveRequest;
  };
  std::list<SuccessfullyTranformedRequest> successfullyTransformedSubrequests;
  uint64_t nbTransformedSubrequest = 0;
  {
    objectstore::RepackRequest::SubrequestStatistics::List failedArchiveSSL;
    std::list<SubrequestInfo *> failedSubrequests;
    struct AsyncTransformerAndReq {
      SubrequestInfo & subrequestInfo;
      std::unique_ptr<objectstore::RetrieveRequest::AsyncRetrieveToArchiveTransformer> transformer;
    };
    std::list<AsyncTransformerAndReq> asyncTransformsAndReqs;
    for (auto &rr: m_subrequestList) {
      try {
        asyncTransformsAndReqs.push_back({
          rr,
          std::unique_ptr<objectstore::RetrieveRequest::AsyncRetrieveToArchiveTransformer>(
            rr.subrequest->asyncTransformToArchiveRequest(*m_oStoreDb.m_agentReference)
          )
        });
      } catch (const cta::exception::NoSuchObject &ex){
        log::ScopedParamContainer params(lc);
        params.add("fileId", rr.archiveFile.archiveFileID)
              .add("subrequestAddress", rr.subrequest->getAddressIfSet())
              .add("exceptionMsg", ex.getMessageValue());
        lc.log(log::WARNING, "In OStoreDB::RepackRetrieveSuccessesReportBatch::report(): failed to asyncTransformToArchiveRequest(), object does not exist in the objectstore.");
      } catch (exception::Exception & ex) {
        // We failed to archive the file (to create the request, in fact). So all the copyNbs
        // can be counted as failed.
        for (auto cnbtr: rr.repackInfo.copyNbsToRearchive) {
          failedArchiveSSL.push_back(objectstore::RepackRequest::SubrequestStatistics());
          auto & fassl = failedArchiveSSL.back();
          fassl.bytes = rr.archiveFile.fileSize;
          fassl.files = 1;
          fassl.fSeq = rr.repackInfo.fSeq;
          fassl.copyNb = cnbtr;
        }
        // We will need to delete the request too.
        failedSubrequests.push_back(&rr);
        // Log the error
        log::ScopedParamContainer params(lc);
        params.add("fileId", rr.archiveFile.archiveFileID)
              .add("subrequestAddress", rr.subrequest->getAddressIfSet())
              .add("exceptionMsg", ex.getMessageValue());
        lc.log(log::ERR, "In OStoreDB::RepackRetrieveSuccessesReportBatch::report(): failed to asyncTransformToArchiveRequest().");
      }
    }
    timingList.insertAndReset("asyncTransformLaunchTime", t);

    // 2. b. Deal with transformation results (and log the transformation...
    for (auto &atar: asyncTransformsAndReqs) {
      try {
        atar.transformer->wait();
        nbTransformedSubrequest++;
        // Log the transformation
        log::ScopedParamContainer params(lc);
        params.add("fileId", atar.subrequestInfo.archiveFile.archiveFileID)
              .add("subrequestAddress", atar.subrequestInfo.subrequest->getAddressIfSet());
        timingList.addToLog(params);
        lc.log(log::INFO, "In OStoreDB::RepackRetrieveSuccessesReportBatch::report(), turned successful retrieve request in archive request.");
        SorterArchiveRequest sorterArchiveRequest;
        for(auto & copyNbToArchive: atar.subrequestInfo.repackInfo.copyNbsToRearchive){
          SorterArchiveJob sorterArchiveJob;
          sorterArchiveJob.archiveFile = atar.subrequestInfo.archiveFile;
          sorterArchiveJob.archiveRequest = std::make_shared<objectstore::ArchiveRequest>(
                atar.subrequestInfo.subrequest->getAddressIfSet(),
                m_oStoreDb.m_objectStore);
          sorterArchiveJob.jobDump.copyNb = copyNbToArchive;
          sorterArchiveJob.jobDump.tapePool = atar.subrequestInfo.repackInfo.archiveRouteMap[copyNbToArchive];
          sorterArchiveJob.jobQueueType = common::dataStructures::JobQueueType::JobsToTransferForRepack;
          sorterArchiveJob.mountPolicy = mountPolicy;
          sorterArchiveJob.previousOwner = atar.subrequestInfo.owner;
          sorterArchiveRequest.archiveJobs.push_back(sorterArchiveJob);
        }
        successfullyTransformedSubrequests.push_back(SuccessfullyTranformedRequest{
          std::make_shared<objectstore::ArchiveRequest>(
              atar.subrequestInfo.subrequest->getAddressIfSet(),
              m_oStoreDb.m_objectStore),
          atar.subrequestInfo,
          sorterArchiveRequest
        });
      } catch (const cta::exception::NoSuchObject &ex){
         log::ScopedParamContainer params(lc);
        params.add("fileId", atar.subrequestInfo.archiveFile.archiveFileID)
              .add("subrequestAddress", atar.subrequestInfo.subrequest->getAddressIfSet())
              .add("exceptionMsg", ex.getMessageValue());
        lc.log(log::WARNING, "In OStoreDB::RepackRetrieveSuccessesReportBatch::report(): async transformation failed on wait(). Object does not exist in the objectstore");
      } catch (exception::Exception & ex) {
        // We failed to archive the file (to create the request, in fact). So all the copyNbs
        // can be counted as failed.
        for (auto cnbtr: atar.subrequestInfo.repackInfo.copyNbsToRearchive) {
          failedArchiveSSL.push_back(objectstore::RepackRequest::SubrequestStatistics());
          auto & fassl = failedArchiveSSL.back();
          fassl.bytes = atar.subrequestInfo.archiveFile.fileSize;
          fassl.files = 1;
          fassl.fSeq = atar.subrequestInfo.repackInfo.fSeq;
          fassl.copyNb = cnbtr;
        }
        // We will need to delete the request too.
        failedSubrequests.push_back(&atar.subrequestInfo);
        // Log the error
        log::ScopedParamContainer params(lc);
        params.add("fileId", atar.subrequestInfo.archiveFile.archiveFileID)
              .add("subrequestAddress", atar.subrequestInfo.subrequest->getAddressIfSet())
              .add("exceptionMsg", ex.getMessageValue());
        lc.log(log::ERR, "In OStoreDB::RepackRetrieveSuccessesReportBatch::report(): async transformation failed on wait().");
      }
    }
    timingList.insertAndReset("asyncTransformCompletionTime", t);

    // 3) Deal with transformation failures (and delete the requests) :
    // - record the deletion intent in + status in repack request results
    // - async delete
    // - wait deletions
    if (failedSubrequests.size()) {
      // Record the stats (before deleting the requests, otherwise we could leak some counting
      // in case of failure).
      objectstore::ScopedExclusiveLock rrl(m_repackRequest);
      timingList.insertAndReset("failureStatsLockTime", t);
      m_repackRequest.fetch();
      timingList.insertAndReset("failureStatsFetchTime", t);
      m_repackRequest.reportArchiveFailures(failedArchiveSSL);
      timingList.insertAndReset("failureStatsUpdateTime", t);
      m_repackRequest.commit();
      timingList.insertAndReset("failureStatsCommitTime", t);
      // And now delete the requests
      struct AsyncDeleteAndReq {
        SubrequestInfo & subrequestInfo;
        std::unique_ptr<RetrieveRequest::AsyncJobDeleter> deleter;
      };
      std::list<std::string> retrieveRequestsToUnown;
      std::list<AsyncDeleteAndReq> asyncDeleterAndReqs;
      for (auto &fs: failedSubrequests) {
        // This is the end of error handling. If we fail to delete a request, so be it.
        retrieveRequestsToUnown.push_back(fs->subrequest->getAddressIfSet());
        try {
          asyncDeleterAndReqs.push_back({*fs,
              std::unique_ptr<RetrieveRequest::AsyncJobDeleter>(fs->subrequest->asyncDeleteJob())});
        } catch (const cta::exception::NoSuchObject &ex) {
          log::ScopedParamContainer params(lc);
          params.add("fileId", fs->archiveFile.archiveFileID)
                .add("subrequestAddress", fs->subrequest->getAddressIfSet())
                .add("exceptionMsg", ex.getMessageValue());
          lc.log(log::WARNING, "In OStoreDB::RepackRetrieveSuccessesReportBatch::report(): failed to asyncDelete() retrieve request. Object does not exist in the objectstore.");
        } catch (cta::exception::Exception &ex) {
          // Log the failure to delete.
          log::ScopedParamContainer params(lc);
          params.add("fileId", fs->archiveFile.archiveFileID)
                .add("subrequestAddress", fs->subrequest->getAddressIfSet())
                .add("exceptionMsg", ex.getMessageValue());
          lc.log(log::ERR, "In OStoreDB::RepackRetrieveSuccessesReportBatch::report(): failed to asyncDelete() retrieve request.");
        }
      }
      timingList.insertAndReset("asyncDeleteRetrieveLaunchTime", t);
      for (auto &adar: asyncDeleterAndReqs) {
        try {
          adar.deleter->wait();
          // Log the deletion
          log::ScopedParamContainer params(lc);
          params.add("fileId", adar.subrequestInfo.archiveFile.archiveFileID)
                .add("subrequestAddress", adar.subrequestInfo.subrequest->getAddressIfSet());
          lc.log(log::INFO, "In OStoreDB::RepackRetrieveSuccessesReportBatch::report(): deleted retrieve request after failure to transform in archive request.");
        } catch (const cta::exception::NoSuchObject & ex) {
          // Log the failure to delete.
          log::ScopedParamContainer params(lc);
          params.add("fileId", adar.subrequestInfo.archiveFile.archiveFileID)
                .add("subrequestAddress", adar.subrequestInfo.subrequest->getAddressIfSet())
                .add("exceptionMsg", ex.getMessageValue());
          lc.log(log::WARNING, "In OStoreDB::RepackRetrieveSuccessesReportBatch::report(): async deletion of retrieve request failed on wait(). Object does not exist in the objectstore.");
        } catch (cta::exception::Exception & ex) {
          // Log the failure to delete.
          log::ScopedParamContainer params(lc);
          params.add("fileId", adar.subrequestInfo.archiveFile.archiveFileID)
                .add("subrequestAddress", adar.subrequestInfo.subrequest->getAddressIfSet())
                .add("exceptionMsg", ex.getMessageValue());
          lc.log(log::ERR, "In OStoreDB::RepackRetrieveSuccessesReportBatch::report(): async deletion of retrieve request failed on wait().");
        }
      }
      timingList.insertAndReset("asyncDeleteRetrieveWaitTime", t);
      m_oStoreDb.m_agentReference->removeBatchFromOwnership(retrieveRequestsToUnown, m_oStoreDb.m_objectStore);
      timingList.insertAndReset("removeDeletedRetrieveFromOwnershipTime", t);
      log::ScopedParamContainer params(lc);
      params.add("agentAddress",m_oStoreDb.m_agentReference->getAgentAddress());
      lc.log(log::INFO,"In OStoreDB::RepackRetrieveSuccessesReportBatch::report(): successfully removed retrieve requests from the agent's ownership.");
    }
  }

  // 3. We now just need to queue the freshly created archive jobs into their respective queues
  {
    objectstore::Sorter sorter(*m_oStoreDb.m_agentReference, m_oStoreDb.m_objectStore, m_oStoreDb.m_catalogue);
    for (auto &sts: successfullyTransformedSubrequests) {
      sorter.insertArchiveRequest(sts.sorterArchiveRequest, *m_oStoreDb.m_agentReference, lc);
    }
    sorter.flushAll(lc);
  }
  timingList.insertAndReset("archiveRequestsQueueingTime", t);
  log::ScopedParamContainer params(lc);
  params.add("numberOfTransformedSubrequests",nbTransformedSubrequest);
  timingList.addToLog(params);
  lc.log(log::INFO,"In OStoreDB::RepackRetrieveSuccessesReportBatch::report(): Processed a batch of reports.");
}

//------------------------------------------------------------------------------
// OStoreDB::RepackRetrieveFailureReportBatch()
//------------------------------------------------------------------------------
void OStoreDB::RepackRetrieveFailureReportBatch::report(log::LogContext& lc){
  // We have a batch of popped failed Retrieve requests to report. We will first record them in the repack requests (update statistics),
  // and then erase the request from the objectstore
  utils::Timer t;
  log::TimingList timingList;

  std::list<uint64_t> fSeqsToDelete;
  // 1) Update statistics. As the repack request is protected against double reporting, we can release its lock
  // before the next step.
  {
    // Prepare the report
    objectstore::RepackRequest::SubrequestStatistics::List ssl;
    uint64_t failedToCreateArchiveReq = 0;
    for (auto &rr: m_subrequestList) {
      ssl.push_back(objectstore::RepackRequest::SubrequestStatistics());
      ssl.back().bytes = rr.archiveFile.fileSize;
      ssl.back().files = 1;
      ssl.back().fSeq = rr.repackInfo.fSeq;
      fSeqsToDelete.push_back(rr.repackInfo.fSeq);
      for(auto& copyNb: rr.repackInfo.copyNbsToRearchive){
        (void) copyNb;
        failedToCreateArchiveReq++;
      }
    }
    // Record it.
    timingList.insertAndReset("failureStatsPrepareTime", t);
    objectstore::ScopedExclusiveLock rrl(m_repackRequest);
    timingList.insertAndReset("failureStatsLockTime", t);
    m_repackRequest.fetch();
    timingList.insertAndReset("failureStatsFetchTime", t);
    m_repackRequest.reportSubRequestsForDeletion(fSeqsToDelete);
    timingList.insertAndReset("failureStatsReportSubRequestsForDeletionTime", t);
    m_repackRequest.reportArchiveCreationFailures(failedToCreateArchiveReq);
    timingList.insertAndReset("failureArchiveCreationStatsUpdateTime",t);
    m_repackRequest.reportRetriveFailures(ssl);
    timingList.insertAndReset("failureStatsUpdateTime", t);
    m_repackRequest.commit();
    timingList.insertAndReset("failureStatsCommitTime", t);

    //Delete all the failed RetrieveRequests
    struct AsyncDeleteAndReq {
      SubrequestInfo & subrequestInfo;
      std::unique_ptr<RetrieveRequest::AsyncJobDeleter> deleter;
    };
    //List of requests to remove from ownership
    std::list<std::string> retrieveRequestsToUnown;
    //List of the deleters of the subrequests
    std::list<AsyncDeleteAndReq> asyncDeleterAndReqs;

    for(auto& fs: m_subrequestList){
      retrieveRequestsToUnown.push_back(fs.subrequest->getAddressIfSet());
      try{
        asyncDeleterAndReqs.push_back({fs,std::unique_ptr<RetrieveRequest::AsyncJobDeleter>(fs.subrequest->asyncDeleteJob())});
      } catch (cta::exception::NoSuchObject &ex) {
        log::ScopedParamContainer params(lc);
        params.add("fileId", fs.archiveFile.archiveFileID)
              .add("subrequestAddress", fs.subrequest->getAddressIfSet())
              .add("exceptionMsg", ex.getMessageValue());
        lc.log(log::WARNING, "In OStoreDB::RepackRetrieveFailureReportBatch::report(): failed to asyncDelete() retrieve request. Object does not exist in the objectstore.");
      } catch (cta::exception::Exception &ex) {
        // Log the failure to delete.
        log::ScopedParamContainer params(lc);
        params.add("fileId", fs.archiveFile.archiveFileID)
              .add("subrequestAddress", fs.subrequest->getAddressIfSet())
              .add("exceptionMsg", ex.getMessageValue());
        lc.log(log::ERR, "In OStoreDB::RepackRetrieveFailureReportBatch::report(): failed to asyncDelete() retrieve request.");
      }
    }

    timingList.insertAndReset("asyncDeleteRetrieveLaunchTime", t);
    for (auto &adar: asyncDeleterAndReqs) {
      try {
        adar.deleter->wait();
        // Log the deletion
        log::ScopedParamContainer params(lc);
        params.add("fileId", adar.subrequestInfo.archiveFile.archiveFileID)
              .add("subrequestAddress", adar.subrequestInfo.subrequest->getAddressIfSet());
        timingList.addToLog(params);
        lc.log(log::INFO, "In OStoreDB::RepackRetrieveFailureReportBatch::report(): deleted retrieve request after multiple failures");
        timingList.clear();
      } catch (const cta::exception::NoSuchObject & ex) {
        // Log the failure to delete.
        log::ScopedParamContainer params(lc);
        params.add("fileId", adar.subrequestInfo.archiveFile.archiveFileID)
              .add("subrequestAddress", adar.subrequestInfo.subrequest->getAddressIfSet())
              .add("exceptionMsg", ex.getMessageValue());
        lc.log(log::WARNING, "In OStoreDB::RepackRetrieveFailureReportBatch::report(): async deletion of retrieve request failed on wait(). Object does not exist in the objectstore.");
      } catch (cta::exception::Exception & ex) {
        // Log the failure to delete.
        log::ScopedParamContainer params(lc);
        params.add("fileId", adar.subrequestInfo.archiveFile.archiveFileID)
              .add("subrequestAddress", adar.subrequestInfo.subrequest->getAddressIfSet())
              .add("exceptionMsg", ex.getMessageValue());
        lc.log(log::ERR, "In OStoreDB::RepackRetrieveFailureReportBatch::report(): async deletion of retrieve request failed on wait().");
      }
    }
    timingList.insertAndReset("asyncDeleteRetrieveWaitTime", t);
    m_oStoreDb.m_agentReference->removeBatchFromOwnership(retrieveRequestsToUnown, m_oStoreDb.m_objectStore);
    timingList.insertAndReset("removeDeletedRetrieveFromOwnershipTime", t);
    log::ScopedParamContainer params(lc);
    timingList.addToLog(params);
    params.add("agentAddress",m_oStoreDb.m_agentReference->getAgentAddress());
    lc.log(log::INFO,"In OStoreDB::RepackRetrieveFailureReportBatch::report(): successfully removed retrieve requests from the agent's ownership.");
  }
}

//------------------------------------------------------------------------------
// OStoreDB::RepackRequest::getLastExpandedFSeq()
//------------------------------------------------------------------------------
uint64_t OStoreDB::RepackRequest::getLastExpandedFSeq() {
  // We own the repack request, so we are only users of it.
  m_repackRequest.fetchNoLock();
  return m_repackRequest.getLastExpandedFSeq();
}

void OStoreDB::RepackRequest::setLastExpandedFSeq(uint64_t fseq){
  objectstore::ScopedExclusiveLock rrl (m_repackRequest);
  m_repackRequest.fetch();
  m_repackRequest.setLastExpandedFSeq(fseq);
  m_repackRequest.commit();
}

//------------------------------------------------------------------------------
// OStoreDB::RepackRequest::addSubrequests()
//------------------------------------------------------------------------------
uint64_t OStoreDB::RepackRequest::addSubrequestsAndUpdateStats(std::list<Subrequest>& repackSubrequests,
    cta::common::dataStructures::ArchiveRoute::FullMap& archiveRoutesMap, uint64_t maxFSeqLowBound,
    const uint64_t maxAddedFSeq, const cta::SchedulerDatabase::RepackRequest::TotalStatsFiles &totalStatsFiles,
    disk::DiskSystemList diskSystemList, log::LogContext& lc) {
  // We need to prepare retrieve requests names and reference them, create them, enqueue them.
  uint64_t nbRetrieveSubrequestsCreated = 0;
  objectstore::ScopedExclusiveLock rrl (m_repackRequest);
  m_repackRequest.fetch();
  std::set<uint64_t> fSeqs;
  for (auto rsr: repackSubrequests) fSeqs.insert(rsr.fSeq);
  auto subrequestsNames = m_repackRequest.getOrPrepareSubrequestInfo(fSeqs, *m_oStoreDB.m_agentReference);
  m_repackRequest.setTotalStats(totalStatsFiles);
  uint64_t fSeq = std::max(maxFSeqLowBound + 1, maxAddedFSeq + 1);
  common::dataStructures::MountPolicy mountPolicy = m_repackRequest.getMountPolicy();
  bool noRecall = repackInfo.noRecall;
  // We make sure the references to subrequests exist persistently before creating them.
  m_repackRequest.commit();
  // We keep holding the repack request lock: we need to ensure de deleted boolean of each subrequest does
  // not change while we attempt creating them (or we would face double creation).
  // Sort the naming results in a fSeq->requestName map for easier access.
  std::map<uint64_t, objectstore::RepackRequest::SubrequestInfo> subReqInfoMap;
  for (auto &rn: subrequestsNames) { subReqInfoMap[rn.fSeq] = rn; }
  // Try to create the retrieve subrequests (owned by this process, to be queued in a second step)
  // subrequests can already fail at that point if we cannot find a copy on a valid tape.
  std::list<Subrequest> notCreatedSubrequests;
  objectstore::RepackRequest::StatsValues failedCreationStats;
  // First loop: we will issue the async insertions of the subrequests.
  struct AsyncInsertionInfo {
    Subrequest & rsr;
    std::shared_ptr<RetrieveRequest> request;
    std::shared_ptr<RetrieveRequest::AsyncInserter> inserter;
    std::string bestVid;
    uint32_t activeCopyNb;
  };
  //We will insert the jobs by batch of 500
  auto subReqItor = repackSubrequests.begin();
  while(subReqItor != repackSubrequests.end()){
    uint64_t nbSubReqProcessed = 0;
    std::list<AsyncInsertionInfo> asyncInsertionInfoList;
    while(subReqItor != repackSubrequests.end() && nbSubReqProcessed < 500){
      auto & rsr = *subReqItor;
      // Requests marked as deleted are guaranteed to have already been created => we will not re-attempt.
      if (!subReqInfoMap.at(rsr.fSeq).subrequestDeleted) {
        // We need to try and create the subrequest.
        // Create the sub request (it's a retrieve request now).
        auto rr=std::make_shared<objectstore::RetrieveRequest>(subReqInfoMap.at(rsr.fSeq).address, m_oStoreDB.m_objectStore);
        rr->initialize();
        // Set the file info
        common::dataStructures::RetrieveRequest schedReq;
        schedReq.archiveFileID = rsr.archiveFile.archiveFileID;
        schedReq.dstURL = rsr.fileBufferURL;
        schedReq.appendFileSizeToDstURL(rsr.archiveFile.fileSize);
        schedReq.diskFileInfo = rsr.archiveFile.diskFileInfo;
        // dsrr.errorReportURL:  We leave this bank as the reporting will be done to the repack request,
        // stored in the repack info.
        rr->setSchedulerRequest(schedReq);
        // Add the disk system information if needed.
        try {
          auto dsName = diskSystemList.getDSName(schedReq.dstURL);
          rr->setDiskSystemName(dsName);
        } catch (std::out_of_range &) {}
        // Set the repack info.
        RetrieveRequest::RepackInfo rRRepackInfo;
        try {
          for (auto & ar: archiveRoutesMap.at(rsr.archiveFile.storageClass)) {
            rRRepackInfo.archiveRouteMap[ar.second.copyNb] = ar.second.tapePoolName;
          }
          //Check that we do not have the same destination tapepool for two different copyNb
          for(auto & currentCopyNbTapePool: rRRepackInfo.archiveRouteMap){
            int nbTapepool = std::count_if(rRRepackInfo.archiveRouteMap.begin(),rRRepackInfo.archiveRouteMap.end(),[&currentCopyNbTapePool](const std::pair<uint64_t,std::string> & copyNbTapepool){
              return copyNbTapepool.second == currentCopyNbTapePool.second;
            });
            if(nbTapepool != 1){
              throw cta::ExpandRepackRequestException("In OStoreDB::RepackRequest::addSubrequestsAndUpdateStats(), found the same destination tapepool for different copyNb.");
            }
          }
        } catch (std::out_of_range &) {
          notCreatedSubrequests.emplace_back(rsr);
          failedCreationStats.files++;
          failedCreationStats.bytes+=rsr.archiveFile.fileSize;
          log::ScopedParamContainer params(lc);
          params.add("fileID", rsr.archiveFile.archiveFileID)
                .add("diskInstance", rsr.archiveFile.diskInstance)
                .add("storageClass", rsr.archiveFile.storageClass);
          std::stringstream storageClassList;
          bool first=true;
          for (auto & sc: archiveRoutesMap) {
            std::string storageClass;
            storageClass = sc.first;
            storageClassList << (first?"":" ") << " sc=" << storageClass << " rc=" << sc.second.size();
          }
          params.add("storageClassList", storageClassList.str());
          lc.log(log::ERR, "In OStoreDB::RepackRequest::addSubrequests(): not such archive route.");
          goto nextSubrequest;
        }
        rRRepackInfo.copyNbsToRearchive = rsr.copyNbsToRearchive;
        rRRepackInfo.fileBufferURL = rsr.fileBufferURL;
        rRRepackInfo.fSeq = rsr.fSeq;
        rRRepackInfo.isRepack = true;
        rRRepackInfo.repackRequestAddress = m_repackRequest.getAddressIfSet();
        if(rsr.hasUserProvidedFile){
          rRRepackInfo.hasUserProvidedFile = true;
        }
        rr->setRepackInfo(rRRepackInfo);
        // Set the queueing parameters
        common::dataStructures::RetrieveFileQueueCriteria fileQueueCriteria;
        fileQueueCriteria.archiveFile = rsr.archiveFile;
        fileQueueCriteria.mountPolicy = mountPolicy;
        rr->setRetrieveFileQueueCriteria(fileQueueCriteria);
        // Decide of which vid we are going to retrieve from. Here, if we can retrieve from the repack VID, we
        // will set the initial recall on it. Retries will we requeue to best VID as usual if needed.
        std::string bestVid;
        uint32_t activeCopyNumber = 0;
        if(noRecall){
          bestVid = repackInfo.vid;
        } else {
          //No --no-recall flag, make sure we have a copy on the vid we intend to repack.
          for (auto & tc: rsr.archiveFile.tapeFiles) {
            if (tc.vid == repackInfo.vid) {
              try {
                // Try to select the repack VID from a one-vid list.
                Helpers::selectBestRetrieveQueue({repackInfo.vid}, m_oStoreDB.m_catalogue, m_oStoreDB.m_objectStore, true);
                bestVid = repackInfo.vid;
                activeCopyNumber = tc.copyNb;
              } catch (Helpers::NoTapeAvailableForRetrieve &) {}
              break;
            }
          }
        }
        for (auto &tc: rsr.archiveFile.tapeFiles)
          if (tc.vid == bestVid) {
            activeCopyNumber = tc.copyNb;
            goto copyNbFound;
          }
        {
          // Count the failure for this subrequest.
          notCreatedSubrequests.emplace_back(rsr);
          failedCreationStats.files++;
          failedCreationStats.bytes += rsr.archiveFile.fileSize;
          log::ScopedParamContainer params(lc);
          params.add("fileId", rsr.archiveFile.archiveFileID)
                .add("repackVid", repackInfo.vid)
                .add("chosenVid", bestVid);
          lc.log(log::ERR,
              "In OStoreDB::RepackRequest::addSubrequests(): could not find the copyNb for the chosen VID. Subrequest failed.");
          goto nextSubrequest;
        }
      copyNbFound:;
        if(rsr.hasUserProvidedFile) {
            /**
             * As the user has provided the file through the Repack buffer folder,
             * we will not Retrieve the file from the tape. We create the Retrieve
             * Request but directly with the status RJS_ToReportToRepackForSuccess so that
             * this retrieve request is queued in the RetrieveQueueToReportToRepackForSuccess
             * and hence be transformed into an ArchiveRequest.
             */
            rr->setJobStatus(activeCopyNumber,serializers::RetrieveJobStatus::RJS_ToReportToRepackForSuccess);
        }
        // We have the best VID. The request is ready to be created after comleting its information.
        rr->setOwner(m_oStoreDB.m_agentReference->getAgentAddress());
        rr->setActiveCopyNumber(activeCopyNumber);
        // We can now try to insert the request. It could alredy have been created (in which case it must exist).
        // We hold the lock to the repack request, no better not waste time, so we async create.
        try {
          std::shared_ptr<objectstore::RetrieveRequest::AsyncInserter> rrai(rr->asyncInsert());
          asyncInsertionInfoList.emplace_back(AsyncInsertionInfo{rsr, rr, rrai, bestVid, activeCopyNumber});
        } catch (cta::objectstore::ObjectOpsBase::NotNewObject &objExists){
          //The retrieve subrequest already exists in the objectstore and is not deleted, we log and don't do anything
          log::ScopedParamContainer params(lc);
          params.add("copyNb",activeCopyNumber)
                .add("repackVid",repackInfo.vid)
                .add("bestVid",bestVid)
                .add("fileId",rsr.archiveFile.archiveFileID);
          lc.log(log::ERR, "In OStoreDB::RepackRequest::addSubrequests(): could not asyncInsert the subrequest because it already exists, continuing expansion");
          goto nextSubrequest;
        } catch (exception::Exception & ex) {
          // We can fail to serialize here...
          // Count the failure for this subrequest.
          notCreatedSubrequests.emplace_back(rsr);
          failedCreationStats.files++;
          failedCreationStats.bytes += rsr.archiveFile.fileSize;
          log::ScopedParamContainer params(lc);
          params.add("fileId", rsr.archiveFile.archiveFileID)
                .add("repackVid", repackInfo.vid)
                .add("bestVid", bestVid)
                .add("ExceptionMessage", ex.getMessageValue());
          lc.log(log::ERR,
              "In OStoreDB::RepackRequest::addSubrequests(): could not asyncInsert the subrequest.");
        }
      }
      nextSubrequest:
        nbSubReqProcessed++;
        subReqItor++;
    }
    // We can now check the subrequests creations succeeded, and prepare their queueing.
    struct AsyncInsertedSubrequestInfo {
      Subrequest & rsr;
      std::string bestVid;
      uint32_t activeCopyNb;
      std::shared_ptr<RetrieveRequest> request;
    };
    std::list <AsyncInsertedSubrequestInfo> asyncInsertedSubrequestInfoList;
    for (auto & aii : asyncInsertionInfoList) {
      // Check the insertion succeeded.
      try {
        aii.inserter->wait();
        log::ScopedParamContainer params(lc);
        params.add("fileID", aii.rsr.archiveFile.archiveFileID);
        std::stringstream copyNbList;
        bool first = true;
        for (auto cn: aii.rsr.copyNbsToRearchive) { copyNbList << (first?"":" ") << cn; first = false; }
        params.add("copyNbsToRearchive", copyNbList.str())
              .add("subrequestObject", aii.request->getAddressIfSet())
              .add("fileBufferURL", aii.rsr.fileBufferURL);
        lc.log(log::INFO, "In OStoreDB::RepackRequest::addSubrequests(): subrequest created.");
        asyncInsertedSubrequestInfoList.emplace_back(AsyncInsertedSubrequestInfo{aii.rsr, aii.bestVid, aii.activeCopyNb, aii.request});
      } catch (exception::Exception & ex) {
        // Count the failure for this subrequest.
        notCreatedSubrequests.emplace_back(aii.rsr);
        failedCreationStats.files++;
        failedCreationStats.bytes += aii.rsr.archiveFile.fileSize;
        log::ScopedParamContainer params(lc);
        params.add("fileId", aii.rsr.archiveFile)
              .add("repackVid", repackInfo.vid)
              .add("bestVid", aii.bestVid)
              .add("bestCopyNb", aii.activeCopyNb)
              .add("ExceptionMessage", ex.getMessageValue());
        lc.log(log::ERR,
            "In OStoreDB::RepackRequest::addSubrequests(): could not asyncInsert the subrequest.");
      }
    }
    if(notCreatedSubrequests.size()){
      log::ScopedParamContainer params(lc);
      params.add("files", failedCreationStats.files);
      params.add("bytes", failedCreationStats.bytes);
      m_repackRequest.reportRetrieveCreationFailures(notCreatedSubrequests);
      m_repackRequest.commit();
      lc.log(log::ERR, "In OStoreDB::RepackRequest::addSubRequests(), reported the failed creation of Retrieve Requests to the Repack request");
    }
    // We now have created the subrequests. Time to enqueue.
    // TODO: the lock/fetch could be parallelized
    {
      objectstore::Sorter sorter(*m_oStoreDB.m_agentReference, m_oStoreDB.m_objectStore, m_oStoreDB.m_catalogue);
      std::list<objectstore::ScopedExclusiveLock> locks;
      for (auto &is: asyncInsertedSubrequestInfoList) {
        locks.emplace_back(*is.request);
        is.request->fetch();
        sorter.insertRetrieveRequest(is.request, *m_oStoreDB.m_agentReference, is.activeCopyNb, lc);
      }
      nbRetrieveSubrequestsCreated = sorter.getAllRetrieve().size();
      locks.clear();
      sorter.flushAll(lc);
    }
  }
  m_repackRequest.setLastExpandedFSeq(fSeq);
  m_repackRequest.commit();
  //General
  return nbRetrieveSubrequestsCreated;
}

//------------------------------------------------------------------------------
// OStoreDB::RepackRequest::expandDone()
//------------------------------------------------------------------------------
void OStoreDB::RepackRequest::expandDone() {
  // We are now done with the repack request. We can set its status.
  ScopedExclusiveLock rrl(m_repackRequest);
  m_repackRequest.fetch();

  m_repackRequest.setExpandFinished(true);
  m_repackRequest.setStatus();
  m_repackRequest.commit();
}

//------------------------------------------------------------------------------
// OStoreDB::RepackRequest::fail()
//------------------------------------------------------------------------------
void OStoreDB::RepackRequest::fail() {
  ScopedExclusiveLock rrl(m_repackRequest);
  m_repackRequest.fetch();
  m_repackRequest.setStatus(common::dataStructures::RepackInfo::Status::Failed);
  m_repackRequest.removeFromOwnerAgentOwnership();
  m_repackRequest.commit();
}

//------------------------------------------------------------------------------
// OStoreDB::RepackRequest::requeueInToExpandQueue()
//------------------------------------------------------------------------------
void OStoreDB::RepackRequest::requeueInToExpandQueue(log::LogContext& lc){
  ScopedExclusiveLock rrl(m_repackRequest);
  m_repackRequest.fetch();
  std::string previousOwner = m_repackRequest.getOwner();
  m_repackRequest.setStatus();
  m_repackRequest.commit();
  rrl.release();
  std::unique_ptr<cta::objectstore::RepackRequest> rr(new cta::objectstore::RepackRequest(m_repackRequest.getAddressIfSet(),m_oStoreDB.m_objectStore));
  typedef objectstore::ContainerAlgorithms<RepackQueue, RepackQueueToExpand> RQTEAlgo;
  RQTEAlgo rqteAlgo(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
  RQTEAlgo::InsertedElement::list insertedElements;
  insertedElements.push_back(RQTEAlgo::InsertedElement());
  insertedElements.back().repackRequest = std::move(rr);
  rqteAlgo.referenceAndSwitchOwnership(std::nullopt, previousOwner, insertedElements, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::RepackRequest::setExpandStartedAndChangeStatus()
//------------------------------------------------------------------------------
void OStoreDB::RepackRequest::setExpandStartedAndChangeStatus(){
  ScopedExclusiveLock rrl(m_repackRequest);
  m_repackRequest.fetch();
  m_repackRequest.setExpandStarted(true);
  m_repackRequest.setStatus();
  m_repackRequest.commit();
}

//------------------------------------------------------------------------------
// OStoreDB::RepackRequest::fillLastExpandedFSeqAndTotalStatsFile()
//------------------------------------------------------------------------------
void OStoreDB::RepackRequest::fillLastExpandedFSeqAndTotalStatsFile(uint64_t& fSeq, TotalStatsFiles& totalStatsFiles) {
  ScopedExclusiveLock rrl(m_repackRequest);
  m_repackRequest.fetch();
  fSeq = m_repackRequest.getLastExpandedFSeq();
  totalStatsFiles = m_repackRequest.getTotalStatsFile();
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
  } catch (cta::exception::Exception &) {
    throw exception::UserError("In OStoreDB::cancelRepack(): No repack request for this VID (index not present).");
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
          throw exception::Exception("In OStoreDB::getRepackInfo(): unexpected vid when reading request");
        // We now have a hold of the repack request.
        // We should delete all the file level subrequests.
        rr.deleteAllSubrequests();
        // And then delete the request
        std::string repackRequestOwner = rr.getOwner();
        try {
          //In the case the owner is not a Repack queue,
          //the owner is an agent. We remove it from its ownership
          rr.removeFromOwnerAgentOwnership();
        } catch(const cta::exception::Exception &ex){
          //The owner is a queue, so continue
        }
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
            std::list<std::string> reqs{rr.getAddressIfSet()};
            rq.removeRequestsAndCommit(reqs);
          }
          catch (cta::exception::NoSuchObject &) { return; }
          catch (objectstore::ObjectOpsBase::WrongType &) {
          }
        }
        //Delete the repack request now
        rr.remove();
        return;
      } catch (cta::exception::Exception &ex) {
        lc.log(cta::log::ERR,ex.getMessageValue());
        return;
      }
    }
  }
  throw exception::UserError("In OStoreDB::cancelRepack(): No repack request for this VID.");
}

//------------------------------------------------------------------------------
// OStoreDB::getNextRetrieveJobsToReportBatch()
//------------------------------------------------------------------------------
std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> OStoreDB::getNextRetrieveJobsToReportBatch(
  uint64_t filesRequested, log::LogContext &logContext) {
  using RQTRAlgo = objectstore::ContainerAlgorithms<RetrieveQueue, RetrieveQueueToReportForUser>;
  RQTRAlgo rqtrAlgo(m_objectStore, *m_agentReference);
  // Decide from which queue we are going to pop
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  while (true) {
    auto queueList = re.dumpRetrieveQueues(common::dataStructures::JobQueueType::JobsToReportToUser);
    std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> ret;
    if (queueList.empty()) return ret;

    // Try to get jobs from the first queue. If it is empty, it will be trimmed, so we can go for another round.
    RQTRAlgo::PopCriteria criteria;
    criteria.files = filesRequested;
    auto jobs = rqtrAlgo.popNextBatch(queueList.front().vid, criteria, logContext);
    if (jobs.elements.empty()) continue;
    for (auto &j : jobs.elements) {
      std::unique_ptr<OStoreDB::RetrieveJob> rj(new OStoreDB::RetrieveJob(j.retrieveRequest->getAddressIfSet(), *this, nullptr));
      rj->archiveFile = j.archiveFile;
      rj->retrieveRequest = j.rr;
      rj->selectedCopyNb = j.copyNb;
      rj->errorReportURL = j.errorReportURL;
      rj->reportType = j.reportType;
      rj->m_repackInfo = j.repackInfo;
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
  auto queueList = re.dumpRetrieveQueues(common::dataStructures::JobQueueType::FailedJobs);
  for (auto &rj : queueList) {
    RetrieveQueue rq(rj.address, m_objectStore);
    try {
      rq.fetchNoLock();
    } catch (cta::exception::Exception &ex) {
      log::ScopedParamContainer params(logContext);
      params.add("queueObject", rj.address)
            .add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::DEBUG, "WARNING: In OStoreDB::getRetrieveJobsFailedSummary(): failed to lock/fetch a retrieve queue.");
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
std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> OStoreDB::getNextRetrieveJobsFailedBatch(
  uint64_t filesRequested, log::LogContext &logContext) {
  using RQTRAlgo = objectstore::ContainerAlgorithms<RetrieveQueue, RetrieveQueueFailed>;
  RQTRAlgo rqtrAlgo(m_objectStore, *m_agentReference);
  // Decide from which queue we are going to pop
  RootEntry re(m_objectStore);
  re.fetchNoLock();
  while (true) {
    auto queueList = re.dumpRetrieveQueues(common::dataStructures::JobQueueType::FailedJobs);
    std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> ret;
    if (queueList.empty()) return ret;

    // Try to get jobs from the first queue. If it is empty, it will be trimmed, so we can go for another round.
    RQTRAlgo::PopCriteria criteria;
    criteria.files = filesRequested;
    auto jobs = rqtrAlgo.popNextBatch(queueList.front().vid, criteria, logContext);
    if (jobs.elements.empty()) continue;
    for (auto &j : jobs.elements) {
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
// OStoreDB::TapeMountDecisionInfo::createArchiveMount()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::ArchiveMount> OStoreDB::TapeMountDecisionInfo::createArchiveMount(
  const cta::SchedulerDatabase::PotentialMount& mount, const catalogue::TapeForWriting& tape,
  const std::string& driveName, const std::string& logicalLibrary, const std::string& hostName) {
  // In order to create the mount, we have to:
  // Check we actually hold the scheduling lock
  // Set the drive status to up, and indicate which tape we use.
  common::dataStructures::JobQueueType queueType;
  switch (mount.type) {
  case common::dataStructures::MountType::ArchiveForUser:
    queueType = common::dataStructures::JobQueueType::JobsToTransferForUser;
    break;
  case common::dataStructures::MountType::ArchiveForRepack:
    queueType = common::dataStructures::JobQueueType::JobsToTransferForRepack;
    break;
  default:
    throw cta::exception::Exception("In OStoreDB::TapeMountDecisionInfo::createArchiveMount(): unexpected mount type.");
  }
  std::unique_ptr<OStoreDB::ArchiveMount> privateRet(new OStoreDB::ArchiveMount(m_oStoreDB, queueType));
  auto &am = *privateRet;
  // Check we hold the scheduling lock
  if (!m_lockTaken)
    throw SchedulingLockNotHeld("In OStoreDB::TapeMountDecisionInfo::createArchiveMount: "
      "cannot create mount without holding scheduling lock");
  objectstore::RootEntry re(m_oStoreDB.m_objectStore);
  re.fetchNoLock();
  auto driveRegisterAddress = re.getDriveRegisterAddress();
  am.nbFilesCurrentlyOnTape = tape.lastFSeq;
  // Fill up the mount info
  am.mountInfo.vid = tape.vid;
  am.mountInfo.drive = driveName;
  am.mountInfo.host = hostName;
  am.mountInfo.vo = mount.vo;
  am.mountInfo.mountId = m_schedulerGlobalLock->getIncreaseCommitMountId();
  m_schedulerGlobalLock->commit();
  am.mountInfo.tapePool = tape.tapePool;
  am.mountInfo.logicalLibrary = logicalLibrary;
  am.mountInfo.mediaType = tape.mediaType;
  am.mountInfo.labelFormat = tape.labelFormat;
  am.mountInfo.vendor = tape.vendor;
  am.mountInfo.capacityInBytes = tape.capacityInBytes;
  am.mountInfo.mountType = mount.type;
  am.mountInfo.encryptionKeyName = tape.encryptionKeyName;

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
// OStoreDB::TapeMountDecisionInfo::createRetrieveMount()
//------------------------------------------------------------------------------
std::unique_ptr<SchedulerDatabase::RetrieveMount>
  OStoreDB::TapeMountDecisionInfo::createRetrieveMount(const cta::SchedulerDatabase::PotentialMount& mount,
                                                       const std::string& driveName, const std::string& logicalLibrary,
                                                       const std::string& hostName) {
  // In order to create the mount, we have to:
  // Check we actually hold the scheduling lock
  // Check the tape exists, add it to ownership and set its activity status to
  // busy, with the current agent pointing to it for unbusying
  // Set the drive status to up, but do not commit anything to the drive register
  // the drive register does not need garbage collection as it should reflect the
  // latest known state of the drive (and its absence of updating if needed)
  // Prepare the return value
  std::unique_ptr<OStoreDB::RetrieveMount> privateRet(new OStoreDB::RetrieveMount(m_oStoreDB));
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
  rm.mountInfo.vid = mount.vid;
  rm.mountInfo.drive = driveName;
  rm.mountInfo.host = hostName;
  rm.mountInfo.vo = mount.vo;
  rm.mountInfo.mountId = m_schedulerGlobalLock->getIncreaseCommitMountId();
  m_schedulerGlobalLock->commit();
  rm.mountInfo.tapePool = mount.tapePool;
  rm.mountInfo.logicalLibrary = logicalLibrary;
  rm.mountInfo.mediaType = mount.mediaType;
  // Setting a default value to Format::CTA is only to satisfy unit tests
  // In normal workflow labelFormat value is taken from the tape catalogue after calling getNextMount()
  rm.mountInfo.labelFormat = mount.labelFormat.value_or(cta::common::dataStructures::Label::Format::CTA);
  rm.mountInfo.vendor = mount.vendor;
  rm.mountInfo.capacityInBytes = mount.capacityInBytes;
  rm.mountInfo.activity = mount.activity;
  rm.mountInfo.encryptionKeyName = mount.encryptionKeyName;

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
  // The lock should be released before the objectstore object m_schedulerGlobalLock gets destroyed.
  // We explicitly release the lock, and then destroy the object.
  try {
    if(m_lockTaken) m_lockOnSchedulerGlobalLock.release();
  } catch(objectstore::ObjectOpsBase::NotLocked&) { }
  m_schedulerGlobalLock.reset(nullptr);
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::ArchiveMount()
//------------------------------------------------------------------------------
OStoreDB::ArchiveMount::ArchiveMount(OStoreDB & oStoreDB, const common::dataStructures::JobQueueType& queueType)
  : m_oStoreDB(oStoreDB), m_queueType(queueType) {}

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
  if (m_queueType == common::dataStructures::JobQueueType::JobsToTransferForUser) {
    using AQAlgos = objectstore::ContainerAlgorithms<ArchiveQueue, ArchiveQueueToTransferForUser>;
    AQAlgos aqAlgos(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
    AQAlgos::PopCriteria popCriteria(filesRequested, bytesRequested);
    auto jobs = aqAlgos.popNextBatch(mountInfo.tapePool, popCriteria, logContext);
    // We can construct the return value.
    std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > ret;
    for (auto & j : jobs.elements) {
      std::unique_ptr<OStoreDB::ArchiveJob> aj(new OStoreDB::ArchiveJob(j.archiveRequest->getAddressIfSet(), m_oStoreDB));
      aj->tapeFile.copyNb = j.copyNb;
      aj->archiveFile = j.archiveFile;
      aj->archiveReportURL = j.archiveReportURL;
      aj->errorReportURL = j.errorReportURL;
      aj->srcURL = j.srcURL;
      aj->tapeFile.fSeq = ++nbFilesCurrentlyOnTape;
      aj->tapeFile.vid = mountInfo.vid;
      aj->tapeFile.blockId = std::numeric_limits<decltype(aj->tapeFile.blockId)>::max();
      aj->m_jobOwned = true;
      aj->m_mountId = mountInfo.mountId;
      aj->m_tapePool = mountInfo.tapePool;
      aj->reportType = j.reportType;
      ret.emplace_back(std::move(aj));
    }
    return ret;
  } else {
    using AQAlgos = objectstore::ContainerAlgorithms<ArchiveQueue, ArchiveQueueToTransferForRepack>;
    AQAlgos aqAlgos(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
    AQAlgos::PopCriteria popCriteria(filesRequested, bytesRequested);
    auto jobs = aqAlgos.popNextBatch(mountInfo.tapePool, popCriteria, logContext);
    // We can construct the return value.
    std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob> > ret;
    for (auto & j : jobs.elements) {
      std::unique_ptr<OStoreDB::ArchiveJob> aj(new OStoreDB::ArchiveJob(j.archiveRequest->getAddressIfSet(), m_oStoreDB));
      aj->tapeFile.copyNb = j.copyNb;
      aj->archiveFile = j.archiveFile;
      aj->archiveReportURL = j.archiveReportURL;
      aj->errorReportURL = j.errorReportURL;
      aj->srcURL = j.srcURL;
      aj->tapeFile.fSeq = ++nbFilesCurrentlyOnTape;
      aj->tapeFile.vid = mountInfo.vid;
      aj->tapeFile.blockId = std::numeric_limits<decltype(aj->tapeFile.blockId)>::max();
      aj->m_jobOwned = true;
      aj->m_mountId = mountInfo.mountId;
      aj->m_tapePool = mountInfo.tapePool;
      aj->reportType = j.reportType;
      ret.emplace_back(std::move(aj));
    }
    return ret;
  }
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::ArchiveJob()
//------------------------------------------------------------------------------
OStoreDB::ArchiveJob::ArchiveJob(const std::string& jobAddress, OStoreDB& oStoreDB) :
  m_jobOwned(false),
  m_mountId(0),
  m_oStoreDB(oStoreDB),
  m_archiveRequest(jobAddress, m_oStoreDB.m_objectStore) { }

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
std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> OStoreDB::RetrieveMount::getNextJobBatch(uint64_t filesRequested,
    uint64_t bytesRequested, log::LogContext& logContext) {
  typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransfer> RQAlgos;
  RQAlgos rqAlgos(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
  RQAlgos::PoppedElementsBatch jobs;
  RQAlgos::PopCriteria popCriteria(filesRequested, bytesRequested);
  for (auto &dsts: m_diskSystemsToSkip) popCriteria.diskSystemsToSkip.insert({dsts.name, dsts.sleepTime});
  jobs = rqAlgos.popNextBatch(mountInfo.vid, popCriteria, logContext);
  // Construct the return value
  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> ret;
  for(auto &j : jobs.elements)
  {
    std::unique_ptr<OStoreDB::RetrieveJob> rj(new OStoreDB::RetrieveJob(j.retrieveRequest->getAddressIfSet(), m_oStoreDB, this));
    rj->archiveFile = j.archiveFile;
    rj->diskSystemName = j.diskSystemName;
    rj->retrieveRequest = j.rr;
    rj->selectedCopyNb = j.copyNb;
    rj->isRepack = j.repackInfo.isRepack;
    rj->m_repackInfo = j.repackInfo;
    rj->m_jobOwned = true;
    rj->m_mountId = mountInfo.mountId;
    ret.emplace_back(std::move(rj));
  }
  return ret;
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::putQueueToSleep()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveMount::putQueueToSleep(const std::string &diskSystemName, const uint64_t sleepTime, log::LogContext &logContext) {
  objectstore::RetrieveQueue rq(m_oStoreDB.m_objectStore);
  objectstore::ScopedExclusiveLock lock;
  auto queueType = cta::common::dataStructures::JobQueueType::JobsToTransferForUser;
  std::optional<std::string> vid(mountInfo.vid);

  Helpers::getLockedAndFetchedJobQueue<RetrieveQueue>(rq, lock, *m_oStoreDB.m_agentReference, vid, queueType, logContext);

  rq.setSleepForFreeSpaceStartTimeAndName(::time(nullptr), diskSystemName, sleepTime);
  rq.commit();
  lock.release();
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::requeueJobBatch()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveMount::requeueJobBatch(std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>>& jobBatch,
  log::LogContext& logContext)
{
  objectstore::Sorter sorter(*m_oStoreDB.m_agentReference, m_oStoreDB.m_objectStore, m_oStoreDB.m_catalogue);
  std::list<std::shared_ptr<objectstore::RetrieveRequest>> rrlist;
  std::list<objectstore::ScopedExclusiveLock> locks;
  for(auto& j : jobBatch) {
    OStoreDB::RetrieveJob *job = dynamic_cast<OStoreDB::RetrieveJob*>(j.get());
    auto rr = std::make_shared<objectstore::RetrieveRequest>(job->m_retrieveRequest.getAddressIfSet(), m_oStoreDB.m_objectStore);
    rrlist.push_back(rr);
    try {
      locks.emplace_back(*rr);
      rr->fetch();
      sorter.insertRetrieveRequest(rr, *m_oStoreDB.m_agentReference, std::nullopt, logContext);
    } catch(exception::NoSuchObject&) {
      log::ScopedParamContainer params(logContext);
      params.add("retrieveRequestId", job->m_retrieveRequest.getAddressIfSet());
      logContext.log(log::INFO, "In OStoreDB::RetrieveMount::requeueJobBatch(): no such retrieve request, ignoring.");
      continue;
    } catch(exception::Exception& ex) {
      job->failTransfer(ex.what(), logContext);
      continue;
    }
  }
  locks.clear();
  rrlist.clear();
  sorter.flushAll(logContext);
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::testReserveDiskSpace()
//------------------------------------------------------------------------------
bool OStoreDB::RetrieveMount::testReserveDiskSpace(const cta::DiskSpaceReservationRequest &diskSpaceReservationRequest,
  const std::string &externalFreeDiskSpaceScript, log::LogContext& logContext) {
  
  // Get the current file systems list from the catalogue
  cta::disk::DiskSystemList diskSystemList;
  diskSystemList = m_oStoreDB.m_catalogue.DiskSystem()->getAllDiskSystems();
  diskSystemList.setExternalFreeDiskSpaceScript(externalFreeDiskSpaceScript);
  cta::disk::DiskSystemFreeSpaceList diskSystemFreeSpace(diskSystemList);

  // Get the existing reservation map from drives.
  auto previousDrivesReservations = m_oStoreDB.m_catalogue.DriveState()->getDiskSpaceReservations();
  // Get the free space from disk systems involved.
  std::set<std::string> diskSystemNames;
  for (auto const & dsrr: diskSpaceReservationRequest) {
    diskSystemNames.insert(dsrr.first);
  }

  try {
    diskSystemFreeSpace.fetchDiskSystemFreeSpace(diskSystemNames, m_oStoreDB.m_catalogue, logContext);
  } catch(const cta::disk::DiskSystemFreeSpaceListException &ex){
    // Could not get free space for one of the disk systems. Currently the retrieve mount will only query
    // one disk system, so just log the failure and put the queue to sleep inside the loop.
    for (const auto &failedDiskSystem: ex.m_failedDiskSystems) {
      cta::log::ScopedParamContainer params(logContext);
      params.add("diskSystemName", failedDiskSystem.first);
      params.add("failureReason", failedDiskSystem.second.getMessageValue());
      logContext.log(cta::log::ERR, "In OStoreDB::RetrieveMount::testReserveDiskSpace(): unable to request EOS free space for disk system, putting queue to sleep");
      auto sleepTime = diskSystemFreeSpace.getDiskSystemList().at(failedDiskSystem.first).sleepTime;
      putQueueToSleep(failedDiskSystem.first, sleepTime, logContext);
    } 
    return false;
  } catch (std::exception &ex) {
    // Leave a log message before letting the possible exception go up the stack.
    cta::log::ScopedParamContainer params(logContext);
    params.add("exceptionWhat", ex.what());
    logContext.log(cta::log::ERR, "In OStoreDB::RetrieveMount::testReserveDiskSpace(): got an exception from diskSystemFreeSpace.fetchDiskSystemFreeSpace().");
    throw;
  }
    
  // If a file system does not have enough space fail the disk space reservation,  put the queue to sleep and 
  // the retrieve mount will immediately stop
  for (auto const &ds: diskSystemNames) {
    uint64_t previousDrivesReservationTotal = 0;
    auto diskSystem = diskSystemFreeSpace.getDiskSystemList().at(ds);
    // Compute previous drives reservation for the physical space of the current disk system.
    for (auto previousDriveReservation: previousDrivesReservations) {
      //avoid empty string when no disk space reservation exists for drive
      if (previousDriveReservation.second != 0) { 
        auto previousDiskSystem = diskSystemFreeSpace.getDiskSystemList().at(previousDriveReservation.first);
        if (diskSystem.diskInstanceSpace.freeSpaceQueryURL == previousDiskSystem.diskInstanceSpace.freeSpaceQueryURL) {
          previousDrivesReservationTotal += previousDriveReservation.second;
        }
      }
    }
    if (diskSystemFreeSpace.at(ds).freeSpace < diskSpaceReservationRequest.at(ds) + diskSystemFreeSpace.at(ds).targetedFreeSpace +
      previousDrivesReservationTotal) {
      cta::log::ScopedParamContainer params(logContext);
      params.add("diskSystemName", ds)
            .add("freeSpace", diskSystemFreeSpace.at(ds).freeSpace)
            .add("existingReservations", previousDrivesReservationTotal)
            .add("spaceToReserve", diskSpaceReservationRequest.at(ds))
            .add("targetedFreeSpace", diskSystemFreeSpace.at(ds).targetedFreeSpace);
      logContext.log(cta::log::WARNING, "In OStoreDB::RetrieveMount::testReserveDiskSpace(): could not allocate disk space for job, applying backpressure");
      
      auto sleepTime = diskSystem.sleepTime;
      putQueueToSleep(ds, sleepTime, logContext);
      return false;
    }
  }
  return true;
}


//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::reserveDiskSpace()
//------------------------------------------------------------------------------
bool OStoreDB::RetrieveMount::reserveDiskSpace(const cta::DiskSpaceReservationRequest &diskSpaceReservationRequest,
  const std::string &externalFreeDiskSpaceScript, log::LogContext& logContext) {

  // Get the current file systems list from the catalogue
  cta::disk::DiskSystemList diskSystemList;
  diskSystemList = m_oStoreDB.m_catalogue.DiskSystem()->getAllDiskSystems();
  diskSystemList.setExternalFreeDiskSpaceScript(externalFreeDiskSpaceScript);
  cta::disk::DiskSystemFreeSpaceList diskSystemFreeSpace(diskSystemList);

  // Get the existing reservation map from drives.
  auto previousDrivesReservations = m_oStoreDB.m_catalogue.DriveState()->getDiskSpaceReservations();
  // Get the free space from disk systems involved.
  std::set<std::string> diskSystemNames;
  for (auto const & dsrr: diskSpaceReservationRequest) {
    diskSystemNames.insert(dsrr.first);
  }

  try {
    diskSystemFreeSpace.fetchDiskSystemFreeSpace(diskSystemNames, m_oStoreDB.m_catalogue, logContext);
  } catch(const cta::disk::DiskSystemFreeSpaceListException &ex){
    // Could not get free space for one of the disk systems. Currently the retrieve mount will only query
    // one disk system, so just log the failure and put the queue to sleep inside the loop.
    for (const auto &failedDiskSystem: ex.m_failedDiskSystems) {
      cta::log::ScopedParamContainer params(logContext);
      params.add("diskSystemName", failedDiskSystem.first);
      params.add("failureReason", failedDiskSystem.second.getMessageValue());
      logContext.log(cta::log::ERR, "In OStoreDB::RetrieveMount::reserveDiskSpace(): unable to request EOS free space for disk system, putting queue to sleep");
      auto sleepTime = diskSystemFreeSpace.getDiskSystemList().at(failedDiskSystem.first).sleepTime;
      putQueueToSleep(failedDiskSystem.first, sleepTime, logContext);
    }
    return false;
  } catch (std::exception &ex) {
    // Leave a log message before letting the possible exception go up the stack.
    cta::log::ScopedParamContainer params(logContext);
    params.add("exceptionWhat", ex.what());
    logContext.log(cta::log::ERR, "In OStoreDB::RetrieveMount::reserveDiskSpace(): got an exception from diskSystemFreeSpace.fetchDiskSystemFreeSpace().");
    throw;
  }

  // If a file system does not have enough space fail the disk space reservation,  put the queue to sleep and
  // the retrieve mount will immediately stop
  for (auto const &ds: diskSystemNames) {
    uint64_t previousDrivesReservationTotal = 0;
    auto diskSystem = diskSystemFreeSpace.getDiskSystemList().at(ds);
    // Compute previous drives reservation for the physical space of the current disk system.
    for (auto previousDriveReservation: previousDrivesReservations) {
      //avoid empty string when no disk space reservation exists for drive
      if (previousDriveReservation.second != 0) {
        auto previousDiskSystem = diskSystemFreeSpace.getDiskSystemList().at(previousDriveReservation.first);
        if (diskSystem.diskInstanceSpace.freeSpaceQueryURL == previousDiskSystem.diskInstanceSpace.freeSpaceQueryURL) {
          previousDrivesReservationTotal += previousDriveReservation.second;
        }
      }
    }
    if (diskSystemFreeSpace.at(ds).freeSpace < diskSpaceReservationRequest.at(ds) + diskSystemFreeSpace.at(ds).targetedFreeSpace +
      previousDrivesReservationTotal) {
      cta::log::ScopedParamContainer params(logContext);
      params.add("diskSystemName", ds)
            .add("freeSpace", diskSystemFreeSpace.at(ds).freeSpace)
            .add("existingReservations", previousDrivesReservationTotal)
            .add("spaceToReserve", diskSpaceReservationRequest.at(ds))
            .add("targetedFreeSpace", diskSystemFreeSpace.at(ds).targetedFreeSpace);
      logContext.log(cta::log::WARNING, "In OStoreDB::RetrieveMount::reservediskSpace(): could not allocate disk space for job, applying backpressure");

      auto sleepTime = diskSystem.sleepTime;
      putQueueToSleep(ds, sleepTime, logContext);
      return false;
    }
  }

  m_oStoreDB.m_catalogue.DriveState()->reserveDiskSpace(mountInfo.drive, mountInfo.mountId, diskSpaceReservationRequest,
    logContext);
  return true;
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::setDriveStatus()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveMount::setDriveStatus(cta::common::dataStructures::DriveStatus status,
                                             cta::common::dataStructures::MountType mountType,
                                             time_t completionTime,
                                             const std::optional<std::string>& reason) {
  // We just report the drive status as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=mountInfo.drive;
  driveInfo.logicalLibrary=mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;
  ReportDriveStatusInputs inputs;
  inputs.mountType = mountType;
  inputs.mountSessionId = mountInfo.mountId;
  inputs.reportTime = completionTime;
  inputs.status = status;
  inputs.vid = mountInfo.vid;
  inputs.tapepool = mountInfo.tapePool;
  inputs.vo = mountInfo.vo;
  inputs.reason = reason;
  inputs.activity = mountInfo.activity;
  // TODO: statistics!
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  log::LogContext lc(m_oStoreDB.m_logger);
  m_oStoreDB.m_tapeDrivesState->updateDriveStatus(driveInfo, inputs, lc);
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
  m_oStoreDB.m_tapeDrivesState->updateDriveStatistics(driveInfo, inputs, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveMount::addDiskSystemToSkip()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveMount::addDiskSystemToSkip(const SchedulerDatabase::RetrieveMount::DiskSystemToSkip &diskSystemToSkip) {
  m_diskSystemsToSkip.insert(diskSystemToSkip);
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
// OStoreDB::RetrieveMount::flushAsyncSuccessReports()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveMount::flushAsyncSuccessReports(std::list<cta::SchedulerDatabase::RetrieveJob*>& jobsBatch,
    log::LogContext& lc) {
  std::list<std::string> rjToUnown;
  std::map<std::string, std::list<OStoreDB::RetrieveJob*>> jobsToRequeueForRepackMap;
  // We will wait on the asynchronously started reports of jobs, queue the retrieve jobs
  // for report and remove them from ownership.
  cta::DiskSpaceReservationRequest diskSpaceReservationRequest;
  // 1) Check the async update result.
  common::dataStructures::MountPolicy mountPolicy;
  for (auto & sDBJob: jobsBatch) {
    auto osdbJob = castFromSchedDBJob(sDBJob);
    if (osdbJob->diskSystemName) diskSpaceReservationRequest.addRequest(osdbJob->diskSystemName.value(), osdbJob->archiveFile.fileSize);
    if (osdbJob->isRepack) {
      try {
        osdbJob->m_jobSucceedForRepackReporter->wait();
        std::string vid = osdbJob->archiveFile.tapeFiles.at(osdbJob->selectedCopyNb).vid;
        {
          cta::log::ScopedParamContainer spc(lc);
           spc.add("tapeVid",vid)
             .add("mountType","RetrieveForRepack")
             .add("fileId",osdbJob->archiveFile.archiveFileID);
          lc.log(cta::log::INFO,"In OStoreDB::RetrieveMount::flushAsyncSuccessReports(), retrieve job successful");
        }
        mountPolicy = osdbJob->m_jobSucceedForRepackReporter->m_MountPolicy;
        jobsToRequeueForRepackMap[vid].emplace_back(osdbJob);
      } catch (cta::exception::NoSuchObject &ex){
        log::ScopedParamContainer params(lc);
        params.add("fileId", osdbJob->archiveFile.archiveFileID)
              .add("requestObject", osdbJob->m_retrieveRequest.getAddressIfSet())
              .add("exceptionMessage", ex.getMessageValue());
        lc.log(log::WARNING,
            "In OStoreDB::RetrieveMount::flushAsyncSuccessReports(): async status update failed, job does not exist in the objectstore.");
      } catch (cta::exception::Exception & ex) {
        log::ScopedParamContainer params(lc);
        params.add("fileId", osdbJob->archiveFile.archiveFileID)
              .add("requestObject", osdbJob->m_retrieveRequest.getAddressIfSet())
              .add("exceptionMessage", ex.getMessageValue());
        lc.log(log::ERR,
            "In OStoreDB::RetrieveMount::flushAsyncSuccessReports(): async status update failed. "
            "Will leave job to garbage collection.");
      }
    } else {
      try {
        osdbJob->m_jobDelete->wait();
        {
          //Log for monitoring
          cta::log::ScopedParamContainer spc(lc);
          std::string vid = osdbJob->archiveFile.tapeFiles.at(osdbJob->selectedCopyNb).vid;
          spc.add("tapeVid",vid)
             .add("mountType","RetrieveForUser")
             .add("fileId",osdbJob->archiveFile.archiveFileID);
          lc.log(cta::log::INFO,"In OStoreDB::RetrieveMount::flushAsyncSuccessReports(), retrieve job successful");
        }
        osdbJob->retrieveRequest.lifecycleTimings.completed_time = time(nullptr);
        std::string requestAddress = osdbJob->m_retrieveRequest.getAddressIfSet();

        rjToUnown.push_back(requestAddress);

        cta::common::dataStructures::LifecycleTimings requestTimings = osdbJob->retrieveRequest.lifecycleTimings;
        log::ScopedParamContainer params(lc);
        params.add("requestAddress",requestAddress)
              .add("fileId",osdbJob->archiveFile.archiveFileID)
              .add("vid",osdbJob->m_retrieveMount->mountInfo.vid)
              .add("timeForSelection",requestTimings.getTimeForSelection())
              .add("timeForCompletion", requestTimings.getTimeForCompletion());
        lc.log(log::INFO, "Retrieve job successfully deleted");
      } catch(const cta::exception::NoSuchObject & ex) {
        log::ScopedParamContainer params(lc);
        params.add("fileId", osdbJob->archiveFile.archiveFileID)
              .add("requestObject", osdbJob->m_retrieveRequest.getAddressIfSet())
              .add("exceptionMessage", ex.getMessageValue());
        lc.log(log::WARNING,
            "In OStoreDB::RetrieveMount::flushAsyncSuccessReports(): async deletion failed because the object does not exist anymore. ");
      } catch (cta::exception::Exception & ex) {
        log::ScopedParamContainer params(lc);
        params.add("fileId", osdbJob->archiveFile.archiveFileID)
              .add("requestObject", osdbJob->m_retrieveRequest.getAddressIfSet())
              .add("exceptionMessage", ex.getMessageValue());
        lc.log(log::ERR,
            "In OStoreDB::RetrieveMount::flushAsyncSuccessReports(): async deletion failed. "
            "Will leave job to garbage collection.");
      }
    }
  }
  this->m_oStoreDB.m_catalogue.DriveState()->releaseDiskSpace(mountInfo.drive, mountInfo.mountId,
    diskSpaceReservationRequest, lc);
  // 2) Queue the retrieve requests for repack.
  for (auto & repackRequestQueue: jobsToRequeueForRepackMap) {
    typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueToReportToRepackForSuccess> RQTRTRFSAlgo;
    RQTRTRFSAlgo::InsertedElement::list insertedRequests;
    // Keep a map of objectstore request -> sDBJob to handle errors.
    std::map<objectstore::RetrieveRequest *, OStoreDB::RetrieveJob *> requestToJobMap;
    for (auto & req: repackRequestQueue.second) {
      insertedRequests.push_back(RQTRTRFSAlgo::InsertedElement{&req->m_retrieveRequest, req->selectedCopyNb,
          req->archiveFile.tapeFiles.at(req->selectedCopyNb).fSeq, req->archiveFile.fileSize,
          mountPolicy, req->m_activity, req->m_diskSystemName});
      requestToJobMap[&req->m_retrieveRequest] = req;
       }
    RQTRTRFSAlgo rQTRTRFSAlgo(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
    try {
      rQTRTRFSAlgo.referenceAndSwitchOwnership(repackRequestQueue.first, insertedRequests, lc);
      //rQTRTRFSAlgo.referenceAndSwitchOwnership(repackRequestQueue.first, insertedRequests, lc);
      // In case all goes well, we can remove ownership of all requests.
      for (auto & req: repackRequestQueue.second)  { rjToUnown.push_back(req->m_retrieveRequest.getAddressIfSet()); }
    } catch (RQTRTRFSAlgo::OwnershipSwitchFailure & failure) {
      // Some requests did not make it to the queue. Log and leave them for GC to sort out (leave them in ownership).
      std::set<std::string> failedElements;
      for (auto & fe: failure.failedElements) {
        // Log error.
        log::ScopedParamContainer params(lc);
        params.add("fileId", requestToJobMap.at(fe.element->retrieveRequest)->archiveFile.archiveFileID)
              .add("copyNb", fe.element->copyNb)
              .add("requestObject", fe.element->retrieveRequest->getAddressIfSet());
        std::string logMessage = "In OStoreDB::RetrieveMount::flushAsyncSuccessReports(): failed to queue request to report for repack."
          " Leaving request to be garbage collected.";
        int priority = log::ERR;
        try {
          std::rethrow_exception(fe.failure);
        } catch (cta::exception::NoSuchObject &ex) {
          priority=log::WARNING;
          logMessage = "In OStoreDB::RetrieveMount::flushAsyncSuccessReports(): failed to queue request to report for repack, job does not exist in the objectstore.";
        } catch (cta::exception::Exception & ex) {
          params.add("exeptionMessage", ex.getMessageValue());
        } catch (std::exception & ex) {
          params.add("exceptionWhat", ex.what())
                .add("exceptionTypeName", typeid(ex).name());
        }
        lc.log(priority, logMessage);
        // Add the failed request to the set.
        failedElements.insert(fe.element->retrieveRequest->getAddressIfSet());
      }
      for (auto & req: repackRequestQueue.second)  {
        if (!failedElements.count(req->m_retrieveRequest.getAddressIfSet())) {
          rjToUnown.push_back(req->m_retrieveRequest.getAddressIfSet());
        }
      }
    } catch (exception::Exception & ex) {
      // Something else happened. We just log the error and let the garbage collector go through all the requests.
      log::ScopedParamContainer params(lc);
      params.add("exceptionMessage", ex.getMessageValue());
      lc.log(log::ERR, "In OStoreDB::RetrieveMount::flushAsyncSuccessReports(): failed to queue a batch of requests.");
    }
  }
  // 3) Remove requests from ownership.
  m_oStoreDB.m_agentReference->removeBatchFromOwnership(rjToUnown, m_oStoreDB.m_objectStore);
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::setDriveStatus()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveMount::setDriveStatus(cta::common::dataStructures::DriveStatus status,
                                            cta::common::dataStructures::MountType mountType,
                                            time_t completionTime,
                                            const std::optional<std::string>& reason) {
  // We just report the drive status as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = mountInfo.drive;
  driveInfo.logicalLibrary = mountInfo.logicalLibrary;
  driveInfo.host = mountInfo.host;
  ReportDriveStatusInputs inputs;
  inputs.mountType = mountType;
  inputs.mountSessionId = mountInfo.mountId;
  inputs.reportTime = completionTime;
  inputs.status = status;
  inputs.vid = mountInfo.vid;
  inputs.tapepool = mountInfo.tapePool;
  inputs.vo = mountInfo.vo;
  inputs.reason = reason;
  // TODO: statistics!
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  log::LogContext lc(m_oStoreDB.m_logger);
  m_oStoreDB.m_tapeDrivesState->updateDriveStatus(driveInfo, inputs, lc);
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
  m_oStoreDB.m_tapeDrivesState->updateDriveStatistics(driveInfo, inputs, lc);
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveMount::setJobBatchTransferred()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveMount::setJobBatchTransferred(std::list<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>>& jobsBatch,
    log::LogContext & lc) {
  std::set<cta::OStoreDB::ArchiveJob*> jobsToQueueForReportingToUser, jobsToQueueForReportingToRepack, failedJobsToQueueForReportingForRepack;
  std::list<std::string> ajToUnown;
  utils::Timer t;
  log::TimingList timingList;
  // We will asynchronously report the archive jobs (which MUST be OStoreDBJobs).
  // We let the exceptions through as failing to report is fatal.
  auto jobsBatchItor = jobsBatch.begin();
  while(jobsBatchItor != jobsBatch.end()){
    {
      log::ScopedParamContainer params (lc);
      params.add("tapeVid", (*jobsBatchItor)->tapeFile.vid)
            .add("fileId", (*jobsBatchItor)->archiveFile.archiveFileID)
            .add("requestObject", castFromSchedDBJob(jobsBatchItor->get())->m_archiveRequest.getAddressIfSet());
      lc.log(log::INFO, "In OStoreDB::ArchiveMount::setJobBatchTransferred(): received a job to be reported.");
    }
    try {
      castFromSchedDBJob(jobsBatchItor->get())->asyncSucceedTransfer();
      jobsBatchItor++;
    } catch (cta::exception::NoSuchObject &ex) {
      jobsBatch.erase(jobsBatchItor++);
      log::ScopedParamContainer params(lc);
      params.add("tapeVid", (*jobsBatchItor)->tapeFile.vid)
            .add("fileId", (*jobsBatchItor)->archiveFile.archiveFileID)
            .add("requestObject", castFromSchedDBJob(jobsBatchItor->get())->m_archiveRequest.getAddressIfSet())
            .add("exceptionMessage", ex.getMessageValue());
      lc.log(log::WARNING,
          "In OStoreDB::ArchiveMount::setJobBatchTransferred(): async succeed transfer failed, job does not exist in the objectstore.");
    }
  }

  timingList.insertAndReset("asyncSucceedLaunchTime", t);
  // We will only know whether we need to queue the requests for user for reporting after updating request. So on a first
  // pass we update the request and on the second, we will queue a batch of them to the report queue. Report queues
  // are per VID and not tape pool: this limits contention (one tape written to at a time per mount, so the queuing should
  // be without contention.
  // Jobs that do not require queuing are done from our perspective and we should just remove them from agent ownership.
  // Jobs for repack always get reported.
  jobsBatchItor = jobsBatch.begin();
  while(jobsBatchItor != jobsBatch.end()){
    try {
      castFromSchedDBJob(jobsBatchItor->get())->waitAsyncSucceed();
      auto repackInfo = castFromSchedDBJob(jobsBatchItor->get())->getRepackInfoAfterAsyncSuccess();
      if (repackInfo.isRepack) {
        jobsToQueueForReportingToRepack.insert(castFromSchedDBJob(jobsBatchItor->get()));
      } else {
        if (castFromSchedDBJob(jobsBatchItor->get())->isLastAfterAsyncSuccess())
          jobsToQueueForReportingToUser.insert(castFromSchedDBJob(jobsBatchItor->get()));
        else
          ajToUnown.push_back(castFromSchedDBJob(jobsBatchItor->get())->m_archiveRequest.getAddressIfSet());
      }
      jobsBatchItor++;
    } catch (cta::exception::NoSuchObject &ex) {
      jobsBatch.erase(jobsBatchItor++);
      log::ScopedParamContainer params(lc);
      params.add("fileId", (*jobsBatchItor)->archiveFile.archiveFileID)
            .add("exceptionMessage", ex.getMessageValue());
      lc.log(log::WARNING,
          "In OStoreDB::ArchiveMount::setJobBatchTransferred(): wait async succeed transfer failed, job does not exist in the objectstore.");
    }
  }
  timingList.insertAndReset("asyncSucceedCompletionTime", t);
  if (jobsToQueueForReportingToUser.size()) {
    typedef objectstore::ContainerAlgorithms<objectstore::ArchiveQueue,objectstore::ArchiveQueueToReportForUser> AqtrCa;
    AqtrCa aqtrCa(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
    std::map<std::string, AqtrCa::InsertedElement::list> insertedElementsLists;
    for (auto & j: jobsToQueueForReportingToUser) {
      insertedElementsLists[j->tapeFile.vid].emplace_back(AqtrCa::InsertedElement{&j->m_archiveRequest, j->tapeFile.copyNb,
          j->archiveFile, std::nullopt, std::nullopt});
      log::ScopedParamContainer params (lc);
      params.add("tapeVid", j->tapeFile.vid)
            .add("fileId", j->archiveFile.archiveFileID)
            .add("requestObject", j->m_archiveRequest.getAddressIfSet());
      lc.log(log::INFO, "In OStoreDB::ArchiveMount::setJobBatchTransferred(): will queue request for reporting to user.");
    }
    for (auto &list: insertedElementsLists) {
      try {
        utils::Timer tLocal;
        aqtrCa.referenceAndSwitchOwnership(list.first, m_oStoreDB.m_agentReference->getAgentAddress(), list.second, lc);
        log::ScopedParamContainer params(lc);
        params.add("tapeVid", list.first)
              .add("jobs", list.second.size())
              .add("enqueueTime", t.secs());
        lc.log(log::INFO, "In OStoreDB::ArchiveMount::setJobBatchTransferred(): queued a batch of requests for reporting to user.");
      } catch (cta::exception::Exception & ex) {
        log::ScopedParamContainer params(lc);
        params.add("tapeVid", list.first)
              .add("exceptionMSG", ex.getMessageValue());
        lc.log(log::ERR, "In OStoreDB::ArchiveMount::setJobBatchTransferred(): failed to queue a batch of requests for reporting to user.");
        lc.logBacktrace(log::INFO, ex.backtrace());
      }
    }
    timingList.insertAndReset("queueingToReportToUserTime", t);
  }
  if (jobsToQueueForReportingToRepack.size()) {
    typedef objectstore::ContainerAlgorithms<objectstore::ArchiveQueue,objectstore::ArchiveQueueToReportToRepackForSuccess> AqtrtrCa;
    AqtrtrCa aqtrtrCa(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
    std::map<std::string, AqtrtrCa::InsertedElement::list> insertedElementsLists;
    for (auto & j: jobsToQueueForReportingToRepack) {
      insertedElementsLists[j->getRepackInfoAfterAsyncSuccess().repackRequestAddress].emplace_back(AqtrtrCa::InsertedElement{&j->m_archiveRequest, j->tapeFile.copyNb,
          j->archiveFile, std::nullopt, std::nullopt});
      log::ScopedParamContainer params (lc);
      params.add("repackRequestAddress", j->getRepackInfoAfterAsyncSuccess().repackRequestAddress)
            .add("fileId", j->archiveFile.archiveFileID)
            .add("requestObject", j->m_archiveRequest.getAddressIfSet());
      lc.log(log::INFO, "In OStoreDB::ArchiveMount::setJobBatchTransferred(): will queue request for reporting to repack.");
    }
    for (auto &list: insertedElementsLists) {
      retry:
      int currentTotalRetries = 0;
      int maxRetries = 10;
      try {
        utils::Timer tLocal;
        aqtrtrCa.referenceAndSwitchOwnership(list.first, m_oStoreDB.m_agentReference->getAgentAddress(), list.second, lc);
        log::ScopedParamContainer params(lc);
        params.add("repackRequestAddress", list.first)
              .add("jobs", list.second.size())
              .add("enqueueTime", t.secs());
        lc.log(log::INFO, "In OStoreDB::ArchiveMount::setJobBatchTransferred(): queued a batch of requests for reporting to repack.");
      } catch (cta::exception::NoSuchObject &ex) {
        log::ScopedParamContainer params(lc);
        params.add("tapeVid", list.first)
              .add("exceptionMSG", ex.getMessageValue());
        lc.log(log::WARNING, "In OStoreDB::ArchiveMount::setJobBatchTransferred(): failed to queue a batch of requests for reporting to repack, jobs do not exist in the objectstore.");
      } catch (const AqtrtrCa::OwnershipSwitchFailure &ex) {
        //We are in the case where the ownership of the elements could not have been change (most probably because of a Rados lockbackoff error)
        //We will then retry 10 times to requeue the failed jobs
        log::ScopedParamContainer params(lc);
        params.add("tapeVid", list.first)
              .add("numberOfRetries",currentTotalRetries)
              .add("numberOfFailedToQueueElements",ex.failedElements.size())
              .add("exceptionMSG", ex.getMessageValue());
        lc.log(log::WARNING, "In OStoreDB::ArchiveMount::setJobBatchTransferred(): unable to queue some elements because of an ownership switch failure, retrying another time");
        typedef objectstore::ContainerTraits<ArchiveQueue,ArchiveQueueToReportToRepackForSuccess>::OpFailure<AqtrtrCa::InsertedElement> OpFailure;
        list.second.remove_if([&ex](const AqtrtrCa::InsertedElement &elt){
          //Remove the elements that are NOT in the failed elements list so that we only retry the failed elements
          return std::find_if(ex.failedElements.begin(),ex.failedElements.end(),[&elt](const OpFailure &insertedElement){
            return elt.archiveRequest->getAddressIfSet() == insertedElement.element->archiveRequest->getAddressIfSet() && elt.copyNb == insertedElement.element->copyNb;
          }) == ex.failedElements.end();
        });
        currentTotalRetries ++;
        if(currentTotalRetries <= maxRetries){
          goto retry;
        } else {
          //All the retries have been done, throw an exception for logging the backtrace
          //afterwards
          throw cta::exception::Exception(ex.getMessageValue());
        }
      } catch (cta::exception::Exception & ex) {
        log::ScopedParamContainer params(lc);
        params.add("tapeVid", list.first)
              .add("exceptionMSG", ex.getMessageValue());
        lc.log(log::ERR, "In OStoreDB::ArchiveMount::setJobBatchTransferred(): failed to queue a batch of requests for reporting to repack.");
        lc.logBacktrace(log::INFO, ex.backtrace());
      }
    }
    timingList.insertAndReset("queueingToReportToRepackTime", t);
  }
  if (ajToUnown.size()) {
    m_oStoreDB.m_agentReference->removeBatchFromOwnership(ajToUnown, m_oStoreDB.m_objectStore);
    timingList.insertAndReset("removeFromOwnershipTime", t);
  }
  {
    log::ScopedParamContainer params(lc);
    params.add("QueuedRequests", jobsToQueueForReportingToUser.size())
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
  std::string failureLog = cta::utils::getCurrentLocalTime() + " " + cta::utils::getShortHostname() +
    " " + failureReason;
  // Lock the archive request. Fail the job.
  objectstore::ScopedExclusiveLock arl(m_archiveRequest);
  m_archiveRequest.fetch();
  // Add a job failure. We will know what to do next..
  typedef objectstore::ArchiveRequest::EnqueueingNextStep EnqueueingNextStep;
  typedef EnqueueingNextStep::NextStep NextStep;
  EnqueueingNextStep enQueueingNextStep =
      m_archiveRequest.addTransferFailure(tapeFile.copyNb, m_mountId, failureLog, lc);
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
    case NextStep::EnqueueForReportForUser: {
      m_archiveRequest.commit();
      auto tapepool = m_archiveRequest.getTapePoolForJob(tapeFile.copyNb);
      auto retryStatus = m_archiveRequest.getRetryStatus(tapeFile.copyNb);
      // Algorithms suppose the objects are not locked.
      arl.release();
      typedef objectstore::ContainerAlgorithms<ArchiveQueue,ArchiveQueueToReportForUser> CaAqtr;
      CaAqtr caAqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
      CaAqtr::InsertedElement::list insertedElements;
      insertedElements.push_back(CaAqtr::InsertedElement{&m_archiveRequest, tapeFile.copyNb, archiveFile, std::nullopt, std::nullopt });
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
    case NextStep::EnqueueForReportForRepack:{
      m_archiveRequest.commit();
      auto retryStatus = m_archiveRequest.getRetryStatus(tapeFile.copyNb);
      std::string repackRequestAddress = m_archiveRequest.getRepackInfo().jobsDestination[tapeFile.copyNb];
      // Algorithms suppose the objects are not locked.
      arl.release();
      typedef objectstore::ContainerAlgorithms<ArchiveQueue,ArchiveQueueToReportToRepackForFailure> CaAqtrtrff;
      CaAqtrtrff caAqtrtrff(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
      CaAqtrtrff::InsertedElement::list insertedElements;
      insertedElements.push_back(CaAqtrtrff::InsertedElement{&m_archiveRequest, tapeFile.copyNb, archiveFile, std::nullopt, std::nullopt });
      caAqtrtrff.referenceAndSwitchOwnership(m_archiveRequest.getRepackInfo().jobsDestination[tapeFile.copyNb].destination_vid, insertedElements, lc);
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
          "In ArchiveJob::failTransfer(): enqueued job for reporting for Repack");
      return;
    }
    case NextStep::EnqueueForTransferForUser: {
      m_archiveRequest.commit();
      auto tapepool = m_archiveRequest.getTapePoolForJob(tapeFile.copyNb);
      auto retryStatus = m_archiveRequest.getRetryStatus(tapeFile.copyNb);
      auto mountPolicy = m_archiveRequest.getMountPolicy();
      // Algorithms suppose the objects are not locked.
      arl.release();
      typedef objectstore::ContainerAlgorithms<ArchiveQueue,ArchiveQueueToTransferForUser> CaAqtr;
      CaAqtr caAqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
      CaAqtr::InsertedElement::list insertedElements;
      insertedElements.push_back(CaAqtr::InsertedElement{&m_archiveRequest, tapeFile.copyNb, archiveFile, mountPolicy , std::nullopt });
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
  case NextStep::EnqueueForTransferForRepack:{
      m_archiveRequest.commit();
      auto tapepool = m_archiveRequest.getTapePoolForJob(tapeFile.copyNb);
      auto retryStatus = m_archiveRequest.getRetryStatus(tapeFile.copyNb);
      auto mountPolicy = m_archiveRequest.getMountPolicy();
      // Algorithms suppose the objects are not locked.
      arl.release();
      typedef objectstore::ContainerAlgorithms<ArchiveQueue,ArchiveQueueToTransferForRepack> CaAqtr;
      CaAqtr caAqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
      CaAqtr::InsertedElement::list insertedElements;
      insertedElements.push_back(CaAqtr::InsertedElement{&m_archiveRequest, tapeFile.copyNb, archiveFile, mountPolicy, std::nullopt });
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
          "In ArchiveJob::failTransfer(): requeued job for (potentially in-mount) retry (repack).");
      return;
  }
  case NextStep::StoreInFailedJobsContainer: {
      m_archiveRequest.setFailed();
      m_archiveRequest.commit();
      auto tapepool = m_archiveRequest.getTapePoolForJob(tapeFile.copyNb);
      auto retryStatus = m_archiveRequest.getRetryStatus(tapeFile.copyNb);
      // Algorithms suppose the objects are not locked.
      arl.release();
      typedef objectstore::ContainerAlgorithms<ArchiveQueue,ArchiveQueueFailed> CaAqtr;
      CaAqtr caAqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
      CaAqtr::InsertedElement::list insertedElements;
      insertedElements.push_back(CaAqtr::InsertedElement{&m_archiveRequest, tapeFile.copyNb, archiveFile, std::nullopt, std::nullopt });
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
  std::string failureLog = cta::utils::getCurrentLocalTime() + " " + cta::utils::getShortHostname() +
    " " + failureReason;
  // Lock the archive request. Fail the job.
  objectstore::ScopedExclusiveLock arl(m_archiveRequest);
  m_archiveRequest.fetch();
  // Add a job failure. We will know what to do next..
  typedef objectstore::ArchiveRequest::EnqueueingNextStep EnqueueingNextStep;
  typedef EnqueueingNextStep::NextStep NextStep;
  EnqueueingNextStep enQueueingNextStep =
      m_archiveRequest.addReportFailure(tapeFile.copyNb, m_mountId, failureLog, lc);
  // Don't re-queue the job if reportType is set to NoReportRequired. This can happen if a previous
  // attempt to report failed due to an exception, for example if the file was deleted on close.
  if(reportType == ReportType::NoReportRequired) {
    enQueueingNextStep.nextStep = NextStep::StoreInFailedJobsContainer;
  }

  // First set the job status
  m_archiveRequest.setJobStatus(tapeFile.copyNb, enQueueingNextStep.nextStatus);
  // Now apply the decision.
  // TODO: this will probably need to be factored out.
  switch (enQueueingNextStep.nextStep) {
    // We have a reduced set of supported next steps as some are not compatible with this event (see default).
  case NextStep::EnqueueForReportForUser: {
      m_archiveRequest.commit();
      auto tapepool = m_archiveRequest.getTapePoolForJob(tapeFile.copyNb);
      auto retryStatus = m_archiveRequest.getRetryStatus(tapeFile.copyNb);
      // Algorithms suppose the objects are not locked.
      arl.release();
      typedef objectstore::ContainerAlgorithms<ArchiveQueue,ArchiveQueueToReportForUser> CaAqtr;
      CaAqtr caAqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
      CaAqtr::InsertedElement::list insertedElements;
      insertedElements.push_back(CaAqtr::InsertedElement{&m_archiveRequest, tapeFile.copyNb, archiveFile, std::nullopt, std::nullopt });
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
      m_archiveRequest.setFailed();
      m_archiveRequest.commit();
      auto tapepool = m_archiveRequest.getTapePoolForJob(tapeFile.copyNb);
      auto retryStatus = m_archiveRequest.getRetryStatus(tapeFile.copyNb);
      // Algorithms suppose the objects are not locked.
      arl.release();
      typedef objectstore::ContainerAlgorithms<ArchiveQueue,ArchiveQueueFailed> CaAqtr;
      CaAqtr caAqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
      CaAqtr::InsertedElement::list insertedElements;
      insertedElements.push_back(CaAqtr::InsertedElement{&m_archiveRequest, tapeFile.copyNb, archiveFile, std::nullopt, std::nullopt });
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
  params.add("requestObject", m_archiveRequest.getAddressIfSet())
        .add("destinationVid",tapeFile.vid)
        .add("copyNb",tapeFile.copyNb);
  lc.log(log::DEBUG, "Will start async update archiveRequest for transfer success");
  m_succesfulTransferUpdater.reset(m_archiveRequest.asyncUpdateTransferSuccessful(tapeFile.vid, tapeFile.copyNb));
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::waitAsyncSucceed()
//------------------------------------------------------------------------------
void OStoreDB::ArchiveJob::waitAsyncSucceed() {
  m_succesfulTransferUpdater->wait();
  log::LogContext lc(m_oStoreDB.m_logger);
  log::ScopedParamContainer params(lc);
  params.add("requestObject", m_archiveRequest.getAddressIfSet());
  lc.log(log::DEBUG, "Async update of archiveRequest for transfer success complete");
  // We no more own the job (which could be gone)
  m_jobOwned = false;
  // Ownership removal will be done globally by the caller.
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::isLastAfterAsyncSuccess()
//------------------------------------------------------------------------------
bool OStoreDB::ArchiveJob::isLastAfterAsyncSuccess() {
  return m_succesfulTransferUpdater->m_doReportTransferSuccess;
}

//------------------------------------------------------------------------------
// OStoreDB::ArchiveJob::isLastAfterAsyncSuccess()
//------------------------------------------------------------------------------
objectstore::ArchiveRequest::RepackInfo OStoreDB::ArchiveJob::getRepackInfoAfterAsyncSuccess() {
  return m_succesfulTransferUpdater->m_repackInfo;
}


//------------------------------------------------------------------------------
// OStoreDB::RepackArchiveSuccessesReportBatch::report()
//------------------------------------------------------------------------------
void OStoreDB::RepackArchiveSuccessesReportBatch::report(log::LogContext& lc) {
  objectstore::RepackRequest req(m_repackRequest.getAddressIfSet(),this->m_oStoreDb.m_objectStore);
  req.fetchNoLock();
  this->m_oStoreDb.m_catalogue.Tape()->setTapeDirty(req.getInfo().vid);
  OStoreDB::RepackArchiveReportBatch::report(lc);
}

//------------------------------------------------------------------------------
// OStoreDB::RepackArchiveSuccessesReportBatch::recordReport()
//------------------------------------------------------------------------------
serializers::RepackRequestStatus OStoreDB::RepackArchiveSuccessesReportBatch::recordReport(objectstore::RepackRequest::SubrequestStatistics::List& ssl, log::TimingList& timingList, utils::Timer& t){
  timingList.insertAndReset("successStatsPrepareTime", t);
  objectstore::ScopedExclusiveLock rrl(m_repackRequest);
  timingList.insertAndReset("successStatsLockTime", t);
  m_repackRequest.fetch();
  timingList.insertAndReset("successStatsFetchTime", t);
  serializers::RepackRequestStatus repackRequestStatus = m_repackRequest.reportArchiveSuccesses(ssl);
  timingList.insertAndReset("successStatsUpdateTime", t);
  m_repackRequest.commit();
  timingList.insertAndReset("successStatsCommitTime", t);
  return repackRequestStatus;
}

//------------------------------------------------------------------------------
// OStoreDB::RepackArchiveSuccessesReportBatch::getNewStatus()
//------------------------------------------------------------------------------
serializers::ArchiveJobStatus OStoreDB::RepackArchiveSuccessesReportBatch::getNewStatus(){
  return serializers::ArchiveJobStatus::AJS_Complete;
}

//------------------------------------------------------------------------------
// OStoreDB::RepackArchiveFailureReportBatch::report()
//------------------------------------------------------------------------------
void OStoreDB::RepackArchiveFailureReportBatch::report(log::LogContext& lc){
  OStoreDB::RepackArchiveReportBatch::report(lc);
}

//------------------------------------------------------------------------------
// OStoreDB::RepackArchiveFailureReportBatch::recordReport()
// Return false as at least 1 archive did not work
//------------------------------------------------------------------------------
serializers::RepackRequestStatus OStoreDB::RepackArchiveFailureReportBatch::recordReport(objectstore::RepackRequest::SubrequestStatistics::List& ssl, log::TimingList& timingList, utils::Timer& t){
  timingList.insertAndReset("failureStatsPrepareTime", t);
  objectstore::ScopedExclusiveLock rrl(m_repackRequest);
  timingList.insertAndReset("failureStatsLockTime", t);
  m_repackRequest.fetch();
  timingList.insertAndReset("failureStatsFetchTime", t);
  serializers::RepackRequestStatus repackRequestStatus = m_repackRequest.reportArchiveFailures(ssl);
  timingList.insertAndReset("failureStatsUpdateTime", t);
  m_repackRequest.commit();
  timingList.insertAndReset("failureStatsCommitTime", t);
  return repackRequestStatus;
}

//------------------------------------------------------------------------------
// OStoreDB::RepackArchiveFailureReportBatch::getNewStatus()
//------------------------------------------------------------------------------
serializers::ArchiveJobStatus OStoreDB::RepackArchiveFailureReportBatch::getNewStatus(){
  return serializers::ArchiveJobStatus::AJS_Failed;
}

//------------------------------------------------------------------------------
// OStoreDB::RepackArchiveReportBatch::prepareReport()
//------------------------------------------------------------------------------
objectstore::RepackRequest::SubrequestStatistics::List OStoreDB::RepackArchiveReportBatch::prepareReport() {
  objectstore::RepackRequest::SubrequestStatistics::List ssl;
  for (auto &sri: m_subrequestList) {
    ssl.push_back(objectstore::RepackRequest::SubrequestStatistics());
    ssl.back().bytes = sri.archiveFile.fileSize;
    ssl.back().files = 1;
    ssl.back().fSeq = sri.repackInfo.fSeq;
    ssl.back().copyNb = sri.archivedCopyNb;
    ssl.back().destinationVid = sri.repackInfo.jobsDestination[sri.archivedCopyNb];
    for(auto &j: sri.archiveJobsStatusMap){
      if(j.first != sri.archivedCopyNb){
        if((j.second != objectstore::serializers::ArchiveJobStatus::AJS_Complete) && (j.second != objectstore::serializers::ArchiveJobStatus::AJS_Failed)){
          break;
        } else {
          ssl.back().subrequestDeleted = true;
          break;
        }
      }
    }
  }
  return ssl;
}

//------------------------------------------------------------------------------
// OStoreDB::RepackArchiveReportBatch::report()
//------------------------------------------------------------------------------
void OStoreDB::RepackArchiveReportBatch::report(log::LogContext& lc){
  // We have a batch of popped jobs to report. We will first record them in the repack requests (update statistics),
  // and then either mark them as complete (if any sibling jobs will still require processing) or
  // simply remove the request.
  // Repack request will be filpped from running to successsful (or failed) if we process the last job.
  utils::Timer t;
  log::TimingList timingList;

  // 1) Update statistics. As the repack request is protected against double reporting, we can release its lock
  // before the next (deletions).
  objectstore::RepackRequest::SubrequestStatistics::List statistics = prepareReport();
  objectstore::serializers::RepackRequestStatus repackRequestStatus = recordReport(statistics,timingList,t);
  std::string bufferURL;

  // 2) For each job, determine if sibling jobs are complete or not. If so, delete, else just update status and set empty owner.
  struct Deleters {
    std::unique_ptr<objectstore::ArchiveRequest::AsyncRequestDeleter> deleter;
    RepackReportBatch::SubrequestInfo<objectstore::ArchiveRequest> & subrequestInfo;
    typedef std::list<Deleters> List;
  };
  struct JobOwnerUpdaters {
    std::unique_ptr<objectstore::ArchiveRequest::AsyncJobOwnerUpdater> jobOwnerUpdater;
    RepackReportBatch::SubrequestInfo<objectstore::ArchiveRequest> & subrequestInfo;
    typedef std::list<JobOwnerUpdaters> List;
  };
  Deleters::List deletersList;
  JobOwnerUpdaters::List jobOwnerUpdatersList;
  cta::objectstore::serializers::ArchiveJobStatus newStatus = getNewStatus();
  for (auto &sri: m_subrequestList) {
    bufferURL = sri.repackInfo.fileBufferURL;
    bool moreJobsToDo = false;
    //Check if the ArchiveRequest contains other jobs that are not finished
    for (auto &j: sri.archiveJobsStatusMap) {
      //Getting the siblings jobs (ie copy nb != current one)
      if (j.first != sri.archivedCopyNb) {
        //Sibling job not finished mean its status is nor AJS_Complete nor AJS_Failed
        if ((j.second != serializers::ArchiveJobStatus::AJS_Complete) &&
        (j.second != serializers::ArchiveJobStatus::AJS_Failed)) {
          //The sibling job is not finished, but maybe it is planned to change its status, checking the jobOwnerUpdaterList that is the list containing the jobs
          //we want to change its status to AJS_Complete
          bool copyNbStatusUpdating = (std::find_if(jobOwnerUpdatersList.begin(), jobOwnerUpdatersList.end(), [&j,&sri](JobOwnerUpdaters &jou){
            return ((jou.subrequestInfo.archiveFile.archiveFileID == sri.archiveFile.archiveFileID) && (jou.subrequestInfo.archivedCopyNb == j.first));
          }) != jobOwnerUpdatersList.end());
          if(!copyNbStatusUpdating){
            //The sibling job is not in the jobOwnerUpdaterList, it means that it is not finished yet, there is more jobs to do
            moreJobsToDo = true;
            break;
          }
        }
      }
    }
    objectstore::ArchiveRequest & ar = *sri.subrequest;
    if (moreJobsToDo) {
      try {
        if(ar.exists()){
          jobOwnerUpdatersList.push_back(JobOwnerUpdaters{std::unique_ptr<objectstore::ArchiveRequest::AsyncJobOwnerUpdater> (
                ar.asyncUpdateJobOwner(sri.archivedCopyNb, "", m_oStoreDb.m_agentReference->getAgentAddress(),
                newStatus)),
              sri});
        }
      } catch(const cta::exception::NoSuchObject &ex) {
        // Log the error
        log::ScopedParamContainer params(lc);
        params.add("fileId", sri.archiveFile.archiveFileID)
              .add("subrequestAddress", sri.subrequest->getAddressIfSet())
              .add("exceptionMsg", ex.getMessageValue());
        lc.log(log::WARNING, "In OStoreDB::RepackArchiveReportBatch::report(): failed to asyncUpdateJobOwner(), object does not exist in the objectstore.");
      } catch (cta::exception::Exception & ex) {
        // Log the error
        log::ScopedParamContainer params(lc);
        params.add("fileId", sri.archiveFile.archiveFileID)
              .add("subrequestAddress", sri.subrequest->getAddressIfSet())
              .add("exceptionMsg", ex.getMessageValue());
        lc.log(log::ERR, "In OStoreDB::RepackArchiveReportBatch::report(): failed to asyncUpdateJobOwner()");
      }
    } else {
      try {
        deletersList.push_back({std::unique_ptr<objectstore::ArchiveRequest::AsyncRequestDeleter>(ar.asyncDeleteRequest()), sri});
      } catch(const cta::exception::NoSuchObject &ex) {
        // Log the error
        log::ScopedParamContainer params(lc);
        params.add("fileId", sri.archiveFile.archiveFileID)
              .add("subrequestAddress", sri.subrequest->getAddressIfSet())
              .add("exceptionMsg", ex.getMessageValue());
        lc.log(log::WARNING, "In OStoreDB::RepackArchiveReportBatch::report(): failed to asyncDelete(), object does not exist in the objectstore.");
      } catch (cta::exception::Exception & ex) {
        // Log the error
        log::ScopedParamContainer params(lc);
        params.add("fileId", sri.archiveFile.archiveFileID)
              .add("subrequestAddress", sri.subrequest->getAddressIfSet())
              .add("exceptionMsg", ex.getMessageValue());
        lc.log(log::ERR, "In OStoreDB::RepackArchiveReportBatch::report(): failed to asyncDelete()");
      }
    }
  }
  timingList.insertAndReset("asyncUpdateOrDeleteLaunchTime", t);
  struct DiskFileRemovers{
    std::unique_ptr<cta::disk::AsyncDiskFileRemover> asyncRemover;
    RepackReportBatch::SubrequestInfo<objectstore::ArchiveRequest> & subrequestInfo;
    typedef std::list<DiskFileRemovers> List;
  };
  DiskFileRemovers::List diskFileRemoverList;
  for (auto & d: deletersList) {
    try {
      d.deleter->wait();
      log::ScopedParamContainer params(lc);
      params.add("fileId", d.subrequestInfo.archiveFile.archiveFileID)
            .add("subrequestAddress", d.subrequestInfo.subrequest->getAddressIfSet());
      lc.log(log::INFO, "In OStoreDB::RepackArchiveReportBatch::report(): deleted request.");
      try {
        //Subrequest deleted, async delete the file from the disk
        cta::disk::AsyncDiskFileRemoverFactory asyncDiskFileRemoverFactory;
        std::unique_ptr<cta::disk::AsyncDiskFileRemover> asyncRemover(asyncDiskFileRemoverFactory.createAsyncDiskFileRemover(d.subrequestInfo.repackInfo.fileBufferURL));
        diskFileRemoverList.push_back({std::move(asyncRemover),d.subrequestInfo});
        diskFileRemoverList.back().asyncRemover->asyncDelete();
      } catch (const cta::exception::Exception &ex){
        log::ScopedParamContainer params(lc);
        params.add("fileId", d.subrequestInfo.archiveFile.archiveFileID)
              .add("subrequestAddress", d.subrequestInfo.subrequest->getAddressIfSet())
              .add("exceptionMsg", ex.getMessageValue());
        lc.log(log::ERR, "In OStoreDB::RepackArchiveReportBatch::report(): async deletion of disk file failed.");
      }
    } catch(const cta::exception::NoSuchObject &ex){
      // Log the error
      log::ScopedParamContainer params(lc);
      params.add("fileId", d.subrequestInfo.archiveFile.archiveFileID)
          .add("subrequestAddress", d.subrequestInfo.subrequest->getAddressIfSet())
          .add("exceptionMsg", ex.getMessageValue());
      lc.log(log::WARNING, "In OStoreDB::RepackArchiveReportBatch::report(): async deletion failed. Object does not exist in the objectstore.");
    } catch (cta::exception::Exception & ex) {
        // Log the error
        log::ScopedParamContainer params(lc);
        params.add("fileId", d.subrequestInfo.archiveFile.archiveFileID)
              .add("subrequestAddress", d.subrequestInfo.subrequest->getAddressIfSet())
              .add("exceptionMsg", ex.getMessageValue());
        lc.log(log::ERR, "In OStoreDB::RepackArchiveReportBatch::report(): async deletion failed.");
    }
  }
  for(auto & dfr: diskFileRemoverList){
    try {
      dfr.asyncRemover->wait();
      log::ScopedParamContainer params(lc);
      params.add("fileId", dfr.subrequestInfo.archiveFile.archiveFileID)
            .add("subrequestAddress", dfr.subrequestInfo.subrequest->getAddressIfSet())
            .add("fileBufferURL", dfr.subrequestInfo.repackInfo.fileBufferURL);
      lc.log(log::INFO, "In OStoreDB::RepackArchiveReportBatch::report(): async deleted file.");
    } catch (const cta::exception::Exception& ex){
      // Log the error
      log::ScopedParamContainer params(lc);
      params.add("fileId", dfr.subrequestInfo.archiveFile.archiveFileID)
            .add("subrequestAddress", dfr.subrequestInfo.subrequest->getAddressIfSet())
            .add("fileBufferURL", dfr.subrequestInfo.repackInfo.fileBufferURL)
            .add("exceptionMsg", ex.getMessageValue());
      lc.log(log::ERR, "In OStoreDB::RepackArchiveReportBatch::report(): async file not deleted.");
    }
  }
  if(repackRequestStatus == objectstore::serializers::RepackRequestStatus::RRS_Complete || repackRequestStatus == objectstore::serializers::RepackRequestStatus::RRS_Failed)
  {
    //Repack request is Complete or Failed, set it as complete
    //to tell the cta-admin repack rm command not to try to delete all the subrequests again
    cta::ScopedExclusiveLock sel(m_repackRequest);
    m_repackRequest.fetch();
    m_repackRequest.setIsComplete(true);
    m_repackRequest.commit();

    if(repackRequestStatus == objectstore::serializers::RepackRequestStatus::RRS_Complete){
      //Repack Request is complete, delete the directory in the buffer
      cta::disk::DirectoryFactory directoryFactory;
      std::string directoryPath = cta::utils::getEnclosingPath(bufferURL);
      std::unique_ptr<cta::disk::Directory> directory;
      try{
        directory.reset(directoryFactory.createDirectory(directoryPath));
        directory->rmdir();
        log::ScopedParamContainer params(lc);
        params.add("repackRequestAddress", m_repackRequest.getAddressIfSet());
        lc.log(log::INFO, "In OStoreDB::RepackArchiveReportBatch::report(): deleted the "+directoryPath+" directory");
      } catch (const cta::exception::Exception &ex){
        log::ScopedParamContainer params(lc);
        params.add("repackRequestAddress", m_repackRequest.getAddressIfSet())
              .add("exceptionMsg", ex.getMessageValue());
        lc.log(log::ERR, "In OStoreDB::RepackArchiveReportBatch::report(): failed to remove the "+directoryPath+" directory");
      }
    }
  }
  for (auto & jou: jobOwnerUpdatersList) {
    try {
      jou.jobOwnerUpdater->wait();
      log::ScopedParamContainer params(lc);
      params.add("fileId", jou.subrequestInfo.archiveFile.archiveFileID)
            .add("subrequestAddress", jou.subrequestInfo.subrequest->getAddressIfSet())
            .add("copyNb", jou.subrequestInfo.archivedCopyNb);
      lc.log(log::INFO, "In OStoreDB::RepackArchiveReportBatch::report(): async updated job.");
    } catch(const cta::exception::NoSuchObject &ex){
      // Log the error
      log::ScopedParamContainer params(lc);
      params.add("fileId", jou.subrequestInfo.archiveFile.archiveFileID)
            .add("subrequestAddress", jou.subrequestInfo.subrequest->getAddressIfSet())
            .add("copyNb", jou.subrequestInfo.archivedCopyNb)
            .add("exceptionMsg", ex.getMessageValue());
      lc.log(log::WARNING, "In OStoreDB::RepackArchiveReportBatch::report(): async job update failed. Object does not exist in the objectstore.");
    } catch (cta::exception::Exception & ex) {
      // Log the error
      log::ScopedParamContainer params(lc);
      params.add("fileId", jou.subrequestInfo.archiveFile.archiveFileID)
            .add("subrequestAddress", jou.subrequestInfo.subrequest->getAddressIfSet())
            .add("copyNb", jou.subrequestInfo.archivedCopyNb)
            .add("exceptionMsg", ex.getMessageValue());
      lc.log(log::ERR, "In OStoreDB::RepackArchiveReportBatch::report(): async job update failed.");
    }
  }
  timingList.insertAndReset("asyncUpdateOrDeleteCompletionTime", t);
  // 3) Just remove all jobs from ownership

  std::list<std::string> jobsToUnown;
  for (auto &sri: m_subrequestList) {
      jobsToUnown.emplace_back(sri.subrequest->getAddressIfSet());
  }
  m_oStoreDb.m_agentReference->removeBatchFromOwnership(jobsToUnown, m_oStoreDb.m_objectStore);
  log::ScopedParamContainer params(lc);
  timingList.insertAndReset("ownershipRemovalTime", t);
  timingList.addToLog(params);
  params.add("archiveReportType",( newStatus == cta::objectstore::serializers::ArchiveJobStatus::AJS_Complete) ? "ArchiveSuccesses" : "ArchiveFailures");
  lc.log(log::INFO, "In OStoreDB::RepackArchiveReportBatch::report(): reported a batch of jobs.");
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

  // Remove the space reservation for this job as we are done with it (if needed).
  if (diskSystemName) {
    cta::DiskSpaceReservationRequest dsrr;
    dsrr.addRequest(diskSystemName.value(), archiveFile.fileSize);
    this->m_oStoreDB.m_catalogue.DriveState()->releaseDiskSpace(m_retrieveMount->getMountInfo().drive,
    m_retrieveMount->getMountInfo().mountId, dsrr, lc);
  }

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

    case NextStep::EnqueueForReportForUser: {
      typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueToReportForUser> CaRqtr;

      // Algorithms suppose the objects are not locked
      auto  retryStatus = m_retrieveRequest.getRetryStatus(selectedCopyNb);
      auto  rfqc        = m_retrieveRequest.getRetrieveFileQueueCriteria();
      auto &af          = rfqc.archiveFile;

      std::set<std::string> candidateVids;
      for(auto &tf: af.tapeFiles) {
        if(m_retrieveRequest.getJobStatus(tf.copyNb) == serializers::RetrieveJobStatus::RJS_ToReportToUserForFailure)
          candidateVids.insert(tf.vid);
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
      for( ; tf_it != af.tapeFiles.end() && tf_it->vid != bestVid; ++tf_it) ;
      if(tf_it == af.tapeFiles.end()) {
        std::stringstream err;
        err << "In OStoreDB::RetrieveJob::failTransfer(): no tape file for requested vid."
            << " archiveId=" << af.archiveFileID << " vid=" << bestVid;
        throw RetrieveRequestHasNoCopies(err.str());
      }
      auto &tf = *tf_it;

      CaRqtr::InsertedElement::list insertedElements;
      insertedElements.push_back(CaRqtr::InsertedElement{
        &m_retrieveRequest, tf.copyNb, tf.fSeq, af.fileSize, rfqc.mountPolicy,
        m_activity, m_diskSystemName
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

    case NextStep::EnqueueForReportForRepack:{
      Sorter sorter(*m_oStoreDB.m_agentReference,m_oStoreDB.m_objectStore,m_oStoreDB.m_catalogue);
      std::shared_ptr<objectstore::RetrieveRequest> rr = std::make_shared<objectstore::RetrieveRequest>(m_retrieveRequest);
      sorter.insertRetrieveRequest(rr,*this->m_oStoreDB.m_agentReference,std::optional<uint32_t>(selectedCopyNb),lc);
      rel.release();
      sorter.flushOneRetrieve(lc);
      return;
    }

    case NextStep::EnqueueForTransferForUser: {
      typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransfer> CaRqtr;

      // Algorithms suppose the objects are not locked
      auto  retryStatus = m_retrieveRequest.getRetryStatus(selectedCopyNb);
      auto  rfqc        = m_retrieveRequest.getRetrieveFileQueueCriteria();
      auto &af          = rfqc.archiveFile;

      std::set<std::string> candidateVids;
      for(auto &tf : af.tapeFiles) {
        if(m_retrieveRequest.getJobStatus(tf.copyNb) == serializers::RetrieveJobStatus::RJS_ToTransfer)
          candidateVids.insert(tf.vid);
      }
      if(candidateVids.empty()) {
        throw cta::exception::Exception(
          "In OStoreDB::RetrieveJob::failTransfer(): no active job after addJobFailure() returned false."
        );
      }
      bool isRepack = m_retrieveRequest.getRepackInfo().isRepack;
      m_retrieveRequest.commit();
      rel.release();

      // Check that the requested retrieve job (for the provided VID) exists, and record the copy number
      std::string bestVid = Helpers::selectBestRetrieveQueue(candidateVids, m_oStoreDB.m_catalogue,
        m_oStoreDB.m_objectStore, isRepack);

      auto tf_it = af.tapeFiles.begin();
      for( ; tf_it != af.tapeFiles.end() && tf_it->vid != bestVid; ++tf_it) ;
      if(tf_it == af.tapeFiles.end()) {
        std::stringstream err;
        err << "In OStoreDB::RetrieveJob::failTransfer(): no tape file for requested vid."
            << " archiveId=" << af.archiveFileID << " vid=" << bestVid;
        throw RetrieveRequestHasNoCopies(err.str());
      }
      auto &tf = *tf_it;

      CaRqtr::InsertedElement::list insertedElements;
      insertedElements.push_back(CaRqtr::InsertedElement{
        &m_retrieveRequest, tf.copyNb, tf.fSeq, af.fileSize, rfqc.mountPolicy,
        m_activity, m_diskSystemName
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
void OStoreDB::RetrieveJob::failReport(const std::string& failureReason, log::LogContext& lc) {
  typedef objectstore::RetrieveRequest::EnqueueingNextStep EnqueueingNextStep;
  typedef EnqueueingNextStep::NextStep NextStep;

  if(!m_jobOwned) {
    throw JobNotOwned("In OStoreDB::RetrieveJob::failReport: cannot fail a job not owned");
  }

  // Lock the retrieve request. Fail the job.
  objectstore::ScopedExclusiveLock rrl(m_retrieveRequest);
  m_retrieveRequest.fetch();

  // Algorithms suppose the objects are not locked
  auto rfqc = m_retrieveRequest.getRetrieveFileQueueCriteria();
  auto& af = rfqc.archiveFile;

  // Handle report failures for all tape copies in turn
  for(auto& tf : af.tapeFiles) {
    // Add a job failure and decide what to do next
    EnqueueingNextStep enQueueingNextStep = m_retrieveRequest.addReportFailure(tf.copyNb, m_mountId, failureReason, lc);

    // Set the job status
    m_retrieveRequest.setJobStatus(tf.copyNb, enQueueingNextStep.nextStatus);

    // Apply the decision
    switch(enQueueingNextStep.nextStep) {
      // We have a reduced set of supported next steps as some are not compatible with this event
      case NextStep::EnqueueForReportForUser: {
        m_retrieveRequest.commit();
        auto retryStatus = m_retrieveRequest.getRetryStatus(tf.copyNb);
        // Algorithms suppose the objects are not locked
        rrl.release();
        typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueToReportForUser> CaRqtr;
        CaRqtr caRqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
        CaRqtr::InsertedElement::list insertedElements;
        insertedElements.push_back(CaRqtr::InsertedElement{
          &m_retrieveRequest, tf.copyNb, tf.fSeq, af.fileSize, rfqc.mountPolicy, m_activity, m_diskSystemName
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
        break;
      }
      default: {
        m_retrieveRequest.setFailed();
        m_retrieveRequest.commit();
        auto retryStatus = m_retrieveRequest.getRetryStatus(tf.copyNb);
        // Algorithms suppose the objects are not locked
        rrl.release();
        typedef objectstore::ContainerAlgorithms<RetrieveQueue,RetrieveQueueFailed> CaRqtr;
        CaRqtr caRqtr(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
        CaRqtr::InsertedElement::list insertedElements;
        insertedElements.push_back(CaRqtr::InsertedElement{
          &m_retrieveRequest, tf.copyNb, tf.fSeq, af.fileSize, rfqc.mountPolicy, m_activity, m_diskSystemName
        });
        caRqtr.referenceAndSwitchOwnership(tf.vid, insertedElements, lc);
        log::ScopedParamContainer params(lc);
        params.add("fileId", archiveFile.archiveFileID)
              .add("copyNb", tf.copyNb)
              .add("failureReason", failureReason)
              .add("requestObject", m_retrieveRequest.getAddressIfSet())
              .add("totalReportRetries", retryStatus.totalReportRetries)
              .add("maxReportRetries", retryStatus.maxReportRetries);
        if(enQueueingNextStep.nextStep == NextStep::StoreInFailedJobsContainer) {
          lc.log(log::INFO, "In RetrieveJob::failReport(): stored job in failed container for operator handling.");
        } else {
          lc.log(log::ERR, "In RetrieveJob::failReport(): stored job in failed container after unexpected next step.");
        }
        break;
      }
    }
  }
}

//------------------------------------------------------------------------------
// OStoreDB::RetrieveJob::abort()
// Aborting a Retrieve job consists of throwing it in the bin :
//  - If the retrieve job is from a User Retrieve Request, it will be queued in the RetrieveQueueFailed
//  - If the retrieve job is from a Repack Retrieve Request, it will be queued in the RetrieveQueueToReportToRepackForFailure
//------------------------------------------------------------------------------
void OStoreDB::RetrieveJob::abort(const std::string& abortReason, log::LogContext& lc){
  typedef objectstore::RetrieveRequest::EnqueueingNextStep EnqueueingNextStep;
  typedef EnqueueingNextStep::NextStep NextStep;

  if(!m_jobOwned)
    throw JobNotOwned("In OStoreDB::RetrieveJob::abort(): cannot abort a job not owned");

  // Lock the retrieve request. Fail the job.
  objectstore::ScopedExclusiveLock rrl(m_retrieveRequest);
  m_retrieveRequest.fetch();

  // Algorithms suppose the objects are not locked
  auto  rfqc = m_retrieveRequest.getRetrieveFileQueueCriteria();

  EnqueueingNextStep enQueueingNextStep = m_retrieveRequest.addReportAbort(selectedCopyNb, m_mountId, abortReason, lc);
  m_retrieveRequest.setJobStatus(selectedCopyNb, enQueueingNextStep.nextStatus);


  switch(enQueueingNextStep.nextStep){
    case NextStep::EnqueueForReportForRepack:
    case NextStep::StoreInFailedJobsContainer: {
      m_retrieveRequest.setFailed();
      m_retrieveRequest.commit();
      Sorter sorter(*m_oStoreDB.m_agentReference,m_oStoreDB.m_objectStore,m_oStoreDB.m_catalogue);
      std::shared_ptr<objectstore::RetrieveRequest> rr = std::make_shared<objectstore::RetrieveRequest>(m_retrieveRequest);
      sorter.insertRetrieveRequest(rr,*this->m_oStoreDB.m_agentReference,std::optional<uint32_t>(selectedCopyNb),lc);
      rrl.release();
      sorter.flushOneRetrieve(lc);
      return;
    }
    default:
      m_retrieveRequest.commit();
      throw cta::exception::Exception("In OStoreDB::RetrieveJob::abort(): Wrong EnqueueingNextStep for queueing the RetrieveRequest");
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
// OStoreDB::RetrieveJob::asyncSetSuccessful()
//------------------------------------------------------------------------------
void OStoreDB::RetrieveJob::asyncSetSuccessful() {
  if (isRepack) {
    // If the job is from a repack subrequest, we change its status (to report
    // for repack success). Queueing will be done in batch in
    m_jobSucceedForRepackReporter.reset(m_retrieveRequest.asyncReportSucceedForRepack(this->selectedCopyNb));
  } else {
    // set the user transfer request as successful (delete it).
    m_jobDelete.reset(m_retrieveRequest.asyncDeleteJob());
  }
}

void OStoreDB::RetrieveJob::fail() {
  if(!m_jobOwned)
    throw JobNotOwned("In OStoreDB::RetrieveJob::failReport: cannot fail a job not owned");

  // Lock the retrieve request. Change the status of the job.
  objectstore::ScopedExclusiveLock rrl(m_retrieveRequest);
  m_retrieveRequest.fetch();
  m_retrieveRequest.setJobStatus(this->selectedCopyNb,serializers::RetrieveJobStatus::RJS_Failed);
  m_retrieveRequest.commit();
}

} // namespace cta
