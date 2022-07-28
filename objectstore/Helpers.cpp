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
#include <random>

#include "AgentReference.hpp"
#include "ArchiveQueue.hpp"
#include "Backend.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/exception/NonRetryableError.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/log/TimingList.hpp"
#include "DriveRegister.hpp"
#include "Helpers.hpp"
#include "RepackIndex.hpp"
#include "RepackQueue.hpp"
#include "RetrieveQueue.hpp"
#include "RootEntry.hpp"

namespace cta { namespace objectstore {

//------------------------------------------------------------------------------
// Helpers::getLockedAndFetchedQueue <ArchiveQueue> ()
//------------------------------------------------------------------------------
template <>
void Helpers::getLockedAndFetchedJobQueue<ArchiveQueue>(ArchiveQueue& archiveQueue,
  ScopedExclusiveLock& archiveQueueLock, AgentReference & agentReference,
  const std::optional<std::string>& tapePool, common::dataStructures::JobQueueType queueType, log::LogContext & lc) {
  // TODO: if necessary, we could use a singleton caching object here to accelerate
  // lookups.
  // Getting a locked AQ is the name of the game.
  // Try and find an existing one first, create if needed
  Backend & be = archiveQueue.m_objectStore;
  for (size_t i=0; i<5; i++) {
    double rootFetchNoLockTime = 0;
    double rootRelockExclusiveTime = 0;
    double rootUnlockExclusiveTime = 0;
    double rootQueueDereferenceTime = 0;
    double rootRefetchTime = 0;
    double addOrGetQueueandCommitTime = 0;
    double queueLockTime = 0;
    double queueFetchTime = 0;
    utils::Timer t;
    {
      RootEntry re(be);
      re.fetchNoLock();
      rootFetchNoLockTime = t.secs(utils::Timer::resetCounter);
      try {
        archiveQueue.setAddress(re.getArchiveQueueAddress(tapePool.value(), queueType));
      } catch (cta::exception::Exception & ex) {
        ScopedExclusiveLock rexl(re);
        rootRelockExclusiveTime = t.secs(utils::Timer::resetCounter);
        re.fetch();
        rootRefetchTime = t.secs(utils::Timer::resetCounter);
        archiveQueue.setAddress(re.addOrGetArchiveQueueAndCommit(tapePool.value(), agentReference, queueType));
        addOrGetQueueandCommitTime = t.secs(utils::Timer::resetCounter);
      }
    }
    if (rootRelockExclusiveTime)
      rootUnlockExclusiveTime = t.secs(utils::Timer::resetCounter);
    try {
      archiveQueueLock.lock(archiveQueue);
      queueLockTime = t.secs(utils::Timer::resetCounter);
      archiveQueue.fetch();
      queueFetchTime = t.secs(utils::Timer::resetCounter);
      log::ScopedParamContainer params(lc);
      params.add("attemptNb", i+1)
            .add("queueObject", archiveQueue.getAddressIfSet())
            .add("rootFetchNoLockTime", rootFetchNoLockTime)
            .add("rootRelockExclusiveTime", rootRelockExclusiveTime)
            .add("rootRefetchTime", rootRefetchTime)
            .add("addOrGetQueueandCommitTime", addOrGetQueueandCommitTime)
            .add("rootUnlockExclusiveTime", rootUnlockExclusiveTime)
            .add("queueLockTime", queueLockTime)
            .add("queueFetchTime", queueFetchTime);
      lc.log(log::INFO, "In Helpers::getLockedAndFetchedQueue<ArchiveQueue>(): Successfully found and locked an archive queue.");
      return;
    } catch (cta::exception::Exception & ex) {
      // We have a (rare) opportunity for a race condition, where we identify the
      // queue and it gets deleted before we manage to lock it.
      // The locking of fetching will fail in this case.
      // We hence allow ourselves to retry a couple times.
      // We also need to make sure the lock on the queue is released (it is in
      // an object and hence not scoped).
      // We should also deal with the case where a queue was deleted but left
      // referenced in the root entry. We will try to clean up if necessary.
      // Failing to do this, we will spin and exhaust all of our retries.
      // We will do this if this is not the first attempt (i.e. failing again
      // in a retry).
      if (i && typeid(ex) == typeid(cta::exception::NoSuchObject)) {
        // The queue has been proven to not exist. Let's make sure we de-reference
        // it form the root entry.
        RootEntry re(be);
        ScopedExclusiveLock rexl(re);
        rootRelockExclusiveTime += t.secs(utils::Timer::resetCounter);
        re.fetch();
        rootRefetchTime += t.secs(utils::Timer::resetCounter);
        try {
          re.removeArchiveQueueAndCommit(tapePool.value(), queueType, lc);
          rootQueueDereferenceTime += t.secs(utils::Timer::resetCounter);
          log::ScopedParamContainer params(lc);
          params.add("tapePool", tapePool.value())
                .add("queueObject", archiveQueue.getAddressIfSet())
                .add("exceptionMsg", ex.getMessageValue());
          lc.log(log::INFO, "In Helpers::getLockedAndFetchedQueue<ArchiveQueue>(): removed reference to gone archive queue from root entry.");
        } catch (...) { /* Failing here is not fatal. We can get an exception if the queue was deleted in the meantime */ }
      }
      if (archiveQueueLock.isLocked()) archiveQueueLock.release();
      log::ScopedParamContainer params(lc);
      params.add("attemptNb", i+1)
            .add("exceptionMessage", ex.getMessageValue())
            .add("queueObject", archiveQueue.getAddressIfSet())
            .add("rootFetchNoLockTime", rootFetchNoLockTime)
            .add("rootRefetchTime", rootRefetchTime)
            .add("rootQueueDereferenceTime", rootQueueDereferenceTime)
            .add("addOrGetQueueandCommitTime", addOrGetQueueandCommitTime)
            .add("rootUnlockExclusiveTime", rootUnlockExclusiveTime)
            .add("queueLockTime", queueLockTime)
            .add("queueFetchTime", queueFetchTime);
      lc.log(log::INFO, "In Helpers::getLockedAndFetchedQueue<ArchiveQueue>(): failed to fetch an existing queue. Retrying.");
      archiveQueue.resetAddress();
      continue;
    } catch (...) {
      // Also release the lock if needed here.
      if (archiveQueueLock.isLocked()) archiveQueueLock.release();
      archiveQueue.resetAddress();
      throw;
    }
  }
  // Also release the lock if needed here.
  if (archiveQueueLock.isLocked()) archiveQueueLock.release();
  archiveQueue.resetAddress();
  throw cta::exception::Exception(std::string(
      "In OStoreDB::getLockedAndFetchedArchiveQueue(): failed to find or create and lock archive queue after 5 retries for tapepool: ")
      + tapePool.value());
}


//------------------------------------------------------------------------------
// Helpers::getLockedAndFetchedQueue <RetrieveQueue> ()
//------------------------------------------------------------------------------
template <>
void Helpers::getLockedAndFetchedJobQueue<RetrieveQueue>(RetrieveQueue& retrieveQueue,
  ScopedExclusiveLock& retrieveQueueLock, AgentReference& agentReference,
  const std::optional<std::string>& vid, common::dataStructures::JobQueueType queueType, log::LogContext & lc) {
  // TODO: if necessary, we could use a singleton caching object here to accelerate
  // lookups.
  // Getting a locked AQ is the name of the game.
  // Try and find an existing one first, create if needed
  Backend & be = retrieveQueue.m_objectStore;
  for (size_t i=0; i<5; i++) {
    double rootFetchNoLockTime = 0;
    double rootRelockExclusiveTime = 0;
    double rootUnlockExclusiveTime = 0;
    double rootQueueDereferenceTime = 0;
    double rootRefetchTime = 0;
    double addOrGetQueueandCommitTime = 0;
    double queueLockTime = 0;
    double queueFetchTime = 0;
    utils::Timer t;
    {
      RootEntry re (be);
      re.fetchNoLock();
      rootFetchNoLockTime = t.secs(utils::Timer::resetCounter);
      try {
        retrieveQueue.setAddress(re.getRetrieveQueueAddress(vid.value(), queueType));
      } catch (cta::exception::Exception & ex) {
        ScopedExclusiveLock rexl(re);
        rootRelockExclusiveTime = t.secs(utils::Timer::resetCounter);
        re.fetch();
        rootRefetchTime = t.secs(utils::Timer::resetCounter);
        retrieveQueue.setAddress(re.addOrGetRetrieveQueueAndCommit(vid.value(), agentReference, queueType));
        addOrGetQueueandCommitTime = t.secs(utils::Timer::resetCounter);
      }
    }
    if (rootRelockExclusiveTime)
      rootUnlockExclusiveTime = t.secs(utils::Timer::resetCounter);
    try {
      retrieveQueueLock.lock(retrieveQueue);
      queueLockTime = t.secs(utils::Timer::resetCounter);
      retrieveQueue.fetch();
      queueFetchTime = t.secs(utils::Timer::resetCounter);
      log::ScopedParamContainer params(lc);
      params.add("attemptNb", i+1)
            .add("queueName", vid.value())
            .add("queueType", toString(queueType))
            .add("queueObject", retrieveQueue.getAddressIfSet())
            .add("rootFetchNoLockTime", rootFetchNoLockTime)
            .add("rootRelockExclusiveTime", rootRelockExclusiveTime)
            .add("rootRefetchTime", rootRefetchTime)
            .add("addOrGetQueueandCommitTime", addOrGetQueueandCommitTime)
            .add("rootUnlockExclusiveTime", rootUnlockExclusiveTime)
            .add("queueLockTime", queueLockTime)
            .add("queueFetchTime", queueFetchTime);
      lc.log(log::INFO, "In Helpers::getLockedAndFetchedQueue<RetrieveQueue>(): Successfully found and locked a retrieve queue.");
      return;
    } catch (cta::exception::Exception & ex) {
      // We have a (rare) opportunity for a race condition, where we identify the
      // queue and it gets deleted before we manage to lock it.
      // The locking of fetching will fail in this case.
      // We hence allow ourselves to retry a couple times.
      // We also need to make sure the lock on the queue is released (it is in
      // an object and hence not scoped).
      // We should also deal with the case where a queue was deleted but left
      // referenced in the root entry. We will try to clean up if necessary.
      // Failing to do this, we will spin and exhaust all of our retries.
      // We will do this if this is not the first attempt (i.e. failing again
      // in a retry).
      if (i && typeid(ex) == typeid(cta::exception::NoSuchObject)) {
        // The queue has been proven to not exist. Let's make sure we de-reference
        // it form the root entry.
        RootEntry re(be);
        ScopedExclusiveLock rexl(re);
        rootRelockExclusiveTime += t.secs(utils::Timer::resetCounter);
        re.fetch();
        rootRefetchTime += t.secs(utils::Timer::resetCounter);
        try {
          re.removeRetrieveQueueAndCommit(vid.value(), queueType, lc);
          rootQueueDereferenceTime += t.secs(utils::Timer::resetCounter);
          log::ScopedParamContainer params(lc);
          params.add("tapeVid", vid.value())
                .add("queueObject", retrieveQueue.getAddressIfSet())
                .add("exceptionMsg", ex.getMessageValue());
          lc.log(log::INFO, "In Helpers::getLockedAndFetchedQueue<RetrieveQueue>(): removed reference to gone retrieve queue from root entry.");
        } catch (...) { /* Failing here is not fatal. We can get an exception if the queue was deleted in the meantime */ }
      }
      if (retrieveQueueLock.isLocked()) retrieveQueueLock.release();
      log::ScopedParamContainer params(lc);
      params.add("attemptNb", i+1)
            .add("exceptionMessage", ex.getMessageValue())
            .add("queueObject", retrieveQueue.getAddressIfSet())
            .add("rootFetchNoLockTime", rootFetchNoLockTime)
            .add("rootRefetchTime", rootRefetchTime)
            .add("rootQueueDereferenceTime", rootQueueDereferenceTime)
            .add("addOrGetQueueandCommitTime", addOrGetQueueandCommitTime)
            .add("rootUnlockExclusiveTime", rootUnlockExclusiveTime)
            .add("queueLockTime", queueLockTime)
            .add("queueFetchTime", queueFetchTime);
      lc.log(log::INFO, "In Helpers::getLockedAndFetchedQueue<RetrieveQueue>(): failed to fetch an existing queue. Retrying.");
      retrieveQueue.resetAddress();
      continue;
    } catch (...) {
      // Also release the lock if needed here.
      if (retrieveQueueLock.isLocked()) retrieveQueueLock.release();
      retrieveQueue.resetAddress();
      throw;
    }
  }
  // Also release the lock if needed here.
  if (retrieveQueueLock.isLocked()) retrieveQueueLock.release();
  retrieveQueue.resetAddress();
  throw cta::exception::Exception(std::string(
      "In OStoreDB::getLockedAndFetchedRetrieveQueue(): failed to find or create and lock archive queue after 5 retries for vid: ")
      + vid.value());
}

//------------------------------------------------------------------------------
// Helpers::getLockedAndFetchedRepackQueue()
//------------------------------------------------------------------------------
void Helpers::getLockedAndFetchedRepackQueue(RepackQueue& queue, ScopedExclusiveLock& queueLock, AgentReference& agentReference,
    common::dataStructures::RepackQueueType queueType, log::LogContext& lc) {
  // Try and find the repack queue.
  Backend & be = queue.m_objectStore;
  const uint8_t MAX_NUMBER_OF_ATTEMPTS = 5;
  for (uint8_t i = 0; i < MAX_NUMBER_OF_ATTEMPTS; i++) {
    utils::Timer t;
    log::TimingList timings;
    {
      RootEntry re(be);
      re.fetchNoLock();
      timings.insertAndReset("rootFetchNoLockTime", t);
      try {
        queue.setAddress(re.getRepackQueueAddress(queueType));
      } catch (cta::exception::Exception & ex) {
        ScopedExclusiveLock rexl(re);
        timings.insertAndReset("rootRelockExclusiveTime", t);
        re.fetch();
        timings.insertAndReset("rootRelockExclusiveTime", t);
        queue.setAddress(re.addOrGetRepackQueueAndCommit(agentReference, queueType));
        timings.insertAndReset("addOrGetQueueandCommitTime", t);
        rexl.release();
        timings.insertAndReset("rootUnlockExclusiveTime", t);
      }
    }
    try {
      queueLock.lock(queue);
      timings.insertAndReset("queueLockTime", t);
      queue.fetch();
      timings.insertAndReset("queueFetchTime", t);
      log::ScopedParamContainer params(lc);
      params.add("attemptNb", i+1)
            .add("queueObject", queue.getAddressIfSet());
      timings.addToLog(params);
      lc.log(log::INFO, "In Helpers::getLockedAndFetchedRepackQueue(): Successfully found and locked a repack queue.");
      return;
    } catch (cta::exception::Exception & ex) {
      // We have a (rare) opportunity for a race condition, where we identify the
      // queue and it gets deleted before we manage to lock it.
      // The locking or fetching will fail in this case.
      // We hence allow ourselves to retry a couple times.
      // We also need to make sure the lock on the queue is released (it is
      // an object and hence not scoped).
      // We should also deal with the case where a queue was deleted but left
      // referenced in the root entry. We will try to clean up if necessary.
      // Failing to do this, we will spin and exhaust all of our retries.
      if (i && typeid(ex) == typeid(cta::exception::NoSuchObject)) {
        // The queue has been proven to not exist. Let's make sure we de-reference
        // it form the root entry.
        RootEntry re(be);
        ScopedExclusiveLock rexl(re);
        timings.insOrIncAndReset("rootRelockExclusiveTime", t);
        re.fetch();
        timings.insOrIncAndReset("rootRefetchTime", t);
        try {
          re.removeRepackQueueAndCommit(queueType, lc);
          timings.insOrIncAndReset("rootQueueDereferenceTime", t);
          log::ScopedParamContainer params(lc);
          params.add("queueObject", queue.getAddressIfSet())
                .add("exceptionMsg", ex.getMessageValue());
          lc.log(log::INFO, "In Helpers::getLockedAndFetchedRepackQueue(): removed reference to gone repack queue from root entry.");
        } catch (...) { /* Failing here is not fatal. We can get an exception if the queue was deleted in the meantime */ }
      }
      if (queueLock.isLocked()) {
        queueLock.release();
        timings.insOrIncAndReset("queueLockReleaseTime", t);
      }
      log::ScopedParamContainer params(lc);
      params.add("attemptNb", i+1)
            .add("exceptionMessage", ex.getMessageValue())
            .add("queueObject", queue.getAddressIfSet());
      timings.addToLog(params);
      lc.log(log::INFO, "In Helpers::getLockedAndFetchedRepackQueue(): failed to fetch an existing queue. Retrying.");
      queue.resetAddress();
      continue;
    } catch (...) {
      // Also release the lock if needed here.
      if (queueLock.isLocked()) queueLock.release();
      queue.resetAddress();
      throw;
    }
  } // end of retry loop.
  // Also release the lock if needed here.
  if (queueLock.isLocked()) queueLock.release();
  queue.resetAddress();
  throw cta::exception::Exception(
      "In OStoreDB::getLockedAndFetchedRepackQueue(): failed to find or create and lock repack queue after 5 retries");
}

//------------------------------------------------------------------------------
// Helpers::selectBestRetrieveQueue()
//------------------------------------------------------------------------------
std::string Helpers::selectBestRetrieveQueue(const std::set<std::string>& candidateVids, cta::catalogue::Catalogue & catalogue,
    objectstore::Backend & objectstore, bool isRepack) {
  // We will build the retrieve stats of the non-disable, non-broken candidate vids here
  std::list<SchedulerDatabase::RetrieveQueueStatistics> candidateVidsStats;
  // We will build the retrieve stats of the disabled vids here, as a fallback
  std::list<SchedulerDatabase::RetrieveQueueStatistics> candidateVidsStatsFallback;
  // A promise we create so we can make users wait on it.
  // Take the global lock
  cta::threading::MutexLocker grqsmLock(g_retrieveQueueStatisticsMutex);
  // Create a promise just in case
  // Find the vids to be fetched (if any).
  for (auto & v: candidateVids) {
    try {
      // Out of range or outdated will be updated the same way.
      // If an update is in progress, we wait on it, and get the result after.
      // We have to release the global lock while doing so.
      if (g_retrieveQueueStatistics.at(v).updating) {
        logUpdateCacheIfNeeded(false,g_retrieveQueueStatistics.at(v),"g_retrieveQueueStatistics.at(v).updating");
        // Cache is updating, we wait on update.
        auto updateFuture = g_retrieveQueueStatistics.at(v).updateFuture;
        grqsmLock.unlock();
        updateFuture.wait();
        grqsmLock.lock();
        if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::ACTIVE && !isRepack) || (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING && isRepack)) {
          logUpdateCacheIfNeeded(false,g_retrieveQueueStatistics.at(v),"(g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::ACTIVE && !isRepack) || (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING && isRepack)");
          candidateVidsStats.emplace_back(g_retrieveQueueStatistics.at(v).stats);
        } else if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::DISABLED && !isRepack)) {
          logUpdateCacheIfNeeded(false,g_retrieveQueueStatistics.at(v),"(g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::DISABLED && !isRepack)");
          candidateVidsStatsFallback.emplace_back(g_retrieveQueueStatistics.at(v).stats);
        }
      } else {
        // We have a cache hit, check it's not stale.
        time_t timeSinceLastUpdate = time(nullptr) - g_retrieveQueueStatistics.at(v).updateTime;
        if (timeSinceLastUpdate > c_retrieveQueueCacheMaxAge){
          logUpdateCacheIfNeeded(false,g_retrieveQueueStatistics.at(v),"timeSinceLastUpdate ("+std::to_string(timeSinceLastUpdate)+")> c_retrieveQueueCacheMaxAge ("
                  +std::to_string(c_retrieveQueueCacheMaxAge)+"), cache needs to be updated");
          throw std::out_of_range("");
        }

        logUpdateCacheIfNeeded(false,g_retrieveQueueStatistics.at(v),"Cache is not updated, timeSinceLastUpdate ("+std::to_string(timeSinceLastUpdate)+
        ") <= c_retrieveQueueCacheMaxAge ("+std::to_string(c_retrieveQueueCacheMaxAge)+")");
        // We're lucky: cache hit (and not stale)
        if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::ACTIVE && !isRepack) || (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING && isRepack)) {
          candidateVidsStats.emplace_back(g_retrieveQueueStatistics.at(v).stats);
        } else if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::DISABLED && !isRepack)) {
          candidateVidsStatsFallback.emplace_back(g_retrieveQueueStatistics.at(v).stats);
        }
      }
    } catch (std::out_of_range &) {
      // We need to update the entry in the cache (miss or stale, we handle the same way).
      // We just update one vid at a time as doing several in parallel would be quite
      // hairy lock-wise (but give a slight performance boost).
      g_retrieveQueueStatistics[v].updating = true;
      std::promise<void> updatePromise;
      g_retrieveQueueStatistics[v].updateFuture = updatePromise.get_future();
      // Give other threads a chance to access the cache for other vids.
      grqsmLock.unlock();
      // Get the informations (stages, so we don't access the global variable without the mutex.
      auto tapeStatus=catalogue.getTapesByVid(v);
      // Build a minimal service  retrieve file queue criteria to query queues.
      common::dataStructures::RetrieveFileQueueCriteria rfqc;
      common::dataStructures::TapeFile tf;
      tf.copyNb = 1;
      tf.vid = v;
      rfqc.archiveFile.tapeFiles.push_back(tf);
      auto queuesStats=Helpers::getRetrieveQueueStatistics(rfqc, {v}, objectstore);
      // We now have the data we need. Update the cache.
      grqsmLock.lock();
      g_retrieveQueueStatistics[v].updating=false;
      g_retrieveQueueStatistics[v].updateFuture=std::shared_future<void>();
      // Check we got the expected vid (and size of stats).
      if (queuesStats.size()!=1)
        throw cta::exception::Exception("In Helpers::selectBestRetrieveQueue(): unexpected size for queueStats.");
      if (queuesStats.front().vid!=v)
        throw cta::exception::Exception("In Helpers::selectBestRetrieveQueue(): unexpected vid in queueStats.");
      if (tapeStatus.size()!=1)
        throw cta::exception::Exception("In Helpers::selectBestRetrieveQueue(): unexpected size for tapeStatus.");
      if (tapeStatus.begin()->first!=v)
        throw cta::exception::Exception("In Helpers::selectBestRetrieveQueue(): unexpected vid in tapeStatus.");
      g_retrieveQueueStatistics[v].stats = queuesStats.front();
      g_retrieveQueueStatistics[v].tapeStatus = tapeStatus.at(v);
      g_retrieveQueueStatistics[v].updateTime = time(nullptr);
      logUpdateCacheIfNeeded(true,g_retrieveQueueStatistics[v]);
      // Signal to potential waiters
      updatePromise.set_value();
      // Update our own candidate list if needed.
      if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::ACTIVE && !isRepack) || (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING && isRepack)) {
        candidateVidsStats.emplace_back(g_retrieveQueueStatistics.at(v).stats);
      } else if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::DISABLED && !isRepack)) {
        candidateVidsStatsFallback.emplace_back(g_retrieveQueueStatistics.at(v).stats);
      }
    }
  }
  // We now have all the candidates listed (if any).
  if (candidateVidsStats.empty()) {
    if (candidateVidsStatsFallback.empty()) {
      throw NoTapeAvailableForRetrieve("In Helpers::selectBestRetrieveQueue(): no tape available to recall from.");
    }
    // If `candidateVidsStats` is empty, insert the DISABLED tapes
    candidateVidsStats.insert(candidateVidsStats.end(), candidateVidsStatsFallback.begin(), candidateVidsStatsFallback.end());
  }
  // Sort the tapes.
  candidateVidsStats.sort(SchedulerDatabase::RetrieveQueueStatistics::leftGreaterThanRight);
  // Get a list of equivalent best tapes
  std::set<std::string> shortSetVids;
  for (auto & s: candidateVidsStats) {
    if (!(s<candidateVidsStats.front()) && !(s>candidateVidsStats.front()))
      shortSetVids.insert(s.vid);
  }
  // If there is only one best tape, we're done
  if (shortSetVids.size()==1) return *shortSetVids.begin();
  // There are several equivalent entries, choose one among them based on the number of days since epoch
  std::vector<std::string> shortListVids(shortSetVids.begin(), shortSetVids.end());
  std::sort(shortListVids.begin(), shortListVids.end());
  const time_t secondsSinceEpoch = time(nullptr);
  const uint64_t daysSinceEpoch = secondsSinceEpoch / (60*60*24);
  return shortListVids[daysSinceEpoch % shortListVids.size()];
}

//------------------------------------------------------------------------------
// Helpers::updateRetrieveQueueStatisticsCache()
//------------------------------------------------------------------------------
void Helpers::updateRetrieveQueueStatisticsCache(const std::string& vid, uint64_t files, uint64_t bytes, uint64_t priority) {
  // We will not update the status of the tape if we already cached it (caller did not check),
  // We will also not update the update time, to force an update after a while.
  // If we update the entry while another thread is updating it, this is harmless (cache users will
  // anyway wait, and just not profit from our update.
  threading::MutexLocker ml(g_retrieveQueueStatisticsMutex);
  try {
    g_retrieveQueueStatistics.at(vid).stats.filesQueued=files;
    g_retrieveQueueStatistics.at(vid).stats.bytesQueued=bytes;
    g_retrieveQueueStatistics.at(vid).stats.currentPriority = priority;
    logUpdateCacheIfNeeded(false,g_retrieveQueueStatistics.at(vid));
  } catch (std::out_of_range &) {
    // The entry is missing. We just create it.
    g_retrieveQueueStatistics[vid].stats.bytesQueued=bytes;
    g_retrieveQueueStatistics[vid].stats.filesQueued=files;
    g_retrieveQueueStatistics[vid].stats.currentPriority=priority;
    g_retrieveQueueStatistics[vid].stats.vid=vid;
    g_retrieveQueueStatistics[vid].tapeStatus.state = common::dataStructures::Tape::ACTIVE;
    g_retrieveQueueStatistics[vid].tapeStatus.full=false;
    g_retrieveQueueStatistics[vid].updating = false;
    g_retrieveQueueStatistics[vid].updateTime = time(nullptr);
    logUpdateCacheIfNeeded(true,g_retrieveQueueStatistics[vid]);
  }
}

void Helpers::flushRetrieveQueueStatisticsCache(){
  threading::MutexLocker ml(g_retrieveQueueStatisticsMutex);
  g_retrieveQueueStatistics.clear();
}

void Helpers::flushRetrieveQueueStatisticsCacheForVid(const std::string & vid){
  threading::MutexLocker ml(g_retrieveQueueStatisticsMutex);
  g_retrieveQueueStatistics.erase(vid);
}

//------------------------------------------------------------------------------
// Helpers::g_retrieveQueueStatistics
//------------------------------------------------------------------------------
std::map<std::string, Helpers::RetrieveQueueStatisticsWithTime> Helpers::g_retrieveQueueStatistics;

//------------------------------------------------------------------------------
// Helpers::g_retrieveQueueStatisticsMutex
//------------------------------------------------------------------------------
cta::threading::Mutex Helpers::g_retrieveQueueStatisticsMutex;

//------------------------------------------------------------------------------
// Helpers::getRetrieveQueueStatistics()
//------------------------------------------------------------------------------
std::list<SchedulerDatabase::RetrieveQueueStatistics> Helpers::getRetrieveQueueStatistics(
  const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria, const std::set<std::string>& vidsToConsider,
  objectstore::Backend & objectstore) {
  std::list<SchedulerDatabase::RetrieveQueueStatistics> ret;
  // Find the retrieve queues for each vid if they exist (absence is possible).
  RootEntry re(objectstore);
  ScopedSharedLock rel(re);
  re.fetch();
  rel.release();
  for (auto &tf:criteria.archiveFile.tapeFiles) {
    if (!vidsToConsider.count(tf.vid))
      continue;
    std::string rqAddr;
    try {
      std::string rqAddr = re.getRetrieveQueueAddress(tf.vid, common::dataStructures::JobQueueType::JobsToTransferForUser);
    } catch (cta::exception::Exception &) {
      ret.push_back(SchedulerDatabase::RetrieveQueueStatistics());
      ret.back().vid=tf.vid;
      ret.back().bytesQueued=0;
      ret.back().currentPriority=0;
      ret.back().filesQueued=0;
      continue;
    }
    RetrieveQueue rq(rqAddr, objectstore);
    ScopedSharedLock rql(rq);
    rq.fetch();
    rql.release();
    if (rq.getVid() != tf.vid)
      throw cta::exception::Exception("In OStoreDB::getRetrieveQueueStatistics(): unexpected vid for retrieve queue");
    ret.push_back(SchedulerDatabase::RetrieveQueueStatistics());
    ret.back().vid=rq.getVid();
    ret.back().currentPriority=rq.getJobsSummary().priority;
    ret.back().bytesQueued=rq.getJobsSummary().bytes;
    ret.back().filesQueued=rq.getJobsSummary().jobs;
  }
  return ret;
}

//------------------------------------------------------------------------------
// Helpers::registerRepackRequestToIndex()
//------------------------------------------------------------------------------
void Helpers::registerRepackRequestToIndex(const std::string& vid, const std::string& requestAddress,
    AgentReference & agentReference, Backend& backend, log::LogContext& lc) {
  // Try to reference the object in the index (will fail if there is already a request with this VID.
  RootEntry re(backend);
  re.fetchNoLock();
  std::string repackIndexAddress;
  // First, try to get the address of of the repack index lockfree.
  try {
    repackIndexAddress = re.getRepackIndexAddress();
  } catch (cta::exception::Exception &){
    ScopedExclusiveLock rel(re);
    re.fetch();
    repackIndexAddress = re.addOrGetRepackIndexAndCommit(agentReference);
  }
  RepackIndex ri(repackIndexAddress, backend);
  ScopedExclusiveLock ril(ri);
  ri.fetch();
  ri.addRepackRequestAddress(vid, requestAddress);
  ri.commit();
}

//------------------------------------------------------------------------------
// Helpers::removeRepackRequestFromIndex()
//------------------------------------------------------------------------------
void Helpers::removeRepackRequestToIndex(const std::string& vid, Backend& backend, log::LogContext& lc) {
  // Try to reference the object in the index (will fail if there is already a request with this VID.
  RootEntry re(backend);
  re.fetchNoLock();
  std::string repackIndexAddress;
  // First, try to get the address of of the repack index lockfree.
  try {
    repackIndexAddress = re.getRepackIndexAddress();
  } catch (cta::exception::Exception &){
    // No repack index, nothing to do.
    return;
  }
  RepackIndex ri(repackIndexAddress, backend);
  ScopedExclusiveLock ril(ri);
  ri.fetch();
  ri.removeRepackRequest(vid);
  ri.commit();
}

void Helpers::logUpdateCacheIfNeeded(const bool entryCreation, const RetrieveQueueStatisticsWithTime& tapeStatistic,
  const std::string& message) {
  #ifdef HELPERS_CACHE_UPDATE_LOGGING
    std::ofstream logFile(HELPERS_CACHE_UPDATE_LOGGING_FILE, std::ofstream::app);
    std::time_t end_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    // Chomp newline in the end
    std::string date = std::ctime(&end_time);
    date.erase(std::remove(date.begin(), date.end(), '\n'), date.end());
    logFile << date << " pid=" << ::getpid() << " tid=" << syscall(SYS_gettid)
            << " message=" << message << " entryCreation="<< entryCreation
            <<" vid=" << tapeStatistic.tapeStatus.vid
            << " state=" << common::dataStructures::Tape::stateToString(tapeStatistic.tapeStatus.state)
            << " filesQueued=" << tapeStatistic.stats.filesQueued <<  std::endl;
  #endif  // HELPERS_CACHE_UPDATE_LOGGING
}

}} // namespace cta::objectstore.
