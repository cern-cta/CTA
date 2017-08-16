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

#include "Helpers.hpp"
#include "Backend.hpp"
#include "ArchiveQueue.hpp"
#include "AgentReference.hpp"
#include "RetrieveQueue.hpp"
#include "RootEntry.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/exception/NonRetryableError.hpp"
#include <random>

namespace cta { namespace objectstore {

//------------------------------------------------------------------------------
// Helpers::getLockedAndFetchedArchiveQueue()
//------------------------------------------------------------------------------
template <>
void Helpers::getLockedAndFetchedQueue<ArchiveQueue>(ArchiveQueue& archiveQueue,
  ScopedExclusiveLock& archiveQueueLock, AgentReference & agentReference,
  const std::string& tapePool, log::LogContext & lc) {
  // TODO: if necessary, we could use a singleton caching object here to accelerate
  // lookups.
  // Getting a locked AQ is the name of the game.
  // Try and find an existing one first, create if needed
  Backend & be = archiveQueue.m_objectStore;
  for (size_t i=0; i<5; i++) {
    double rootLockSharedTime = 0;
    double rootFetchTime = 0;
    double rootUnlockSharedTime = 0;
    double rootRelockExclusiveTime = 0;
    double rootUnlockExclusiveTime = 0;
    double rootRefetchTime = 0;
    double addOrGetQueueandCommitTime = 0;
    double queueLockTime = 0;
    double queueFetchTime = 0;
    utils::Timer t;
    {
      RootEntry re (be);
      ScopedSharedLock rel(re);
      rootLockSharedTime = t.secs(utils::Timer::resetCounter);
      re.fetch();
      rootFetchTime = t.secs(utils::Timer::resetCounter);
      try {
        archiveQueue.setAddress(re.getArchiveQueueAddress(tapePool));
      } catch (cta::exception::Exception & ex) {
        rel.release();
        rootUnlockSharedTime = t.secs(utils::Timer::resetCounter);
        ScopedExclusiveLock rexl(re);
        rootRelockExclusiveTime = t.secs(utils::Timer::resetCounter);
        re.fetch();
        rootRefetchTime = t.secs(utils::Timer::resetCounter);
        archiveQueue.setAddress(re.addOrGetArchiveQueueAndCommit(tapePool, agentReference));
        addOrGetQueueandCommitTime = t.secs(utils::Timer::resetCounter);
      }
    }
    if (!rootUnlockSharedTime)
      rootUnlockSharedTime = t.secs(utils::Timer::resetCounter);
    else
      rootUnlockExclusiveTime = t.secs(utils::Timer::resetCounter);
    try {
      archiveQueueLock.lock(archiveQueue);
      queueLockTime = t.secs(utils::Timer::resetCounter);
      archiveQueue.fetch();
      queueFetchTime = t.secs(utils::Timer::resetCounter);
      log::ScopedParamContainer params(lc);
      params.add("attemptNb", i+1)
            .add("queueObject", archiveQueue.getAddressIfSet())
            .add("rootLockSharedTime", rootLockSharedTime)
            .add("rootFetchTime", rootFetchTime)
            .add("rootUnlockSharedTime", rootUnlockSharedTime)
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
      if (archiveQueueLock.isLocked()) archiveQueueLock.release();
      log::ScopedParamContainer params(lc);
      params.add("attemptNb", i+1)
            .add("exceptionMessage", ex.getMessageValue())
            .add("queueObject", archiveQueue.getAddressIfSet())
            .add("rootLockSharedTime", rootLockSharedTime)
            .add("rootFetchTime", rootFetchTime)
            .add("rootUnlockSharedTime", rootUnlockSharedTime)
            .add("rootRefetchTime", rootRefetchTime)
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
      + tapePool);
}


//------------------------------------------------------------------------------
// Helpers::getLockedAndFetchedRetrieveQueue()
//------------------------------------------------------------------------------
template <>
void Helpers::getLockedAndFetchedQueue<RetrieveQueue>(RetrieveQueue& retrieveQueue,
  ScopedExclusiveLock& retrieveQueueLock, AgentReference& agentReference,
  const std::string& vid, log::LogContext & lc) {
  // TODO: if necessary, we could use a singleton caching object here to accelerate
  // lookups.
  // Getting a locked AQ is the name of the game.
  // Try and find an existing one first, create if needed
  Backend & be = retrieveQueue.m_objectStore;
  for (size_t i=0; i<5; i++) {
    {
      RootEntry re (be);
      ScopedSharedLock rel(re);
      re.fetch();
      try {
        retrieveQueue.setAddress(re.getRetrieveQueueAddress(vid));
      } catch (cta::exception::Exception & ex) {
        rel.release();
        ScopedExclusiveLock rexl(re);
        re.fetch();
        retrieveQueue.setAddress(re.addOrGetRetrieveQueueAndCommit(vid, agentReference));
      }
    }
    try {
      retrieveQueueLock.lock(retrieveQueue);
      retrieveQueue.fetch();
      return;
    } catch (cta::exception::Exception & ex) {
      // We have a (rare) opportunity for a race condition, where we identify the
      // queue and it gets deleted before we manage to lock it.
      // The locking of fetching will fail in this case.
      // We hence allow ourselves to retry a couple times.
      continue;
    }
  }
  throw cta::exception::Exception(std::string(
      "In OStoreDB::getLockedAndFetchedRetrieveQueue(): failed to find or create and lock archive queue after 5 retries for vid: ")
      + vid);
}

//------------------------------------------------------------------------------
// Helpers::selectBestRetrieveQueue()
//------------------------------------------------------------------------------
std::string Helpers::selectBestRetrieveQueue(const std::set<std::string>& candidateVids, cta::catalogue::Catalogue & catalogue,
    objectstore::Backend & objectstore) {
  // We will build the retrieve stats of the non-disable candidate vids here
  std::list<SchedulerDatabase::RetrieveQueueStatistics> candidateVidsStats;
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
        // Cache is updating, we wait on update.
        auto updateFuture = g_retrieveQueueStatistics.at(v).updateFuture;
        grqsmLock.unlock();
        updateFuture.wait();
        grqsmLock.lock();
        if (!g_retrieveQueueStatistics.at(v).tapeStatus.disabled) {
          candidateVidsStats.emplace_back(g_retrieveQueueStatistics.at(v).stats);
        }
      } else {
        // We have a cache hit, check it's not stale.
        if (g_retrieveQueueStatistics.at(v).updateTime + c_retrieveQueueCacheMaxAge > time(nullptr))
          throw std::out_of_range("");
        // We're lucky: cache hit (and not stale)
        if (!g_retrieveQueueStatistics.at(v).tapeStatus.disabled)
          candidateVidsStats.emplace_back(g_retrieveQueueStatistics.at(v).stats);
      }
    } catch (std::out_of_range) {
      // We need to update the entry in the cache (miss or stale, we handle the same way).
      // We just update one vid at a time as doing several in parallel would be quite
      // hairy lock-wise (but give a slight performance boost).
      g_retrieveQueueStatistics[v].updating = true;
      std::promise<void> updatePromise;
      g_retrieveQueueStatistics[v].updateFuture = updatePromise.get_future();
      // Give other threads a chance to access the cache for other vids.
      grqsmLock.unlock();
      // Get the informations (stages, so we don't access the global variable without the mutex.
      auto tapeStatus=catalogue.getTapesByVid({v});
      // Build a minimal service  retrieve file queue criteria to query queues.
      common::dataStructures::RetrieveFileQueueCriteria rfqc;
      rfqc.archiveFile.tapeFiles[1].vid=v;
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
      // Signal to potential waiters
      updatePromise.set_value();
      // Update our own candidate list if needed.
      if(!g_retrieveQueueStatistics.at(v).tapeStatus.disabled)
        candidateVidsStats.emplace_back(g_retrieveQueueStatistics.at(v).stats);
    }
  }
  // We now have all the candidates listed (if any).
  if (candidateVidsStats.empty())
    throw exception::NonRetryableError("In Helpers::selectBestRetrieveQueue(): no tape available to recall from.");
  // Sort the tapes.
  candidateVidsStats.sort(SchedulerDatabase::RetrieveQueueStatistics::leftGreaterThanRight);
  // Get a list of equivalent best tapes
  std::set<std::string> shortlistVids;
  for (auto & s: candidateVidsStats) {
    if (!(s<candidateVidsStats.front()) && !(s>candidateVidsStats.front()))
      shortlistVids.insert(s.vid);
  }
  // If there is only one best tape, we're done
  if (shortlistVids.size()==1) return *shortlistVids.begin();
  // There are several equivalent entries, choose randomly among them.
  // First element will always be selected.
  // We need to get a random number [0, candidateVids.size() -1]
  std::default_random_engine dre(std::chrono::system_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<size_t> distribution(0, candidateVids.size() -1);
  size_t index=distribution(dre);
  auto it=candidateVids.cbegin();
  std::advance(it, index);
  return *it;
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
  } catch (std::out_of_range &) {
    // The entry is missing. We just create it.
    g_retrieveQueueStatistics[vid].stats.bytesQueued=bytes;
    g_retrieveQueueStatistics[vid].stats.filesQueued=files;
    g_retrieveQueueStatistics[vid].stats.currentPriority=priority;
    g_retrieveQueueStatistics[vid].stats.vid=vid;
    g_retrieveQueueStatistics[vid].tapeStatus.disabled=false;
    g_retrieveQueueStatistics[vid].tapeStatus.full=false;
    g_retrieveQueueStatistics[vid].updating = false;
    g_retrieveQueueStatistics[vid].updateTime = time(nullptr);
  }
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
    if (!vidsToConsider.count(tf.second.vid))
      continue;
    std::string rqAddr;
    try {
      std::string rqAddr = re.getRetrieveQueueAddress(tf.second.vid);
    } catch (cta::exception::Exception &) {
      ret.push_back(SchedulerDatabase::RetrieveQueueStatistics());
      ret.back().vid=tf.second.vid;
      ret.back().bytesQueued=0;
      ret.back().currentPriority=0;
      ret.back().filesQueued=0;
      continue;
    }
    RetrieveQueue rq(rqAddr, objectstore);
    ScopedSharedLock rql(rq);
    rq.fetch();
    rql.release();
    if (rq.getVid() != tf.second.vid)
      throw cta::exception::Exception("In OStoreDB::getRetrieveQueueStatistics(): unexpected vid for retrieve queue");
    ret.push_back(SchedulerDatabase::RetrieveQueueStatistics());
    ret.back().vid=rq.getVid();
    ret.back().currentPriority=rq.getJobsSummary().priority;
    ret.back().bytesQueued=rq.getJobsSummary().bytes;
    ret.back().filesQueued=rq.getJobsSummary().files;
  }
  return ret;
}


}} // namespace cta::objectstore.