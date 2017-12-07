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
#include "DriveRegister.hpp"
#include "DriveState.hpp"
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
    double rootFetchNoLockTime = 0;
    double rootRelockExclusiveTime = 0;
    double rootUnlockExclusiveTime = 0;
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
        archiveQueue.setAddress(re.getArchiveQueueAddress(tapePool));
      } catch (cta::exception::Exception & ex) {
        ScopedExclusiveLock rexl(re);
        rootRelockExclusiveTime = t.secs(utils::Timer::resetCounter);
        re.fetch();
        rootRefetchTime = t.secs(utils::Timer::resetCounter);
        archiveQueue.setAddress(re.addOrGetArchiveQueueAndCommit(tapePool, agentReference, lc));
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
      if (archiveQueueLock.isLocked()) archiveQueueLock.release();
      log::ScopedParamContainer params(lc);
      params.add("attemptNb", i+1)
            .add("exceptionMessage", ex.getMessageValue())
            .add("queueObject", archiveQueue.getAddressIfSet())
            .add("rootFetchNoLockTime", rootFetchNoLockTime)
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
    double rootFetchNoLockTime = 0;
    double rootRelockExclusiveTime = 0;
    double rootUnlockExclusiveTime = 0;
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
        retrieveQueue.setAddress(re.getRetrieveQueueAddress(vid));
      } catch (cta::exception::Exception & ex) {
        ScopedExclusiveLock rexl(re);
        rootRelockExclusiveTime = t.secs(utils::Timer::resetCounter);
        re.fetch();
        rootRefetchTime = t.secs(utils::Timer::resetCounter);
        retrieveQueue.setAddress(re.addOrGetRetrieveQueueAndCommit(vid, agentReference));
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
      if (retrieveQueueLock.isLocked()) retrieveQueueLock.release();
      log::ScopedParamContainer params(lc);
      params.add("attemptNb", i+1)
            .add("exceptionMessage", ex.getMessageValue())
            .add("queueObject", retrieveQueue.getAddressIfSet())
            .add("rootFetchNoLockTime", rootFetchNoLockTime)
            .add("rootRefetchTime", rootRefetchTime)
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
    throw NoTapeAvailableForRetrieve("In Helpers::selectBestRetrieveQueue(): no tape available to recall from.");
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
  re.fetchNoLock();
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
    rq.fetchNoLock();
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

//------------------------------------------------------------------------------
// Helpers::getLockedAndFetchedDriveState()
//------------------------------------------------------------------------------
void Helpers::getLockedAndFetchedDriveState(DriveState& driveState, ScopedExclusiveLock& driveStateLock, 
  AgentReference& agentReference, const std::string& driveName, log::LogContext& lc, CreateIfNeeded doCreate) {
  Backend & be = driveState.m_objectStore;
  // Try and get the location of the derive state lockfree (this should be most of the cases).
  try {
    RootEntry re(be);
    re.fetchNoLock();
    DriveRegister dr(re.getDriveRegisterAddress(), be);
    dr.fetchNoLock();
    driveState.setAddress(dr.getDriveAddress(driveName));
    driveStateLock.lock(driveState);
    driveState.fetch();
    if (driveState.getOwner() != dr.getAddressIfSet()) {
      std::string previouslySeenOwner=driveState.getOwner();
      // We have a special case: the drive state is not owned by the
      // drive register.
      // As we are lock free, we will re-lock in proper order.
      if (driveStateLock.isLocked()) driveStateLock.release();
      ScopedExclusiveLock drl(dr);
      dr.fetch();
      // Re-get the state (could have changed).
      driveState.resetAddress();
      driveState.setAddress(dr.getDriveAddress(driveName));
      driveStateLock.lock(driveState);
      driveState.fetch();
      // We have an exclusive lock on everything. We can now
      // safely switch the owner of the drive status to the drive register
      // (there is no other steady state ownership).
      // return all as we are done.
      log::ScopedParamContainer params (lc);
      params.add("driveRegisterObject", dr.getAddressIfSet())
            .add("driveStateObject", driveState.getAddressIfSet())
            .add("driveStateCurrentOwner", driveState.getOwner())
            .add("driveStatePreviouslySeenOwner", previouslySeenOwner);
      lc.log(log::WARNING, "In Helpers::getLockedAndFetchedDriveState(): unexpected owner for driveState (should be register, will fix it).");
      if (driveState.getOwner() != dr.getAddressIfSet()) {
        driveState.setOwner(dr.getAddressIfSet());
        driveState.commit();
      }
      // The drive register lock will be released automatically
    }
    // We're done with good day scenarios.
    return;
  } catch (...) {
    // If anything goes wrong, we will suppose we have to create the drive state and do every step,
    // one at time. Of course, this is far more costly (lock-wise).
    // ... except if we were not supposed to create it.
    if (doCreate == CreateIfNeeded::doNotCreate) {
      throw NoSuchDrive("In Helpers::getLockedAndFetchedDriveState(): no such drive. Will not create it as instructed.");
    }
    RootEntry re(be);
    re.fetchNoLock();
    DriveRegister dr(re.getDriveRegisterAddress(), be);
    ScopedExclusiveLock drl(dr);
    dr.fetch();
  checkDriveKnown:
    try {
      std::string dsAddress=dr.getDriveAddress(driveName);
      // The drive is known. Check it does exist.
      // We work in this order here because we are in one-off mode, so
      // efficiency is not problematic.
      if (be.exists(dsAddress)) {
        driveState.setAddress(dsAddress);
        driveStateLock.lock(driveState);
        driveState.fetch();
        if (driveState.getOwner() != dr.getAddressIfSet()) {
          driveState.setOwner(dr.getAddressIfSet());
          driveState.commit();
        }
      } else {
        dr.removeDrive(driveName);
        goto checkDriveKnown;
      }
    } catch (DriveRegister::NoSuchDrive &) {
      // OK, we do need to create the drive status.
      driveState.setAddress(agentReference.nextId(std::string ("DriveStatus-")+driveName));
      driveState.initialize(driveName);
      agentReference.addToOwnership(driveState.getAddressIfSet(), be);
      driveState.setOwner(agentReference.getAgentAddress());
      driveState.insert();
      dr.setDriveAddress(driveName, driveState.getAddressIfSet());
      dr.commit();
      driveStateLock.lock(driveState);
      driveState.fetch();
      driveState.setOwner(dr.getAddressIfSet());
      driveState.commit();
      agentReference.removeFromOwnership(driveState.getAddressIfSet(), be);
      return;
    }
  }
}


//------------------------------------------------------------------------------
// Helpers::getAllDriveStates()
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::DriveState> Helpers::getAllDriveStates(Backend& backend, log::LogContext &lc) {
  std::list<cta::common::dataStructures::DriveState> ret;
  // Get the register. Parallel get the states. Report... BUT if there are discrepancies, we 
  // will need to do some cleanup.
  RootEntry re(backend);
  re.fetchNoLock();
  DriveRegister dr(re.getDriveRegisterAddress(), backend);
  dr.fetchNoLock();
  std::list<DriveState> driveStates;
  std::list<std::unique_ptr<DriveState::AsyncLockfreeFetcher>> driveStateFetchers;
  for (auto & d: dr.getDriveAddresses()) {
    driveStates.emplace_back(DriveState(d.driveStateAddress, backend));
    driveStateFetchers.emplace_back(driveStates.back().asyncLockfreeFetch());
  }
  for (auto & df: driveStateFetchers) {
    df->wait();
  }
  for (auto &d: driveStates) {
    ret.emplace_back(d.getState());
  }
  return ret;
}


}} // namespace cta::objectstore.