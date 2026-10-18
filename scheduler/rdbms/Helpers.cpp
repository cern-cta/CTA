/**
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/Helpers.hpp"

#include "scheduler/rdbms/postgres/RetrieveJobSummary.hpp"

#include <algorithm>

namespace cta::schedulerdb {

/** Time between cache updates */
time_t Helpers::g_tapeCacheMaxAge = 600;          // Default 10 minutes
time_t Helpers::g_retrieveQueueCacheMaxAge = 10;  // Default 10 seconds

//------------------------------------------------------------------------------
// Helpers::g_tapeStatuses
//------------------------------------------------------------------------------
std::map<std::string, Helpers::TapeStatusWithTime, std::less<>> Helpers::g_tapeStatuses;

//------------------------------------------------------------------------------
// Helpers::g_retrieveQueueStatisticsMutex
//------------------------------------------------------------------------------
cta::threading::Mutex Helpers::g_retrieveQueueStatisticsMutex;

//------------------------------------------------------------------------------
// Helpers::g_retrieveQueueStatistics
//------------------------------------------------------------------------------
std::map<std::string, Helpers::RetrieveQueueStatisticsWithTime, std::less<>> Helpers::g_retrieveQueueStatistics;

void Helpers::warmTapeStatusCache(const std::set<std::string, std::less<>>& vids,
                                  cta::catalogue::Catalogue& catalogue) {
  cta::threading::MutexLocker grqsmLock(g_retrieveQueueStatisticsMutex);
  warmTapeStatusCacheLocked(vids, catalogue);
}

void Helpers::warmTapeStatusCacheLocked(const std::set<std::string, std::less<>>& vids,
                                        cta::catalogue::Catalogue& catalogue) {
  // Ensure the tape status cache contains all the entries we need
  try {
    for (auto& v : vids) {
      // throw std::out_of_range() if cache item not found or if it is stale
      auto timeSinceLastUpdate =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - g_tapeStatuses.at(v).updateTime;
      if (timeSinceLastUpdate >= g_tapeCacheMaxAge) {
        throw std::out_of_range("");
      }
    }
  } catch (std::out_of_range&) {
    // Remove stale cache entries
    for (auto it = g_tapeStatuses.cbegin(); it != g_tapeStatuses.cend();) {
      auto timeSinceLastUpdate =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - it->second.updateTime;
      if (timeSinceLastUpdate >= g_tapeCacheMaxAge) {
        it = g_tapeStatuses.erase(it);
      } else {
        ++it;
      }
    }
    // Add in all the entries we need for this batch of candidates
    auto tapeStatuses = catalogue.Tape()->getTapesByVid(vids);
    for (const auto& [tv, ts] : tapeStatuses) {
      g_tapeStatuses[tv].tapeStatus = ts;
      g_tapeStatuses[tv].updateTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    }
  }
}

// Called once per opportunistic-batching round (RelationalDB.cpp) before the per-item
// selectBestVid4Retrieve() calls. Collects every candidate vid from the whole batch and fetches
// their queue statistics in a single bulk DB query, replacing what would otherwise be one
// individual query per stale vid per item. Only vids whose cache entry is missing or older than
// g_retrieveQueueCacheMaxAge are fetched; fresh entries are left untouched and cost nothing here.
// Writes complete entries (stats + timestamp + tape status) so selectBestVid4Retrieve() finds them
// fresh and skips its own per-vid fallback fetches entirely.
void Helpers::warmRetrieveQueueStatisticsCache(const std::set<std::string, std::less<>>& vids, rdbms::Conn& conn) {
  if (vids.empty()) {
    return;
  }

  std::set<std::string, std::less<>> missingOrStale;
  {
    threading::MutexLocker grqsmLock(g_retrieveQueueStatisticsMutex);
    const time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    for (const auto& v : vids) {
      auto it = g_retrieveQueueStatistics.find(v);
      if (it == g_retrieveQueueStatistics.end() || it->second.updating
          || (now - it->second.updateTime) >= g_retrieveQueueCacheMaxAge) {
        missingOrStale.insert(v);
      }
    }
  }
  if (missingOrStale.empty()) {
    return;
  }

  // Deliberately not tracked via the updating/updateFuture machinery selectBestVid4Retrieve() uses
  // for its own single-vid refreshes: this is a batch-only fast path, and coordinating it with that
  // machinery for what would only ever be a rare simultaneous request for the same vid from another
  // caller isn't worth the complexity -- worst case, both fetch it and whichever write lands last
  // wins, same as any other cache race.
  struct FetchedStats {
    uint64_t files;
    uint64_t bytes;
    uint64_t priority;
  };
  std::map<std::string, FetchedStats, std::less<>> fetched;
  auto summaryRows = cta::schedulerdb::postgres::RetrieveJobSummaryRow::selectVids(missingOrStale, conn);
  while (summaryRows.next()) {
    cta::schedulerdb::postgres::RetrieveJobSummaryRow rjs(summaryRows);
    fetched[rjs.vid] = {rjs.jobsCount, rjs.jobsTotalSize, rjs.priority};
  }

  // Write complete, refreshed entries in a single lock acquisition. updateRetrieveQueueStatisticsCache()
  // deliberately preserves existing timestamps and tape status on update — leaving stale entries
  // still stale after the bulk fetch, so selectBestVid4Retrieve() would redo a per-vid DB query for
  // each of them anyway. Write directly here to publish a fully fresh entry: stats + timestamp +
  // tape status. warmTapeStatusCache() ran just before us (in RelationalDB.cpp's batch resolve path),
  // so g_tapeStatuses already has current values for all candidate vids.
  threading::MutexLocker grqsmLock(g_retrieveQueueStatisticsMutex);
  const time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  for (const auto& v : missingOrStale) {
    auto& entry = g_retrieveQueueStatistics[v];
    auto it = fetched.find(v);
    if (it != fetched.end()) {
      entry.stats.filesQueued = it->second.files;
      entry.stats.bytesQueued = it->second.bytes;
      entry.stats.currentPriority = it->second.priority;
    } else {
      // A vid with no queued jobs simply has no row in the result: cache it as zeroed (same fallback
      // as getRetrieveQueueStatistics()'s single-vid path) rather than leaving it missing, which would
      // just make selectBestVid4Retrieve() redo this fetch itself right afterwards.
      entry.stats.filesQueued = 0;
      entry.stats.bytesQueued = 0;
      entry.stats.currentPriority = 0;
    }
    entry.stats.vid = v;
    entry.updating = false;
    entry.updateTime = now;
    auto tapeIt = g_tapeStatuses.find(v);
    if (tapeIt != g_tapeStatuses.end()) {
      entry.tapeStatus = tapeIt->second.tapeStatus;
    } else {
      entry.tapeStatus.state = common::dataStructures::Tape::ACTIVE;
      entry.tapeStatus.full = false;
    }
    logUpdateCacheIfNeeded(true, entry);
  }
}

std::string Helpers::selectBestVid4Retrieve(const std::set<std::string, std::less<>>& candidateVids,
                                            cta::catalogue::Catalogue& catalogue,
                                            rdbms::Conn& conn,
                                            bool isRepack) {
  // We will build the retrieve stats of the non-disabled, non-broken/exported candidate vids here
  std::list<SchedulerDatabase::RetrieveQueueStatistics> candidateVidsStats;

  // We will build the retrieve stats of the disabled vids here, as a fallback
  std::list<SchedulerDatabase::RetrieveQueueStatistics> candidateVidsStatsFallback;

  // Take the global lock
  cta::threading::MutexLocker grqsmLock(g_retrieveQueueStatisticsMutex);

  warmTapeStatusCacheLocked(candidateVids, catalogue);

  // Find the vids to be fetched (if any)
  for (auto& v : candidateVids) {
    try {
      // Out of range or outdated will be updated the same way.
      // If an update is in progress, we wait on it, and get the result after.
      // We have to release the global lock while doing so.
      if (g_retrieveQueueStatistics.at(v).updating) {
        logUpdateCacheIfNeeded(false, g_retrieveQueueStatistics.at(v), "g_retrieveQueueStatistics.at(v).updating");

        // Cache is updating, we wait on update.
        auto updateFuture = g_retrieveQueueStatistics.at(v).updateFuture;
        grqsmLock.unlock();
        updateFuture.wait();

        grqsmLock.lock();
        if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::ACTIVE && !isRepack)
            || (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING
                && isRepack)) {
          logUpdateCacheIfNeeded(
            false,
            g_retrieveQueueStatistics.at(v),
            "(g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::ACTIVE && !isRepack) "
            "|| (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING && "
            "isRepack)");
          candidateVidsStats.emplace_back(g_retrieveQueueStatistics.at(v).stats);
        } else if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::DISABLED
                    && !isRepack)
                   || (g_retrieveQueueStatistics.at(v).tapeStatus.state
                         == common::dataStructures::Tape::REPACKING_DISABLED
                       && isRepack)) {
          logUpdateCacheIfNeeded(
            false,
            g_retrieveQueueStatistics.at(v),
            "(g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::DISABLED && !isRepack) "
            "|| (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING_DISABLED "
            "&& isRepack)");
          candidateVidsStatsFallback.emplace_back(g_retrieveQueueStatistics.at(v).stats);
        }
      } else {
        // We have a cache hit, check it's not stale.
        time_t timeSinceLastUpdate = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())
                                     - g_retrieveQueueStatistics.at(v).updateTime;
        if (timeSinceLastUpdate >= g_retrieveQueueCacheMaxAge) {
          if (g_retrieveQueueCacheMaxAge) {
            logUpdateCacheIfNeeded(false,
                                   g_retrieveQueueStatistics.at(v),
                                   "timeSinceLastUpdate (" + std::to_string(timeSinceLastUpdate)
                                     + ")> g_retrieveQueueCacheMaxAge (" + std::to_string(g_retrieveQueueCacheMaxAge)
                                     + "), cache needs to be updated");
          }
          throw std::out_of_range("");
        }

        logUpdateCacheIfNeeded(false,
                               g_retrieveQueueStatistics.at(v),
                               "Cache is not updated, timeSinceLastUpdate (" + std::to_string(timeSinceLastUpdate)
                                 + ") <= g_retrieveQueueCacheMaxAge (" + std::to_string(g_retrieveQueueCacheMaxAge)
                                 + ")");

        // We're lucky: cache hit (and not stale)
        if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::ACTIVE && !isRepack)
            || (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING
                && isRepack)) {
          candidateVidsStats.emplace_back(g_retrieveQueueStatistics.at(v).stats);
        } else if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::DISABLED
                    && !isRepack)
                   || (g_retrieveQueueStatistics.at(v).tapeStatus.state
                         == common::dataStructures::Tape::REPACKING_DISABLED
                       && isRepack)) {
          candidateVidsStatsFallback.emplace_back(g_retrieveQueueStatistics.at(v).stats);
        }
      }
    } catch (std::out_of_range&) {
      // We need to update the entry in the cache (miss or stale, we handle the same way).
      // We just update one vid at a time as doing several in parallel would be quite
      // hairy lock-wise (but give a slight performance boost).

      g_retrieveQueueStatistics[v].updating = true;
      std::promise<void> updatePromise;
      g_retrieveQueueStatistics[v].updateFuture = updatePromise.get_future();

      // Get the cached tape status value before releasing the lock
      if (!g_tapeStatuses.contains(v)) {
        // Handle corner case where there are two candidate vids and the second candidate was evicted because it is stale
        auto tapeStatuses = catalogue.Tape()->getTapesByVid(v);
        if (tapeStatuses.size() != 1) {
          throw cta::exception::Exception(
            "In Helpers::selectBestRetrieveQueue(): candidate vid not found in the TAPE table.");
        }
        g_tapeStatuses[v].tapeStatus = tapeStatuses.begin()->second;
        g_tapeStatuses[v].updateTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      }

      common::dataStructures::Tape tapeStatus = g_tapeStatuses.at(v).tapeStatus;
      // Give other threads a chance to access the cache for other vids.
      grqsmLock.unlock();

      // Build a minimal service retrieve file queue criteria to query queues
      common::dataStructures::RetrieveFileQueueCriteria rfqc;
      common::dataStructures::TapeFile tf;
      tf.copyNb = 1;
      tf.vid = v;
      rfqc.archiveFile.tapeFiles.push_back(tf);
      auto queuesStats = Helpers::getRetrieveQueueStatistics(rfqc, {v}, conn);

      // We now have the data we need. Update the cache.
      grqsmLock.lock();
      g_retrieveQueueStatistics[v].updating = false;
      g_retrieveQueueStatistics[v].updateFuture = std::shared_future<void>();

      // Check size of stats
      if (queuesStats.size() != 1) {
        throw cta::exception::Exception("In Helpers::selectBestRetrieveQueue(): unexpected size for queueStats.");
      }
      if (queuesStats.front().vid != v) {
        throw cta::exception::Exception("In Helpers::selectBestRetrieveQueue(): unexpected vid in queueStats.");
      }

      g_retrieveQueueStatistics[v].stats = queuesStats.front();
      g_retrieveQueueStatistics[v].tapeStatus = tapeStatus;
      g_retrieveQueueStatistics[v].updateTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      logUpdateCacheIfNeeded(true, g_retrieveQueueStatistics[v]);

      // Signal to potential waiters
      updatePromise.set_value();
      // Update our own candidate list if needed.
      if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::ACTIVE && !isRepack)
          || (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING
              && isRepack)) {
        candidateVidsStats.emplace_back(g_retrieveQueueStatistics.at(v).stats);
      } else if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::DISABLED
                  && !isRepack)
                 || (g_retrieveQueueStatistics.at(v).tapeStatus.state
                       == common::dataStructures::Tape::REPACKING_DISABLED
                     && isRepack)) {
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
    candidateVidsStats.insert(candidateVidsStats.end(),
                              candidateVidsStatsFallback.begin(),
                              candidateVidsStatsFallback.end());
  }

  // Sort the tapes.
  candidateVidsStats.sort(SchedulerDatabase::RetrieveQueueStatistics::leftGreaterThanRight);
  // Get a list of equivalent best tapes
  std::set<std::string> shortSetVids;
  for (const auto& s : candidateVidsStats) {
    if (!(s < candidateVidsStats.front()) && !(s > candidateVidsStats.front())) {
      shortSetVids.insert(s.vid);
    }
  }

  // If there is only one best tape, we're done
  if (shortSetVids.size() == 1) {
    return *shortSetVids.begin();
  }

  // There are several equivalent entries, choose one among them based on the
  // number of days since epoch
  std::vector<std::string> shortListVids(shortSetVids.begin(), shortSetVids.end());
  std::sort(shortListVids.begin(), shortListVids.end());

  const time_t secondsSinceEpoch = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  const uint64_t daysSinceEpoch = secondsSinceEpoch / (60 * 60 * 24);

  return shortListVids[daysSinceEpoch % shortListVids.size()];
}

void Helpers::logUpdateCacheIfNeeded([[maybe_unused]] const bool entryCreation,
                                     [[maybe_unused]] const RetrieveQueueStatisticsWithTime& tapeStatistic,
                                     [[maybe_unused]] std::string_view message) {
#ifdef HELPERS_CACHE_UPDATE_LOGGING
  std::ofstream logFile(HELPERS_CACHE_UPDATE_LOGGING_FILE, std::ofstream::app);
  std::time_t end_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  // Chomp newline in the end
  std::string date = std::ctime(&end_time);
  date.erase(std::remove(date.begin(), date.end(), '\n'), date.end());
  logFile << date << " pid=" << ::getpid() << " tid=" << syscall(SYS_gettid) << " message=" << message
          << " entryCreation=" << entryCreation << " vid=" << tapeStatistic.tapeStatus.vid
          << " state=" << common::dataStructures::Tape::stateToString(tapeStatistic.tapeStatus.state)
          << " filesQueued=" << tapeStatistic.stats.filesQueued << std::endl;
#endif  // HELPERS_CACHE_UPDATE_LOGGING
}

// Single-vid fallback used by selectBestVid4Retrieve() when a cache entry is missing or stale.
// Issues one DB query per vid, returns the live stats, and records them in the cache as a
// side-effect via updateRetrieveQueueStatisticsCache(). The caller (selectBestVid4Retrieve())
// immediately overwrites that entry with a fully fresh one (stats + timestamp + tape status), so
// the incomplete write from updateRetrieveQueueStatisticsCache() is harmless here — it exists only
// to avoid a second lookup if another concurrent caller races in for the same vid.
std::list<SchedulerDatabase::RetrieveQueueStatistics>
Helpers::getRetrieveQueueStatistics(const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria,
                                    const std::set<std::string, std::less<>>& vidsToConsider,
                                    rdbms::Conn& conn) {
  std::list<SchedulerDatabase::RetrieveQueueStatistics> ret;
  for (auto& tf : criteria.archiveFile.tapeFiles) {
    if (!vidsToConsider.count(tf.vid)) {
      continue;
    }

    rdbms::Rset summary = cta::schedulerdb::postgres::RetrieveJobSummaryRow::selectVid(
      tf.vid,
      common::dataStructures::JobQueueType::JobsToTransferForUser,
      conn);

    if (!summary.next()) {
      ret.emplace_back(SchedulerDatabase::RetrieveQueueStatistics());
      ret.back().vid = tf.vid;
      ret.back().bytesQueued = 0;
      ret.back().currentPriority = 0;
      ret.back().filesQueued = 0;
      continue;
    }
    cta::schedulerdb::postgres::RetrieveJobSummaryRow rjs(summary);

    ret.emplace_back(SchedulerDatabase::RetrieveQueueStatistics());
    ret.back().vid = rjs.vid;
    ret.back().currentPriority = rjs.priority;
    ret.back().bytesQueued = rjs.jobsTotalSize;
    ret.back().filesQueued = rjs.jobsCount;

    updateRetrieveQueueStatisticsCache(rjs.vid, rjs.jobsCount, rjs.jobsTotalSize, rjs.priority);
  }

  return ret;
}

//------------------------------------------------------------------------------
// Helpers::setTapeCacheMaxAgeSecs()
//------------------------------------------------------------------------------
void Helpers::setTapeCacheMaxAgeSecs(int cacheMaxAgeSecs) {
  g_tapeCacheMaxAge = cacheMaxAgeSecs;
}

//------------------------------------------------------------------------------
// Helpers::setRetrieveQueueCacheMaxAgeSecs()
//------------------------------------------------------------------------------
void Helpers::setRetrieveQueueCacheMaxAgeSecs(int cacheMaxAgeSecs) {
  g_retrieveQueueCacheMaxAge = cacheMaxAgeSecs;
}

//------------------------------------------------------------------------------
// Helpers::updateRetrieveQueueStatisticsCache()
//------------------------------------------------------------------------------
// Stats-only cache writer: updates filesQueued/bytesQueued/currentPriority for an existing entry,
// or creates a new one. Deliberately does NOT update updateTime or tapeStatus: it is called from
// getRetrieveQueueStatistics() as a side-effect during a single-vid fetch, and the real caller
// (selectBestVid4Retrieve()) always overwrites the full entry immediately after with a fresh
// timestamp and tape status. Do NOT use this from warmRetrieveQueueStatisticsCache(): preserving
// the old timestamp leaves stale entries still stale, defeating the bulk warm entirely.
void Helpers::updateRetrieveQueueStatisticsCache(const std::string& vid,
                                                 uint64_t files,
                                                 uint64_t bytes,
                                                 uint64_t priority) {
  // We will not update the status of the tape if we already cached it (caller did not check),
  // We will also not update the update time, to force an update after a while.
  // If we update the entry while another thread is updating it, this is harmless (cache users will
  // anyway wait, and just not profit from our update.
  threading::MutexLocker ml(g_retrieveQueueStatisticsMutex);
  try {
    g_retrieveQueueStatistics.at(vid).stats.filesQueued = files;
    g_retrieveQueueStatistics.at(vid).stats.bytesQueued = bytes;
    g_retrieveQueueStatistics.at(vid).stats.currentPriority = priority;
    logUpdateCacheIfNeeded(false, g_retrieveQueueStatistics.at(vid));
  } catch (std::out_of_range&) {
    // The entry is missing. We just create it.
    g_retrieveQueueStatistics[vid].stats.filesQueued = files;
    g_retrieveQueueStatistics[vid].stats.bytesQueued = bytes;
    g_retrieveQueueStatistics[vid].stats.currentPriority = priority;
    g_retrieveQueueStatistics[vid].stats.vid = vid;
    g_retrieveQueueStatistics[vid].updating = false;
    g_retrieveQueueStatistics[vid].updateTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    try {
      // Use the cached tape status if we have it, otherwise fake it
      g_retrieveQueueStatistics[vid].tapeStatus = g_tapeStatuses.at(vid).tapeStatus;
    } catch (std::out_of_range&) {
      g_retrieveQueueStatistics[vid].tapeStatus.state = common::dataStructures::Tape::ACTIVE;
      g_retrieveQueueStatistics[vid].tapeStatus.full = false;
    }
    logUpdateCacheIfNeeded(true, g_retrieveQueueStatistics[vid]);
  }
}

void Helpers::flushStatisticsCacheForVid(const std::string& vid) {
  threading::MutexLocker ml(g_retrieveQueueStatisticsMutex);
  g_retrieveQueueStatistics.erase(vid);
  g_tapeStatuses.erase(vid);
}

}  // namespace cta::schedulerdb
