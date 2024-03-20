/**
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright © 2023 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "scheduler/rdbms/Helpers.hpp"
#include "scheduler/rdbms/postgres/RetrieveJobSummary.hpp"

#include <algorithm>

namespace cta::schedulerdb {

/** Time between cache updates */
time_t Helpers::g_tapeCacheMaxAge = 600;         // Default 10 minutes
time_t Helpers::g_retrieveQueueCacheMaxAge = 10; // Default 10 seconds

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

std::string Helpers::selectBestVid4Retrieve
(
  const std::set<std::string, std::less<>>  &candidateVids,
  cta::catalogue::Catalogue                 &catalogue,
  schedulerdb::Transaction                  &txn,
  bool                                       isRepack
)
{
  // We will build the retrieve stats of the non-disabled, non-broken/exported candidate vids here
  std::list<SchedulerDatabase::RetrieveQueueStatistics> candidateVidsStats;

  // We will build the retrieve stats of the disabled vids here, as a fallback
  std::list<SchedulerDatabase::RetrieveQueueStatistics> candidateVidsStatsFallback;

  // Take the global lock
  cta::threading::MutexLocker grqsmLock(g_retrieveQueueStatisticsMutex);

  // Ensure the tape status cache contains all the entries we need
  try {
    for(auto& v : candidateVids) {
      // throw std::out_of_range() if cache item not found or if it is stale
      auto timeSinceLastUpdate = time(nullptr) - g_tapeStatuses.at(v).updateTime;
      if(timeSinceLastUpdate >= g_tapeCacheMaxAge) {
        throw std::out_of_range("");
      }
    }
  } catch (std::out_of_range&) {
    // Remove stale cache entries
    for(auto it = g_tapeStatuses.cbegin(); it != g_tapeStatuses.cend(); ) {
      auto timeSinceLastUpdate = time(nullptr) - it->second.updateTime;
      if(timeSinceLastUpdate >= g_tapeCacheMaxAge) {
        it = g_tapeStatuses.erase(it);
      } else {
        ++it;
      }
    }
    // Add in all the entries we need for this batch of candidates
    auto tapeStatuses = catalogue.Tape()->getTapesByVid(candidateVids);
    for(const auto& [tv, ts] : tapeStatuses) {
      g_tapeStatuses[tv].tapeStatus = ts;
      g_tapeStatuses[tv].updateTime = time(nullptr);
    }
  }

  // Find the vids to be fetched (if any)
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
        if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::ACTIVE && !isRepack) ||
            (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING && isRepack)) {
          logUpdateCacheIfNeeded(false,g_retrieveQueueStatistics.at(v),
            "(g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::ACTIVE && !isRepack) "
            "|| (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING && isRepack)");
          candidateVidsStats.emplace_back(g_retrieveQueueStatistics.at(v).stats);
        } else if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::DISABLED && !isRepack) ||
                  (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING_DISABLED && isRepack)) {
          logUpdateCacheIfNeeded(false,g_retrieveQueueStatistics.at(v),
            "(g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::DISABLED && !isRepack) "
            "|| (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING_DISABLED && isRepack)");
          candidateVidsStatsFallback.emplace_back(g_retrieveQueueStatistics.at(v).stats);
        }
      } else {
        // We have a cache hit, check it's not stale.
        time_t timeSinceLastUpdate = time(nullptr) - g_retrieveQueueStatistics.at(v).updateTime;
        if (timeSinceLastUpdate >= g_retrieveQueueCacheMaxAge) {
          if (g_retrieveQueueCacheMaxAge) {
            logUpdateCacheIfNeeded(false,g_retrieveQueueStatistics.at(v),
                                   "timeSinceLastUpdate (" + std::to_string(timeSinceLastUpdate) + ")> g_retrieveQueueCacheMaxAge ("
                                   + std::to_string(g_retrieveQueueCacheMaxAge) + "), cache needs to be updated");
          }
          throw std::out_of_range("");
        }

        logUpdateCacheIfNeeded(false,g_retrieveQueueStatistics.at(v),
                               "Cache is not updated, timeSinceLastUpdate (" + std::to_string(timeSinceLastUpdate) +
                               ") <= g_retrieveQueueCacheMaxAge (" + std::to_string(g_retrieveQueueCacheMaxAge) + ")");

        // We're lucky: cache hit (and not stale)
        if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::ACTIVE && !isRepack) ||
            (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING && isRepack)) {
          candidateVidsStats.emplace_back(g_retrieveQueueStatistics.at(v).stats);
        } else if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::DISABLED && !isRepack) ||
                   (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING_DISABLED && isRepack)) {
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

      // Get the cached tape status value before releasing the lock
      if(g_tapeStatuses.find(v) == g_tapeStatuses.end()) {
        // Handle corner case where there are two candidate vids and the second candidate was evicted because it is stale
        auto tapeStatuses = catalogue.Tape()->getTapesByVid(v);
        if(tapeStatuses.size() != 1) {
          throw cta::exception::Exception("In Helpers::selectBestRetrieveQueue(): candidate vid not found in the TAPE table.");
        }
        g_tapeStatuses[v].tapeStatus = tapeStatuses.begin()->second;
        g_tapeStatuses[v].updateTime = time(nullptr);
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
      auto queuesStats=Helpers::getRetrieveQueueStatistics(rfqc, {v}, txn);

      // We now have the data we need. Update the cache.
      grqsmLock.lock();
      g_retrieveQueueStatistics[v].updating=false;
      g_retrieveQueueStatistics[v].updateFuture=std::shared_future<void>();

      // Check size of stats
      if (queuesStats.size()!=1)
        throw cta::exception::Exception("In Helpers::selectBestRetrieveQueue(): unexpected size for queueStats.");
      if (queuesStats.front().vid!=v)
        throw cta::exception::Exception("In Helpers::selectBestRetrieveQueue(): unexpected vid in queueStats.");

      g_retrieveQueueStatistics[v].stats = queuesStats.front();
      g_retrieveQueueStatistics[v].tapeStatus = tapeStatus;
      g_retrieveQueueStatistics[v].updateTime = time(nullptr);
      logUpdateCacheIfNeeded(true,g_retrieveQueueStatistics[v]);

      // Signal to potential waiters
      updatePromise.set_value();
      // Update our own candidate list if needed.
      if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::ACTIVE && !isRepack) ||
          (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING && isRepack)) {
        candidateVidsStats.emplace_back(g_retrieveQueueStatistics.at(v).stats);
      } else if ((g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::DISABLED && !isRepack) ||
                 (g_retrieveQueueStatistics.at(v).tapeStatus.state == common::dataStructures::Tape::REPACKING_DISABLED && isRepack)) {
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
  for (const auto & s: candidateVidsStats) {
    if (!(s<candidateVidsStats.front()) && !(s>candidateVidsStats.front()))
      shortSetVids.insert(s.vid);
  }

  // If there is only one best tape, we're done
  if (shortSetVids.size()==1) return *shortSetVids.begin();

  // There are several equivalent entries, choose one among them based on the
  // number of days since epoch
  std::vector<std::string> shortListVids(shortSetVids.begin(), shortSetVids.end());
  std::sort(shortListVids.begin(), shortListVids.end());

  const time_t secondsSinceEpoch = time(nullptr);
  const uint64_t daysSinceEpoch  = secondsSinceEpoch / (60*60*24);

  return shortListVids[daysSinceEpoch % shortListVids.size()];
}

void Helpers::logUpdateCacheIfNeeded
(
  [[maybe_unused]] const bool                            entryCreation,
  [[maybe_unused]] const RetrieveQueueStatisticsWithTime &tapeStatistic,
  [[maybe_unused]] std::string_view                      message)
{
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

std::list<SchedulerDatabase::RetrieveQueueStatistics> Helpers::getRetrieveQueueStatistics
(
  const cta::common::dataStructures::RetrieveFileQueueCriteria &criteria,
  const std::set<std::string, std::less<>>                     &vidsToConsider,
  schedulerdb::Transaction                                     &txn)
{

  std::list<SchedulerDatabase::RetrieveQueueStatistics> ret;
  for (auto &tf:criteria.archiveFile.tapeFiles) {
    if (!vidsToConsider.count(tf.vid))
      continue;

    rdbms::Rset summary = 
      cta::schedulerdb::postgres::RetrieveJobSummaryRow::selectVid(
          tf.vid,
          common::dataStructures::JobQueueType::JobsToTransferForUser,
          txn
      );

    if (!summary.next()) {
      ret.emplace_back(SchedulerDatabase::RetrieveQueueStatistics());
      ret.back().vid=tf.vid;
      ret.back().bytesQueued=0;
      ret.back().currentPriority=0;
      ret.back().filesQueued=0;
      continue;
    }
    cta::schedulerdb::postgres::RetrieveJobSummaryRow rjs(summary);

    ret.emplace_back(SchedulerDatabase::RetrieveQueueStatistics());
    ret.back().vid=rjs.vid;
    ret.back().currentPriority=rjs.priority;
    ret.back().bytesQueued=rjs.jobsTotalSize;
    ret.back().filesQueued=rjs.jobsCount;

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
    g_retrieveQueueStatistics[vid].stats.filesQueued=files;
    g_retrieveQueueStatistics[vid].stats.bytesQueued=bytes;
    g_retrieveQueueStatistics[vid].stats.currentPriority=priority;
    g_retrieveQueueStatistics[vid].stats.vid=vid;
    g_retrieveQueueStatistics[vid].updating = false;
    g_retrieveQueueStatistics[vid].updateTime = time(nullptr);
    try {
      // Use the cached tape status if we have it, otherwise fake it
      g_retrieveQueueStatistics[vid].tapeStatus = g_tapeStatuses.at(vid).tapeStatus;
    } catch(std::out_of_range&) {
      g_retrieveQueueStatistics[vid].tapeStatus.state = common::dataStructures::Tape::ACTIVE;
      g_retrieveQueueStatistics[vid].tapeStatus.full = false;
    }
    logUpdateCacheIfNeeded(true,g_retrieveQueueStatistics[vid]);
  }
}

void Helpers::flushStatisticsCacheForVid(const std::string & vid){
  threading::MutexLocker ml(g_retrieveQueueStatisticsMutex);
  g_retrieveQueueStatistics.erase(vid);
  g_tapeStatuses.erase(vid);
}

} // namespace cta::schedulerdb
