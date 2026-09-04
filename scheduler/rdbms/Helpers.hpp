/**
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/dataStructures/RepackQueueType.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/process/threading/Mutex.hpp"
#include "common/process/threading/MutexLocker.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"

#include <future>
#include <set>
#include <string>
#include <vector>

namespace cta::schedulerdb {

class Helpers {
public:
  CTA_GENERATE_EXCEPTION_CLASS(NoTapeAvailableForRetrieve);

  static std::string selectBestVid4Retrieve(const std::set<std::string, std::less<>>& candidateVids,
                                            cta::catalogue::Catalogue& catalogue,
                                            cta::rdbms::Conn& conn,
                                            bool isRepack);

  static std::list<SchedulerDatabase::RetrieveQueueStatistics>
  getRetrieveQueueStatistics(const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria,
                             const std::set<std::string, std::less<>>& vidsToConsider,
                             cta::rdbms::Conn& conn);

  /*
   * Opportunistic updating of the queue stats cache as we access it. This implies the
   * tape is not disabled (full status not fetched).
   */
  static void
  updateRetrieveQueueStatisticsCache(const std::string& vid, uint64_t files, uint64_t bytes, uint64_t priority);

  static void flushStatisticsCacheForVid(const std::string& vid);

  /**
   * Ensures the tape status cache holds a fresh entry for every vid in `vids`, fetching whichever
   * are missing/stale from the catalogue in a single batched call, rather than one call per vid.
   * selectBestVid4Retrieve() already does this internally for its own (typically small, per-file)
   * candidate set. Callers about to make many selectBestVid4Retrieve() calls at once for possibly
   * many different files (e.g. opportunistic retrieve batching) should call this once up front with
   * the union of every file's candidate vids, to fetch cold vids in one catalogue round trip instead
   * of one per cold vid encountered lazily across those calls.
   */
  static void warmTapeStatusCache(const std::set<std::string, std::less<>>& vids, cta::catalogue::Catalogue& catalogue);

  static void setTapeCacheMaxAgeSecs(int cacheMaxAgeSecs);
  static void setRetrieveQueueCacheMaxAgeSecs(int cacheMaxAgeSecs);

  /** A struct holding together RetrieveQueueStatistics, tape status and an update time. */
  struct RetrieveQueueStatisticsWithTime {
    cta::SchedulerDatabase::RetrieveQueueStatistics stats;
    cta::common::dataStructures::Tape tapeStatus;
    bool updating;
    /** The shared future will allow all updating safely an entry of the cache while
     * releasing the global mutex to allow threads interested in other VIDs to carry on.*/
    std::shared_future<void> updateFuture;
    time_t updateTime;
  };

private:
  /** A struct holding together tape statistics and an update time */
  struct TapeStatusWithTime {
    common::dataStructures::Tape tapeStatus;
    time_t updateTime;
  };

  /** Cache for tape statistics */
  static std::map<std::string, TapeStatusWithTime, std::less<>> g_tapeStatuses;

  /** Lock for the retrieve queues stats */
  static cta::threading::Mutex g_retrieveQueueStatisticsMutex;

  /** The stats for the queues */
  static std::map<std::string, RetrieveQueueStatisticsWithTime, std::less<>> g_retrieveQueueStatistics;

  /** Time between cache updates */
  static time_t g_tapeCacheMaxAge;
  static time_t g_retrieveQueueCacheMaxAge;

  static void logUpdateCacheIfNeeded(const bool entryCreation,
                                     const RetrieveQueueStatisticsWithTime& tapeStatistic,
                                     std::string_view message = "");

  // Actual logic behind warmTapeStatusCache(), assuming g_retrieveQueueStatisticsMutex is already
  // held by the caller (used internally by selectBestVid4Retrieve(), which holds it for longer).
  static void warmTapeStatusCacheLocked(const std::set<std::string, std::less<>>& vids,
                                        cta::catalogue::Catalogue& catalogue);
};

}  // namespace cta::schedulerdb
