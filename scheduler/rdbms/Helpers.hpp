/**
 * SPDX-FileCopyrightText: 2023 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "scheduler/rdbms/postgres/Transaction.hpp"
#include "common/threading/Mutex.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/dataStructures/RepackQueueType.hpp"
#include "common/dataStructures/Tape.hpp"
#include "catalogue/Catalogue.hpp"
#include "scheduler/SchedulerDatabase.hpp"

#include <vector>
#include <string>
#include <set>
#include <future>

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
};

}  // namespace cta::schedulerdb
