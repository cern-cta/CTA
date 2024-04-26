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

  static std::string selectBestVid4Retrieve(
                const std::set<std::string, std::less<>>  &candidateVids,
                cta::catalogue::Catalogue                 &catalogue,
                postgresscheddb::Transaction              &txn,
                bool                                       isRepack);

  static std::list<SchedulerDatabase::RetrieveQueueStatistics> getRetrieveQueueStatistics(
                const cta::common::dataStructures::RetrieveFileQueueCriteria &criteria,
                const std::set<std::string, std::less<>>                     &vidsToConsider,
                schedulerdb::Transaction                                 &txn);

  /*
   * Opportunistic updating of the queue stats cache as we access it. This implies the
   * tape is not disabled (full status not fetched).
   */
  static void updateRetrieveQueueStatisticsCache(
                const std::string & vid,
                uint64_t files,
                uint64_t bytes,
                uint64_t priority);

  static void flushRetrieveQueueStatisticsCacheForVid(const std::string & vid);

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
  static const time_t c_tapeCacheMaxAge = 600;
  static const time_t c_retrieveQueueCacheMaxAge = 10;

  static void logUpdateCacheIfNeeded(
                const bool                             entryCreation,
                const RetrieveQueueStatisticsWithTime &tapeStatistic,
                std::string_view                      message = "");
};

} // namespace cta::schedulerdb
