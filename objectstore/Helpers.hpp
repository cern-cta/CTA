/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/JobQueueType.hpp"
#include "common/dataStructures/RepackQueueType.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/process/threading/Mutex.hpp"
#include "common/process/threading/MutexLocker.hpp"
#include "scheduler/OStoreDB/OStoreDB.hpp"
#include "scheduler/SchedulerDatabase.hpp"

#include <fstream>
#include <future>
#include <list>
#include <map>
#include <set>
#include <string>
#include <syscall.h>

/**
 * A collection of helper functions for commonly used multi-object operations
 */

namespace cta {

namespace catalogue {
class Catalogue;
}

namespace objectstore {

class ScopedExclusiveLock;
class AgentReference;
class RepackQueue;

/**
 * A class with static functions allowing multi-object operations
 */
class Helpers {
public:
  /**
   * Find or create an archive or retrieve queue, and return it locked and fetched to the caller
   * (Queue and ScopedExclusiveLock objects are provided empty)
   * @param queue the queue object, empty
   * @param queueLock the lock, not initialized
   * @param agentReference the agent reference that will be needed in case of object creation
   * @param tapePool or vid the name of the needed tape pool
   */
  template<class Queue>
  static void getLockedAndFetchedJobQueue(Queue& queue,
                                          ScopedExclusiveLock& queueLock,
                                          AgentReference& agentReference,
                                          const std::optional<std::string>& tapePoolOrVid,
                                          common::dataStructures::JobQueueType queueType,
                                          log::LogContext& lc);

  /**
   * Find or create a repack queue, and return it locked and fetched to the caller
   * (Queue and ScopedExclusiveLock objects are provided empty)
   * @param queue the queue object, empty
   * @param queueLock the lock, not initialized
   * @param agentReference the agent reference that will be needed in case of object creation
   */
  static void getLockedAndFetchedRepackQueue(RepackQueue& queue,
                                             ScopedExclusiveLock& queueLock,
                                             AgentReference& agentReference,
                                             common::dataStructures::RepackQueueType queueType,
                                             log::LogContext& lc);

  CTA_GENERATE_EXCEPTION_CLASS(NoTapeAvailableForRetrieve);
  /**
   * Find the most appropriate queue (bid) to add the retrieve request to. The potential
   * VIDs (VIDs for non-failed copies) is provided by the caller. The status of the
   * the tapes (and available queue size) are all cached to avoid
   * frequent access to the object store. The caching create a small inefficiency
   * to the algorithm, but will help performance drastically for a very similar result
   */
  static std::string selectBestRetrieveQueue(const std::set<std::string, std::less<>>& candidateVids,
                                             cta::catalogue::Catalogue& catalogue,
                                             objectstore::Backend& objectstore,
                                             log::LogContext& lc,
                                             bool isRepack = false);

  /**
   * Gets the retrieve queue statistics for a set of Vids (extracted from the OStoreDB
   * so it can be used in the Helper context without passing the DB object.
   */
  static std::list<SchedulerDatabase::RetrieveQueueStatistics>
  getRetrieveQueueStatistics(const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria,
                             const std::set<std::string>& vidsToConsider,
                             objectstore::Backend& objectstore);

  /**
   * Opportunistic updating of the queue stats cache as we access it. This implies the
   * tape is not disabled (full status not fetched).
   */
  static void updateRetrieveQueueStatisticsCache(const std::string& vid,
                                                 uint64_t files,
                                                 uint64_t bytes,
                                                 uint64_t priority,
                                                 log::LogContext& lc);

  /**
   * Allows to flush the statistics caches
   * Required by the unit tests
   */
  static void flushStatisticsCache();
  static void flushStatisticsCacheForVid(const std::string& vid);

  /**
   * Helper function to set the time between cache updates.
   * Set to zero to disable entirely the caching capabilities.
   * Useful to reducing the value during tests.
   */
  static void setTapeCacheMaxAgeSecs(int cacheMaxAgeSecs);
  static void setRetrieveQueueCacheMaxAgeSecs(int cacheMaxAgeSecs);

private:
  /** A struct holding together tape statistics and an update time */
  struct TapeStatusWithTime {
    common::dataStructures::Tape tapeStatus;
    time_t updateTime;
  };

  /** Cache for tape statistics */
  static std::map<std::string, TapeStatusWithTime> g_tapeStatuses;
  /** Lock for the retrieve queues stats */
  static cta::threading::Mutex g_retrieveQueueStatisticsMutex;

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

  /** The stats for the queues */
  static std::map<std::string, RetrieveQueueStatisticsWithTime> g_retrieveQueueStatistics;
  /** Time between cache updates */
  static time_t g_tapeCacheMaxAge;
  static time_t g_retrieveQueueCacheMaxAge;
  static void logUpdateCacheIfNeeded(const bool entryCreation,
                                     const RetrieveQueueStatisticsWithTime& tapeStatistic,
                                     log::LogContext& lc,
                                     const std::string& message = "");

public:
  enum class CreateIfNeeded : bool { create = true, doNotCreate = false };

  /**
   * Helper to register a repack request in the repack index.
   * As this structure was developed late, we potentially have to create it on the fly.
   */
  static void registerRepackRequestToIndex(const std::string& vid,
                                           const std::string& requestAddress,
                                           AgentReference& agentReference,
                                           Backend& backend,
                                           log::LogContext& lc);

  /**
   * Helper to remove an entry form the repack index.
   */
  static void removeRepackRequestToIndex(const std::string& vid, Backend& backend, log::LogContext& lc);
};

}  // namespace objectstore
}  // namespace cta
