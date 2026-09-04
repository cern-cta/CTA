/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/log/LogContext.hpp"

#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <vector>

namespace cta {

/**
 * Generic opportunistic batching: concurrent callers each submit one item and block. The first
 * caller to arrive when no batch is in progress becomes leader and waits up to `window`, or until
 * the batch reaches `maxBatchSize`, whichever comes first, then hands the accumulated batch to
 * `resolveBatch`. resolveBatch MUST resolve every item's promise (set_value/set_exception) before
 * returning, however it internally succeeds or fails partially — that is the caller-supplied,
 * workflow-specific part (e.g. archive's stage-1/stage-2 split). Followers are released right after
 * resolveBatch returns; only then does the optional, slower `afterRelease` run (audit logging,
 * metrics), so no follower ever waits on work done for someone else's batch.
 *
 * ItemType must have a public member `std::promise<ResultType> promise`.
 */
template<typename ItemType, typename ResultType>
class OpportunisticBatcher {
public:
  using ResolveBatchFn = std::function<void(std::vector<ItemType>&, log::LogContext&)>;
  using AfterReleaseFn = std::function<void(std::vector<ItemType>&, log::LogContext&)>;

  OpportunisticBatcher(std::chrono::milliseconds window,
                       size_t maxBatchSize,
                       ResolveBatchFn resolveBatch,
                       AfterReleaseFn afterRelease = {})
      : m_window(window),
        m_maxBatchSize(maxBatchSize),
        m_resolveBatch(std::move(resolveBatch)),
        m_afterRelease(std::move(afterRelease)) {}

  // Submits item, blocks until its own result is ready (as leader or follower), and returns it (or
  // rethrows whatever exception resolveBatch set on its promise).
  ResultType enqueueAndWait(ItemType&& item, log::LogContext& lc) {
    std::future<ResultType> future;
    bool isLeader = false;
    {
      std::unique_lock<std::mutex> lock(m_mutex);
      m_pendingBatch.push_back(std::move(item));
      future = m_pendingBatch.back().promise.get_future();

      if (m_pendingBatch.size() >= m_maxBatchSize) {
        // Wakes every waiter on this cv, not just a leader currently waiting out its window: any
        // other follower woken here just finds its own two conditions below still false and goes
        // straight back to sleep. Harmless, and no different from the notify_all() at release time.
        m_cv.notify_all();
      }

      // Leadership election loop
      while (!isLeader) {
        // If my own request has already been resolved by a leader, return its result.
        if (future.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
          return future.get();
        }
        // If nobody is currently leading a round, I become leader.
        if (!m_leaderInProgress) {
          m_leaderInProgress = true;
          isLeader = true;
          break;
        }
        // Otherwise wait to be woken, either as the batch fills up or once the current round ends.
        m_cv.wait(lock);
      }
    }  // end of scope with the lock

    // ---- LEADER PATH ----
    std::vector<ItemType> batch;
    {
      // wait_for() re-checks the predicate under the lock before ever sleeping (so a cap already
      // reached by the time we get here returns instantly, no lost-wakeup race with the notify_all()
      // above), and always re-acquires the lock before returning — whether woken by that notify, a
      // wakeup meant for someone else on this shared cv, a spurious OS wakeup, or the timeout — so
      // stealing the batch right after is always safe, in the same locked scope, no separate re-lock.
      std::unique_lock<std::mutex> lock(m_mutex);
      m_cv.wait_for(lock, m_window, [this] { return m_pendingBatch.size() >= m_maxBatchSize; });
      batch.swap(m_pendingBatch);
    }

    m_resolveBatch(batch, lc);

    // Release followers waiting on this batch as soon as their results exist, before doing any
    // slower work in afterRelease below. Followers only need their own promise to be ready and to
    // be woken; they have no stake in this batch's own post-processing.
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_leaderInProgress = false;
    }
    m_cv.notify_all();

    if (m_afterRelease) {
      m_afterRelease(batch, lc);
    }

    // Return the leader's own result.
    return future.get();
  }

private:
  const std::chrono::milliseconds m_window;
  const size_t m_maxBatchSize;
  ResolveBatchFn m_resolveBatch;
  AfterReleaseFn m_afterRelease;

  std::mutex m_mutex;
  std::condition_variable m_cv;
  bool m_leaderInProgress = false;
  std::vector<ItemType> m_pendingBatch;
};

}  // namespace cta