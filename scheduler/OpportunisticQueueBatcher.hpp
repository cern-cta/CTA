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
class OpportunisticQueueBatcher {
public:
  using ResolveBatchFn = std::function<void(std::vector<ItemType>&, log::LogContext&)>;
  using AfterReleaseFn = std::function<void(std::vector<ItemType>&, log::LogContext&)>;

  OpportunisticQueueBatcher(std::chrono::milliseconds window,
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
    bool isLeader = false;
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

    // ---- LEADER PATH ----
    // Still holding `lock`, uninterrupted, from the enqueue/election above: wait_for() re-checks the
    // predicate immediately under this same lock, before ever sleeping, so a cap already met right
    // now (e.g. maxBatchSize==1, satisfied by this leader's own item alone) returns instantly with
    // no other thread ever getting a chance to join this round. Deliberately never released and
    // re-acquired between becoming leader and this check: doing so used to leave a real gap where
    // m_mutex was momentarily free, during which another caller could enqueue and, since
    // m_leaderInProgress was already true, quietly join this round as a follower before the leader
    // ever got back to look — letting more items into "capped" batches than the cap implied.
    // wait_for() always re-acquires the lock before returning — by timeout, by the notify_all()
    // above, by a wakeup meant for someone else on this shared cv, or spuriously — so stealing the
    // batch right after is always safe here too.
    std::vector<ItemType> batch;
    m_cv.wait_for(lock, m_window, [this] { return m_pendingBatch.size() >= m_maxBatchSize; });
    batch.swap(m_pendingBatch);
    lock.unlock();

    try {
      m_resolveBatch(batch, lc);
    } catch (...) {
      // resolveBatch's contract is to resolve every item's promise itself; if it throws instead of
      // doing that (a bug in it, or e.g. a bad_alloc from some unrelated allocation), items it never
      // got to would be left unresolved forever — and, far worse, m_leaderInProgress below would
      // never be reset, permanently blocking every follower already asleep on m_cv and every future
      // caller of this batcher. Best-effort fallback: propagate this exception to whichever items
      // aren't already resolved; a promise resolveBatch did manage to resolve before throwing just
      // rejects the redundant set_exception, which is ignored.
      auto ex = std::current_exception();
      for (auto& item : batch) {
        try {
          item.promise.set_exception(ex);
        } catch (const std::future_error&) {}
      }
    }

    // Release followers waiting on this batch as soon as their results exist, before doing any
    // slower work in afterRelease below. Followers only need their own promise to be ready and to
    // be woken; they have no stake in this batch's own post-processing. Reuses `lock` (already
    // unlocked above) rather than a second lock object, since m_mutex is not recursive.
    lock.lock();
    m_leaderInProgress = false;
    lock.unlock();
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

/**
 * Shared stage-2 "give up on the whole batch" policy: fails every item in `items` with the
 * exception currently being handled (call only from inside a catch block), logging one WARNING for
 * the batch rather than one per item. `countPerItem` lets each workflow define what a "failure"
 * counts as for its own telemetry — e.g. archive counts jobs (copyToPoolMap.size(), since one
 * archive request can produce several), retrieve counts 1 per item (a retrieve request is always
 * exactly one job).
 */
// countPerItem is its own deduced template parameter (any callable taking `const ItemType&` and
// returning something convertible to uint64_t) rather than a std::function<uint64_t(const
// ItemType&)>: with the latter, ItemType appears inside the parameter type itself, so the compiler
// attempts to deduce it there too — and a raw lambda closure type never matches std::function<...>,
// so that deduction fails outright before the implicit lambda-to-std::function conversion ever gets
// a chance to run. Deducing the callable's own type sidesteps that entirely.
template<typename ItemType, typename CountPerItemFn>
void failWholeBatch(std::vector<ItemType>& items,
                    log::LogContext& lc,
                    const std::string& exceptionMessage,
                    const char* logMsg,
                    uint64_t& failedCount,
                    CountPerItemFn&& countPerItem) {
  log::ScopedParamContainer(lc)
    .add("batchSize", items.size())
    .add("exceptionMessage", exceptionMessage)
    .log(log::WARNING, logMsg);
  for (auto& item : items) {
    item.promise.set_exception(std::current_exception());
    failedCount += countPerItem(item);
  }
}

}  // namespace cta
