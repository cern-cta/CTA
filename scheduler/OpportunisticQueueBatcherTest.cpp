/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/OpportunisticQueueBatcher.hpp"

#include "common/log/DummyLogger.hpp"
#include "common/log/LogContext.hpp"

#include <atomic>
#include <condition_variable>
#include <gtest/gtest.h>
#include <mutex>
#include <stdexcept>
#include <thread>

namespace unitTests {

using namespace std::chrono_literals;

namespace {

// Minimal ItemType: only what OpportunisticQueueBatcher itself requires (a public
// std::promise<ResultType> member), plus a payload to verify batching actually happened.
struct TestItem {
  int value = 0;
  std::promise<int> promise;
};

// Launches `count` threads that all park on a barrier before calling `fn(i)`, then releases them
// together so their calls land within the same opportunistic-batching window. Returns once every
// thread has been joined.
void runConcurrently(int count, const std::function<void(int)>& fn) {
  std::mutex mtx;
  std::condition_variable cv;
  int readyCount = 0;
  bool go = false;

  std::vector<std::thread> threads;
  threads.reserve(count);
  for (int i = 0; i < count; ++i) {
    threads.emplace_back([&, i] {
      {
        std::unique_lock<std::mutex> lock(mtx);
        ++readyCount;
        cv.wait(lock, [&] { return go; });
      }
      fn(i);
    });
  }

  {
    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [&] { return readyCount == count; });
    go = true;
  }
  cv.notify_all();

  for (auto& t : threads) {
    t.join();
  }
}

}  // namespace

class OpportunisticQueueBatcherTest : public ::testing::Test {
protected:
  cta::log::DummyLogger m_dummyLog {"dummy", "OpportunisticQueueBatcherTest"};
};

TEST_F(OpportunisticQueueBatcherTest, singleCallerGetsItsOwnResult) {
  using namespace cta;
  log::LogContext lc(m_dummyLog);

  int batchesSeen = 0;
  int lastBatchSize = 0;
  OpportunisticQueueBatcher<TestItem, int> batcher(100ms, 1000, [&](std::vector<TestItem>& batch, log::LogContext&) {
    ++batchesSeen;
    lastBatchSize = static_cast<int>(batch.size());
    for (auto& item : batch) {
      item.promise.set_value(item.value * 2);
    }
  });

  TestItem item;
  item.value = 21;
  const int result = batcher.enqueueAndWait(std::move(item), lc);

  ASSERT_EQ(42, result);
  ASSERT_EQ(1, batchesSeen);
  ASSERT_EQ(1, lastBatchSize);
}

TEST_F(OpportunisticQueueBatcherTest, concurrentCallersAreBatchedTogether) {
  using namespace cta;
  log::LogContext lc(m_dummyLog);

  constexpr int nbCallers = 20;
  std::atomic<int> resolveBatchCalls {0};
  std::atomic<int> maxBatchSizeSeen {0};

  OpportunisticQueueBatcher<TestItem, int> batcher(500ms, 1000, [&](std::vector<TestItem>& batch, log::LogContext&) {
    ++resolveBatchCalls;
    int expected = maxBatchSizeSeen.load();
    while (static_cast<int>(batch.size()) > expected
           && !maxBatchSizeSeen.compare_exchange_weak(expected, static_cast<int>(batch.size()))) {}
    for (auto& item : batch) {
      item.promise.set_value(item.value * 2);
    }
  });

  std::vector<int> results(nbCallers, -1);
  runConcurrently(nbCallers, [&](int i) {
    log::LogContext threadLc(m_dummyLog);
    TestItem item;
    item.value = i;
    results[i] = batcher.enqueueAndWait(std::move(item), threadLc);
  });

  // All callers got their own, individually correct result.
  for (int i = 0; i < nbCallers; ++i) {
    ASSERT_EQ(i * 2, results[i]);
  }
  // resolveBatch ran far fewer times than there were callers: they were genuinely batched together,
  // not each waiting for its own separate round.
  ASSERT_LT(resolveBatchCalls.load(), nbCallers);
  ASSERT_GT(maxBatchSizeSeen.load(), 1);
}

TEST_F(OpportunisticQueueBatcherTest, capEndsTheWaitEarly) {
  using namespace cta;
  log::LogContext lc(m_dummyLog);

  constexpr int cap = 10;
  OpportunisticQueueBatcher<TestItem, int> batcher(
    10s,  // deliberately long: if the cap did not cut the wait short, this test would time out
    cap,
    [&](std::vector<TestItem>& batch, log::LogContext&) {
      for (auto& item : batch) {
        item.promise.set_value(item.value);
      }
    });

  const auto start = std::chrono::steady_clock::now();
  runConcurrently(cap, [&](int i) {
    log::LogContext threadLc(m_dummyLog);
    TestItem item;
    item.value = i;
    batcher.enqueueAndWait(std::move(item), threadLc);
  });
  const auto elapsed = std::chrono::steady_clock::now() - start;

  ASSERT_LT(elapsed, 5s);
}

TEST_F(OpportunisticQueueBatcherTest, windowClosesTheBatchWhenCapIsNotReached) {
  using namespace cta;
  log::LogContext lc(m_dummyLog);

  const auto window = 150ms;
  OpportunisticQueueBatcher<TestItem, int> batcher(window,
                                                   1000,  // cap far larger than what will ever be submitted
                                                   [&](std::vector<TestItem>& batch, log::LogContext&) {
                                                     for (auto& item : batch) {
                                                       item.promise.set_value(item.value);
                                                     }
                                                   });

  const auto start = std::chrono::steady_clock::now();
  TestItem item;
  item.value = 7;
  const int result = batcher.enqueueAndWait(std::move(item), lc);
  const auto elapsed = std::chrono::steady_clock::now() - start;

  ASSERT_EQ(7, result);
  // The leader had to wait out (most of) the window before processing a batch of just itself.
  ASSERT_GE(elapsed, window / 2);
}

TEST_F(OpportunisticQueueBatcherTest, exceptionOnPromiseIsRethrownToCaller) {
  using namespace cta;
  log::LogContext lc(m_dummyLog);

  OpportunisticQueueBatcher<TestItem, int> batcher(50ms, 1000, [&](std::vector<TestItem>& batch, log::LogContext&) {
    for (auto& item : batch) {
      try {
        throw std::runtime_error("stage-2 failure");
      } catch (...) {
        item.promise.set_exception(std::current_exception());
      }
    }
  });

  TestItem item;
  item.value = 1;
  ASSERT_THROW(batcher.enqueueAndWait(std::move(item), lc), std::runtime_error);
}

TEST_F(OpportunisticQueueBatcherTest, followersAreReleasedBeforeAfterReleaseFinishes) {
  using namespace cta;
  log::LogContext lc(m_dummyLog);

  const auto afterReleaseDelay = 300ms;
  std::atomic<bool> afterReleaseStarted {false};

  OpportunisticQueueBatcher<TestItem, int> batcher(
    100ms,
    1000,
    [&](std::vector<TestItem>& batch, log::LogContext&) {
      for (auto& item : batch) {
        item.promise.set_value(item.value);
      }
    },
    [&](std::vector<TestItem>&, log::LogContext&) {
      afterReleaseStarted = true;
      std::this_thread::sleep_for(afterReleaseDelay);
    });

  std::atomic<bool> followerReturned {false};
  std::chrono::steady_clock::duration followerElapsed {};

  std::thread leaderThread([&] {
    log::LogContext leaderLc(m_dummyLog);
    TestItem item;
    item.value = 1;
    batcher.enqueueAndWait(std::move(item), leaderLc);
  });

  // Give the first thread time to become leader and enter its window wait, then submit a follower.
  std::this_thread::sleep_for(20ms);
  const auto start = std::chrono::steady_clock::now();
  {
    log::LogContext followerLc(m_dummyLog);
    TestItem item;
    item.value = 2;
    batcher.enqueueAndWait(std::move(item), followerLc);
  }
  followerElapsed = std::chrono::steady_clock::now() - start;
  followerReturned = true;

  leaderThread.join();

  // The follower's own enqueueAndWait() must not include afterRelease's delay: it only had to wait
  // out (part of) the 100ms window plus resolveBatch's own (near-instant) work, not the leader's
  // subsequent 300ms afterRelease call.
  ASSERT_TRUE(afterReleaseStarted.load());
  ASSERT_LT(followerElapsed, afterReleaseDelay);
}

TEST_F(OpportunisticQueueBatcherTest, sequentialRoundsDoNotDeadlock) {
  using namespace cta;
  log::LogContext lc(m_dummyLog);

  OpportunisticQueueBatcher<TestItem, int> batcher(20ms, 1000, [&](std::vector<TestItem>& batch, log::LogContext&) {
    for (auto& item : batch) {
      item.promise.set_value(item.value);
    }
  });

  for (int round = 0; round < 5; ++round) {
    log::LogContext roundLc(m_dummyLog);
    TestItem item;
    item.value = round;
    ASSERT_EQ(round, batcher.enqueueAndWait(std::move(item), roundLc));
  }
}

TEST_F(OpportunisticQueueBatcherTest, failWholeBatchSetsExceptionOnEveryItemAndCountsPerItem) {
  using namespace cta;
  log::LogContext lc(m_dummyLog);

  std::vector<TestItem> items(3);
  std::vector<std::future<int>> futures;
  for (auto& item : items) {
    futures.push_back(item.promise.get_future());
  }

  uint64_t failedCount = 0;
  try {
    throw std::runtime_error("bulk insert failed");
  } catch (...) {
    cta::failWholeBatch(items,
                        lc,
                        std::string("bulk insert failed"),
                        "test: failing this batch",
                        failedCount,
                        [](const TestItem&) -> uint64_t { return 3; });
  }

  ASSERT_EQ(9u, failedCount);  // 3 items * 3 per item
  for (auto& f : futures) {
    ASSERT_THROW(f.get(), std::runtime_error);
  }
}

}  // namespace unitTests