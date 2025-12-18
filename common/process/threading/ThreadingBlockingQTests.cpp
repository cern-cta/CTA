/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/process/threading/BlockingQueue.hpp"

#include <gtest/gtest.h>

namespace threadedUnitTests {

using QueueType = cta::threading::BlockingQueue<int>;

const int numberOfElements = 10000;

class PingThread : public cta::threading::Thread {
  QueueType& queue;

protected:
  virtual void run()  //@override
  {
    for (int i = 0; i < numberOfElements; ++i) {
      int n = queue.pop();
      queue.push(n + 1);
    }
  }

public:
  explicit PingThread(QueueType& q) : queue(q) {}
};

class PongThread : public cta::threading::Thread {
  QueueType& queue;

protected:
  virtual void run()  //@override
  {
    for (int i = 0; i < numberOfElements; ++i) {
      int n = queue.pop();
      queue.push(n + 1);
    }
  }

public:
  explicit PongThread(QueueType& q) : queue(q) {}
};

TEST(cta_threading, BlockingQ_properly_working_on_helgrind) {
  QueueType sharedQueue;

  PingThread ping(sharedQueue);
  PongThread pong(sharedQueue);

  pong.start();
  sharedQueue.push(0);
  sharedQueue.push(-100);

  ping.start();

  sharedQueue.push(42);
  sharedQueue.push(21);

  pong.wait();
  ping.wait();

  ASSERT_EQ(4U, sharedQueue.size());
}

}  // namespace threadedUnitTests
