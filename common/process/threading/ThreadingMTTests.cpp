/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/process/threading/MutexLocker.hpp"
#include "common/process/threading/Semaphores.hpp"
#include "common/process/threading/Thread.hpp"

#include <gtest/gtest.h>

/* This is a collection of multi threaded unit tests, which can (and should)
 be passed through helgrind, as well as valgrind */

namespace threadedUnitTests {

class Thread_and_basic_locking : public cta::threading::Thread {
public:
  int counter;
  cta::threading::Mutex mutex;

private:
  void run() {
    for (int i = 0; i < 100; i++) {
      cta::threading::MutexLocker ml(mutex);
      counter++;
    }
  }
};

TEST(cta_threading, Thread_and_basic_locking) {
  /* If we have race conditions here, helgrind will trigger. */
  Thread_and_basic_locking mt;
  mt.counter = 0;
  mt.start();
  for (int i = 0; i < 100; i++) {
    cta::threading::MutexLocker ml(mt.mutex);
    mt.counter--;
  }
  mt.wait();
  ASSERT_EQ(0, mt.counter);
}

template<class S>
class Semaphore_ping_pong : public cta::threading::Thread {
public:
  void thread0() {
    int i = 100;
    while (i > 0) {
      m_sem1.release();
      m_sem0.acquire();
      i--;
    }
  }

private:
  S m_sem0, m_sem1;

  void run() {
    int i = 100;
    while (i > 0) {
      m_sem0.release();
      m_sem1.acquire();
      i--;
    }
  }
};

TEST(cta_threading, PosixSemaphore_ping_pong) {
  Semaphore_ping_pong<cta::threading::PosixSemaphore> spp;
  spp.start();
  spp.thread0();
  spp.wait();
}

TEST(cta_threading, CondVarSemaphore_ping_pong) {
  Semaphore_ping_pong<cta::threading::CondVarSemaphore> spp;
  spp.start();
  spp.thread0();
  spp.wait();
}

class Thread_exception_throwing : public cta::threading::Thread {
private:
  void run() { throw cta::exception::Exception("Exception in child thread"); }
};

TEST(cta_threading, Thread_exception_throwing) {
  Thread_exception_throwing t, t2;
  t.start();
  t2.start();
  ASSERT_THROW(t.wait(), cta::threading::UncaughtExceptionInThread);
  try {
    t2.wait();
  } catch (std::exception& e) {
    std::string w(e.what());
    ASSERT_NE(std::string::npos, w.find("Exception in child thread"));
  }
}
}  // namespace threadedUnitTests
