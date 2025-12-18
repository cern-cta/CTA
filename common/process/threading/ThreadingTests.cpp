/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/process/threading/ChildProcess.hpp"
#include "common/process/threading/MutexLocker.hpp"
#include "common/process/threading/Semaphores.hpp"
#include "common/process/threading/Thread.hpp"

#include <gtest/gtest.h>
#include <time.h>

/* Note: those tests create multi threading errors on purpose and should not
 * be run in helgrind */

namespace unitTests {
TEST(cta_threading, Mutex_properly_throws_exceptions) {
  /* Check that we properly get exception when doing wrong semaphore
     operations */
  cta::threading::Mutex m;
  ASSERT_NO_THROW(m.lock());
  /* Duplicate lock */
  ASSERT_THROW(m.lock(), cta::exception::Errnum);
  ASSERT_NO_THROW(m.unlock());
  /* Duplicate release */
  ASSERT_THROW(m.unlock(), cta::exception::Errnum);
}

TEST(cta_threading, MutexLocker_locks_and_properly_throws_exceptions) {
  cta::threading::Mutex m;
  {
    cta::threading::MutexLocker ml(m);
    /* This is a different flavourr of duplicate locking */
    ASSERT_THROW(m.lock(), cta::exception::Errnum);
    ASSERT_NO_THROW(m.unlock());
    ASSERT_NO_THROW(m.lock());
  }
  /* As the locker has been destroyed, and the mutex released, another
     * lock/unlock should be possible */
  ASSERT_NO_THROW(m.lock());
  ASSERT_NO_THROW(m.unlock());
}

TEST(cta_threading, PosixSemaphore_basic_counting) {
  cta::threading::PosixSemaphore s(2);
  ASSERT_NO_THROW(s.acquire());
  ASSERT_EQ(true, s.tryAcquire());
  ASSERT_FALSE(s.tryAcquire());
}

TEST(cta_threading, CondVarSemaphore_basic_counting) {
  cta::threading::CondVarSemaphore s(2);
  ASSERT_NO_THROW(s.acquire());
  ASSERT_EQ(true, s.tryAcquire());
  ASSERT_FALSE(s.tryAcquire());
}

TEST(cta_threading, Semaphore_basic_counting) {
  cta::threading::Semaphore s(2);
  ASSERT_NO_THROW(s.acquire());
  ASSERT_EQ(true, s.tryAcquire());
  ASSERT_FALSE(s.tryAcquire());
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
}  // namespace unitTests
