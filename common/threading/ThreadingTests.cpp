/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/threading/MutexLocker.hpp"
#include "common/threading/Thread.hpp"
#include "common/threading/ChildProcess.hpp"
#include "common/threading/Semaphores.hpp"

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
  }
  catch (std::exception& e) {
    std::string w(e.what());
    ASSERT_NE(std::string::npos, w.find("Exception in child thread"));
  }
}
}  // namespace unitTests
