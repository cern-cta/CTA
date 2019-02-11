/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <gtest/gtest.h>
#include <iostream>

#include "common/threading/RWLock.hpp"
#include "common/threading/RWLockRdLocker.hpp"
#include "common/threading/RWLockWrLocker.hpp"
#include "common/threading/Thread.hpp"

namespace unitTests {

class cta_threading_RWLockTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

  class RdLockerThread: public ::cta::threading::Thread {
  public:

    RdLockerThread(cta::threading::RWLock &lock, uint32_t &counter): m_lock(lock), m_counter(counter) {
    }

    void run() {
      cta::threading::RWLockRdLocker rdLocker(m_lock);

      // Just read the counter - do not write to it
      uint32_t copyOfCounter = m_counter;
      copyOfCounter++;
    }

  private:

    cta::threading::RWLock &m_lock;

    uint32_t &m_counter;
  };

  class WrLockerThread: public ::cta::threading::Thread {
  public:

    WrLockerThread(cta::threading::RWLock &lock, uint32_t &counter): m_lock(lock), m_counter(counter) {
    }

    void run() {
      cta::threading::RWLockWrLocker wrLocker(m_lock);
      m_counter++;
    }

  private:

    cta::threading::RWLock &m_lock;

    uint32_t &m_counter;
  };
};

TEST_F(cta_threading_RWLockTest, rdlock) {
  using namespace cta::threading;

  RWLock lock;

  lock.rdlock();
  lock.unlock();
}

TEST_F(cta_threading_RWLockTest, wrlock) {
  using namespace cta::threading;

  RWLock lock;

  lock.wrlock();
  lock.unlock();
}

TEST_F(cta_threading_RWLockTest, RWLockRdLocker) {
  using namespace cta::threading;

  RWLock lock;

  RWLockRdLocker rdLocker(lock);
}

TEST_F(cta_threading_RWLockTest, RWLockWrLocker) {
  using namespace cta::threading;

  RWLock lock;

  RWLockWrLocker wrLocker(lock);
}

TEST_F(cta_threading_RWLockTest, multiple_threads) {
  using namespace cta::threading;

  RWLock lock;
  uint32_t counter = 0;

  RdLockerThread rdLockerThreads[4] = {{lock, counter}, {lock, counter}, {lock, counter}, {lock, counter}};
  WrLockerThread wrLockerThreads[4] = {{lock, counter}, {lock, counter}, {lock, counter}, {lock, counter}};

  for(uint32_t i = 0; i <4; i++) {
    rdLockerThreads[i].start();
    wrLockerThreads[i].start();
  }
  for(uint32_t i = 0; i <4; i++) {
    rdLockerThreads[i].wait();
    wrLockerThreads[i].wait();
  }

  ASSERT_EQ(4, counter);
}

} // namespace unitTests
