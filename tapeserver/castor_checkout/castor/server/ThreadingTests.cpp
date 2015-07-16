/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/server/MutexLocker.hpp"
#include "castor/server/Threading.hpp"
#include "castor/server/ChildProcess.hpp"
#include "castor/server/Semaphores.hpp"

#include <gtest/gtest.h>
#include <time.h>

/* Note: those tests create multi threading errors on purpose and should not
 * be run in helgrind */

namespace unitTests {
  TEST(castor_tape_threading, Mutex_properly_throws_exceptions) {
    /* Check that we properly get exception when doing wrong semaphore 
     operations */
    castor::server::Mutex m;
    ASSERT_NO_THROW(m.lock());
    /* Duplicate lock */
    ASSERT_THROW(m.lock(),castor::exception::Errnum);
    ASSERT_NO_THROW(m.unlock());
    /* Duplicate release */
    ASSERT_THROW(m.unlock(),castor::exception::Errnum);
  }
  
  TEST(castor_tape_threading, MutexLocker_locks_and_properly_throws_exceptions) {
    castor::server::Mutex m;
    {
      castor::server::MutexLocker ml(&m);
      /* This is a different flavourr of duplicate locking */
      ASSERT_THROW(m.lock(),castor::exception::Errnum);
      ASSERT_NO_THROW(m.unlock());
      ASSERT_NO_THROW(m.lock());
    }
    /* As the locker has been destroyed, and the mutex released, another 
     * lock/unlock should be possible */
    ASSERT_NO_THROW(m.lock());
    ASSERT_NO_THROW(m.unlock());
  }

  TEST(castor_tape_threading, PosixSemaphore_basic_counting) {
    castor::server::PosixSemaphore s(2);
    ASSERT_NO_THROW(s.acquire());
    ASSERT_EQ(true, s.tryAcquire());
    ASSERT_EQ(false, s.tryAcquire());
  }

  TEST(castor_tape_threading, CondVarSemaphore_basic_counting) {
    castor::server::CondVarSemaphore s(2);
    ASSERT_NO_THROW(s.acquire());
    ASSERT_EQ(true, s.tryAcquire());
    ASSERT_EQ(false, s.tryAcquire());
  }

  TEST(castor_tape_threading, Semaphore_basic_counting) {
    castor::server::Semaphore s(2);
    ASSERT_NO_THROW(s.acquire());
    ASSERT_EQ(true, s.tryAcquire());
    ASSERT_EQ(false, s.tryAcquire());
  }
  
  class Thread_exception_throwing: public castor::server::Thread {
  private:
    void run() {
      throw castor::exception::Exception("Exception in child thread");
    }
  };
  TEST(castor_tape_threading, Thread_exception_throwing) {
    Thread_exception_throwing t, t2;
    t.start();
    t2.start();
    ASSERT_THROW(t.wait(), castor::server::UncaughtExceptionInThread);
    try {
      t2.wait();
    } catch (std::exception & e) {
      std::string w(e.what());
      ASSERT_NE(std::string::npos, w.find("Exception in child thread"));
    }
  }
} // namespace unitTests
