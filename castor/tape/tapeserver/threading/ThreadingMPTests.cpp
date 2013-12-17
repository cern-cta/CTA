/******************************************************************************
 *                      ThreadingTests.cpp
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

#include <gtest/gtest.h>
#include "Threading.hpp"
#include "ChildProcess.hpp"

/* This is a collection of multi process unit tests, which can (and should)
 be passed through helgrind, but not valgrind as the test framework creates
 many memory leaks in the child process. */

namespace ThreadedUnitTests {
  class emptyCleanup : public castor::tape::threading::ChildProcess::Cleanup {
  public:

    virtual void operator ()() { };
  };

  class myOtherProcess : public castor::tape::threading::ChildProcess {
  private:

    int run() {
      /* Just sleep a bit so the parent process gets a chance to see us running */
      struct timespec ts;
      ts.tv_sec = 0;
      ts.tv_nsec = 10*1000*1000;
      nanosleep(&ts, NULL);
      return 123;
    }
  };

  TEST(castor_tape_threading, ChildProcess_return_value) {
    myOtherProcess cp;
    emptyCleanup cleanup;
    EXPECT_THROW(cp.exitCode(), castor::tape::threading::ChildProcess::ProcessNeverStarted);
    EXPECT_NO_THROW(cp.start(cleanup));
    EXPECT_THROW(cp.exitCode(), castor::tape::threading::ChildProcess::ProcessStillRunning);
    EXPECT_NO_THROW(cp.wait());
    ASSERT_EQ(123, cp.exitCode());
  }

  class myInfiniteSpinner : public castor::tape::threading::ChildProcess {
  private:

    int run() {
      /* Loop forever (politely) */
      while (true) {
        struct timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = 10*1000*1000;
        nanosleep(&ts, NULL);
      }
      return 321;
    }
  };

  TEST(castor_tape_threading, ChildProcess_killing) {
    myInfiniteSpinner cp;
    emptyCleanup cleanup;
    EXPECT_THROW(cp.kill(), castor::tape::threading::ChildProcess::ProcessNeverStarted);
    EXPECT_NO_THROW(cp.start(cleanup));
    EXPECT_THROW(cp.exitCode(), castor::tape::threading::ChildProcess::ProcessStillRunning);
    ASSERT_EQ(true, cp.running());
    EXPECT_NO_THROW(cp.kill());
    /* The effect is not immediate, wait a bit. */
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 10*1000*1000;
    nanosleep(&ts, NULL);
    ASSERT_EQ(false, cp.running());
    EXPECT_THROW(cp.exitCode(), castor::tape::threading::ChildProcess::ProcessWasKilled);
  }
};
