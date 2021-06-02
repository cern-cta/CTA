/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "castor/messages/TapeserverProxyDummy.hpp"
#include "common/log/StringLogger.hpp"
#include "scheduler/TapeMountDummy.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>


namespace unitTests {
  using namespace castor::tape;
  using ::testing::_;
  
TEST(castor_tape_tapeserver_daemon, WatchdogTestStuckWithNothing) {
  const double periodToReport = 10; // We wont report in practice
  const double stuckPeriod = 0.01;
  const double pollPeriod = 0.01;
  
  cta::log::StringLogger log("dummy","castor_tape_tapeserver_daemon_WatchdogTestStuck",cta::log::DEBUG);
  cta::log::LogContext lc(log);
  
  castor::messages::TapeserverProxyDummy dummyInitialProcess;
  cta::TapeMountDummy dummyTapeMount;

  tapeserver::daemon::RecallWatchDog watchdog(periodToReport,
    stuckPeriod,dummyInitialProcess,dummyTapeMount,"testTapeDrive",lc,pollPeriod);
  
  watchdog.startThread();
  usleep(100000);
  watchdog.stopAndWaitThread();
  //we dont tell the watchdog we are working on file, 
  //it should not report as being stuck
  ASSERT_EQ(std::string::npos, log.getLog().find("No tape block movement for too long"));
}

TEST(castor_tape_tapeserver_daemon, MigrationWatchdogTestStuck) {
  const double reportPeriod = 10; // We wont report in practice
  const double stuckPeriod = 0.01;
  const double pollPeriod = 0.01;
  
  cta::log::StringLogger log("dummy","castor_tape_tapeserver_daemon_WatchdogTestStuck",cta::log::DEBUG);
  cta::log::LogContext lc(log);
  
  castor::messages::TapeserverProxyDummy dummyInitialProcess;
  cta::TapeMountDummy dummyTapeMount;
  
  // We will poll for a 
  tapeserver::daemon::MigrationWatchDog watchdog(reportPeriod,stuckPeriod,
    dummyInitialProcess, dummyTapeMount, "testTapeDrive",  lc, pollPeriod);
  
  watchdog.startThread();
  watchdog.notifyBeginNewJob(64, 64);
  usleep(100000);
  watchdog.stopAndWaitThread();
  // This time the internal watchdog should have triggered
  ASSERT_NE(std::string::npos, log.getLog().find("No tape block movement for too long"));
}

}
