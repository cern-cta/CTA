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

#include <castor/tape/tapeserver/daemon/TapeserverProxyMock.hpp>

#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "common/log/StringLogger.hpp"
#include "scheduler/TapeMountDummy.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>


namespace unitTests {
  using namespace castor::tape;
  using ::testing::_;
  
TEST(castor_tape_tapeserver_daemon, WatchdogTestStuckWithNothing) {
  const double reportPeriodSecs = 10; // We wont report in practice
  const double stuckPeriod = 0.01;
  const double pollPeriod = 0.01;

  cta::log::StringLogger log("dummy","castor_tape_tapeserver_daemon_WatchdogTestStuck",cta::log::DEBUG);
  cta::log::LogContext lc(log);

  cta::tape::daemon::TapeserverProxyMock dummyInitialProcess;
  cta::TapeMountDummy dummyTapeMount;

  tapeserver::daemon::RecallWatchDog watchdog(reportPeriodSecs,
    stuckPeriod,dummyInitialProcess,dummyTapeMount,"testTapeDrive",lc,pollPeriod);
  
  watchdog.startThread();
  usleep(100000);
  watchdog.stopAndWaitThread();
  //we dont tell the watchdog we are working on file, 
  //it should not report as being stuck
  ASSERT_EQ(std::string::npos, log.getLog().find("No tape block movement for too long"));
}

TEST(castor_tape_tapeserver_daemon, MigrationWatchdogTestStuck) {
  const double reportPeriodSecs = 10; // We wont report in practice
  const double stuckPeriod = 0.01;
  const double pollPeriod = 0.01;

  cta::log::StringLogger log("dummy","castor_tape_tapeserver_daemon_WatchdogTestStuck",cta::log::DEBUG);
  cta::log::LogContext lc(log);

  cta::tape::daemon::TapeserverProxyMock dummyInitialProcess;
  cta::TapeMountDummy dummyTapeMount;

  // We will poll for a
  tapeserver::daemon::MigrationWatchDog watchdog(reportPeriodSecs,stuckPeriod,
    dummyInitialProcess, dummyTapeMount, "testTapeDrive",  lc, pollPeriod);
  
  watchdog.startThread();
  watchdog.notifyBeginNewJob(64, 64);
  usleep(100000);
  watchdog.stopAndWaitThread();
  // This time the internal watchdog should have triggered
  ASSERT_NE(std::string::npos, log.getLog().find("No tape block movement for too long"));
}

TEST(castor_tape_tapeserver_daemon, MigrationWatchdog_DoNotReportParamsAddedAndDeleted) {
  const double reportPeriodSecs = 10; // We wont report in practice
  const double stuckPeriod = 0.01;
  const double pollPeriod = 0.01;

  cta::log::StringLogger log("dummy","castor_tape_tapeserver_daemon_DoNotReportParamsAddedAndDeleted",cta::log::DEBUG);
  cta::log::LogContext lc(log);

  cta::tape::daemon::TapeserverProxyMock dummyInitialProcess;
  cta::TapeMountDummy dummyTapeMount;

  tapeserver::daemon::RecallWatchDog watchdog(reportPeriodSecs,
    stuckPeriod,dummyInitialProcess,dummyTapeMount,"testTapeDrive",lc,pollPeriod);

  std::list<cta::log::Param> paramsToAdd {
      {"param1", 10},
      {"param1", 11}, // Will override the first param1 entry
      {"param2", 20},
      {"param3", 30},
      {"param3", 31}, // Will override the first param3 entry
      {"param4", 40}
  };

  std::list<std::string> paramsToDelete {
        {"param0"},
        {"param1"},
        {"param2"}
  };

  for (const auto & param : paramsToAdd) {
    watchdog.addParameter(param);
  }
  for (const auto & param : paramsToDelete) {
    watchdog.deleteParameter(param);
  }

  // Capture the parameters sent by TapedProxyMock with addLogParams() and deleteLogParams()
  std::list<cta::log::Param> capturedParamsToAdd;
  std::list<std::string> capturedParamsToDelete;

  EXPECT_CALL(dummyInitialProcess, addLogParams(_))
      .WillOnce(testing::SaveArg<0>(&capturedParamsToAdd));
  EXPECT_CALL(dummyInitialProcess, deleteLogParams(_))
      .WillOnce(testing::SaveArg<0>(&capturedParamsToDelete));

  watchdog.startThread();
  usleep(100000);
  watchdog.stopAndWaitThread();

  // Should only be adding "param3" (twice with value 30 and 31) and "param4" (once with value 40)
  {
    ASSERT_EQ(capturedParamsToAdd.size(), 2);
    std::map<std::string, cta::log::Param> paramsToAddMap;
    paramsToAddMap.emplace(capturedParamsToAdd.front().getName(), capturedParamsToAdd.front());
    capturedParamsToAdd.pop_front();
    paramsToAddMap.emplace(capturedParamsToAdd.front().getName(), capturedParamsToAdd.front());
    capturedParamsToAdd.pop_front();
    ASSERT_TRUE(paramsToAddMap.find("param3") != paramsToAddMap.end());
    ASSERT_EQ(std::get<int64_t>(paramsToAddMap.at("param3").getValueVariant().value()), 31);
    ASSERT_TRUE(paramsToAddMap.find("param4") != paramsToAddMap.end());
    ASSERT_EQ(std::get<int64_t>(paramsToAddMap.at("param4").getValueVariant().value()), 40);
  }

  // Should only be adding "param0" (only one that was not added before)
  {
    ASSERT_EQ(capturedParamsToDelete.size(), 1);
    auto paramA = capturedParamsToDelete.front();
    ASSERT_EQ(paramA, "param0");
  }
}

}
