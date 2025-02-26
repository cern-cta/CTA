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

#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "common/log/StringLogger.hpp"
#include "scheduler/TapeMountDummy.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>


namespace unitTests {
  using namespace castor::tape;
  using ::testing::_;

class TapedProxyMock final : public cta::tape::daemon::TapedProxy {
public:
  MOCK_METHOD(void, reportState, (const cta::tape::session::SessionState state, const cta::tape::session::SessionType type, const std::string& vid), (override));
  MOCK_METHOD(void, reportHeartbeat, (uint64_t totalTapeBytesMoved, uint64_t totalDiskBytesMoved), (override));
  MOCK_METHOD(void, addLogParams, (const std::list<cta::log::Param>& params), (override));
  MOCK_METHOD(void, deleteLogParams, (const std::list<std::string>& paramNames), (override));
  MOCK_METHOD(void, resetLogParams, (), (override));
  MOCK_METHOD(void, labelError, (const std::string& unitName, const std::string& message), (override));
  MOCK_METHOD(void, setRefreshLoggerHandler, (std::function<void()> handler), (override));
};
  
TEST(castor_tape_tapeserver_daemon, WatchdogTestStuckWithNothing) {
  const double periodToReport = 10; // We wont report in practice
  const double stuckPeriod = 0.01;
  const double pollPeriod = 0.01;
  
  cta::log::StringLogger log("dummy","castor_tape_tapeserver_daemon_WatchdogTestStuck",cta::log::DEBUG);
  cta::log::LogContext lc(log);
  
  TapedProxyMock dummyInitialProcess;
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
  
  TapedProxyMock dummyInitialProcess;
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

TEST(castor_tape_tapeserver_daemon, MigrationWatchdog_DoNotReportParamsAddedAndDeleted) {
  const double reportPeriod = 10; // We wont report in practice
  const double stuckPeriod = 0.01;
  const double pollPeriod = 0.01;

  cta::log::StringLogger log("dummy","castor_tape_tapeserver_daemon_DoNotReportParamsAddedAndDeleted",cta::log::DEBUG);
  cta::log::LogContext lc(log);

  TapedProxyMock dummyInitialProcess;
  cta::TapeMountDummy dummyTapeMount;

  tapeserver::daemon::RecallWatchDog watchdog(reportPeriod,
    stuckPeriod,dummyInitialProcess,dummyTapeMount,"testTapeDrive",lc,pollPeriod);

  std::list<cta::log::Param> paramsToAdd {
      {"param1", 10},
      {"param1", 11}, // Repeated intentionally
      {"param2", 20},
      {"param3", 30}, // Repeated intentionally
      {"param3", 31},
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
    ASSERT_EQ(capturedParamsToAdd.size(), 3);
    auto paramA = capturedParamsToAdd.front();
    capturedParamsToAdd.pop_front();
    auto paramB = capturedParamsToAdd.front();
    capturedParamsToAdd.pop_front();
    auto paramC = capturedParamsToAdd.front();
    capturedParamsToAdd.pop_front();
    ASSERT_EQ(paramA.getName(), "param3");
    ASSERT_EQ(std::get<int64_t>(paramA.getValueVariant().value()), 30);
    ASSERT_EQ(paramB.getName(), "param3");
    ASSERT_EQ(std::get<int64_t>(paramB.getValueVariant().value()), 31);
    ASSERT_EQ(paramC.getName(), "param4");
    ASSERT_EQ(std::get<int64_t>(paramC.getValueVariant().value()), 40);
  }

  // Should only be adding "param0" (only one that was not added before)
  {
    ASSERT_EQ(capturedParamsToDelete.size(), 1);
    auto paramA = capturedParamsToDelete.front();
    ASSERT_EQ(paramA, "param0");
  }
}

}
