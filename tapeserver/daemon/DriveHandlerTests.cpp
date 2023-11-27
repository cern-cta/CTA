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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/dataStructures/DriveStatus.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StringLogger.hpp"
#include "scheduler/IScheduler.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/Session.hpp"
#include "tapeserver/daemon/DriveHandler.hpp"
#include "tapeserver/daemon/DriveHandlerProxy.hpp"
#include "tapeserver/daemon/ProcessManager.hpp"
#include "tapeserver/daemon/TapedConfiguration.hpp"
#include "tapeserver/daemon/TapedProxy.hpp"
#include "tapeserver/daemon/TpconfigLine.hpp"

using ::testing::_;
using ::testing::ByMove;
using ::testing::Invoke;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::Throw;

namespace cta {
namespace tape {
namespace daemon {

class DriveHandlerMock : public DriveHandler {
public:
  using DriveHandler::DriveHandler;
  MOCK_CONST_METHOD1(createCatalogue, std::shared_ptr<cta::catalogue::Catalogue>(const std::string& methodCaller));
  MOCK_METHOD3(createScheduler, std::shared_ptr<cta::IScheduler>(const std::string& prefixProcessName,
    const uint64_t minFilesToWarrantAMount, const uint64_t minBytesToWarrantAMount));
  MOCK_CONST_METHOD0(createDriveHandlerProxy, std::shared_ptr<cta::tape::daemon::TapedProxy>());
  MOCK_CONST_METHOD1(executeCleanerSession,
    castor::tape::tapeserver::daemon::Session::EndOfSessionAction(cta::IScheduler* scheduler));
  MOCK_CONST_METHOD2(executeDataTransferSession,
    castor::tape::tapeserver::daemon::Session::EndOfSessionAction(cta::IScheduler* scheduler,
    cta::tape::daemon::TapedProxy* driveHandlerProxy));
    MOCK_METHOD1(resetToDefault, void(const std::string& methodCaller));

  // Just for testing.
  void setPreviousSession(PreviousSession previousSessionState, session::SessionState previousState,
    session::SessionType previousType, std::string_view vid) {
    m_previousSession = previousSessionState;
    m_previousState = previousState;
    m_previousType = previousType;
    m_previousVid = vid;
  }

  void setSessionVid(std::string_view vid) {
    m_sessionVid = vid;
  }

  void setSessionState(session::SessionState state) {
    m_sessionState = state;
  }
};

class TapedProxyMock : public TapedProxy {
public:
  MOCK_METHOD3(reportState,
    void(const cta::tape::session::SessionState state, const cta::tape::session::SessionType type, const std::string& vid));
  MOCK_METHOD2(reportHeartbeat, void(uint64_t totalTapeBytesMoved, uint64_t totalDiskBytesMoved));
  MOCK_METHOD2(addLogParams, void(const std::string& unitName, const std::list<cta::log::Param>& params));
  MOCK_METHOD2(deleteLogParams, void(const std::string& unitName, const std::list<std::string>& paramNames));
  MOCK_METHOD2(labelError, void(const std::string& unitName, const std::string& message));
};

class ProcessManagerMock : public cta::tape::daemon::ProcessManager {
public:
  using cta::tape::daemon::ProcessManager::ProcessManager;
  MOCK_METHOD2(addFile, void(int fd, cta::tape::daemon::SubprocessHandler * sh));
  MOCK_METHOD1(removeFile, void(int fd));
};

} // namespace daemon
} // namespace tape

class SchedulerMock : public cta::IScheduler {
public:
  MOCK_METHOD1(ping, void(cta::log::LogContext& lc));
  MOCK_METHOD4(reportDriveStatus, void(const cta::common::dataStructures::DriveInfo& driveInfo,
    cta::common::dataStructures::MountType type,
    cta::common::dataStructures::DriveStatus status,
    cta::log::LogContext& lc));
  MOCK_METHOD4(setDesiredDriveState, void(const cta::common::dataStructures::SecurityIdentity& cliIdentity,
    const std::string& driveName,
    const cta::common::dataStructures::DesiredDriveState& desiredState,
    cta::log::LogContext& lc));
  MOCK_METHOD2(checkDriveCanBeCreated,
    bool(const cta::common::dataStructures::DriveInfo & driveInfo, cta::log::LogContext& lc));
  MOCK_METHOD2(getDesiredDriveState,
    cta::common::dataStructures::DesiredDriveState(const std::string& driveName, cta::log::LogContext& lc));
  MOCK_METHOD(void, createTapeDriveStatus, (const common::dataStructures::DriveInfo& driveInfo,
    const common::dataStructures::DesiredDriveState & desiredState, const common::dataStructures::MountType& type,
    const common::dataStructures::DriveStatus& status, const tape::daemon::TpconfigLine& tpConfigLine,
    const common::dataStructures::SecurityIdentity& identity, log::LogContext & lc));
  MOCK_METHOD3(reportDriveConfig, void(const cta::tape::daemon::TpconfigLine& tpConfigLine,
    const cta::tape::daemon::TapedConfiguration& tapedConfig, log::LogContext& lc));
};

} // namespace cta

namespace unitTests {

class DriveHandlerTests: public ::testing::Test {
public:
  DriveHandlerTests()
    : m_lc(m_logger), m_processManager(m_lc) {}

  void SetUp() override {
    setUpTapedProxyMock();
    setUpSchedulerMock();
    setUpProcessManagerMock();

    m_catalogue = std::make_unique<cta::catalogue::DummyCatalogue>();
    m_driveHandler = std::make_unique<NiceMock<cta::tape::daemon::DriveHandlerMock>>(m_tapedConfig, m_driveConfig,
      m_processManager);
    ON_CALL(*m_driveHandler, createDriveHandlerProxy()).WillByDefault(Return(m_tapedProxy));
    ON_CALL(*m_driveHandler, createCatalogue(_)).WillByDefault(Return(m_catalogue));
    ON_CALL(*m_driveHandler, createScheduler(_, _, _)).WillByDefault(Return(m_scheduler));
    ON_CALL(*m_driveHandler, executeCleanerSession(_)).WillByDefault(
      Return(castor::tape::tapeserver::daemon::Session::EndOfSessionAction::MARK_DRIVE_AS_DOWN));
    ON_CALL(*m_driveHandler, executeDataTransferSession(_, _)).WillByDefault(
      Return(castor::tape::tapeserver::daemon::Session::EndOfSessionAction::MARK_DRIVE_AS_UP));
  }

  void TearDown() override {
    m_driveHandler.reset();
    m_tapedProxy.reset();
    m_catalogue.reset();
  }

  void setUpProcessManagerMock() {
    ON_CALL(m_processManager, addFile(_, _)).WillByDefault(Return());
    ON_CALL(m_processManager, removeFile(_)).WillByDefault(Return());
  }

  void setUpTapedProxyMock() {
    m_tapedProxy = std::make_shared<NiceMock<cta::tape::daemon::TapedProxyMock>>();
    ON_CALL(*m_tapedProxy, reportState(_, _, _)).WillByDefault(Return());
    ON_CALL(*m_tapedProxy, reportHeartbeat(_, _)).WillByDefault(Return());
    ON_CALL(*m_tapedProxy, addLogParams(_, _)).WillByDefault(Return());
    ON_CALL(*m_tapedProxy, deleteLogParams(_, _)).WillByDefault(Return());
    ON_CALL(*m_tapedProxy, labelError(_, _)).WillByDefault(Return());
  }

  void setUpSchedulerMock() {
    m_scheduler = std::make_shared<NiceMock<cta::SchedulerMock>>();
    ON_CALL(*m_scheduler, ping(_)).WillByDefault(Return());
    ON_CALL(*m_scheduler, reportDriveStatus(_, _, _, _)).WillByDefault(Return());
    ON_CALL(*m_scheduler, setDesiredDriveState(_, _, _, _)).WillByDefault(Return());
    ON_CALL(*m_scheduler, checkDriveCanBeCreated(_, _)).WillByDefault(Return(true));
    ON_CALL(*m_scheduler, getDesiredDriveState(_, _)).WillByDefault(Return(cta::common::dataStructures::DesiredDriveState()));
    ON_CALL(*m_scheduler, createTapeDriveStatus(_, _, _, _, _, _, _)).WillByDefault(Return());
    ON_CALL(*m_scheduler, reportDriveConfig(_, _, _)).WillByDefault(Return());
  }

protected:
  std::unique_ptr<NiceMock<cta::tape::daemon::DriveHandlerMock>> m_driveHandler;
  std::shared_ptr<NiceMock<cta::tape::daemon::TapedProxyMock>> m_tapedProxy;
  std::shared_ptr<NiceMock<cta::SchedulerMock>> m_scheduler;
  std::shared_ptr<cta::catalogue::Catalogue> m_catalogue;

  cta::log::StringLogger m_logger{"dummy", "driveHandlerTests", cta::log::DEBUG};
  cta::log::LogContext m_lc;
  NiceMock<cta::tape::daemon::ProcessManagerMock> m_processManager;
  cta::tape::daemon::TapedConfiguration m_tapedConfig;
  cta::tape::daemon::TpconfigLine m_driveConfig{"drive0", "lib0", "/dev/tape0", "smc0"};

  
};

TEST_F(DriveHandlerTests, getInitialStatus) {
  const auto status = m_driveHandler->getInitialStatus();
  // Check that the status is correct
  ASSERT_FALSE(status.shutdownRequested);
  ASSERT_FALSE(status.shutdownComplete);
  ASSERT_FALSE(status.killRequested);
  ASSERT_TRUE(status.forkRequested);
  ASSERT_FALSE(status.sigChild);
  ASSERT_EQ(status.forkState, cta::tape::daemon::SubprocessHandler::ForkState::notForking);
}

TEST_F(DriveHandlerTests, runSigChild) {
  auto status = m_driveHandler->fork();
  if (status.forkState == cta::tape::daemon::SubprocessHandler::ForkState::parent) {
    m_lc.log(cta::log::DEBUG, "DriveHandlerTests::runSigChild(): Parent process");
    m_driveHandler->shutdown();
    auto logToCheck = m_logger.getLog();
    ASSERT_NE(std::string::npos, logToCheck.find("Tape session finished"));
    ASSERT_NE(std::string::npos, logToCheck.find("In DriveHandler::kill(): sub process completed"));
    ASSERT_EQ(std::string::npos, logToCheck.find("Drive subprocess exited. Will spawn a new one."));
  }
  if (status.forkState == cta::tape::daemon::SubprocessHandler::ForkState::child) {
    m_lc.log(cta::log::DEBUG, "DriveHandlerTests::runSigChild(): Child process");
    m_driveHandler->runChild();
  }

  status = m_driveHandler->processSigChild();
  // Check that the status is correct
  ASSERT_FALSE(status.shutdownRequested);
  ASSERT_FALSE(status.killRequested);
  ASSERT_FALSE(status.forkRequested);
  ASSERT_TRUE(status.shutdownComplete);
  ASSERT_FALSE(status.sigChild);
}

TEST_F(DriveHandlerTests, runSigChildAfterCrash) {
  using EndOfSessionAction = castor::tape::tapeserver::daemon::Session::EndOfSessionAction;
  m_driveHandler->setPreviousSession(cta::tape::daemon::DriveHandler::PreviousSession::Crashed,
                                     cta::tape::session::SessionState::Running,
                                     cta::tape::session::SessionType::Undetermined,
                                     std::string("TAPE0001"));
  auto status = m_driveHandler->fork();
  if (status.forkState == cta::tape::daemon::SubprocessHandler::ForkState::child) {
    ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_DOWN);
    auto logToCheck = m_logger.getLog();
    ASSERT_NE(std::string::npos, logToCheck.find("In DriveHandler::runChild(): starting cleaner after crash "
                                                 "with tape potentially loaded."));
    ASSERT_NE(std::string::npos, logToCheck.find("In DriveHandler::runChild(): will create cleaner session."));
  }

  m_logger.clearLog();
  status = m_driveHandler->processSigChild();
  // Check that the status is correct
  ASSERT_FALSE(status.shutdownRequested);
  ASSERT_FALSE(status.killRequested);
  ASSERT_FALSE(status.forkRequested);
  ASSERT_FALSE(status.shutdownComplete);
  ASSERT_FALSE(status.sigChild);
  m_driveHandler->setSessionVid("TAPE0001");
  status = m_driveHandler->shutdown();
  ASSERT_FALSE(status.shutdownRequested);
  ASSERT_FALSE(status.killRequested);
  ASSERT_FALSE(status.forkRequested);
  ASSERT_TRUE(status.shutdownComplete);
  ASSERT_FALSE(status.sigChild);
  auto logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("In DriveHandler::kill(): sub process completed"));
  ASSERT_NE(std::string::npos, logToCheck.find("In DriveHandler::shutdown(): starting cleaner."));

}

TEST_F(DriveHandlerTests, childTimeOut) {
  using EndOfSessionAction = castor::tape::tapeserver::daemon::Session::EndOfSessionAction;
  m_driveHandler->setSessionState(cta::tape::session::SessionState::Running);
  auto status = m_driveHandler->processTimeout();
  // Check that the status is correct
  ASSERT_FALSE(status.shutdownRequested);
  ASSERT_FALSE(status.killRequested);
  ASSERT_FALSE(status.forkRequested);
  ASSERT_FALSE(status.shutdownComplete);
  ASSERT_FALSE(status.sigChild);

  auto logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("In DriveHandler::processTimeout(): Received timeout "
                                               "without child process present."));
  ASSERT_NE(std::string::npos, logToCheck.find("Re-launching child process."));

  status = m_driveHandler->fork();
  if (status.forkState == cta::tape::daemon::SubprocessHandler::ForkState::child) {
    ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_UP);
    m_driveHandler->setPreviousSession(cta::tape::daemon::DriveHandler::PreviousSession::Crashed,
                                     cta::tape::session::SessionState::Running,
                                     cta::tape::session::SessionType::Undetermined,
                                     std::string("TAPE0001"));
    m_logger.clearLog();
    ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_DOWN);
    logToCheck = m_logger.getLog();
    ASSERT_NE(std::string::npos, logToCheck.find("In DriveHandler::runChild(): will create cleaner session."));
  }

  status = m_driveHandler->shutdown();
  ASSERT_FALSE(status.shutdownRequested);
  ASSERT_FALSE(status.killRequested);
  ASSERT_FALSE(status.forkRequested);
  ASSERT_TRUE(status.shutdownComplete);
  ASSERT_FALSE(status.sigChild);
}

TEST_F(DriveHandlerTests, shutdown) {
  std::string logToCheck;
  // Fork and shutdown
  // m_driveHandler->fork();
  // m_driveHandler->shutdown();
  // logToCheck = m_logger.getLog();
  // // This message is not generated in the log
  // ASSERT_EQ(std::string::npos, logToCheck.find("In DriveHandler::kill(): no subprocess to kill"));

  // Shutdown without forking
  m_logger.clearLog();
  const auto status = m_driveHandler->shutdown();
  // Check that the status is correct
  ASSERT_FALSE(status.shutdownRequested);
  ASSERT_TRUE(status.shutdownComplete);
  ASSERT_FALSE(status.killRequested);
  ASSERT_FALSE(status.forkRequested);
  ASSERT_FALSE(status.sigChild);
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("In DriveHandler::shutdown(): simply killing the process."));
  // Because we didn´t create a subprocess using fork
  ASSERT_NE(std::string::npos, logToCheck.find("In DriveHandler::kill(): no subprocess to kill"));

  // Fail to create scheduler
  EXPECT_CALL(*m_driveHandler, createScheduler(_, _, _)).WillOnce(
    Throw(cta::exception::Exception("createScheduler failed to create scheduler"))).WillRepeatedly(
    Return(m_scheduler));
  m_logger.clearLog();
  m_driveHandler->shutdown();
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("In DriveHandler::shutdown(): failed to instantiate scheduler."));

  // Shutdown but m_sessionVid is empty
  m_driveHandler->setPreviousSession(cta::tape::daemon::DriveHandler::PreviousSession::Crashed,
                                     cta::tape::session::SessionState::Running,
                                     cta::tape::session::SessionType::Undetermined,
                                     std::string(""));
  m_logger.clearLog();
  m_driveHandler->shutdown();
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("Should run cleaner but VID is missing. Do nothing."));

  // Shutdown but m_sessionVid is not empty and it starts cleaner
  m_driveHandler->setSessionVid("TAPE0001");
  m_logger.clearLog();
  m_driveHandler->shutdown();
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("In DriveHandler::shutdown(): starting cleaner."));
}

TEST_F(DriveHandlerTests, runChildAndExecuteDataTransferSession) {
  using EndOfSessionAction = castor::tape::tapeserver::daemon::Session::EndOfSessionAction;
  EXPECT_CALL(*m_driveHandler, executeDataTransferSession(_, _)).WillOnce(Invoke(
      [this](cta::IScheduler*, cta::tape::daemon::TapedProxy*) {
        m_lc.log(cta::log::DEBUG, "DriveHandlerTests::runChild(): Executing data transfer session. "
          "And marking drive as up");
        return EndOfSessionAction::MARK_DRIVE_AS_UP;
      })).WillOnce(Invoke(
      [this](cta::IScheduler*, cta::tape::daemon::TapedProxy*) {
        m_lc.log(cta::log::DEBUG, "DriveHandlerTests::runChild(): Executing data transfer session. "
          "And marking drive as down");
        return EndOfSessionAction::MARK_DRIVE_AS_DOWN;
      }));

  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_UP);
  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_DOWN);
}

TEST_F(DriveHandlerTests, runChildAndFailSchedulerMethods) {
  using EndOfSessionAction = castor::tape::tapeserver::daemon::Session::EndOfSessionAction;

  // Methods to check the outputs of the tests
  std::string logToCheck;
  cta::tape::session::SessionState sessionState;
  cta::tape::session::SessionType sessionType;
  std::string tapeVid;
  EXPECT_CALL(*m_tapedProxy, reportState(_, _, _)).WillRepeatedly(Invoke(
      [&](const cta::tape::session::SessionState state, const cta::tape::session::SessionType type,
        const std::string& vid) {
        m_lc.log(cta::log::DEBUG, "DriveHandlerTests::runChild(): Reporting state");
        sessionState = state;
        sessionType = type;
        tapeVid = vid;
        return;
      }));
  auto checkReport = [&]() -> void {
    ASSERT_EQ(sessionState, cta::tape::session::SessionState::Fatal);
    ASSERT_EQ(sessionType, cta::tape::session::SessionType::Undetermined);
    ASSERT_EQ(tapeVid, "");
  };

  // It cannot create the scheduler instance, so it should mark the drive as down
  EXPECT_CALL(*m_driveHandler, createScheduler(_, _, _)).WillOnce(
    Throw(cta::exception::Exception("createScheduler failed to create scheduler"))).WillRepeatedly(
    Return(m_scheduler));

  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_DOWN);
  checkReport();
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("LVL=\"CRIT\""));
  ASSERT_NE(std::string::npos, logToCheck.find("MSG=\"In DriveHandler::runChild(): failed to instantiate scheduler."));
  ASSERT_NE(std::string::npos, logToCheck.find("errorMessage=\"createScheduler failed to create scheduler\""));

  // It cannot ping the database or the objectstore, so it should mark the drive as down
  // There are two reasons to fail the ping, one produced by WrongSchemaVersionException and a general exception
  EXPECT_CALL(*m_scheduler, ping(_)).WillOnce(
    Throw(cta::exception::Exception("Failed to ping scheduler"))).WillOnce(
    Throw(cta::catalogue::WrongSchemaVersionException("Catalogue MAJOR version mismatch"))).WillRepeatedly(
    Return());
  // Frist exception (general)
  m_logger.clearLog();
  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_DOWN);
  checkReport();
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("LVL=\"CRIT\""));
  ASSERT_NE(std::string::npos, logToCheck.find("MSG=\"In DriveHandler::runChild(): "
                                               "failed to ping central storage before session."));
  ASSERT_NE(std::string::npos, logToCheck.find("errorMessage=\"Failed to ping scheduler\""));
  // Second exception (WrongSchemaVersionException)
  m_logger.clearLog();
  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_DOWN);
  checkReport();
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("LVL=\"CRIT\""));
  ASSERT_NE(std::string::npos, logToCheck.find("MSG=\"In DriveHandler::runChild(): "
                                               "catalogue MAJOR version mismatch"));
  ASSERT_NE(std::string::npos, logToCheck.find("errorMessage=\"Catalogue MAJOR version mismatch\""));

  // It cannot create the drive status, so it should mark the drive as down
  EXPECT_CALL(*m_scheduler, checkDriveCanBeCreated(_, _)).WillOnce(
    Return(false)).WillRepeatedly(Return(true));
  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_DOWN);
  checkReport();
  
  // It cannot create the drive status because the drive doesn't exist in the catalogue
  // This is not a critical error, so it should continue with the session
  m_logger.clearLog();
  EXPECT_CALL(*m_scheduler, getDesiredDriveState(_, _)).WillOnce(
    Throw(cta::Scheduler::NoSuchDrive())).WillRepeatedly(
    Return(cta::common::dataStructures::DesiredDriveState()));
  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_UP);
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("MSG=\"In DriveHandler::runChild(): "
                                               "the desired drive state doesn't exist in the Catalogue DB"));

  // It cannot create the tape drive in the catalogue, so it should mark the drive as down, and the session
  // cannot continue.
  m_logger.clearLog();
  EXPECT_CALL(*m_scheduler, createTapeDriveStatus(_, _, _, _, _, _, _)).WillOnce(
    Throw(cta::exception::Exception("Failed to create tape drive status"))).WillRepeatedly(Return());
  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_DOWN);
  checkReport();
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("LVL=\"CRIT\""));
  ASSERT_NE(std::string::npos, logToCheck.find("Backtrace="));
  ASSERT_NE(std::string::npos, logToCheck.find("MSG=\"In DriveHandler::runChild(): "
                                               "failed to set drive down"));
  ASSERT_NE(std::string::npos, logToCheck.find("Message=\"Failed to create tape drive status\""));

  // It cannot set desired drive state into the catalogue, so it should mark the drive as down, and the session
  // cannot continue.
  m_logger.clearLog();
  EXPECT_CALL(*m_scheduler, setDesiredDriveState(_, _, _, _)).WillOnce(
    Throw(cta::exception::Exception("Failed to set desired drive state"))).WillRepeatedly(Return());
  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_DOWN);
  checkReport();
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("LVL=\"CRIT\""));
  ASSERT_NE(std::string::npos, logToCheck.find("Backtrace="));
  ASSERT_NE(std::string::npos, logToCheck.find("MSG=\"In DriveHandler::runChild(): "
                                               "failed to set drive down"));
  ASSERT_NE(std::string::npos, logToCheck.find("Message=\"Failed to set desired drive state\""));

  // It cannot report the drive configuration to the catalogue, so it should mark the drive as down, and the session
  // cannot continue.
  m_logger.clearLog();
  EXPECT_CALL(*m_scheduler, reportDriveConfig(_, _, _)).WillOnce(
    Throw(cta::exception::Exception("Failed to report drive config"))).WillRepeatedly(Return());
  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_DOWN);
  checkReport();
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("LVL=\"CRIT\""));
  ASSERT_NE(std::string::npos, logToCheck.find("Backtrace="));
  ASSERT_NE(std::string::npos, logToCheck.find("MSG=\"In DriveHandler::runChild(): "
                                               "failed to set drive down"));
  ASSERT_NE(std::string::npos, logToCheck.find("Message=\"Failed to report drive config\""));
  
  // After all the problems with scheduler, we should be able to run a good session
  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_UP);
}

TEST_F(DriveHandlerTests, runChildAfterCrashedSessionWhenRunning) {
  using EndOfSessionAction = castor::tape::tapeserver::daemon::Session::EndOfSessionAction;

  std::string logToCheck;
  
  EXPECT_CALL(*m_scheduler, reportDriveStatus(_, _, _, _)).WillOnce(Invoke(
      [&](const cta::common::dataStructures::DriveInfo&,
        const cta::common::dataStructures::MountType& type,
        const cta::common::dataStructures::DriveStatus& status,
        cta::log::LogContext& lc) {
        m_lc.log(cta::log::DEBUG, "DriveHandlerTests::runChild(): Reporting drive status");
        ASSERT_EQ(type, cta::common::dataStructures::MountType::NoMount);
        ASSERT_EQ(status, cta::common::dataStructures::DriveStatus::Down);
        return;
      })).WillOnce(
        Throw(cta::exception::Exception("Failed to report drive status"))).WillOnce(Invoke(
      [&](const cta::common::dataStructures::DriveInfo&,
        const cta::common::dataStructures::MountType& type,
        const cta::common::dataStructures::DriveStatus& status,
        cta::log::LogContext& lc) {
        m_lc.log(cta::log::DEBUG, "DriveHandlerTests::runChild(): Reporting drive status");
        ASSERT_EQ(type, cta::common::dataStructures::MountType::NoMount);
        ASSERT_EQ(status, cta::common::dataStructures::DriveStatus::CleaningUp);
        return;
      })).WillRepeatedly(Return());

  // Previous Vid is empty, it should have a vid of the previous session
  m_logger.clearLog();
  m_driveHandler->setPreviousSession(cta::tape::daemon::DriveHandler::PreviousSession::Crashed,
                                     cta::tape::session::SessionState::Running,
                                     cta::tape::session::SessionType::Undetermined,
                                     std::string(""));
  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_DOWN);
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("LVL=\"ERROR\""));
  ASSERT_NE(std::string::npos, logToCheck.find("Should run cleaner but VID is missing. Putting the drive down."));

  // Previous Vid is empty, it should have a vid of the previous session, but some problem to report drive status
  m_logger.clearLog();
  m_driveHandler->setPreviousSession(cta::tape::daemon::DriveHandler::PreviousSession::Crashed,
                                     cta::tape::session::SessionState::Running,
                                     cta::tape::session::SessionType::Undetermined,
                                     std::string(""));
  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_DOWN);
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("Should run cleaner but VID is missing. Putting the drive down."));
  ASSERT_NE(std::string::npos, logToCheck.find("LVL=\"CRIT\""));
  ASSERT_NE(std::string::npos, logToCheck.find("failed to set the drive down. Reporting fatal error."));

  // Correct starting of the cleaning session
  m_logger.clearLog();
  m_driveHandler->setPreviousSession(cta::tape::daemon::DriveHandler::PreviousSession::Crashed,
                                     cta::tape::session::SessionState::Running,
                                     cta::tape::session::SessionType::Undetermined,
                                     std::string("TAPE0001"));
  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_DOWN);
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("starting cleaner after crash with tape potentially loaded"));
  ASSERT_NE(std::string::npos, logToCheck.find("will create cleaner session"));

  // Session crashed during the cleaning session
  m_logger.clearLog();
  m_driveHandler->setPreviousSession(cta::tape::daemon::DriveHandler::PreviousSession::Crashed,
                                     cta::tape::session::SessionState::Running,
                                     cta::tape::session::SessionType::Cleanup,
                                     std::string("TAPE0001"));
  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_DOWN);
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("LVL=\"ERROR\""));
  ASSERT_NE(std::string::npos, logToCheck.find("the cleaner session crashed. Putting the drive down."));

  // Session crashed during the cleaning session something happens with scheduler method setDesiredDriveState
  EXPECT_CALL(*m_scheduler, setDesiredDriveState(_, _, _, _)).WillOnce(
    Throw(cta::exception::Exception("Failed to set desired drive state."))).WillRepeatedly(Return());
  m_logger.clearLog();
  m_driveHandler->setPreviousSession(cta::tape::daemon::DriveHandler::PreviousSession::Crashed,
                                     cta::tape::session::SessionState::Running,
                                     cta::tape::session::SessionType::Cleanup,
                                     std::string("TAPE0001"));
  ASSERT_EQ(m_driveHandler->runChild(), EndOfSessionAction::MARK_DRIVE_AS_DOWN);
  logToCheck = m_logger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("LVL=\"CRIT\""));
  ASSERT_NE(std::string::npos, logToCheck.find("failed to set the drive down. Reporting fatal error."));
  ASSERT_NE(std::string::npos, logToCheck.find("Failed to set desired drive state."));
}

} // namespace unitTests
