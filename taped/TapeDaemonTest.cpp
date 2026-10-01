/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeDaemon.hpp"

#include "catalogue/dummy/DummyCatalogue.hpp"
#include "catalogue/dummy/DummyLogicalLibraryCatalogue.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/log/StringLogger.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdexcept>

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBTestFactory.hpp"
#else
#include "objectstore/BackendVFS.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#endif

namespace cta::tape::daemon {
namespace {
using namespace common::dataStructures;
using testing::_;
using testing::Invoke;
using testing::Return;
using testing::Throw;

class MockDriveScheduler : public Scheduler {
public:
  using Scheduler::Scheduler;

  MOCK_METHOD(void, ping, (log::LogContext&), (override));

  MOCK_METHOD(void, reportDriveStatus, (const DriveInfo&, MountType, DriveStatus, log::LogContext&), (override));

  MOCK_METHOD(void, setDesiredDriveState, (const std::string&, const DesiredDriveState&, log::LogContext&), (override));

  MOCK_METHOD(bool, checkDriveCanBeCreated, (const DriveInfo&, log::LogContext&), (override));

  MOCK_METHOD(DesiredDriveState, getDesiredDriveState, (const std::string&, log::LogContext&), (override));

  MOCK_METHOD(void,
              createTapeDriveStatus,
              (const DriveInfo&,
               const DesiredDriveState&,
               const MountType&,
               const DriveStatus&,
               const SecurityIdentity&,
               log::LogContext&),
              (override));

  MOCK_METHOD(void, reportSchedulerBackendName, (const std::string&, log::LogContext&), (override));
};

class MockDriveState : public catalogue::DummyDriveStateCatalogue {
public:
  MOCK_METHOD(std::optional<TapeDrive>, getTapeDrive, (const std::string&), (const, override));
  MOCK_METHOD(void, setDesiredTapeDriveState, (const std::string&, const DesiredDriveState&), (override));
};

class MockLogicalLibrary : public catalogue::DummyLogicalLibraryCatalogue {
public:
  MOCK_METHOD(std::vector<common::dataStructures::LogicalLibrary>, getLogicalLibraries, (), (const, override));
};

class DaemonCatalogue : public catalogue::DummyCatalogue {
public:
  DaemonCatalogue() {
    m_driveState = std::make_unique<testing::NiceMock<MockDriveState>>();
    m_logicalLibrary = std::make_unique<testing::StrictMock<MockLogicalLibrary>>();
  }

  MockDriveState& driveState() { return static_cast<MockDriveState&>(*m_driveState); }

  MockLogicalLibrary& library() { return static_cast<MockLogicalLibrary&>(*m_logicalLibrary); }
};
}  // namespace

class TapeDaemonTest : public testing::Test {
protected:
  TapedConfig config;
  log::StringLogger logger {"host", "TapeDaemonTest", log::DEBUG};
  std::unique_ptr<catalogue::Catalogue> catalogue = std::make_unique<DaemonCatalogue>();
  std::unique_ptr<SchedulerDatabase> db;
  std::unique_ptr<testing::StrictMock<MockDriveScheduler>> scheduler;
  std::unique_ptr<SchedulerContext> context;
  std::unique_ptr<TapeDaemon> daemon;
  std::optional<TapeDrive> previousDrive {TapeDrive {}};

  MockDriveState& driveState() { return static_cast<DaemonCatalogue&>(*catalogue).driveState(); }

  MockLogicalLibrary& library() { return static_cast<DaemonCatalogue&>(*catalogue).library(); }

  void SetUp() override {
    config.drive.name = "drive";
    config.drive.logical_library_name = "library";
#ifdef CTA_PGSCHED
    db = RelationalDBTestFactory().create(catalogue);
#else
    db = OStoreDBFactory<objectstore::BackendVFS>().create(catalogue);
#endif
    scheduler = std::make_unique<testing::StrictMock<MockDriveScheduler>>(*catalogue, *db, "test");
    // Fail unexpected polls immediately instead of entering the production wait loop.
    ON_CALL(*scheduler, getDesiredDriveState(_, _))
      .WillByDefault(Throw(std::logic_error("Unexpected drive-state poll")));
    ON_CALL(library(), getLogicalLibraries()).WillByDefault(Throw(std::logic_error("Unexpected library poll")));
    context = std::make_unique<SchedulerContext>(config, logger, *scheduler);
    daemon = std::make_unique<TapeDaemon>(config, logger, *catalogue, *context);
    previousDrive->desiredUp = false;
    previousDrive->driveStatus = DriveStatus::Down;
    ON_CALL(driveState(), getTapeDrive("drive")).WillByDefault(Invoke([this] { return previousDrive; }));
  }

  bool registerDrive() { return daemon->registerDrive(false); }

  void waitForUp() { daemon->waitUntilDriveIsRequestedUp(); }

  void waitForLibrary() { daemon->waitForLogicalLibrary(); }

  bool stopRequested() { return daemon->m_stopSource.stop_requested(); }

  int shutdown(bool unsafe = false) {
    return daemon->shutdown(unsafe ? TapeDaemon::ExitCause::UnsafeWorkerTeardown : TapeDaemon::ExitCause::Normal,
                            unsafe ? TapeDaemon::DownPublication::DesiredOnly :
                                     TapeDaemon::DownPublication::DesiredAndReported);
  }

  void expectRunStartup() {
    EXPECT_CALL(*scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
    EXPECT_CALL(*scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
    EXPECT_CALL(*scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _));
    EXPECT_CALL(*scheduler, reportSchedulerBackendName("drive", _));
  }
};

TEST_F(TapeDaemonTest, RecoveryStatusPublicationFailureDoesNotTouchHardware) {
  testing::InSequence sequence;
  previousDrive.emplace();
  previousDrive->driveStatus = DriveStatus::Transferring;
  previousDrive->desiredUp = true;
  EXPECT_CALL(*scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(*scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _))
    .WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
}

TEST_F(TapeDaemonTest, RegistrationConflictStopsStartup) {
  EXPECT_CALL(*scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(false));
  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
}

TEST_F(TapeDaemonTest, RegistrationPreservesOperatorReasonAndComment) {
  DesiredDriveState state;
  state.up = false;
  state.reason = "Operator intervention";
  state.comment = "Inspect drive";
  EXPECT_CALL(*scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(*scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
  EXPECT_CALL(*scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
    .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired, const auto&, const auto&, const auto&, auto&) {
      EXPECT_FALSE(desired.up);
      EXPECT_EQ(state.reason, desired.reason);
      EXPECT_EQ(state.comment, desired.comment);
    }));
  EXPECT_CALL(*scheduler, reportSchedulerBackendName("drive", _));
  EXPECT_TRUE(registerDrive());
}

TEST_F(TapeDaemonTest, RegistrationPreservesNewUpRequestAndPublishesReadinessLast) {
  DesiredDriveState state;
  state.up = true;
  state.reason = "Setting drive up";
  state.comment = "Operator comment";
  EXPECT_FALSE(daemon->isReady());
  EXPECT_CALL(*scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(*scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
  EXPECT_CALL(*scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
    .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired, const auto&, const auto&, const auto&, auto&) {
      EXPECT_FALSE(daemon->isReady());
      EXPECT_TRUE(desired.up);
      EXPECT_EQ(state.reason, desired.reason);
      EXPECT_EQ(state.comment, desired.comment);
    }));
  EXPECT_CALL(*scheduler, reportSchedulerBackendName("drive", _)).WillOnce(Invoke([&](const auto&, auto&) {
    EXPECT_FALSE(daemon->isReady());
  }));
  EXPECT_TRUE(registerDrive());
  EXPECT_TRUE(daemon->isReady());
}

TEST_F(TapeDaemonTest, RegistrationReplacesAbsentAndCleanShutdownReasonsWithStartup) {
  // Check both states that should receive a fresh startup reason.
  for (const auto& reason :
       std::vector<std::optional<std::string>> {std::nullopt, formatDriveDownReason(DriveDownReason::Shutdown)}) {
    SCOPED_TRACE(reason.value_or("absent"));
    DesiredDriveState state;
    state.reason = reason;

    // Capture the state published to the catalogue during registration.
    EXPECT_CALL(*scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
    EXPECT_CALL(*scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
    EXPECT_CALL(*scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
      .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired, const auto&, const auto&, const auto&, auto&) {
        EXPECT_FALSE(desired.up);
        EXPECT_EQ(formatDriveDownReason(DriveDownReason::Startup), desired.reason);
      }));
    EXPECT_CALL(*scheduler, reportSchedulerBackendName("drive", _));

    EXPECT_TRUE(registerDrive());

    // Remove this case's expectations before setting up the next reason.
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(scheduler.get()));
  }
}

TEST_F(TapeDaemonTest, MissingDrivePropagatesFromWaitForUp) {
  EXPECT_CALL(*scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(Scheduler::NoSuchDrive("missing")));
  EXPECT_THROW(waitForUp(), Scheduler::NoSuchDrive);
}

TEST_F(TapeDaemonTest, RegistrationFailureDoesNotPublishShutdown) {
  EXPECT_CALL(*scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Throw(std::runtime_error("unavailable")));
  EXPECT_CALL(driveState(), setDesiredTapeDriveState(_, _)).Times(0);
  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_TRUE(daemon->isLive());
}

TEST_F(TapeDaemonTest, BackendPublicationFailureDoesNotEstablishReadiness) {
  EXPECT_CALL(*scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(*scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(*scheduler, createTapeDriveStatus(_, _, _, _, _, _));
  EXPECT_CALL(*scheduler, reportSchedulerBackendName("drive", _)).WillOnce(Throw(std::runtime_error("backend")));
  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
}

TEST_F(TapeDaemonTest, ExistingLibraryAndDesiredUpReturnWithoutWaiting) {
  common::dataStructures::LogicalLibrary logicalLibrary;
  logicalLibrary.name = "library";
  EXPECT_CALL(library(), getLogicalLibraries())
    .WillOnce(Return(std::vector<common::dataStructures::LogicalLibrary> {logicalLibrary}));
  waitForLibrary();
  DesiredDriveState desired;
  desired.up = true;
  EXPECT_CALL(*scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(desired));
  waitForUp();
}

TEST_F(TapeDaemonTest, LibraryFailureAfterRegistrationPublishesDownAndClearsReadiness) {
  expectRunStartup();
  EXPECT_CALL(library(), getLogicalLibraries()).WillOnce(Throw(std::runtime_error("library unavailable")));
  EXPECT_CALL(driveState(), setDesiredTapeDriveState("drive", _));
  EXPECT_CALL(*scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_TRUE(daemon->isLive());
}

TEST_F(TapeDaemonTest, StopBeforeRegistrationSkipsWaitingAndExitsNormally) {
  EXPECT_FALSE(daemon->isReady());
  EXPECT_TRUE(daemon->isLive());
  daemon->stop();
  EXPECT_TRUE(stopRequested());
  expectRunStartup();
  EXPECT_CALL(driveState(), setDesiredTapeDriveState("drive", _));
  EXPECT_CALL(*scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_EQ(0, daemon->run());
  EXPECT_FALSE(daemon->isReady());
}

TEST_F(TapeDaemonTest, StopRetainsExitRequestWhenPublicationFails) {
  expectRunStartup();
  ASSERT_TRUE(registerDrive());
  EXPECT_CALL(driveState(), setDesiredTapeDriveState("drive", _)).WillOnce(Throw(std::runtime_error("unavailable")));
  EXPECT_NO_THROW(daemon->stop());
  EXPECT_TRUE(stopRequested());
  // Repeated stop requests do not repeat publication.
  EXPECT_NO_THROW(daemon->stop());
}

TEST_F(TapeDaemonTest, UnsafeShutdownRequestsDownWithoutReportingHardwareRelease) {
  EXPECT_CALL(driveState(), setDesiredTapeDriveState("drive", _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired) {
      EXPECT_FALSE(desired.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::SessionDidNotStopSafely), desired.reason);
    }));
  EXPECT_CALL(*scheduler, reportDriveStatus(_, _, _, _)).Times(0);
  EXPECT_EQ(1, shutdown(true));
}

TEST_F(TapeDaemonTest, ShutdownStillReportsDownWhenDesiredPublicationFails) {
  EXPECT_CALL(driveState(), setDesiredTapeDriveState("drive", _)).WillOnce(Throw(std::runtime_error("unavailable")));
  EXPECT_CALL(*scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_EQ(1, shutdown());
}

TEST_F(TapeDaemonTest, MissingDriveEndsRunWithoutShutdownPublication) {
  testing::InSequence sequence;
  expectRunStartup();
  common::dataStructures::LogicalLibrary logicalLibrary;
  logicalLibrary.name = "library";
  EXPECT_CALL(library(), getLogicalLibraries())
    .WillOnce(Return(std::vector<common::dataStructures::LogicalLibrary> {logicalLibrary}));
  EXPECT_CALL(*scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(Scheduler::NoSuchDrive("missing")));
  EXPECT_CALL(driveState(), setDesiredTapeDriveState(_, _)).Times(0);
  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_TRUE(daemon->isLive());
}

}  // namespace cta::tape::daemon
