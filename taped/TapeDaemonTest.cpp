/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeDaemon.hpp"

#include "catalogue/TapeDrivesCatalogueState.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "catalogue/dummy/DummyDriveConfigCatalogue.hpp"
#include "catalogue/dummy/DummyLogicalLibraryCatalogue.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/log/StringLogger.hpp"
#include "common/utils/utils.hpp"

#include <exception>
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
  MOCK_METHOD(void, createTapeDrive, (const TapeDrive&), (override));
  MOCK_METHOD(bool, updateTapeDriveStatus, (const TapeDrive&), (override));
};

class MockDriveConfig : public catalogue::DummyDriveConfigCatalogue {
public:
  MOCK_METHOD(void,
              createTapeDriveConfig,
              (const std::string&, const std::string&, const std::string&, const std::string&, const std::string&),
              (override));
};

class MockLogicalLibrary : public catalogue::DummyLogicalLibraryCatalogue {
public:
  MOCK_METHOD(std::vector<common::dataStructures::LogicalLibrary>, getLogicalLibraries, (), (const, override));
};

class DaemonCatalogue : public catalogue::DummyCatalogue {
public:
  DaemonCatalogue() {
    m_driveState = std::make_unique<testing::StrictMock<MockDriveState>>();
    m_driveConfig = std::make_unique<testing::StrictMock<MockDriveConfig>>();
    m_logicalLibrary = std::make_unique<testing::StrictMock<MockLogicalLibrary>>();
  }

  MockDriveState& driveState() { return static_cast<MockDriveState&>(*m_driveState); }

  MockLogicalLibrary& library() { return static_cast<MockLogicalLibrary&>(*m_logicalLibrary); }

  MockDriveConfig& driveConfig() { return static_cast<MockDriveConfig&>(*m_driveConfig); }
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

  MockDriveConfig& driveConfig() { return static_cast<DaemonCatalogue&>(*catalogue).driveConfig(); }

  void SetUp() override {
    config.drive.name = "drive";
    config.drive.logical_library_name = "library";
#ifdef CTA_PGSCHED
    db = RelationalDBTestFactory().create(catalogue);
#else
    db = OStoreDBFactory<objectstore::BackendVFS>().create(catalogue);
#endif
    scheduler = std::make_unique<testing::StrictMock<MockDriveScheduler>>(*catalogue, *db, "test");
    // Fail unexpected library polls instead of entering the production wait loop.
    ON_CALL(library(), getLogicalLibraries()).WillByDefault(Throw(std::logic_error("Unexpected library poll")));
    context = std::make_unique<SchedulerContext>(config, logger, *scheduler);
    daemon = std::make_unique<TapeDaemon>(config, logger, *catalogue, *context);
    previousDrive->host = utils::getShortHostname();
    previousDrive->logicalLibrary = config.drive.logical_library_name;
    previousDrive->desiredUp = false;
    previousDrive->driveStatus = DriveStatus::Down;
    allowDriveReads();
  }

  void allowDriveReads() {
    EXPECT_CALL(driveState(), getTapeDrive("drive")).WillRepeatedly(Invoke([this] { return previousDrive; }));
  }

  bool registerDrive() { return daemon->registerDrive(); }

  void waitForUp() { daemon->waitUntilDriveIsRequestedUp(); }

  void waitForLibrary() { daemon->waitForLogicalLibrary(); }

  bool stopRequested() { return daemon->m_stopSource.stop_requested(); }

  int shutdown() { return daemon->shutdown(TapeDaemon::ExitCause::Normal, true); }

  void expectRunStartup() {
    EXPECT_CALL(driveState(),
                createTapeDrive(testing::AllOf(testing::Field(&TapeDrive::mountType, MountType::NoMount),
                                               testing::Field(&TapeDrive::driveStatus, DriveStatus::Down))));
    EXPECT_CALL(driveConfig(),
                createTapeDriveConfig("drive",
                                      "general",
                                      "SchedulerBackendName",
                                      config.scheduler.backend_name,
                                      "TOML config file"));
  }
};

TEST_F(TapeDaemonTest, RecoveryStatusPublicationFailureDoesNotTouchHardware) {
  config.drive.startup.recover_existing_up = true;
  testing::InSequence sequence;
  previousDrive->driveStatus = DriveStatus::Transferring;
  previousDrive->desiredUp = true;

  EXPECT_CALL(driveState(),
              updateTapeDriveStatus(testing::AllOf(testing::Field(&TapeDrive::mountType, MountType::NoMount),
                                                   testing::Field(&TapeDrive::driveStatus, DriveStatus::Starting))))
    .WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
}

TEST_F(TapeDaemonTest, RestartPreservesDesiredUpAndPreparesInterruptedDrive) {
  config.drive.startup.recover_existing_up = true;
  previousDrive->desiredUp = true;
  previousDrive->driveStatus = DriveStatus::Transferring;
  previousDrive->reasonUpDown = "Operator requested Up";
  testing::InSequence sequence;

  EXPECT_CALL(driveState(), setDesiredTapeDriveState(_, _)).Times(0);
  EXPECT_CALL(driveState(), createTapeDrive(_)).Times(0);
  EXPECT_CALL(driveState(),
              updateTapeDriveStatus(testing::AllOf(testing::Field(&TapeDrive::mountType, MountType::NoMount),
                                                   testing::Field(&TapeDrive::driveStatus, DriveStatus::Starting))));
  EXPECT_CALL(driveConfig(),
              createTapeDriveConfig("drive",
                                    "general",
                                    "SchedulerBackendName",
                                    config.scheduler.backend_name,
                                    "TOML config file"));
  EXPECT_TRUE(registerDrive());
  EXPECT_TRUE(daemon->isReady());
  EXPECT_TRUE(previousDrive->desiredUp);
  EXPECT_EQ("Operator requested Up", previousDrive->reasonUpDown);
}

TEST_F(TapeDaemonTest, DisabledRecoveryPreservesEntryAndDoesNotFallThroughToAutoUp) {
  config.drive.startup.auto_up = true;
  previousDrive->desiredUp = true;
  previousDrive->driveStatus = DriveStatus::Transferring;
  previousDrive->userComment = "Inspect drive";

  EXPECT_CALL(driveState(), createTapeDrive(_)).Times(0);
  EXPECT_CALL(driveState(), setDesiredTapeDriveState("drive", _))
    .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired) {
      EXPECT_FALSE(desired.up);
      EXPECT_FALSE(desired.comment);
      ASSERT_TRUE(desired.reason);
      EXPECT_THAT(desired.reason,
                  testing::Optional(testing::MatchesRegex(
                    R"(\[cta-taped\] Daemon started: desired Up detected, but startup recovery is disabled)")));
      previousDrive->desiredUp = desired.up;
    }));
  EXPECT_CALL(driveState(),
              updateTapeDriveStatus(testing::AllOf(testing::Field(&TapeDrive::mountType, MountType::NoMount),
                                                   testing::Field(&TapeDrive::driveStatus, DriveStatus::Down))));
  EXPECT_CALL(driveConfig(),
              createTapeDriveConfig("drive",
                                    "general",
                                    "SchedulerBackendName",
                                    config.scheduler.backend_name,
                                    "TOML config file"));
  EXPECT_TRUE(registerDrive());
  EXPECT_FALSE(previousDrive->desiredUp);
  EXPECT_EQ("Inspect drive", previousDrive->userComment);
}

TEST_F(TapeDaemonTest, NewDrivePublishesAutomaticUpDecision) {
  previousDrive.reset();
  config.drive.startup.auto_up = true;
  testing::InSequence sequence;
  EXPECT_CALL(driveState(), createTapeDrive(_)).WillOnce(Invoke([&](const TapeDrive& published) {
    EXPECT_FALSE(daemon->isReady());
    EXPECT_EQ(MountType::NoMount, published.mountType);
    EXPECT_EQ(DriveStatus::Down, published.driveStatus);
    EXPECT_TRUE(published.desiredUp);
    EXPECT_THAT(published.reasonUpDown, testing::Optional(testing::MatchesRegex(R"(\[cta-taped\] Daemon started)")));
  }));
  EXPECT_CALL(driveConfig(),
              createTapeDriveConfig("drive",
                                    "general",
                                    "SchedulerBackendName",
                                    config.scheduler.backend_name,
                                    "TOML config file"));
  EXPECT_TRUE(registerDrive());
  EXPECT_TRUE(daemon->isReady());
}

TEST_F(TapeDaemonTest, RegistrationConflictStopsStartup) {
  previousDrive->host = "another-host";
  EXPECT_CALL(driveState(), createTapeDrive(_)).Times(0);
  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
}

TEST_F(TapeDaemonTest, RegistrationPublishesReadinessLast) {
  DesiredDriveState state;
  state.reason = "Maintenance";
  state.comment = "Operator comment";
  EXPECT_FALSE(daemon->isReady());

  previousDrive->reasonUpDown = state.reason;
  previousDrive->userComment = state.comment;
  EXPECT_CALL(driveState(),
              createTapeDrive(testing::AllOf(testing::Field(&TapeDrive::mountType, MountType::NoMount),
                                             testing::Field(&TapeDrive::driveStatus, DriveStatus::Down))))
    .WillOnce(Invoke([&](const TapeDrive& published) {
      EXPECT_FALSE(daemon->isReady());
      EXPECT_FALSE(published.desiredUp);
      EXPECT_EQ(state.reason, published.reasonUpDown);
      EXPECT_EQ(state.comment, published.userComment);
    }));
  EXPECT_CALL(driveConfig(),
              createTapeDriveConfig("drive",
                                    "general",
                                    "SchedulerBackendName",
                                    config.scheduler.backend_name,
                                    "TOML config file"))
    .WillOnce(Invoke(
      [&](const auto&, const auto&, const auto&, const auto&, const auto&) { EXPECT_FALSE(daemon->isReady()); }));
  EXPECT_TRUE(registerDrive());
  EXPECT_TRUE(daemon->isReady());
}

TEST_F(TapeDaemonTest, MissingDrivePropagatesFromWaitForUp) {
  EXPECT_CALL(driveState(), getTapeDrive("drive")).WillOnce(Return(std::nullopt));
  EXPECT_THROW(waitForUp(), TapeDrivesCatalogueState::NoSuchDrive);
}

TEST_F(TapeDaemonTest, RegistrationFailureDoesNotPublishShutdown) {
  for (const auto& failure : {std::make_exception_ptr(exception::Exception("unavailable")),
                              std::make_exception_ptr(std::runtime_error("unavailable")),
                              std::make_exception_ptr(42)}) {
    EXPECT_CALL(driveState(), getTapeDrive("drive")).WillOnce(Invoke([&](const auto&) -> std::optional<TapeDrive> {
      std::rethrow_exception(failure);
    }));
    EXPECT_CALL(driveState(), setDesiredTapeDriveState(_, _)).Times(0);
    EXPECT_CALL(driveState(), updateTapeDriveStatus(_)).Times(0);
    EXPECT_EQ(1, daemon->run());
    EXPECT_FALSE(daemon->isReady());
    EXPECT_TRUE(daemon->isLive());
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(scheduler.get()));
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&driveState()));
    allowDriveReads();
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&driveConfig()));
  }
}

TEST_F(TapeDaemonTest, BackendPublicationFailureDoesNotEstablishReadiness) {
  EXPECT_CALL(driveState(), createTapeDrive(_));
  EXPECT_CALL(driveConfig(),
              createTapeDriveConfig("drive",
                                    "general",
                                    "SchedulerBackendName",
                                    config.scheduler.backend_name,
                                    "TOML config file"))
    .WillOnce(Throw(std::runtime_error("backend")));
  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
}

TEST_F(TapeDaemonTest, ExistingLibraryAndDesiredUpReturnWithoutWaiting) {
  common::dataStructures::LogicalLibrary logicalLibrary;
  logicalLibrary.name = "library";
  EXPECT_CALL(library(), getLogicalLibraries())
    .WillOnce(Return(std::vector<common::dataStructures::LogicalLibrary> {logicalLibrary}));
  waitForLibrary();
  previousDrive->desiredUp = true;
  EXPECT_CALL(driveState(), getTapeDrive("drive")).WillOnce(Return(previousDrive));
  waitForUp();
}

TEST_F(TapeDaemonTest, LibraryFailureAfterRegistrationPublishesDownAndClearsReadiness) {
  expectRunStartup();
  EXPECT_CALL(library(), getLogicalLibraries()).WillOnce(Throw(std::runtime_error("library unavailable")));
  EXPECT_CALL(driveState(), setDesiredTapeDriveState("drive", _));
  EXPECT_CALL(driveState(),
              updateTapeDriveStatus(testing::AllOf(testing::Field(&TapeDrive::mountType, MountType::NoMount),
                                                   testing::Field(&TapeDrive::driveStatus, DriveStatus::Down))));
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
  EXPECT_CALL(driveState(),
              updateTapeDriveStatus(testing::AllOf(testing::Field(&TapeDrive::mountType, MountType::NoMount),
                                                   testing::Field(&TapeDrive::driveStatus, DriveStatus::Down))));
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

TEST_F(TapeDaemonTest, ShutdownStillReportsDownWhenDesiredPublicationFails) {
  EXPECT_CALL(driveState(), setDesiredTapeDriveState("drive", _)).WillOnce(Throw(std::runtime_error("unavailable")));
  EXPECT_CALL(driveState(),
              updateTapeDriveStatus(testing::AllOf(testing::Field(&TapeDrive::mountType, MountType::NoMount),
                                                   testing::Field(&TapeDrive::driveStatus, DriveStatus::Down))));
  EXPECT_EQ(1, shutdown());
}

TEST_F(TapeDaemonTest, MissingDriveEndsRunWithoutShutdownPublication) {
  testing::InSequence sequence;
  expectRunStartup();
  common::dataStructures::LogicalLibrary logicalLibrary;
  logicalLibrary.name = "library";
  EXPECT_CALL(library(), getLogicalLibraries()).WillOnce(Invoke([&] {
    previousDrive.reset();
    return std::vector<common::dataStructures::LogicalLibrary> {logicalLibrary};
  }));
  EXPECT_CALL(driveState(), setDesiredTapeDriveState(_, _)).Times(0);
  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_TRUE(daemon->isLive());
}

}  // namespace cta::tape::daemon
