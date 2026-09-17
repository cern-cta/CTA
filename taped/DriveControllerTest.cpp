/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveController.hpp"

#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/TimeoutException.hpp"
#include "common/log/StringLogger.hpp"
#include "runtime/config/parsing/TomlParser.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/TapeMount.hpp"

#include <functional>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdexcept>
#include <vector>

namespace cta::tape::daemon {
namespace {
using namespace common::dataStructures;
using testing::_;
using testing::Invoke;
using testing::Return;
using testing::Throw;

class MockDriveScheduler : public IScheduler {
public:
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

class TestMount : public TapeMount {
public:
  std::function<void()> onDestroy;

  ~TestMount() override {
    if (onDestroy) {
      onDestroy();
    }
  }

  MountType getMountType() const override { return MountType::Retrieve; }

  std::string getVid() const override { return "V00001"; }

  std::string getMountTransactionId() const override { return "1"; }

  std::optional<std::string> getActivity() const override { return std::nullopt; }

  uint32_t getNbFiles() const override { return 1; }

  std::string getVo() const override { return "vo"; }

  std::string getMediaType() const override { return "media"; }

  std::string getVendor() const override { return "vendor"; }

  Label::Format getLabelFormat() const override { return Label::Format::CTA; }

  std::string getPoolName() const override { return "pool"; }

  uint64_t getCapacityInBytes() const override { return 1; }

  std::optional<std::string> getEncryptionKeyName() const override { return std::nullopt; }

  void complete() override {}

  void setDriveStatus(DriveStatus, const std::optional<std::string>&) override {}

  void setTapeSessionStats(const TapeTransferStats&) override {}

  void setTapeMounted(log::LogContext&) const override {}
};
}  // namespace

class DriveControllerTest : public testing::Test {
protected:
  TapedConfig config;
  log::StringLogger logger {"host", "DriveControllerTest", log::DEBUG};
  testing::StrictMock<MockDriveScheduler> scheduler;
  std::unique_ptr<DriveController> controller;
  testing::Expectation preparationComplete;
  bool empty = true;
  std::optional<std::string> probeError;
  unsigned int probes = 0;
  unsigned int schedules = 0;
  unsigned int transfers = 0;
  unsigned int destroyed = 0;
  TapeMount* m_liveMount = nullptr;
  std::vector<unsigned int> sleeps;
  std::function<std::unique_ptr<TapeMount>()> schedule;
  std::function<TapeSessionResult(TapeMount&)> transfer;
  std::function<bool()> libraryExists = [] { return true; };
  std::function<bool()> clean = [] { return true; };

  class FakeDriveOperations final : public DriveOperations {
  public:
    explicit FakeDriveOperations(DriveControllerTest& fixture) : fixture(fixture) {}

    IScheduler& scheduler() override { return fixture.scheduler; }

    bool logicalLibraryExists() override { return fixture.libraryExists(); }

    std::pair<bool, std::optional<std::string>> probeDrive() override {
      ++fixture.probes;
      EXPECT_EQ(nullptr, fixture.liveMount());
      return {fixture.empty, fixture.probeError};
    }

    std::unique_ptr<TapeMount> getNextMount() override {
      ++fixture.schedules;
      return fixture.schedule ? fixture.schedule() : nullptr;
    }

    TapeSessionResult runTapeSession(TapeMount& tapeMount) override {
      ++fixture.transfers;
      EXPECT_EQ(&tapeMount, fixture.liveMount());
      return fixture.transfer ? fixture.transfer(tapeMount) : TapeSessionResult {};
    }

    bool clean(const std::optional<std::string>&, bool) override { return fixture.clean(); }

    void sleep(unsigned int seconds) override {
      if (fixture.sleeps.size() >= 10) {
        throw std::runtime_error("Unexpected repeated controller wait");
      }
      fixture.sleeps.push_back(seconds);
    }

  private:
    DriveControllerTest& fixture;
  } operations {*this};

  void SetUp() override {
    config.drive.name = "drive";
    config.mounts.idle_scheduling_interval_secs = 7;
    config.mounts.backend_recovery_interval_secs = 11;
    config.mounts.logical_library_poll_interval_secs = 17;
    controller = std::make_unique<DriveController>(config, logger, operations);
  }

  void waitForLibrary() { controller->waitForLogicalLibrary(); }

  void iteration() { controller->runIteration(); }

  int shutdown() { return controller->shutdownDrive(); }

  bool registerDrive() { return controller->registerDrive(false); }

  void waitForUp() { controller->waitUntilDriveIsRequestedUp(); }

  TapeMount* liveMount() { return m_liveMount; }

  void down(bool preserve = false) { controller->putDriveDown(DriveDownReason::Shutdown, {}, preserve); }

  void expectRunStartup() {
    EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
    EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _));
    EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));
  }

  void expectPreparation() {
    testing::InSequence sequence;
    DesiredDriveState state;
    state.up = true;
    preparationComplete = EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
    if (empty) {
      preparationComplete = EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
    }
  }

  void supplyMount() {
    schedule = [this] {
      auto tapeMount = std::make_unique<TestMount>();
      EXPECT_EQ(nullptr, liveMount());
      m_liveMount = tapeMount.get();
      tapeMount->onDestroy = [this] {
        m_liveMount = nullptr;
        ++destroyed;
      };
      return tapeMount;
    };
  }
};

// Startup and registration.

/*
 * If the drive name is owned by another host or logical library, registration fails.
 * Startup must stop before probing or requesting work for that drive.
 */
TEST_F(DriveControllerTest, RegistrationConflictStopsStartup) {
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(false));
  EXPECT_EQ(1, controller->run());
  EXPECT_EQ(0, schedules);
}

/*
 * If the drive already has an operator reason and comment, registration preserves both.
 * Startup must not erase an operator's explanation or make the drive available.
 */
TEST_F(DriveControllerTest, RegistrationPreservesOperatorReasonAndComment) {
  DesiredDriveState state;
  state.up = true;
  state.reason = "Operator intervention";
  state.comment = "Inspect drive";
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
    .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired, const auto&, const auto&, const auto&, auto&) {
      EXPECT_FALSE(desired.up);
      EXPECT_EQ(state.reason, desired.reason);
      EXPECT_EQ(state.comment, desired.comment);
    }));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));
  EXPECT_TRUE(registerDrive());
}

/*
 * If the drive has no down reason or was cleanly shut down, drive registration replaces that state with
 * the startup reason and leaves the drive down. 
 */
TEST_F(DriveControllerTest, RegistrationReplacesAbsentAndCleanShutdownReasonsWithStartup) {
  // Check both states that should receive a fresh startup reason.
  for (const auto& reason :
       std::vector<std::optional<std::string>> {std::nullopt, formatDriveDownReason(DriveDownReason::Shutdown)}) {
    SCOPED_TRACE(reason.value_or("absent"));
    DesiredDriveState state;
    state.reason = reason;

    // Capture the state published to the catalogue during registration.
    EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
    EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
      .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired, const auto&, const auto&, const auto&, auto&) {
        EXPECT_FALSE(desired.up);
        EXPECT_EQ(formatDriveDownReason(DriveDownReason::Startup), desired.reason);
      }));
    EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));

    EXPECT_TRUE(registerDrive());

    // Remove this case's expectations before setting up the next reason.
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
}

/*
 * If the desired-state lookup finds no drive entry, the controller registers the missing drive as down.
 * It must wait for an explicit up request before using the drive.
 */
TEST_F(DriveControllerTest, MissingDriveIsRegisteredDownUntilOperatorUp) {
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _))
    .WillOnce(Throw(Scheduler::NoSuchDrive("missing")))
    .WillOnce(Throw(Scheduler::NoSuchDrive("missing")))
    .WillOnce(Return(up));
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired, const auto&, const auto&, const auto&, auto&) {
      EXPECT_FALSE(desired.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::Startup), desired.reason);
    }));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  waitForUp();
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.drive_state_poll_interval_secs));
}

/*
 * If a missing drive cannot be registered because of a conflict, desired-state lookup fails.
 * The controller must not proceed under an entry owned elsewhere.
 */
TEST_F(DriveControllerTest, MissingDriveRegistrationConflictPropagates) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(Scheduler::NoSuchDrive("missing")));
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(false));
  EXPECT_THROW(waitForUp(), exception::Exception);
}

/*
 * If publishing the drive registration fails, run should end with a failure result.
 * It must not check the library or schedule work for an unregistered drive.
 */
TEST_F(DriveControllerTest, RegistrationPublicationFailureAbortsStartupBeforeLibraryOrScheduling) {
  unsigned int libraryChecks = 0;
  libraryExists = [&] {
    ++libraryChecks;
    return true;
  };
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
    .WillOnce(Throw(std::runtime_error("registration publication failed")));

  EXPECT_EQ(1, controller->run());
  EXPECT_EQ(0, libraryChecks);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

/*
 * If publishing the scheduler backend name fails, run should end with a failure result.
 * The drive must not begin scheduling with incomplete registration.
 */
TEST_F(DriveControllerTest, SchedulerBackendPublicationFailureAbortsStartupBeforeLibraryOrScheduling) {
  unsigned int libraryChecks = 0;
  libraryExists = [&] {
    ++libraryChecks;
    return true;
  };
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _))
    .WillOnce(Throw(std::runtime_error("backend publication failed")));

  EXPECT_EQ(1, controller->run());
  EXPECT_EQ(0, libraryChecks);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

/*
 * If registration loses its database connection, run should end with a failure result.
 * Library checks and scheduling must not start without a registered drive.
 */
TEST_F(DriveControllerTest, StartupDatabaseFailureAbortsBeforeLibraryOrScheduling) {
  clean = [] {
    ADD_FAILURE() << "Startup failure must not touch tape hardware";
    return true;
  };
  unsigned int libraryChecks = 0;
  libraryExists = [&] {
    ++libraryChecks;
    return true;
  };
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _))
    .WillOnce(Throw(exception::LostDatabaseConnection("objectstore unavailable")));

  EXPECT_EQ(1, controller->run());
  EXPECT_EQ(0, libraryChecks);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

/*
 * If the configured logical library is absent, startup waits for it to appear.
 * It must not request mounts before the library exists.
 */
TEST_F(DriveControllerTest, MissingLogicalLibraryWaitsUntilAvailable) {
  unsigned int checks = 0;
  libraryExists = [&] { return ++checks == 3; };
  waitForLibrary();
  EXPECT_EQ(3, checks);
  EXPECT_THAT(sleeps,
              testing::ElementsAre(config.mounts.logical_library_poll_interval_secs,
                                   config.mounts.logical_library_poll_interval_secs));
  EXPECT_EQ(0, schedules);
}

/*
 * If the logical-library lookup loses its database connection, startup propagates the error.
 * A backend failure must not be mistaken for a library awaiting creation.
 */
TEST_F(DriveControllerTest, LogicalLibraryDatabaseFailurePropagatesWithoutWaiting) {
  libraryExists = []() -> bool { throw exception::LostDatabaseConnection("database unavailable"); };
  EXPECT_THROW(waitForLibrary(), exception::LostDatabaseConnection);
  EXPECT_TRUE(sleeps.empty());
}

/*
 * If the logical-library lookup loses its database connection, run should end with a failure result.
 * Scheduling cannot begin until startup has verified the library.
 */
TEST_F(DriveControllerTest, LogicalLibraryDatabaseFailureEndsStartupWithoutScheduling) {
  libraryExists = []() -> bool { throw exception::LostDatabaseConnection("objectstore unavailable"); };
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));

  EXPECT_EQ(1, controller->run());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

// Drive preparation and scheduling.

/*
 * If the desired-state lookup loses its database connection, the iteration propagates the error.
 * Probing and scheduling cannot proceed without a reliable operator state.
 */
TEST_F(DriveControllerTest, DesiredStateDatabaseFailurePropagates) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _))
    .WillOnce(Throw(exception::LostDatabaseConnection("database unavailable")));
  EXPECT_THROW(iteration(), exception::LostDatabaseConnection);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

/*
 * If the desired state remains down, the controller refreshes its reported status and waits.
 * It probes and schedules only after the operator requests the drive up.
 */
TEST_F(DriveControllerTest, DownDriveWaitsForOperatorUpBeforeProbing) {
  testing::InSequence sequence;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  iteration();
  EXPECT_EQ(1, probes);
  EXPECT_EQ(1, schedules);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.drive_state_poll_interval_secs, 7));
}

/*
 * If publishing Up fails after a successful probe, the controller must not schedule work.
 */
TEST_F(DriveControllerTest, UpPublicationFailureAfterProbePreventsScheduling) {
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _))
    .WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_THROW(iteration(), std::runtime_error);
  EXPECT_EQ(1, probes);
  EXPECT_EQ(0, schedules);
}

/*
 * If the probe finds retained media, the controller requests down for operator inspection.
 * It must neither clean the tape nor schedule work on the occupied drive.
 */
TEST_F(DriveControllerTest, RetainedTapeRequestsDownWithoutCleaningOrScheduling) {
  empty = false;
  unsigned int cleanAttempts = 0;
  clean = [&] {
    ++cleanAttempts;
    return true;
  };
  expectPreparation();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::TapeDetected), state.reason);
    }));

  iteration();
  EXPECT_EQ(0, cleanAttempts);
  EXPECT_EQ(1, probes);
  EXPECT_EQ(0, schedules);
}

/*
 * If the drive probe fails, the controller publishes the probe failure as the down reason.
 * Scheduling must stop because the drive's empty state is unknown.
 */
TEST_F(DriveControllerTest, ProbeFailurePublishesItsReasonAndPreventsScheduling) {
  empty = false;
  probeError = "Cannot open drive";
  expectPreparation();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([this](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::DriveProbeFailed, *probeError), state.reason);
    }));
  iteration();
  EXPECT_EQ(0, schedules);
}

/*
 * If scheduling returns no mount, the controller waits before the next attempt.
 * It checks the desired state, probes again, and refreshes the idle status.
 */
TEST_F(DriveControllerTest, IdleMountWaitsAndRechecksDriveBeforeRetry) {
  expectPreparation();
  iteration();
  expectPreparation();
  iteration();
  EXPECT_EQ(2, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_EQ(0, transfers);
  EXPECT_THAT(
    sleeps,
    testing::ElementsAre(config.mounts.idle_scheduling_interval_secs, config.mounts.idle_scheduling_interval_secs));
}

// Scheduling resumes with a fresh probe after an operator down/up request.
TEST_F(DriveControllerTest, OperatorDownUpRequiresAnotherProbe) {
  expectPreparation();
  iteration();

  {
    testing::InSequence sequence;
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
    expectPreparation();
  }
  iteration();

  EXPECT_EQ(2, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_THAT(sleeps,
              testing::ElementsAre(config.mounts.idle_scheduling_interval_secs,
                                   config.mounts.drive_state_poll_interval_secs,
                                   config.mounts.idle_scheduling_interval_secs));
}

/*
 * If mount scheduling times out, the controller waits and allows a later iteration.
 * A timeout should not permanently stop an otherwise usable drive.
 */
TEST_F(DriveControllerTest, SchedulingTimeoutWaitsAndAllowsAnotherIteration) {
  schedule = []() -> std::unique_ptr<TapeMount> { throw exception::TimeoutException("timeout"); };
  expectPreparation();
  EXPECT_NO_THROW(iteration());
  schedule = {};
  expectPreparation();
  iteration();
  EXPECT_EQ(2, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_THAT(sleeps, testing::ElementsAre(7, 7));
}

/*
 * If scheduling loses its database connection, the controller logs an error and waits for the backend.
 * Once a ping succeeds, it can probe and schedule again without an operator up request.
 */
TEST_F(DriveControllerTest, SchedulingDatabaseFailureWaitsForRecoveryBeforeRetry) {
  schedule = []() -> std::unique_ptr<TapeMount> { throw exception::LostDatabaseConnection("database unavailable"); };
  expectPreparation();
  testing::Expectation failedPing =
    EXPECT_CALL(scheduler, ping(_)).WillOnce(Throw(exception::LostDatabaseConnection("database unavailable")));
  EXPECT_CALL(scheduler, ping(_)).After(failedPing);

  EXPECT_NO_THROW(iteration());
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("LVL=\"ERROR\""));
  EXPECT_EQ(nullptr, liveMount());

  schedule = {};
  expectPreparation();
  iteration();
  EXPECT_EQ(2, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_THAT(
    sleeps,
    testing::ElementsAre(config.mounts.backend_recovery_interval_secs, config.mounts.idle_scheduling_interval_secs));
}

/*
 * Unexpected scheduling failures are logged and retried after the idle delay without cleaning.
 * The next attempt checks the desired state and probes again before scheduling.
 */
TEST_F(DriveControllerTest, UnexpectedSchedulingFailureWaitsAndAllowsAnotherIteration) {
  schedule = []() -> std::unique_ptr<TapeMount> { throw std::runtime_error("unexpected scheduler failure"); };
  unsigned int cleanAttempts = 0;
  clean = [&] {
    ++cleanAttempts;
    return true;
  };
  expectPreparation();

  EXPECT_NO_THROW(iteration());
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("Scheduling failed unexpectedly"));
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("LVL=\"ERROR\""));
  EXPECT_EQ(nullptr, liveMount());
  EXPECT_EQ(0, cleanAttempts);
  EXPECT_EQ(0, transfers);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.idle_scheduling_interval_secs));

  supplyMount();
  expectPreparation();
  EXPECT_NO_THROW(iteration());
  EXPECT_EQ(2, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(nullptr, liveMount());
  EXPECT_EQ(0, cleanAttempts);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.idle_scheduling_interval_secs));
}

/*
 * Scheduling without work does not start a TapeSession.
 */
TEST_F(DriveControllerTest, IdleDriveDoesNotStartATapeSession) {
  expectPreparation();

  iteration();

  EXPECT_EQ(0, transfers);
  EXPECT_EQ(nullptr, liveMount());
}

// Transfers and recovery.

/*
 * A reusable drive without a recovery request remains available for scheduling.
 */
TEST_F(DriveControllerTest, ReusableDriveWithoutRecoveryDoesNotRequestDown) {
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
    return result;
  };
  expectPreparation();
  iteration();
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(1, destroyed);
  EXPECT_TRUE(sleeps.empty());
}

/*
 * Each session receives its own live mount, released before scheduling again.
 */
TEST_F(DriveControllerTest, SuccessiveSessionsReceiveTheirOwnMount) {
  supplyMount();
  transfer = [&](TapeMount& tapeMount) {
    EXPECT_EQ(&tapeMount, liveMount());
    return TapeSessionResult {};
  };

  expectPreparation();
  iteration();
  EXPECT_EQ(nullptr, liveMount());
  expectPreparation();
  iteration();

  EXPECT_EQ(2, transfers);
  EXPECT_EQ(2, probes);
  EXPECT_EQ(2, destroyed);
  EXPECT_EQ(nullptr, liveMount());
}

// A handled finalization failure has already completed the session's local cleanup.
TEST_F(DriveControllerTest, HandledSessionFailureRetriesWithoutCleaning) {
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
    result.retryDelayRequired = true;
    return result;
  };
  clean = [] {
    ADD_FAILURE() << "Handled session failures must not trigger another cleanup";
    return false;
  };
  expectPreparation();

  EXPECT_NO_THROW(iteration());
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(nullptr, liveMount());
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.idle_scheduling_interval_secs));
}

TEST_F(DriveControllerTest, HandledSessionFailureWaitsForBackendWithoutCleaning) {
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
    result.backendRecoveryRequired = true;
    result.retryDelayRequired = true;
    return result;
  };
  clean = [] {
    ADD_FAILURE() << "Handled session failures must not trigger another cleanup";
    return false;
  };
  expectPreparation();
  testing::Expectation failedPing =
    EXPECT_CALL(scheduler, ping(_)).WillOnce(Throw(exception::LostDatabaseConnection("database unavailable")));
  EXPECT_CALL(scheduler, ping(_)).After(failedPing);

  EXPECT_NO_THROW(iteration());
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(nullptr, liveMount());
  EXPECT_THAT(
    sleeps,
    testing::ElementsAre(config.mounts.backend_recovery_interval_secs, config.mounts.idle_scheduling_interval_secs));
}

// Escaping session exceptions are fatal, including database disconnections.
// Only final shutdown cleaning runs, after the mount has been released.
TEST_F(DriveControllerTest, EscapingSessionExceptionsExitWithoutRetrying) {
  for (const auto failure : {"standard", "database", "unknown"}) {
    SCOPED_TRACE(failure);
    testing::InSequence sequence;
    expectRunStartup();
    expectPreparation();
    supplyMount();
    transfer = [failure](TapeMount&) -> TapeSessionResult {
      if (std::string(failure) == "database") {
        throw exception::LostDatabaseConnection("database unavailable");
      }
      if (std::string(failure) == "standard") {
        throw std::runtime_error("session failed");
      }
      throw 42;
    };
    const auto previousTransfers = transfers;
    const auto previousSchedules = schedules;
    const auto previousDestroyed = destroyed;
    unsigned int cleanAttempts = 0;
    clean = [&] {
      ++cleanAttempts;
      EXPECT_EQ(nullptr, liveMount());
      EXPECT_EQ(previousDestroyed + 1, destroyed);
      return true;
    };
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
    EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
      .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
        EXPECT_FALSE(state.up);
        EXPECT_EQ(formatDriveDownReason(DriveDownReason::Shutdown), state.reason);
      }));

    EXPECT_EQ(1, controller->run());
    EXPECT_EQ(1, cleanAttempts);
    EXPECT_EQ(previousTransfers + 1, transfers);
    EXPECT_EQ(previousSchedules + 1, schedules);
    EXPECT_TRUE(sleeps.empty());
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
}

/*
 * If a transfer reports an unusable drive with a specific reason, the controller requests it down.
 * It preserves that reason so the more precise diagnosis is not overwritten.
 */
TEST_F(DriveControllerTest, UnusableDriveRequestsDownAndPreservesSpecificReason) {
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
    result.driveUsability = DriveUsability::MustRemainDown;
    return result;
  };
  expectPreparation();
  DesiredDriveState state;
  state.reason = "Unload failed";
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).After(preparationComplete).WillOnce(Return(state));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired, auto&) {
      EXPECT_FALSE(desired.up);
      EXPECT_FALSE(desired.reason);
    }));
  iteration();
  EXPECT_EQ(1, destroyed);
}

/*
 * If a transfer marks the drive unusable without a specific reason, the controller requests it down.
 * It supplies a transfer-failure reason so the down state is explained.
 */
TEST_F(DriveControllerTest, UnusableDriveWithoutSpecificReasonPublishesTransferFailure) {
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
    result.driveUsability = DriveUsability::MustRemainDown;
    return result;
  };
  expectPreparation();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _))
    .After(preparationComplete)
    .WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::TransferSessionFailed), state.reason);
    }));
  iteration();
  EXPECT_EQ(1, destroyed);
}

/*
 * If down-state publication fails after an unusable transfer, the error propagates.
 * The controller must still release the mount.
 */
TEST_F(DriveControllerTest, DownPublicationFailureAfterTransferStillReleasesMount) {
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
    result.driveUsability = DriveUsability::MustRemainDown;
    return result;
  };
  expectPreparation();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _))
    .After(preparationComplete)
    .WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _))
    .WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _));
  EXPECT_THROW(iteration(), std::runtime_error);
  EXPECT_EQ(1, destroyed);
}

// Down-state publication.

/*
 * If publishing the reported down status fails, the controller still tries the desired down state.
 * When both publications fail, the first error remains the one propagated.
 */
TEST_F(DriveControllerTest, DownPublicationsBothRunAndFirstExceptionIsPreserved) {
  testing::InSequence sequence;
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _))
    .WillOnce(Throw(std::runtime_error("reported state failed")));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _)).WillOnce(Throw(std::logic_error("desired state failed")));
  try {
    down();
    FAIL() << "Expected publication failure";
  } catch (const std::runtime_error& ex) {
    EXPECT_STREQ("reported state failed", ex.what());
  }
}

/*
 * If publishing the desired down state fails, the controller propagates the error.
 * The caller must know that the drive was not reliably taken out of service.
 */
TEST_F(DriveControllerTest, DesiredPublicationFailurePropagates) {
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _)).WillOnce(Throw(std::runtime_error("failed")));
  EXPECT_THROW(down(), std::runtime_error);
}

/*
 * If reading the existing down reason fails, the controller still attempts both down publications.
 * It avoids overwriting an unknown reason while preserving the lookup error.
 */
TEST_F(DriveControllerTest, ReasonLookupFailureStillAttemptsBothDownPublications) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(std::runtime_error("lookup failed")));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_FALSE(state.reason);
    }));
  EXPECT_THROW(down(true), std::runtime_error);
}

// Shutdown.

/*
 * An exception escaping drive preparation triggers final cleaning and both down publications.
 * Successful, failed, or throwing cleanup must all retain a failing process exit status.
 */
TEST_F(DriveControllerTest, IterationExceptionCleansAndPublishesDownBeforeFailingExit) {
  for (const auto cleanupOutcome : {"success", "failure", "exception"}) {
    SCOPED_TRACE(cleanupOutcome);
    testing::InSequence sequence;
    expectRunStartup();
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(std::runtime_error("iteration failed")));

    unsigned int cleanAttempts = 0;
    clean = [&]() -> bool {
      ++cleanAttempts;
      EXPECT_EQ(nullptr, liveMount());
      if (std::string(cleanupOutcome) == "exception") {
        throw std::runtime_error("cleaner failed");
      }
      return std::string(cleanupOutcome) == "success";
    };
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Invoke([&](const auto&, auto&) {
      EXPECT_EQ(1, cleanAttempts);
      return DesiredDriveState {};
    }));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
    EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
      .WillOnce(Invoke([&](const auto&, const DesiredDriveState& state, auto&) {
        EXPECT_FALSE(state.up);
        const auto reason =
          std::string(cleanupOutcome) == "success" ? DriveDownReason::Shutdown : DriveDownReason::CleanerFailed;
        EXPECT_EQ(formatDriveDownReason(reason), state.reason);
      }));

    EXPECT_EQ(1, controller->run());
    EXPECT_EQ(1, cleanAttempts);
    EXPECT_THAT(logger.getLog(), testing::HasSubstr("Drive controller failed. Cleaning before exit."));
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
}

/*
 * An unknown transfer exception unwinds the active mount before final cleaning.
 * Cleanup preserves an existing operator reason and cannot turn the exit into success.
 */
TEST_F(DriveControllerTest, UnknownIterationExceptionReleasesMountBeforeFinalCleaning) {
  testing::InSequence sequence;
  expectRunStartup();
  expectPreparation();
  supplyMount();
  transfer = [](TapeMount&) -> TapeSessionResult { throw 42; };
  unsigned int cleanAttempts = 0;
  clean = [&] {
    ++cleanAttempts;
    EXPECT_EQ(nullptr, liveMount());
    EXPECT_EQ(1, destroyed);
    return true;
  };
  DesiredDriveState state;
  state.reason = "Operator intervention";
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired, auto&) {
      EXPECT_EQ(1, cleanAttempts);
      EXPECT_FALSE(desired.up);
      EXPECT_FALSE(desired.reason);
    }));

  EXPECT_EQ(1, controller->run());
  EXPECT_EQ(1, cleanAttempts);
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(nullptr, liveMount());
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("Drive controller failed with an unknown exception"));
}

/*
 * If final cleaning returns failure, shutdown still publishes the drive as down.
 * It records the cleaner failure and returns a nonzero result.
 */
TEST_F(DriveControllerTest, FailedShutdownCleaningStillPublishesDown) {
  clean = [] { return false; };
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::CleanerFailed), state.reason);
    }));
  EXPECT_EQ(1, shutdown());
}

/*
 * If final cleaning throws, shutdown still attempts both down-state publications.
 * A publication failure must not prevent the other publication attempt.
 */
TEST_F(DriveControllerTest, ShutdownCleaningExceptionStillAttemptsDownAfterPublicationFailure) {
  clean = []() -> bool { throw std::runtime_error("cleaner failed"); };
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _))
    .WillOnce(Throw(std::runtime_error("reported state failed")));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _));
  EXPECT_EQ(1, shutdown());
}

/*
 * If final cleaning succeeds but down-state publication fails, shutdown returns failure.
 * Successful hardware cleanup cannot hide an unreported drive state.
 */
TEST_F(DriveControllerTest, SuccessfulCleaningWithPublicationFailureReturnsNonzero) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _)).WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_EQ(1, shutdown());
}

/*
 * If shutdown finds no active failure reason, it publishes a clean-shutdown reason.
 * An existing operator reason remains intact so shutdown does not erase it.
 */
TEST_F(DriveControllerTest, ShutdownReplacesStartupAndCleanReasonsButPreservesOperatorReason) {
  for (const auto& reason : std::vector<std::string> {"",
                                                      formatDriveDownReason(DriveDownReason::Startup),
                                                      formatDriveDownReason(DriveDownReason::Shutdown),
                                                      "Operator intervention"}) {
    SCOPED_TRACE(reason);
    DesiredDriveState state;
    state.reason = reason;
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
    EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
      .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired, auto&) {
        EXPECT_FALSE(desired.up);
        if (reason == "Operator intervention") {
          EXPECT_FALSE(desired.reason);
        } else {
          EXPECT_EQ(formatDriveDownReason(DriveDownReason::Shutdown), desired.reason);
        }
      }));
    EXPECT_EQ(0, shutdown());
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
}

}  // namespace cta::tape::daemon
